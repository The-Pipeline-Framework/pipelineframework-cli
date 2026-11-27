#!/usr/bin/env python3
"""End-to-end zero-JDK SIGKILL crash-recovery proof for the native `tpf` CLI.

Exercises the real `tpf` executable (with JAVA_HOME/JDK_* stripped and an empty PATH so no
JVM is reachable) across:
  1. Offline `tpf generate --lang py` emitting canonical v3 contract + typed Python stubs.
  2. Live Python foreign function host built directly from the generated `types.py`/`host.py`.
  3. `tpf dev` execution suspending at a durable `WAITING_EXTERNAL` Await boundary,
     killed via `SIGKILL`, restarted against the same `--data-dir`, and completed with exact
     29-digit `Decimal` and `int64 > 2^53` preservation plus duplicate completion suppression.
  4. `tpf dev` execution paused in durable `WAIT_RETRY` after a transient foreign error,
     killed via `SIGKILL`, restarted against the same `--data-dir`, swept, resumed across
     Await, and verified `SUCCEEDED`.
"""
import argparse
from decimal import Decimal
import json
import os
from pathlib import Path
import queue
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

EXACT_DECIMAL = "99999999999999999999.123456789"
EXACT_INT64 = 9007199254740993
SHARED_SECRET = "tpf-native-sigkill-proof-secret"

PIPELINE_YAML = """version: 3
appName: "Sigkill Proof"
basePackage: "com.example.sigkill"
transport: "LOCAL"
platform: "COMPUTE"

types:
  PaymentInput:
    fields:
      - name: paymentId
        type: string
      - name: sequenceNumber
        type: int64
      - name: amount
        type: decimal
      - name: failFirstAttempt
        type: bool
  ValidatedPayment:
    fields:
      - name: paymentId
        type: string
      - name: sequenceNumber
        type: int64
      - name: amount
        type: decimal
  SettlementNotice:
    fields:
      - name: paymentId
        type: string
      - name: sequenceNumber
        type: int64
      - name: settledAmount
        type: decimal
  PaymentReceipt:
    fields:
      - name: paymentId
        type: string
      - name: sequenceNumber
        type: int64
      - name: settledAmount
        type: decimal
      - name: finalizedCount
        type: int32

steps:
  - name: "Validate Payment"
    cardinality: "ONE_TO_ONE"
    input: "PaymentInput"
    output: "ValidatedPayment"
  - name: "Await Settlement"
    cardinality: "ONE_TO_ONE"
    input: "ValidatedPayment"
    output: "SettlementNotice"
  - name: "Finalize Receipt"
    cardinality: "ONE_TO_ONE"
    input: "SettlementNotice"
    output: "PaymentReceipt"
"""

PYTHON_HOST_SOURCE = """import os
import signal
import sys
import threading
import time

sys.path.insert(0, os.environ["TPF_GEN_PARENT"])
from sigkill_proof import host, types as t

lock = threading.Lock()
transient_seen = set()
finalize_counts = {}


class Handlers:
    def validate_payment(self, inp: t.PaymentInput, ctx: t.FunctionContext) -> t.ValidatedPayment:
        if inp.failFirstAttempt:
            with lock:
                first_time = inp.paymentId not in transient_seen
                transient_seen.add(inp.paymentId)
            if first_time:
                raise t.TpfTransientError("TRANSIENT_DOWNSTREAM", "Simulated transient outage")
        return t.ValidatedPayment(
            paymentId=inp.paymentId,
            sequenceNumber=inp.sequenceNumber + 1,
            amount=inp.amount,
        )

    def finalize_receipt(self, inp: t.SettlementNotice, ctx: t.FunctionContext) -> t.PaymentReceipt:
        with lock:
            count = finalize_counts.get(inp.paymentId, 0) + 1
            finalize_counts[inp.paymentId] = count
        return t.PaymentReceipt(
            paymentId=inp.paymentId,
            sequenceNumber=inp.sequenceNumber,
            settledAmount=inp.settledAmount,
            finalizedCount=count,
        )


running = host.start_app_host(
    Handlers(),
    host="127.0.0.1",
    port=0,
    shared_secret=os.environ["TPF_DEV_SECRET"],
)
print(f"READY {running.base_url}", flush=True)


def stop(_sig, _frame):
    running.close()
    sys.exit(0)


signal.signal(signal.SIGTERM, stop)
while True:
    time.sleep(0.2)
"""


def parse_json_exact(raw: bytes):
    return json.loads(raw.decode("utf-8"), parse_float=Decimal)


def http_json(method: str, url: str, body: str | None = None, timeout: float = 15.0):
    data = body.encode("utf-8") if body is not None else None
    headers = {"Accept": "application/json"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, parse_json_exact(resp.read())
    except urllib.error.HTTPError as err:
        payload = err.read()
        raise AssertionError(
            f"HTTP {err.code} from {method} {url}: {payload.decode('utf-8', errors='replace')}"
        ) from err


def wait_for_ready_line(proc: subprocess.Popen, prefix: str, timeout_s: float = 30.0) -> str:
    lines: queue.Queue[str | None] = queue.Queue()
    captured: list[str] = []

    def pump():
        assert proc.stdout is not None
        try:
            for line in proc.stdout:
                captured.append(line)
                lines.put(line)
        finally:
            lines.put(None)

    thread = threading.Thread(target=pump, daemon=True)
    thread.start()

    deadline = time.monotonic() + timeout_s
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            proc.kill()
            raise AssertionError(
                f"Timed out waiting for '{prefix}' after {timeout_s}s.\nOutput:\n{''.join(captured)}"
            )
        try:
            line = lines.get(timeout=min(0.5, remaining))
        except queue.Empty:
            continue
        if line is None:
            raise AssertionError(
                f"Process exited before printing '{prefix}' (rc={proc.poll()}).\nOutput:\n{''.join(captured)}"
            )
        if line.startswith(prefix):
            return line[len(prefix):].strip()


def build_tpf_invocation(args, home: Path, command: list[str]) -> tuple[list[str], dict[str, str]]:
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("TPF_", "JAVA_", "JDK_"))
    }
    env["HOME"] = str(home)
    if args.executable:
        empty_bin = home / "empty-path"
        empty_bin.mkdir(exist_ok=True)
        env["PATH"] = str(empty_bin)
        if shutil.which("java", path=env["PATH"]) is not None:
            raise AssertionError("Native tpf test environment unexpectedly exposed java on PATH")
        return [str(args.executable), "-Duser.home=" + str(home), *command], env
    return [args.java, "-Duser.home=" + str(home), "-jar", str(args.java_jar), *command], env


def start_tpf_dev(args, home: Path, yaml_path: Path, host_url: str, data_dir: Path):
    cmd, env = build_tpf_invocation(
        args,
        home,
        [
            "dev",
            "--pipeline", str(yaml_path),
            "--function-host", host_url,
            "--data-dir", str(data_dir),
            "--secret", SHARED_SECRET,
            "--port", "0",
            "--max-retries", "2",
        ],
    )
    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
    )
    ready = wait_for_ready_line(proc, "READY ", timeout_s=30.0)
    port = int(ready.split()[0])
    status, health = http_json("GET", f"http://127.0.0.1:{port}/health")
    if status != 200 or health.get("status") != "UP":
        raise AssertionError(f"Unexpected /health response: {status} {health}")
    return proc, f"http://127.0.0.1:{port}", int(health["pid"])


def sigkill_and_verify(proc: subprocess.Popen):
    os.kill(proc.pid, signal.SIGKILL)
    rc = proc.wait(timeout=10)
    if rc != -signal.SIGKILL:
        raise AssertionError(f"Expected SIGKILL exit code {-signal.SIGKILL}, got {rc}")


def main():
    parser = argparse.ArgumentParser()
    dist = parser.add_mutually_exclusive_group(required=True)
    dist.add_argument("--executable", type=Path)
    dist.add_argument("--java-jar", type=Path)
    parser.add_argument("--java", default="java")
    args = parser.parse_args()
    if args.executable:
        args.executable = args.executable.resolve()
    if args.java_jar:
        args.java_jar = args.java_jar.resolve()

    with tempfile.TemporaryDirectory(prefix="tpf-sigkill-e2e-") as tmp:
        root = Path(tmp).resolve()
        home = root / "home"
        home.mkdir()
        yaml_path = root / "pipeline.yaml"
        yaml_path.write_text(PIPELINE_YAML, encoding="utf-8")
        gen_dir = root / "sigkill_proof"
        data_dir = root / "dev-state"

        # 1. Offline `tpf generate --lang py` (zero JDK when --executable)
        gen_cmd, gen_env = build_tpf_invocation(
            args,
            home,
            ["generate", "--pipeline", str(yaml_path), "--lang", "py", "--out", str(gen_dir), "--output", "json"],
        )
        gen_res = subprocess.run(gen_cmd, env=gen_env, text=True, capture_output=True, timeout=60)
        if gen_res.returncode != 0:
            raise AssertionError(f"tpf generate failed ({gen_res.returncode}):\n{gen_res.stdout}\n{gen_res.stderr}")
        gen_json = json.loads(gen_res.stdout)
        assert gen_json["status"] == "GENERATED", gen_json
        assert (gen_dir / "host.py").is_file(), f"Generated Python host.py missing in {list(gen_dir.rglob('*'))}"
        assert (gen_dir / "META-INF" / "pipeline" / "pipeline-contract.json").is_file(), "Compiled contract missing"

        # 2. Launch live generated Python foreign function host
        host_script = root / "run_host.py"
        host_script.write_text(PYTHON_HOST_SOURCE, encoding="utf-8")
        host_env = dict(os.environ)
        host_env["TPF_GEN_PARENT"] = str(root)
        host_env["TPF_DEV_SECRET"] = SHARED_SECRET
        host_proc = subprocess.Popen(
            [sys.executable, str(host_script)],
            env=host_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
        )
        dev_procs: list[subprocess.Popen] = []
        try:
            host_url = wait_for_ready_line(host_proc, "READY ", timeout_s=15.0)

            # 3. Proof A: WAITING_EXTERNAL Await boundary survives SIGKILL
            dev1, url1, pid1 = start_tpf_dev(args, home, yaml_path, host_url, data_dir)
            dev_procs.append(dev1)
            run_body = (
                '{"tenantId":"default","executionKey":"await-sigkill-1","payload":{'
                f'"paymentId":"pay-await-1","sequenceNumber":{EXACT_INT64},'
                f'"amount":{EXACT_DECIMAL},"failFirstAttempt":false'
                "}}"
            )
            status, run1 = http_json("POST", f"{url1}/runs", run_body)
            assert status == 200 and run1["status"] == "WAITING_EXTERNAL", f"Expected WAITING_EXTERNAL, got {run1}"
            exec1_id = run1["executionId"]
            interaction1_id = run1["interactionId"]
            assert interaction1_id, f"Missing interactionId: {run1}"

            # Hard SIGKILL while suspended in WAITING_EXTERNAL
            sigkill_and_verify(dev1)

            # Restart brand-new native tpf dev against the exact same --data-dir
            dev2, url2, pid2 = start_tpf_dev(args, home, yaml_path, host_url, data_dir)
            dev_procs.append(dev2)
            assert pid2 != pid1, "Restarted tpf dev should have a new OS PID"

            _, persisted1 = http_json("GET", f"{url2}/runs/default/{exec1_id}")
            assert persisted1["status"] == "WAITING_EXTERNAL", f"Lost WAITING_EXTERNAL after SIGKILL: {persisted1}"
            assert persisted1["interactionId"] == interaction1_id, "interactionId drifted across SIGKILL"

            complete1_body = (
                f'{{"tenantId":"default","interactionId":"{interaction1_id}","completionId":"comp-await-1",'
                f'"payload":{{"paymentId":"pay-await-1","sequenceNumber":{EXACT_INT64 + 1},'
                f'"settledAmount":{EXACT_DECIMAL}}}}}'
            )
            _, done1 = http_json("POST", f"{url2}/awaits/complete", complete1_body)
            assert done1["status"] == "SUCCEEDED", f"Post-SIGKILL Await completion failed: {done1}"
            assert done1["duplicate"] is False
            assert done1["resultTypeId"] == "PaymentReceipt"
            assert done1["result"]["sequenceNumber"] == EXACT_INT64 + 1
            assert done1["result"]["settledAmount"] == Decimal(EXACT_DECIMAL)
            assert done1["result"]["finalizedCount"] == 1

            # Duplicate completion delivery must be idempotent
            _, dup1 = http_json("POST", f"{url2}/awaits/complete", complete1_body)
            assert dup1["duplicate"] is True
            assert dup1["result"]["finalizedCount"] == 1

            # 4. Proof B: Durable WAIT_RETRY / QUEUED execution survives SIGKILL + sweep
            run2_body = (
                '{"tenantId":"default","executionKey":"retry-sigkill-1","payload":{'
                f'"paymentId":"pay-retry-1","sequenceNumber":{EXACT_INT64},'
                f'"amount":{EXACT_DECIMAL},"failFirstAttempt":true'
                "}}"
            )
            _, run2 = http_json("POST", f"{url2}/runs", run2_body)
            assert run2["status"] == "WAIT_RETRY", f"Expected WAIT_RETRY, got {run2}"
            exec2_id = run2["executionId"]

            # Hard SIGKILL while waiting for retry
            sigkill_and_verify(dev2)

            # Restart tpf dev Run #3 against the exact same --data-dir
            dev3, url3, pid3 = start_tpf_dev(args, home, yaml_path, host_url, data_dir)
            dev_procs.append(dev3)
            assert pid3 != pid2

            future_ms = int(time.time() * 1000) + 60_000
            http_json("POST", f"{url3}/sweep", json.dumps({"nowEpochMs": future_ms}))

            # Poll briefly until recovered execution advances to WAITING_EXTERNAL
            recovered2 = None
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                _, recovered2 = http_json("GET", f"{url3}/runs/default/{exec2_id}")
                if recovered2["status"] == "WAITING_EXTERNAL":
                    break
                time.sleep(0.1)
            assert recovered2 is not None and recovered2["status"] == "WAITING_EXTERNAL", (
                f"Expected recovered WAIT_RETRY execution to reach WAITING_EXTERNAL, got {recovered2}"
            )
            interaction2_id = recovered2["interactionId"]
            complete2_body = (
                f'{{"tenantId":"default","interactionId":"{interaction2_id}","completionId":"comp-retry-1",'
                f'"payload":{{"paymentId":"pay-retry-1","sequenceNumber":{EXACT_INT64 + 1},'
                f'"settledAmount":{EXACT_DECIMAL}}}}}'
            )
            _, done2 = http_json("POST", f"{url3}/awaits/complete", complete2_body)
            assert done2["status"] == "SUCCEEDED", f"Post-retry-SIGKILL completion failed: {done2}"
            assert done2["result"]["sequenceNumber"] == EXACT_INT64 + 1
            assert done2["result"]["settledAmount"] == Decimal(EXACT_DECIMAL)
            assert done2["result"]["finalizedCount"] == 1

            print(
                f"PASS: zero-JDK polyglot native SIGKILL E2E proof verified "
                f"(pids={pid1}->{pid2}->{pid3}, decimal={EXACT_DECIMAL}, int64={EXACT_INT64 + 1})"
            )
        finally:
            for p in dev_procs:
                if p.poll() is None:
                    p.terminate()
                    try:
                        p.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        p.kill()
            if host_proc.poll() is None:
                host_proc.terminate()
                try:
                    host_proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    host_proc.kill()


if __name__ == "__main__":
    main()
