#!/usr/bin/env python3
"""Exercise the installed CLI from descriptor-only directories (isolated fixture network).

Uses authenticated Maven/OCI and Cloud protocol fixtures, not a live private Cloud service.
The same command is run against the local candidate and the anonymously pulled published digest.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import zipfile

parser = argparse.ArgumentParser()
parser.add_argument("--image", required=True)
parser.add_argument("--engine", default="docker")
args = parser.parse_args()


def digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


with tempfile.TemporaryDirectory(prefix="tpf-container-") as directory:
    root = Path(directory)
    contract_version = "sha256:" + "a" * 64
    contract = dict(schemaVersion=3, pipelineId="container-proof", contractVersion=contract_version,
                    contractHash="a" * 64, platform="COMPUTE", transport="REST", module="proof",
                    pluginHost=False, runtimeLayout="monolith", steps=[dict(index=0, authoredName="Validate", kind="service",
                                cardinality="ONE_TO_ONE", inputTypeId="input", outputTypeId="output",
                                runtimeClass="example.Validate", clientClass="", deferredCompletion={})],
                    capabilities=dict(localTransitionExecution=False, transitionWorkerProtocols=[]))
    jar = root / "proof.jar"
    with zipfile.ZipFile(jar, "w") as archive:
        archive.writestr("META-INF/pipeline/pipeline-contract.json", json.dumps(contract))
        archive.writestr("META-INF/pipeline/order.json", '["Validate"]\n')
        archive.writestr("META-INF/pipeline/telemetry.json", "{}\n")
    jar_bytes = jar.read_bytes()
    manifest = json.dumps(dict(schemaVersion=2, mediaType="application/vnd.oci.image.manifest.v1+json")).encode()
    manifest_digest = digest(manifest)
    basic = "Basic " + base64.b64encode(b"proof:fixture-password").decode()
    (root / "manifest.json").write_bytes(manifest)
    fixture_script = Path(__file__).resolve().with_name("container-fixture-server.py")
    registry = "127.0.0.1:8080"
    endpoint = "http://" + registry
    resolver = root / "resolver"
    resolver.mkdir()
    (resolver / "settings.xml").write_text(f'''<settings>
      <servers><server><id>fixture</id><username>proof</username><password>fixture-password</password></server></servers>
      <profiles><profile><id>fixture</id><repositories><repository><id>fixture</id>
      <url>{endpoint}/maven</url></repository></repositories></profile></profiles>
      <activeProfiles><activeProfile>fixture</activeProfile></activeProfiles></settings>''')
    (resolver / "config.json").write_text(json.dumps({"auths": {registry: {"auth": basic.removeprefix("Basic ")}}}))
    configuration = dict(resolverProfiles={"default": dict(file=False,
                         maven=dict(settings="/resolver/settings.xml"),
                         oci=dict(credentials="docker-config", insecureRegistries=[registry]))},
                         environments={"staging": dict(resolverProfile="default", target=dict(
                             type="tpf-cloud", endpoint=endpoint, organization="example", application="proof",
                             environment="staging", mode="CUSTOMER_MANAGED", credential="cloud"))})
    explicit = json.loads(json.dumps(configuration["resolverProfiles"]["default"]))
    explicit["maven"]["localRepository"] = "/home/tpf/.tpf/maven"
    configuration["resolverProfiles"]["explicit"] = explicit
    (resolver / "tpf-deploy.yaml").write_text(json.dumps(configuration))
    descriptor = dict(schemaVersion=1, pipelineId="container-proof", contractVersion=contract_version,
                      releaseVersion="container-proof-1", compiledTruthArtifactId="carrier", artifacts=[
                          dict(artifactId="carrier", kind="jar", uri="maven:example:proof:1.0.0",
                               digest=digest(jar_bytes), stepIds=["Validate"], capabilities=[]),
                          dict(artifactId="image", kind="container-image",
                               uri=f"oci://{registry}/example/proof@{manifest_digest}", digest=manifest_digest,
                               stepIds=[], capabilities=[])])
    descriptor_bytes = (json.dumps(descriptor, indent=2) + "\n").encode()

    def invoke(work, credentials, command, token="fixture-cloud-token"):
        environment = dict(os.environ)
        environment["TPF_CREDENTIAL_CLOUD"] = token or ""
        (credentials / "maven").mkdir(exist_ok=True)
        container = [args.engine, "run", "--rm", "--init", "--network", "container:" + fixture,
                     "--user", f"{os.getuid()}:{os.getgid()}",
                     "--mount", f"type=bind,src={work},dst=/work",
                     "--mount", f"type=bind,src={credentials},dst=/home/tpf/.tpf",
                     "--mount", f"type=bind,src={credentials / 'maven'},dst=/home/tpf/.m2/repository",
                     "--mount", f"type=bind,src={resolver},dst=/resolver,readonly",
                     "--env", "DOCKER_CONFIG=/resolver"]
        if token:
            container += ["--env", "TPF_CREDENTIAL_CLOUD"]
        if command[0] == "release":
            command = [*command, "--resolver-profile", "explicit"]
        container += [args.image, *command, "--release", "pipeline-release.json",
                      "--config", "/resolver/tpf-deploy.yaml", "--output", "json"]
        return subprocess.run(container, env=environment, text=True, capture_output=True, timeout=120)

    fixture = subprocess.check_output([
        args.engine, "run", "--rm", "--detach",
        "--mount", f"type=bind,src={root},dst=/fixture",
        "--mount", f"type=bind,src={fixture_script},dst=/server.py,readonly",
        "python:3.12-slim", "python", "/server.py"], text=True).strip()

    try:
        subprocess.run([args.engine, "exec", fixture, "python", "-c",
                        "import time,urllib.request\n"
                        "for attempt in range(50):\n"
                        " try:\n"
                        "  urllib.request.urlopen('http://127.0.0.1:8080/ready'); break\n"
                        " except OSError:\n"
                        "  time.sleep(0.2)\n"
                        "else: raise RuntimeError('Fixture did not become ready')"], check=True)
        inspected = json.loads(subprocess.check_output([args.engine, "image", "inspect", args.image]))[0]
        assert inspected["Config"]["User"] == "10001:10001", "Image must default to non-root"
        version = subprocess.check_output([args.engine, "run", "--rm", args.image, "--version"], text=True).strip()
        assert version == "tpf " + inspected["Config"]["Labels"]["org.opencontainers.image.version"], version
        assert inspected["Config"]["Labels"]["org.opencontainers.image.source"].endswith("/pipelineframework-cli")
        for operation in (["release", "verify"], ["deploy", "staging"]):
            work = root / ("verify" if operation[0] == "release" else "deploy")
            work.mkdir()
            (work / "pipeline-release.json").write_bytes(descriptor_bytes)
            credentials = root / (work.name + "-credentials")
            credentials.mkdir()
            assert list(work.iterdir()) == [work / "pipeline-release.json"]
            assert not list(credentials.iterdir()), "Fresh-machine cache must start empty"
            result = invoke(work, credentials, operation)
            assert result.returncode == 0, result.stderr + result.stdout + "\nFixture requests: " + str((root / "requests.jsonl").read_text() if (root / "requests.jsonl").exists() else "none")
            output = json.loads(result.stdout)
            assert output["descriptorDigest"] == digest(descriptor_bytes), output
            assert output["status"] == ("VERIFIED" if operation[0] == "release" else "REGISTERED"), output
            if operation[0] == "deploy":
                assert output["deploymentId"] == "container-proof-1"
                assert all(output[stage] == "NOT_REQUESTED" for stage in
                           ("physicalDeployment", "runtimeVerification", "activation"))
                denied = invoke(work, credentials, operation, token=False)
                assert denied.returncode == 5, denied.stderr
                rejected = invoke(work, credentials, operation, token="invalid-fixture-token")
                assert rejected.returncode == 5, rejected.stderr
            assert (work / "pipeline-release.json").read_bytes() == descriptor_bytes
        invalid_work = root / "invalid-digest"
        invalid_work.mkdir()
        invalid_credentials = root / "invalid-credentials"
        invalid_credentials.mkdir()
        corrupted = json.loads(descriptor_bytes)
        corrupted["artifacts"][0]["digest"] = "sha256:" + "b" * 64
        invalid_bytes = json.dumps(corrupted).encode()
        (invalid_work / "pipeline-release.json").write_bytes(invalid_bytes)
        invalid = invoke(invalid_work, invalid_credentials, ["deploy", "staging"])
        assert invalid.returncode == 4, invalid.stderr
        assert (invalid_work / "pipeline-release.json").read_bytes() == invalid_bytes
        submitted = [json.loads(line) for line in (root / "submitted.jsonl").read_text().splitlines()]
        requests = [json.loads(line) for line in (root / "requests.jsonl").read_text().splitlines()]
        assert len(submitted) == 1, submitted
        observation = submitted[0]
        assert base64.b64decode(observation["body"]) == descriptor_bytes and observation["key"]
        assert observation["mediaType"] == "application/vnd.tpf.pipeline-release+json"
        assert observation["mode"] == "CUSTOMER_MANAGED"
        assert requests.count("/maven/example/proof/1.0.0/proof-1.0.0.jar") >= 2, requests
        assert requests.count("/v2/example/proof/manifests/" + manifest_digest) >= 2, requests
        print("Container proof passed: fresh Maven/OCI resolution, exact-byte Cloud registration, JSON, authentication and digest rejection")
    finally:
        subprocess.run([args.engine, "rm", "--force", fixture], check=True, stdout=subprocess.DEVNULL)
