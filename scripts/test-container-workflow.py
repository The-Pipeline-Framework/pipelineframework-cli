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
import socket
import shutil
import sys
import time
import urllib.request
from pathlib import Path
import subprocess
import tempfile
import zipfile

parser = argparse.ArgumentParser()
distribution = parser.add_mutually_exclusive_group(required=True)
distribution.add_argument("--image")
distribution.add_argument("--executable", type=Path)
distribution.add_argument("--java-jar", type=Path)
parser.add_argument("--java", default="java")
parser.add_argument("--agent-output", type=Path)
parser.add_argument("--report", type=Path)
parser.add_argument("--engine", default="docker")
args = parser.parse_args()
if args.executable: args.executable = args.executable.resolve()
if args.java_jar: args.java_jar = args.java_jar.resolve()
if args.agent_output: args.agent_output = args.agent_output.resolve(); args.agent_output.mkdir(parents=True, exist_ok=True)
timings = []
version_text = "unavailable"
passed = False


def digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


with tempfile.TemporaryDirectory(prefix="tpf-container-") as directory:
    root = Path(directory).resolve()
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
    port = 8080
    if not args.image:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0)); port = probe.getsockname()[1]
    registry = "127.0.0.1:" + str(port)
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
    if not args.image:
        configuration["resolverProfiles"]["default"]["maven"]["settings"] = str(resolver / "settings.xml")
    explicit = json.loads(json.dumps(configuration["resolverProfiles"]["default"]))
    explicit["maven"]["localRepository"] = "/home/tpf/.tpf/maven" if args.image else str(root / "explicit-cache")
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

    home = root / "home"; home.mkdir()
    helpers = root / "helpers"; helpers.mkdir()
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(("TPF_", "JAVA_", "JDK_"))}
    environment.update(HOME=str(home), DOCKER_CONFIG=str(resolver),
                       TPF_CREDENTIAL_DIRECTORY=str(home / ".tpf" / "credentials"))
    if args.executable: environment["PATH"] = str(helpers)  # no Java or Docker needed
    else: environment["PATH"] = str(helpers) + os.pathsep + environment.get("PATH", os.defpath)

    def installed(work, command, extra=None, properties=()):
        env = dict(environment); env.update(extra or {})
        if args.executable:
            invocation = [str(args.executable), "-Duser.home=" + str(home), *properties, *command]
        else:
            invocation = [args.java, "-Duser.home=" + str(home), *properties]
            if args.agent_output: invocation += ["-agentlib:native-image-agent=config-merge-dir=" + str(args.agent_output)]
            invocation += ["-jar", str(args.java_jar), *command]
        start = time.monotonic()
        result = subprocess.run(invocation, cwd=work, env=env, text=True, capture_output=True, timeout=120)
        timings.append(dict(command=command[:2], seconds=time.monotonic()-start))
        return result

    def invoke(work, credentials, command, token="fixture-cloud-token"):
        if not args.image:
            return installed(work, [*command, "--config", str(resolver / "tpf-deploy.yaml"), "--output", "json"],
                             {"TPF_CREDENTIAL_CLOUD": token or ""})
        env = dict(os.environ); env["TPF_CREDENTIAL_CLOUD"] = token or ""
        (credentials / "maven").mkdir(exist_ok=True)
        container = [args.engine, "run", "--rm", "--init", "--network", "container:" + fixture,
                     "--user", f"{os.getuid()}:{os.getgid()}",
                     "--mount", f"type=bind,src={work},dst=/work",
                     "--mount", f"type=bind,src={credentials},dst=/home/tpf/.tpf",
                     "--mount", f"type=bind,src={credentials / 'maven'},dst=/home/tpf/.m2/repository",
                     "--mount", f"type=bind,src={resolver},dst=/resolver,readonly", "--env", "DOCKER_CONFIG=/resolver"]
        if token: container += ["--env", "TPF_CREDENTIAL_CLOUD"]
        if command[0] == "release": command = [*command, "--resolver-profile", "explicit"]
        return subprocess.run([*container, args.image, *command, "--release", "pipeline-release.json",
                               "--config", "/resolver/tpf-deploy.yaml", "--output", "json"],
                              env=env, text=True, capture_output=True, timeout=120)

    if args.image:
        fixture = subprocess.check_output([args.engine, "run", "--rm", "--detach",
            "--mount", f"type=bind,src={root},dst=/fixture", "--mount", f"type=bind,src={fixture_script},dst=/server.py,readonly",
            "python:3.12-slim", "python", "/server.py"], text=True).strip()
    else:
        fixture_process = subprocess.Popen([sys.executable, str(fixture_script)], env={**os.environ,
            "TPF_FIXTURE_ROOT": str(root), "TPF_FIXTURE_PORT": str(port)})

    def test_installed_auth_and_paths():
        work = root / "path with spaces"; work.mkdir()
        local = json.loads(descriptor_bytes); local["artifacts"] = local["artifacts"][:1]
        local["artifacts"][0]["uri"] = jar.as_uri()
        original = json.dumps(local).encode()
        (work / "pipeline-release.json").write_bytes(original)
        assert installed(work, ["release", "verify"]).returncode == 0
        (work / "target").mkdir(); (work / "target/pipeline-release.json").write_bytes(original)
        assert installed(work, ["release", "verify"]).returncode == 2
        assert installed(work, ["release", "verify", "--release", str(work / "pipeline-release.json")]).returncode == 0
        (work / "pipeline-release.json").unlink()
        assert installed(work, ["release", "verify"]).returncode == 0
        login = ["auth", "login", "--issuer", endpoint, "--client-id", "public-fixture"]
        result = installed(work, login)
        assert result.returncode == 0, result.stderr + result.stdout
        assert all(secret not in result.stdout + result.stderr for secret in ("fixture-device-secret", "fixture-cloud-token", "fixture-refresh-secret"))
        credential = home / ".tpf/credentials/cloud.json"
        configuration["environments"]["staging"]["target"]["credential"] = "oauth-session:cloud"
        (resolver / "tpf-deploy.yaml").write_text(json.dumps(configuration))
        human = installed(work, ["deploy", "staging", "--release", str(root / "deploy/pipeline-release.json"),
                                "--config", str(resolver / "tpf-deploy.yaml"), "--output", "json"])
        assert human.returncode == 0, human.stderr
        assert credential.stat().st_mode & 0o777 == 0o600
        assert credential.parent.stat().st_mode & 0o777 == 0o700
        link = home / "credential-link"; link.symlink_to(credential.parent, target_is_directory=True)
        assert installed(work, ["auth", "status"], {"TPF_CREDENTIAL_DIRECTORY": str(link)}).returncode == 5
        link.unlink()
        value = json.loads(credential.read_text()); value["expiresAt"] = 0; credential.write_text(json.dumps(value))
        assert installed(work, ["auth", "status"]).returncode == 0
        assert json.loads(credential.read_text())["refreshToken"] == "fixture-rotated-secret"
        credential.chmod(0o644)
        assert installed(work, ["auth", "status"]).returncode == 5
        credential.chmod(0o600)
        for refresh_error in ("unavailable", "invalid_grant"):
            value = json.loads(credential.read_text()); value["expiresAt"] = 0
            credential.write_text(json.dumps(value)); (root / "refresh-state").write_text(refresh_error)
            assert installed(work, ["auth", "status"]).returncode == 5
            assert credential.exists() == (refresh_error == "unavailable")
        (root / "refresh-state").unlink()
        assert installed(work, login).returncode == 0
        assert installed(work, ["auth", "logout"]).returncode == 0 and not credential.exists()
        assert installed(work, ["auth", "status"]).returncode == 5
        (root / "verification-host").write_text("evil.example")
        assert installed(work, login).returncode == 5
        (root / "verification-host").write_text("localhost")
        assert installed(work, [*login, "--verification-host", "localhost"]).returncode == 0
        assert installed(work, ["auth", "logout"]).returncode == 0
        (root / "verification-host").unlink()
        configuration["environments"]["staging"]["target"]["credential"] = "oauth-client:ci"
        (resolver / "tpf-deploy.yaml").write_text(json.dumps(configuration))
        ci = dict(TPF_OAUTH_CI_ISSUER=endpoint, TPF_OAUTH_CI_CLIENT_ID="fixture", TPF_OAUTH_CI_CLIENT_SECRET="fixture-ci-secret")
        command = ["deploy", "staging", "--release", str(root / "deploy/pipeline-release.json"), "--config", str(resolver / "tpf-deploy.yaml"), "--output", "json"]
        assert installed(work, command, ci).returncode == 0
        assert not credential.exists(), "CI must never create a human token file"
        assert installed(work, command).returncode == 5
        (root / "forbidden").touch()
        assert installed(work, command, ci).returncode == 5
        (root / "forbidden").unlink()
        # Credential helpers execute locally; the helper does not depend on Java or Docker.
        helper = helpers / "docker-credential-fixture"
        helper.write_text("#!/bin/sh\nread registry\nprintf '%s' '{\"Username\":\"proof\",\"Secret\":\"fixture-password\"}'\n")
        helper.chmod(0o755)
        (resolver / "config.json").write_text(json.dumps(dict(credHelpers={registry:"fixture"})))
        check = root / "helper-work"; check.mkdir(); (check / "pipeline-release.json").write_bytes(descriptor_bytes)
        yaml_config = resolver / "real-yaml.yaml"
        yaml_config.write_text(f"resolverProfiles:\n  default:\n    file: true\n    maven:\n      settings: '{resolver / 'settings.xml'}'\n    oci:\n      credentials: docker-config\n      insecureRegistries: ['{registry}']\nenvironments: {{}}\n")
        yaml_result = installed(check, ["release", "verify", "--config", str(yaml_config), "--output", "json"])
        assert yaml_result.returncode == 0 and json.loads(yaml_result.stdout)["status"] == "VERIFIED", yaml_result.stderr
        result = invoke(check, home, ["release", "verify"])
        assert result.returncode == 0, result.stderr
        # HTTPS uses a private test CA. Exercise native/JVM runtime trust configuration;
        # no public network, disabled certificate checks, or baked-in test trust.
        cert = root / "cert.pem"; key = root / "key.pem"; store = root / "trust.p12"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-subj", "/CN=localhost", "-addext", "subjectAltName=IP:127.0.0.1,DNS:localhost",
            "-keyout", str(key), "-out", str(cert)], check=True, capture_output=True)
        subprocess.run(["keytool", "-importcert", "-noprompt", "-alias", "fixture", "-file", str(cert),
            "-keystore", str(store), "-storetype", "PKCS12", "-storepass", "fixture-password"], check=True, capture_output=True)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0)); tls_port = probe.getsockname()[1]
        tls = subprocess.Popen([sys.executable, str(fixture_script)], env={**os.environ,
            "TPF_FIXTURE_ROOT": str(root), "TPF_FIXTURE_PORT": str(tls_port),
            "TPF_FIXTURE_CERT": str(cert), "TPF_FIXTURE_KEY": str(key)})
        try:
            for attempt in range(50):
                with socket.socket() as probe:
                    if probe.connect_ex(("127.0.0.1", tls_port)) == 0: break
                time.sleep(0.1)
            secure_ci = {**ci, "TPF_OAUTH_CI_ISSUER": "https://127.0.0.1:" + str(tls_port)}
            assert installed(work, command, secure_ci).returncode == 5
            result = installed(work, command, secure_ci, properties=["-Djavax.net.ssl.trustStore=" + str(store),
                "-Djavax.net.ssl.trustStorePassword=fixture-password", "-Djavax.net.ssl.trustStoreType=PKCS12"])
            assert result.returncode == 0, result.stderr
        finally:
            tls.terminate(); tls.wait(timeout=10)

    try:
        if args.image:
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
        else:
            for attempt in range(50):
                try: urllib.request.urlopen(endpoint + "/ready").close(); break
                except OSError: time.sleep(0.1)
            else: raise RuntimeError("Fixture did not become ready")
            for path in ([], ["release"], ["release", "verify"], ["deploy"], ["auth"], ["auth", "login"], ["auth", "status"], ["auth", "logout"]):
                for flag in ("--help", "-h"):
                    result = installed(root, [*path, flag])
                    assert result.returncode == 0 and not result.stderr, result
                    assert "Usage: tpf" + (" " + " ".join(path) if path else "") in result.stdout, result.stdout
            version = installed(root, ["--version"])
            version_text = version.stdout.strip()
            assert version.returncode == 0 and "development" not in version.stdout and "${" not in version.stdout, version
            assert installed(root, ["release", "verify"]).returncode == 2
        for operation in (["release", "verify"], ["deploy", "staging"]):
            work = root / ("verify" if operation[0] == "release" else "deploy")
            work.mkdir()
            (work / "pipeline-release.json").write_bytes(descriptor_bytes)
            credentials = root / (work.name + "-credentials")
            credentials.mkdir()
            assert list(work.iterdir()) == [work / "pipeline-release.json"]
            assert not list(credentials.iterdir()), "Fresh-machine cache must start empty"
            if not args.image: shutil.rmtree(home / ".m2", ignore_errors=True)
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
        if not args.image:
            test_installed_auth_and_paths()
        passed = True
        print("Distribution proof passed: fresh Maven/OCI resolution, exact-byte Cloud registration, JSON, authentication and digest rejection")
    finally:
        if args.image: subprocess.run([args.engine, "rm", "--force", fixture], check=True, stdout=subprocess.DEVNULL)
        else: fixture_process.terminate(); fixture_process.wait(timeout=10)
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(dict(passed=passed, version=version_text if not args.image else "container",
                executableBytes=args.executable.stat().st_size if args.executable else 0, timings=timings), indent=2))
