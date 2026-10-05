#!/usr/bin/env python3
"""Guard the CLI's one-reactor publication surface without deploying anything."""
import pathlib
import re
import subprocess
import xml.etree.ElementTree as ET

ROOT = pathlib.Path(__file__).resolve().parents[1]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
pom = ET.parse(ROOT / "pom.xml").getroot()
assert re.fullmatch(r"\d+\.\d+\.\d+(?:-SNAPSHOT)?", pom.findtext("m:version", namespaces=NS))
profiles = pom.findall("m:profiles/m:profile", NS)
assert [profile.findtext("m:id", namespaces=NS) for profile in profiles] == ["central-publishing"]
assert profiles[0].find("m:modules", NS) is None, "publication must not select a different reactor"
plugin = next(plugin for plugin in profiles[0].findall("m:build/m:plugins/m:plugin", NS)
              if plugin.findtext("m:artifactId", namespaces=NS) == "central-publishing-maven-plugin")
assert plugin.findtext("m:configuration/m:checksums", namespaces=NS) == "required"
expected = {"tpf-release-resolver", "tpf-deployment-api", "tpf-deployment-core",
            "tpf-deployment-target-local", "tpf-deployment-target-cloud", "tpf-cli"}
modules = {module.text for module in pom.findall("m:modules/m:module", NS)}
assert modules == expected
for module in modules:
    child = ET.parse(ROOT / module / "pom.xml").getroot()
    assert child.findtext("m:parent/m:version", namespaces=NS) == pom.findtext("m:version", namespaces=NS)
    assert child.findtext("m:properties/m:maven.deploy.skip", namespaces=NS) != "true"
    assert child.findtext("m:packaging", default="jar", namespaces=NS) == "jar"
    source_path = f"{module}/src/main/java/org/pipelineframework/deployment/target/Example.java"
    ignored = subprocess.run(["git", "check-ignore", "-q", source_path], cwd=ROOT)
    assert ignored.returncode == 1, "Java target packages must not be ignored as Maven output"
snapshot = (ROOT / ".github/workflows/publish-snapshot.yml").read_text()
assert "  push:\n    branches: [main]" in snapshot and "workflow_dispatch:" in snapshot
assert "schedule:" not in snapshot, "snapshots publish on merge, not a redundant nightly schedule"
assert "ref: refs/heads/main" in snapshot and "persist-credentials: false" in snapshot
assert "clean deploy -Pcentral-publishing" in snapshot
assert "-Dmaven.repo.local=" in snapshot and "-DskipTests" not in snapshot
for name in ("CENTRAL_USERNAME", "CENTRAL_PASSWORD", "GPG_PRIVATE_KEY", "GPG_PASSPHRASE"):
    assert "secrets." + name in snapshot
print("CLI snapshot publication configuration passed")
