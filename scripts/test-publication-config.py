#!/usr/bin/env python3
"""Guard the CLI's one-reactor publication surface without deploying anything."""
import pathlib
import re
import subprocess
import tempfile
import textwrap
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
    assert child.findtext("m:name", namespaces=NS), f"Central requires an explicit project name: {module}"
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
release = (ROOT / ".github/workflows/publish-release.yml").read_text()
assert "tags: ['v*']" in release and "refs/heads/main" not in release
assert "git merge-base --is-ancestor HEAD origin/main" in release
assert 'test "${RELEASE_TAG#v}" = "$VERSION"' in release
assert "workflow_dispatch:" in release and "ref: ${{ env.RELEASE_TAG }}" in release
assert 'refs/tags/$RELEASE_TAG^{commit}' in release
assert 'case "$VERSION" in *-SNAPSHOT) exit 1;; esac' in release
assert "clean deploy -Pcentral-publishing" in release and "-Dgpg.skip=false" in release
assert "-Dmaven.repo.local=" in release and "-DskipTests" not in release
assert "contents: read" in release and "contents: write" not in release
assert "action-gh-release" not in release, "native distribution owns the GitHub release"
for name in ("CENTRAL_USERNAME", "CENTRAL_PASSWORD", "GPG_PRIVATE_KEY", "GPG_PASSPHRASE"):
    assert "secrets." + name in release
# Execute the trusted recovery transformation against old-style metadata and
# prove that it changes only display names, then becomes a no-op.
recovery = textwrap.dedent(release.split("python3 - <<'PY'\n", 1)[1].split("\n          PY", 1)[0])
with tempfile.TemporaryDirectory(prefix="tpf-central-metadata-") as directory:
    fixture = pathlib.Path(directory)
    (fixture / "pom.xml").write_text((ROOT / "pom.xml").read_text())
    for module in modules:
        path = fixture / module / "pom.xml"
        path.parent.mkdir()
        original = (ROOT / module / "pom.xml").read_text()
        artifact = ET.parse(ROOT / module / "pom.xml").getroot().findtext("m:artifactId", namespaces=NS)
        path.write_text(original.replace(f"\n  <name>{artifact}</name>", "", 1))
    subprocess.run(["python3", "-c", recovery], cwd=fixture, check=True, capture_output=True)
    for module in modules:
        assert (fixture / module / "pom.xml").read_bytes() == (ROOT / module / "pom.xml").read_bytes()
    repeated = subprocess.run(["python3", "-c", recovery], cwd=fixture, check=True, capture_output=True)
    assert not repeated.stdout, "recovery must not rewrite complete POMs"
print("CLI snapshot and release publication configuration passed")
