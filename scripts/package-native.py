#!/usr/bin/env python3
"""Package an already-tested executable; never compiles or publishes."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import xml.etree.ElementTree as ET
import zipfile

PLATFORMS = ("osx-aarch_64", "linux-x86_64", "linux-aarch_64")

def package(executable, target, version, revision, platform, report):
    if platform not in PLATFORMS: raise ValueError("Unsupported native distribution platform")
    if not re.fullmatch(r"\d+\.\d+\.\d+(?:-SNAPSHOT)?", version): raise ValueError("Invalid version")
    if not re.fullmatch(r"[a-f0-9]{40}", revision): raise ValueError("Full source commit required")
    proof = json.loads(report.read_text())
    if proof.get("passed") is not True or proof.get("version") != "tpf " + version:
        raise ValueError("A successful matching native conformance report is required")
    observed = subprocess.check_output([str(executable.resolve()), "--version"], text=True).strip()
    if observed != "tpf " + version: raise ValueError("Executable version does not match release")
    name = f"tpf-{version}-{platform}"; target.mkdir(parents=True, exist_ok=True)
    archive = target / (name + ".zip")
    content = {"bin/tpf": executable.read_bytes(), "LICENSE": Path("LICENSE").read_bytes(),
        "README.md": b"Install bin/tpf on PATH. Run tpf --help, then tpf release verify --help.\nNo Java or Docker is needed. macOS archives are unsigned.\nhttps://pipelineframework.org/deploy/cli-installation\n"}
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as output:
        for path, payload in content.items():
            entry = zipfile.ZipInfo(name + "/" + path, (2020, 1, 1, 0, 0, 0)); entry.create_system = 3
            entry.external_attr = (0o100755 if path == "bin/tpf" else 0o100644) << 16
            entry.compress_type = zipfile.ZIP_DEFLATED; output.writestr(entry, payload)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    (target / (archive.name + ".sha256")).write_text(digest + "  " + archive.name + "\n")
    metadata = dict(version=version, revision=revision, platform=platform, mandrel="25.0.4.1-Final",
        java=25, archive=archive.name, archiveBytes=archive.stat().st_size, sha256=digest,
        conformance=proof)
    (target / (name + ".json")).write_text(json.dumps(metadata, indent=2) + "\n")
    return archive

if __name__ == "__main__":
    parser=argparse.ArgumentParser(); parser.add_argument("--executable", type=Path, required=True)
    parser.add_argument("--platform", choices=PLATFORMS, required=True); parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("target/distributions"))
    parser.add_argument("--revision", required=True)
    args=parser.parse_args(); version=ET.parse("pom.xml").getroot().findtext("{http://maven.apache.org/POM/4.0.0}version")
    print(package(args.executable,args.output,version,args.revision,args.platform,args.report))
