#!/usr/bin/env python3
"""Read-only identity and artifact gates for native builds and trusted publication."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import xml.etree.ElementTree as ET
from zipfile import ZipFile
PLATFORMS = ("osx-aarch_64", "linux-x86_64", "linux-aarch_64")


def identity(tag=""):
    version = ET.parse("pom.xml").getroot().findtext("{http://maven.apache.org/POM/4.0.0}version")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if not re.fullmatch(r"\d+\.\d+\.\d+(?:-SNAPSHOT)?", version): raise ValueError("Invalid release version")
    if tag:
        if version.endswith("-SNAPSHOT") or tag != "v" + version: raise ValueError("Tag must match a non-SNAPSHOT reactor version")
        subprocess.run(["git", "merge-base", "--is-ancestor", "HEAD", "origin/main"], check=True)
    return version, revision

def check(directory, version, revision):
    for platform in PLATFORMS:
        name = f"tpf-{version}-{platform}"
        archive = directory / (name + ".zip")
        metadata = json.loads((directory / (name + ".json")).read_text())
        if any(metadata.get(key) != value for key,value in dict(version=version,revision=revision,platform=platform,java=25,mandrel="25.0.4.1-Final").items()):
            raise ValueError("Artifact identity mismatch: " + name)
        if hashlib.sha256(archive.read_bytes()).hexdigest() != metadata['sha256']: raise ValueError("Archive digest mismatch")
        with ZipFile(archive) as source:
            entry=source.getinfo(name + "/bin/tpf")
            if entry.external_attr >> 16 & 0o111 != 0o111: raise ValueError("Archive executable permissions missing")
        if metadata['conformance'].get('passed') is not True: raise ValueError('Native conformance did not pass')
        if metadata['conformance']['version'] != 'tpf ' + version: raise ValueError("Conformance version mismatch")

if __name__ == '__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--tag',default='');parser.add_argument('--artifacts',type=Path)
    args=parser.parse_args();version,revision=identity(args.tag)
    if args.artifacts: check(args.artifacts,version,revision)
    print('version='+version);print('revision='+revision)
