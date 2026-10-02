#!/usr/bin/env python3
"""Push the tested image and record the registry's authoritative manifest digest."""
import argparse
import json
import re
import subprocess
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--metadata", type=Path, default=Path("container-publication.json"))
parser.add_argument("--engine", default="docker")
args = parser.parse_args()
metadata = json.loads(args.metadata.read_text())
for tag in metadata["tags"]:
    subprocess.run([args.engine, "tag", "tpf-tested", tag], check=True)
    subprocess.run([args.engine, "push", tag], check=True)
manifest = json.loads(subprocess.check_output([
    args.engine, "buildx", "imagetools", "inspect", metadata["tags"][0],
    "--format", "{{json .Manifest}}"], text=True))
digest = manifest.get("digest", "")
if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
    parser.error("Registry did not report a valid SHA-256 manifest digest")
metadata["digestReference"] = metadata["image"] + "@" + digest
args.metadata.write_text(json.dumps(metadata, indent=2) + "\n")
