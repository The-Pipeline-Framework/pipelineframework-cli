#!/usr/bin/env python3
"""Derive image publication metadata from tested source, not workflow text interpolation."""
import argparse
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--revision", required=True)
parser.add_argument("--release-tag", default="")
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
version = ET.parse("pom.xml").getroot().findtext("{http://maven.apache.org/POM/4.0.0}version")
if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-SNAPSHOT)?", version):
    parser.error("Unsupported Maven version")
if not re.fullmatch(r"[0-9a-f]{40}", args.revision):
    parser.error("A full source commit is required")
image = "ghcr.io/the-pipeline-framework/tpf"
tags = [f"{image}:sha-{args.revision}", f"{image}:{version}-sha-{args.revision}"]
if args.release_tag:
    if args.release_tag != "v" + version or version.endswith("-SNAPSHOT"):
        parser.error("Release tag must match a non-snapshot Maven version")
    tags.append(f"{image}:{version}")
else:
    tags.append(f"{image}:main")
args.output.write_text(json.dumps({"image": image, "version": version, "revision": args.revision,
                                  "tags": tags}, indent=2) + "\n")
