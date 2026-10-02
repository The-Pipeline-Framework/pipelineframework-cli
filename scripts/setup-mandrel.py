#!/usr/bin/env python3
"""Install a checksum-pinned Java 25 native toolchain into a build-only directory."""
import argparse
import hashlib
import os
from pathlib import Path
import platform
import tarfile
import urllib.request

VERSION = "25.0.4.1-Final"
CHECKSUMS = {
    ("Linux", "x86_64"): ("linux-amd64", "e47ff6bfe6a8dbb482fdc65c9a49fc6f3ba2fa61ef3cc8fb6229a88989054c31"),
    ("Linux", "aarch64"): ("linux-aarch64", "86033db0337ba64694a1f3680b991b671ba26d192f26fd525e39b714847fbf20"),
    ("Darwin", "arm64"): ("macos-aarch64", "2770286376e2bfa82c3c096de0cfe6e84f22bd6b02ac39eb94ed8ad1c2faea46"),
}

def install(destination):
    key = (platform.system(), platform.machine())
    if key not in CHECKSUMS:
        raise ValueError("Unsupported native platform: " + repr(key))
    name, checksum = CHECKSUMS[key]
    archive = destination / ("mandrel-java25-" + name + "-" + VERSION + ".tar.gz")
    destination.mkdir(parents=True, exist_ok=True)
    if not archive.exists():
        urllib.request.urlretrieve("https://github.com/graalvm/mandrel/releases/download/mandrel-" + VERSION + "/" + archive.name, archive)
    if hashlib.sha256(archive.read_bytes()).hexdigest() != checksum:
        raise ValueError("Mandrel archive checksum mismatch")
    with tarfile.open(archive) as source:
        source.extractall(destination, filter="data")
    home = destination / ("mandrel-java25-" + VERSION)
    if key[0] == "Darwin": home = home / "Contents/Home"
    return home.resolve()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(); parser.add_argument("destination", type=Path)
    home = install(parser.parse_args().destination)
    if os.environ.get("GITHUB_ENV"):
        with open(os.environ["GITHUB_ENV"], "a") as out: out.write("JAVA_HOME=" + str(home) + "\n")
        with open(os.environ["GITHUB_PATH"], "a") as out: out.write(str(home / "bin") + "\n")
    print(home)
