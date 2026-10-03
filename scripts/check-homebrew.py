#!/usr/bin/env python3
"""Check the prepared formula and all three supported platform URLs."""
import argparse
from pathlib import Path
import subprocess
parser=argparse.ArgumentParser();parser.add_argument('directory',type=Path);args=parser.parse_args()
formulae=list(args.directory.rglob('tpf.rb'))
if len(formulae)!=1: raise SystemExit('Expected exactly one prepared Homebrew formula')
formula=formulae[0];source=formula.read_text()
subprocess.run(['ruby','-c',str(formula)],check=True)
for platform in ('osx-aarch_64','linux-x86_64','linux-aarch_64'):
    if platform not in source: raise SystemExit('Missing Homebrew platform: '+platform)
if 'sha256' not in source or 'bin.install' not in source: raise SystemExit('Formula must verify and install native binaries')
if 'openjdk' in source: raise SystemExit('Native Homebrew installation must not require Java')
print(formula)
