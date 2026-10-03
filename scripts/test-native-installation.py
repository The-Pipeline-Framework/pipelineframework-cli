#!/usr/bin/env python3
"""Verify extracted or Homebrew-installed binaries against native conformance."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import urllib.request
import zipfile

parser=argparse.ArgumentParser();parser.add_argument('--platform',required=True);parser.add_argument('--version',required=True)
parser.add_argument('--published',action='store_true');parser.add_argument('--homebrew',action='store_true')
parser.add_argument('--formula',type=Path);args=parser.parse_args()
root=Path(__file__).resolve().parents[1]
with tempfile.TemporaryDirectory(prefix='tpf install ') as directory:
    work=Path(directory).resolve();name=f'tpf-{args.version}-{args.platform}'
    archive=work/(name+'.zip')
    if args.published:
        base=f'https://github.com/The-Pipeline-Framework/pipelineframework-cli/releases/download/v{args.version}/'
        urllib.request.urlretrieve(base+archive.name,archive)
        metadata=json.loads(urllib.request.urlopen(base+name+'.json').read())
    else:
        source=root/'target/distributions'/archive.name
        archive.write_bytes(source.read_bytes());metadata=json.loads(source.with_suffix('.json').read_text())
    if hashlib.sha256(archive.read_bytes()).hexdigest()!=metadata['sha256']: raise SystemExit('Downloaded archive digest mismatch')
    with zipfile.ZipFile(archive) as source:
        for item in source.infolist():
            destination=work/item.filename
            if not destination.resolve().is_relative_to(work): raise SystemExit('Unsafe archive entry')
            source.extract(item,work);destination.chmod(item.external_attr>>16 & 0o777)
    executable=work/name/'bin/tpf'
    candidate_tap = False
    try:
        if args.homebrew:
            if args.published:
                subprocess.run(['brew','tap','The-Pipeline-Framework/tap'],check=True)
                subprocess.run(['brew','install','The-Pipeline-Framework/tap/tpf'],check=True)
            else:
                # Formula generation checks all platforms; this runner downloads its candidate
                # using local file URLs, before public release URLs exist.
                formula=work/'tpf.rb';text=args.formula.read_text()
                for platform in ('osx-aarch_64','linux-x86_64','linux-aarch_64'):
                    remote=f'https://github.com/The-Pipeline-Framework/pipelineframework-cli/releases/download/v{args.version}/tpf-{args.version}-{platform}.zip'
                    local=(root/'target/distributions'/f'tpf-{args.version}-{platform}.zip').as_uri()
                    text=text.replace(remote,local)
                if subprocess.run(['brew','list','--versions','tpf'],capture_output=True).returncode == 0:
                    raise SystemExit('Candidate test requires a runner without an existing tpf installation')
                # This disposable local formula needs no Git history or commit identity.
                subprocess.run(['brew','tap-new','--no-git','tpf-conformance/native'],check=True)
                candidate_tap = True
                tap=Path(subprocess.check_output(['brew','--repo','tpf-conformance/native'],text=True).strip())
                formula=tap/'Formula/tpf.rb';formula.write_text(text)
                subprocess.run(['brew','install','tpf-conformance/native/tpf'],check=True)
            executable=Path(subprocess.check_output(['brew','--prefix','tpf'],text=True).strip())/'bin/tpf'
        if subprocess.check_output([str(executable),'--version'],text=True).strip()!=metadata['conformance']['version']:
            raise SystemExit('Installed CLI version mismatch')
        subprocess.run([sys.executable,str(root/'scripts/test-container-workflow.py'),'--executable',str(executable)],check=True)
        if args.homebrew: subprocess.run(['brew','test','tpf'],check=True)
    finally:
        if candidate_tap:
            subprocess.run(['brew','uninstall','tpf'],check=False)
            subprocess.run(['brew','untap','tpf-conformance/native'],check=True)
