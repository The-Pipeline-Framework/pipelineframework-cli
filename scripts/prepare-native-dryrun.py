#!/usr/bin/env python3
"""Stage candidate archives under a release-like version for packager-only validation."""
import os
import hashlib
import json
from pathlib import Path
import xml.etree.ElementTree as ET
import zipfile
version=ET.parse('pom.xml').getroot().findtext('{http://maven.apache.org/POM/4.0.0}version')
release=version.removesuffix('-SNAPSHOT')
if release!=version:
    for path in Path('target/distributions').glob('*.zip'):
        if version not in path.name: continue
        destination=path.with_name(path.name.replace(version,release))
        with zipfile.ZipFile(path) as original,zipfile.ZipFile(destination,'w') as staged:
            for entry in original.infolist():
                payload=original.read(entry);entry.filename=entry.filename.replace(version,release,1)
                staged.writestr(entry,payload)
        metadata=json.loads(path.with_suffix('.json').read_text())
        metadata.update(version=release, archive=destination.name, sha256=hashlib.sha256(destination.read_bytes()).hexdigest(), candidateOnly=True)
        destination.with_suffix('.json').write_text(json.dumps(metadata))
if os.environ.get('GITHUB_ENV'):
    with open(os.environ['GITHUB_ENV'],'a') as out: out.write('JRELEASER_PROJECT_VERSION='+release+'\n')
print(release)
