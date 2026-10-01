#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
case "${1:-}" in
  candidate-version)
    mode=${2:?}; pr_number=${3:?}; sha=${4:?}
    actual=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["candidateVersion"])' "$repo_root/.candidate-version.json")
    base_version=${actual%%-*}
    if [[ "$mode" == pull_request ]]; then expected="${base_version}-pr.${pr_number}.${sha:0:12}"; else expected="${base_version}-main.${sha:0:12}"; fi
    [[ "$actual" == "$expected" ]]
    ;;
  reactor-coordinates)
    python3 - "$repo_root" <<'PY'
import pathlib, sys, xml.etree.ElementTree as ET
root = pathlib.Path(sys.argv[1])
ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
parent = ET.parse(root / 'pom.xml').getroot()
version = parent.findtext('m:version', namespaces=ns)
expected = {'pipelineframework-cli-parent', 'tpf-release-resolver', 'tpf-deployment-api', 'tpf-deployment-core', 'tpf-deployment-target-local', 'tpf-deployment-target-cloud', 'tpf-cli'}
actual = {parent.findtext('m:artifactId', namespaces=ns)}
for pom in sorted(root.glob('*/pom.xml')):
    project = ET.parse(pom).getroot()
    actual.add(project.findtext('m:artifactId', namespaces=ns))
    p = project.find('m:parent', namespaces=ns)
    assert p is not None and p.findtext('m:version', namespaces=ns) == version, pom
assert actual == expected, actual ^ expected
PY
    ;;
  reactor-dependencies)
    python3 - "$repo_root" <<'PY'
import pathlib, sys, xml.etree.ElementTree as ET
root = pathlib.Path(sys.argv[1]); ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
version = ET.parse(root / 'pom.xml').getroot().findtext('m:version', namespaces=ns)
reactor = {'tpf-release-resolver', 'tpf-deployment-api', 'tpf-deployment-core', 'tpf-deployment-target-local', 'tpf-deployment-target-cloud', 'tpf-cli'}
for pom in sorted(root.glob('*/pom.xml')):
    project = ET.parse(pom).getroot()
    for dependency in project.findall('.//m:dependency', namespaces=ns):
        if dependency.findtext('m:groupId', namespaces=ns) == 'org.pipelineframework' and dependency.findtext('m:artifactId', namespaces=ns) in reactor:
            assert dependency.findtext('m:version', namespaces=ns) in (version, '${project.version}'), pom
PY
    ;;
  *) echo "usage: system-tests.sh candidate-version EVENT PR SHA | reactor-coordinates | reactor-dependencies" >&2; exit 2 ;;
esac
