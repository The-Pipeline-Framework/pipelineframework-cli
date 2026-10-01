#!/usr/bin/env bash
set -euo pipefail

mode=${1:?usage: prepare-candidate.sh pull_request|push PR_NUMBER SHA}
pr_number=${2:--}
sha=${3:?}
repo_root=$(cd "$(dirname "$0")/.." && pwd)
base_version=$(python3 - "$repo_root/pom.xml" <<'PY'
import sys, xml.etree.ElementTree as ET
root = ET.parse(sys.argv[1]).getroot()
ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
print(root.findtext('m:version', namespaces=ns))
PY
)
[[ "$base_version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)-SNAPSHOT$ ]] || {
  echo "root project version must be a SNAPSHOT semver" >&2
  exit 2
}
base_version=${base_version%-SNAPSHOT}
case "$mode" in
  pull_request) version_mode=pr ;;
  push) version_mode=main ;;
  *) echo "mode must be pull_request or push" >&2; exit 2 ;;
esac
candidate=$(bash "$repo_root/scripts/candidate-version.sh" "$version_mode" "$base_version" "$pr_number" "$sha")

"$repo_root/mvnw" -B -N -Dmaven.repo.local="$repo_root/.m2/repository" \
  -Dmaven.deploy.skip=true -Dgpg.skip=true \
  org.codehaus.mojo:versions-maven-plugin:2.22.0:set \
  -DnewVersion="$candidate" -DprocessAllModules=true -DgenerateBackupPoms=false
python3 "$repo_root/scripts/rewrite-reactor-dependencies.py" "$repo_root" "$candidate"

echo "candidate=$candidate" >> "${GITHUB_OUTPUT:-/dev/null}"
python3 - "$candidate" "$repo_root" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[2]) / '.candidate-version.json'
path.write_text(json.dumps({'candidateVersion': sys.argv[1]}, sort_keys=True) + '\n')
PY
if [[ -n "${RUNNER_TEMP:-}" ]]; then
  cp "$repo_root/.candidate-version.json" "$RUNNER_TEMP/tpf-candidate-version.json"
else
  cp "$repo_root/.candidate-version.json" "$repo_root/candidate-manifest.json"
fi
