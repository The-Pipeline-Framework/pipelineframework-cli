#!/usr/bin/env bash
set -euo pipefail

destination=${1:?usage: collect-candidate-files.sh DESTINATION}
repo_root=$(cd "$(dirname "$0")/.." && pwd)
local_repository="$repo_root/.m2/repository/org/pipelineframework"
version=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["candidateVersion"])' "$repo_root/.candidate-version.json")
mkdir -p "$destination/repository/org/pipelineframework"
artifacts=(pipelineframework-cli-parent tpf-release-resolver tpf-deployment-api tpf-deployment-core tpf-deployment-target-local tpf-deployment-target-cloud tpf-cli)
for artifact_id in "${artifacts[@]}"; do
  source_dir="$local_repository/$artifact_id/$version"
  target_dir="$destination/repository/org/pipelineframework/$artifact_id/$version"
  [[ -d "$source_dir" ]] || { echo "candidate artifact directory is missing: $source_dir" >&2; exit 1; }
  mkdir -p "$target_dir"
  cp "$source_dir/$artifact_id-$version.pom" "$target_dir/"
  if [[ "$artifact_id" != pipelineframework-cli-parent ]]; then cp "$source_dir/$artifact_id-$version.jar" "$target_dir/"; fi
done
