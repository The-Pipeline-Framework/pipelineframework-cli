#!/usr/bin/env python3
import hashlib, json, os, pathlib, sys

root = pathlib.Path(sys.argv[1])
repository = os.environ['GITHUB_REPOSITORY']
source_repository = os.environ['SOURCE_REPOSITORY']
source_sha = os.environ['SOURCE_SHA'].lower()
event = os.environ['GITHUB_EVENT_NAME']
pull_request = os.environ.get('PULL_REQUEST_NUMBER', '')
version = json.loads((root / 'candidate-manifest.json').read_text())['candidateVersion']
if event == 'pull_request':
    if not pull_request.isdigit(): raise SystemExit('pull request number is required')
    pull_request_number = int(pull_request)
    expected_suffix = f'-pr.{pull_request_number}.{source_sha[:12]}'
elif event == 'push':
    pull_request_number = None
    expected_suffix = f'-main.{source_sha[:12]}'
else: raise SystemExit(f'unsupported candidate build event: {event}')
if not version.endswith(expected_suffix): raise SystemExit('candidate version does not match source event and SHA')
coordinates = [('pipelineframework-cli-parent', 'pom'), ('tpf-release-resolver', 'jar'), ('tpf-deployment-api', 'jar'), ('tpf-deployment-core', 'jar'), ('tpf-deployment-target-local', 'jar'), ('tpf-deployment-target-cloud', 'jar'), ('tpf-cli', 'jar')]
artifacts = []
for artifact_id, packaging in coordinates:
    directory = root / 'repository' / 'org' / 'pipelineframework' / artifact_id / version
    names = [f'{artifact_id}-{version}.pom'] + ([] if packaging == 'pom' else [f'{artifact_id}-{version}.jar'])
    artifacts.append({'groupId': 'org.pipelineframework', 'artifactId': artifact_id, 'version': version, 'packaging': packaging,
                      'files': [{'name': name, 'sha256': hashlib.sha256((directory / name).read_bytes()).hexdigest()} for name in names]})
pull_request = os.environ.get('PULL_REQUEST_NUMBER')
metadata = {
    'schemaVersion': 1, 'repository': repository, 'sourceRepository': source_repository, 'component': 'cli',
    'sourceSha': source_sha, 'pullRequestNumber': pull_request_number, 'candidateVersion': version,
    'provenance': {'build': {'repository': repository, 'workflowPath': '.github/workflows/tpf-candidate-build.yml',
              'runId': int(os.environ['GITHUB_RUN_ID']), 'runAttempt': int(os.environ['GITHUB_RUN_ATTEMPT']), 'event': event}},
    'mavenArtifacts': artifacts
}
(root / 'build-metadata.json').write_text(json.dumps(metadata, indent=2, sort_keys=True) + '\n')
