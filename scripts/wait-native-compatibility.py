#!/usr/bin/env python3
"""Require owner verification and coordinator approval on the exact source commit."""
import json
import subprocess
import sys
import time

REPOSITORY = 'repos/The-Pipeline-Framework/pipelineframework-cli'


def pages(endpoint):
    return json.loads(subprocess.check_output(['gh', 'api', '--paginate', '--slurp', endpoint]))


def owner_verification(revision):
    runs = [run for page in pages(f'{REPOSITORY}/actions/workflows/ci.yml/runs?head_sha={revision}&per_page=100')
            for run in page['workflow_runs']
            if run['head_sha'] == revision and run['path'] == '.github/workflows/ci.yml']
    if not runs:
        return False
    latest = max(runs, key=lambda run: (run['run_number'], run['id']))
    jobs = [job for page in pages(f"{REPOSITORY}/actions/runs/{latest['id']}/attempts/{latest['run_attempt']}/jobs?per_page=100")
            for job in page['jobs'] if job['name'] == 'verify']
    if len(jobs) != 1:
        return False
    job = jobs[0]
    if job['status'] == 'completed' and job['conclusion'] != 'success':
        raise SystemExit('Owner verification rejected publication: ' + job.get('html_url', ''))
    return job['status'] == 'completed' and job['conclusion'] == 'success'


def wait(revision):
    for attempt in range(120):
        result = json.loads(subprocess.check_output(['gh', 'api', f'{REPOSITORY}/commits/{revision}/status']))
        matching = [item for item in result['statuses'] if item['context'] == 'tpf/system-tests']
        owner_passed = owner_verification(revision)
        if matching:
            state = matching[0]['state']
            if state == 'success' and owner_passed:
                print('Exact-source owner verification and compatibility passed')
                return
            if state in ('failure', 'error'):
                raise SystemExit('Exact-source compatibility rejected publication: ' + matching[0].get('target_url', ''))
        time.sleep(10)
    raise SystemExit('Missing successful exact-source owner verification or compatibility; publication refused')


if __name__ == '__main__':
    wait(sys.argv[1])
