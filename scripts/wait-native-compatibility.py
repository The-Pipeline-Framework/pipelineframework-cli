#!/usr/bin/env python3
"""Require owner verification and coordinator approval on the exact source commit."""
import json
import subprocess
import sys
import time
revision=sys.argv[1]
for attempt in range(120):
    result=json.loads(subprocess.check_output(['gh','api',f'repos/The-Pipeline-Framework/pipelineframework-cli/commits/{revision}/status']))
    matching=[item for item in result['statuses'] if item['context']=='tpf/system-tests']
    checks=json.loads(subprocess.check_output(['gh','api',f'repos/The-Pipeline-Framework/pipelineframework-cli/commits/{revision}/check-runs']))
    owner=[item for item in checks['check_runs'] if item['name']=='verify' and item['app']['slug']=='github-actions']
    owner_passed=bool(owner) and owner[0]['conclusion']=='success'
    if owner and owner[0]['status']=='completed' and not owner_passed:
        raise SystemExit('Owner verification rejected publication: '+owner[0].get('html_url',''))
    if matching:
        state=matching[0]['state']
        if state=='success' and owner_passed: print('Exact-source owner verification and compatibility passed'); break
        if state in ('failure','error'): raise SystemExit('Exact-source compatibility rejected publication: '+matching[0].get('target_url',''))
    time.sleep(10)
else: raise SystemExit('Missing successful exact-source owner verification or compatibility; publication refused')
