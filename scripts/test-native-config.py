#!/usr/bin/env python3
"""Regression gates for native artifact validation and publication authority."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET
import json
ROOT=Path(__file__).resolve().parents[1]

def load(name):
    spec=importlib.util.spec_from_file_location(name,ROOT/'scripts'/f'{name}.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

class NativePolicyTest(unittest.TestCase):
    def test_toolchain_is_pinned_for_exactly_supported_architectures(self):
        setup=load('setup-mandrel')
        self.assertEqual('25.0.4.1-Final',setup.VERSION)
        self.assertEqual({('Darwin','arm64'),('Linux','x86_64'),('Linux','aarch64')},set(setup.CHECKSUMS))
        self.assertTrue(all(len(value[1])==64 for value in setup.CHECKSUMS.values()))

    def test_unverified_binaries_cannot_be_packaged(self):
        package=load('package-native')
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory);report=path/'report.json';report.write_text(json.dumps(dict(passed=False,version='tpf 1.2.3')))
            with self.assertRaisesRegex(ValueError,'conformance'):
                package.package(path/'missing',path,'1.2.3','a'*40,'osx-aarch_64',report)

    def test_release_tags_must_match_non_snapshot_source(self):
        identity=load('check-native-release')
        with patch.object(identity.ET,'parse',return_value=ET.ElementTree(ET.fromstring('<project xmlns="http://maven.apache.org/POM/4.0.0"><version>1.2.3-SNAPSHOT</version></project>'))), patch.object(identity.subprocess,'check_output',return_value='a'*40):
            with self.assertRaisesRegex(ValueError,'non-SNAPSHOT'): identity.identity('v1.2.3')

    def test_publication_credentials_are_separate_from_builds(self):
        workflow=(ROOT/'.github/workflows/native-distribution.yml').read_text()
        build,publish=workflow.split('\n  publish:\n')
        self.assertNotIn('secrets.',build);self.assertNotIn('contents: write',build)
        self.assertIn('needs: [identity, build, prepare, install-candidate]',publish)
        self.assertIn('repositories: homebrew-tap',publish)
        self.assertIn('permission-contents: write',publish)
        self.assertIn('wait-native-compatibility.py',publish)
        self.assertNotIn('TPF_CLI_PUBLISH',workflow)
        self.assertIn("startsWith(github.ref, 'refs/tags/v')",publish)

    def test_compatibility_success_cannot_override_failed_owner_verification(self):
        gate=load('wait-native-compatibility')
        coordinator=json.dumps({'statuses':[{'context':'tpf/system-tests','state':'success'}]}).encode()
        runs=json.dumps([{'workflow_runs':[dict(id=42,run_number=1,run_attempt=2,
            path='.github/workflows/ci.yml',head_sha='a'*40)]}]).encode()
        owner=json.dumps([{'jobs':[{'name':'verify','status':'completed','conclusion':'failure'}]}]).encode()
        with patch('subprocess.check_output',side_effect=[coordinator,runs,owner]) as api:
            with self.assertRaisesRegex(SystemExit,'Owner verification rejected'):
                gate.wait('a'*40)
            self.assertIn('/attempts/2/jobs?per_page=100',api.call_args.args[0][-1])

    def test_only_expected_workflow_and_revision_can_satisfy_owner_gate(self):
        gate=load('wait-native-compatibility')
        runs=[{'workflow_runs':[
            dict(id=1,run_number=1,run_attempt=1,path='.github/workflows/other.yml',head_sha='a'*40),
            dict(id=2,run_number=2,run_attempt=1,path='.github/workflows/ci.yml',head_sha='b'*40)]}]
        with patch('subprocess.check_output',return_value=json.dumps(runs).encode()) as api:
            self.assertFalse(gate.owner_verification('a'*40))
            self.assertEqual(1,api.call_count)

    def test_latest_workflow_attempt_and_paginated_verify_job_are_used(self):
        gate=load('wait-native-compatibility')
        runs=[{'workflow_runs':[dict(id=20,run_number=2,run_attempt=3,
            path='.github/workflows/ci.yml',head_sha='a'*40)]},
            {'workflow_runs':[dict(id=10,run_number=1,run_attempt=1,
            path='.github/workflows/ci.yml',head_sha='a'*40)]}]
        jobs=[{'jobs':[dict(name='other',status='completed',conclusion='success')]},
              {'jobs':[dict(name='verify',status='completed',conclusion='success')]}]
        with patch('subprocess.check_output',side_effect=[json.dumps(runs).encode(),json.dumps(jobs).encode()]) as api:
            self.assertTrue(gate.owner_verification('a'*40))
            self.assertIn('/runs/20/attempts/3/jobs?per_page=100',api.call_args.args[0][-1])
            for call in api.call_args_list:
                self.assertIn('--paginate',call.args[0])

if __name__=='__main__': unittest.main()
