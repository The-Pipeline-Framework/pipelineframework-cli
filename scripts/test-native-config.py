#!/usr/bin/env python3
"""Regression gates for native artifact validation and publication authority."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET
import json
import os
import shutil
import subprocess
ROOT=Path(__file__).resolve().parents[1]

def load(name):
    spec=importlib.util.spec_from_file_location(name,ROOT/'scripts'/f'{name}.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

class NativePolicyTest(unittest.TestCase):
    def test_preinstalled_linux_homebrew_off_path_is_reused(self):
        for linked in (True,False):
            with self.subTest(linked=linked), tempfile.TemporaryDirectory() as directory:
                root=Path(directory);prefix=root/'linuxbrew';tools=root/'tools';tools.mkdir()
                brew=prefix/'Homebrew/bin/brew';brew.parent.mkdir(parents=True)
                brew.write_text("#!/bin/sh\nprintf 'Homebrew fixture\\n'\n");brew.chmod(0o755)
                if linked:
                    (prefix/'bin').mkdir();(prefix/'bin/brew').symlink_to(brew)
                uname=tools/'uname';uname.write_text("#!/bin/sh\nprintf 'Linux\\n'\n");uname.chmod(0o755)
                for command in ('mkdir','ln'):
                    (tools/command).symlink_to(shutil.which(command))
                script=root/'setup.sh'
                script.write_text((ROOT/'scripts/setup-homebrew.sh').read_text().replace(
                    'prefix=/home/linuxbrew/.linuxbrew','prefix='+str(prefix)))
                path_file=root/'github-path'
                env={**os.environ,'PATH':str(tools),'GITHUB_PATH':str(path_file)}
                for attempt in range(2):
                    result=subprocess.run([shutil.which('sh'),str(script)],env=env,text=True,capture_output=True)
                    self.assertEqual(0,result.returncode,result.stderr)
                    self.assertEqual('Homebrew fixture\n',result.stdout)
                self.assertEqual([str(prefix/'bin')]*2,path_file.read_text().splitlines())
                self.assertTrue((prefix/'bin/brew').is_file())

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

    def test_latest_requires_current_main_snapshot(self):
        gate=load('check-native-release')
        for version,main,tag,allowed in (
            ('1.2.3-SNAPSHOT','a'*40,'',True),
            ('1.2.3-SNAPSHOT','b'*40,'',False),
            ('1.2.3','a'*40,'',False),
            ('1.2.3-SNAPSHOT','a'*40,'v1.2.3-SNAPSHOT',False)):
            with self.subTest(version=version,main=main,tag=tag):
                pom=ET.ElementTree(ET.fromstring('<project xmlns="http://maven.apache.org/POM/4.0.0"><version>'+version+'</version></project>'))
                with patch.object(gate.ET,'parse',return_value=pom), patch.object(gate.subprocess,'check_output',side_effect=['a'*40,main+' refs/heads/main']):
                    if allowed:
                        self.assertEqual((version,'a'*40),gate.identity(latest=True))
                    else:
                        with self.assertRaises(ValueError): gate.identity(tag,latest=True)

    def test_latest_archives_have_no_tap_credentials_and_keep_all_gates(self):
        workflow=(ROOT/'.github/workflows/native-distribution.yml').read_text()
        latest=workflow.split('\n  publish-latest:\n')[1].split('\n  install-latest:')[0]
        self.assertIn("github.ref == 'refs/heads/main'",latest)
        self.assertIn("github.repository == 'The-Pipeline-Framework/pipelineframework-cli'",latest)
        self.assertIn("(github.event_name == 'schedule' || github.event_name == 'workflow_dispatch')",latest)
        self.assertIn("- cron: '47 20 * * *'",workflow)
        self.assertIn("endsWith(needs.identity.outputs.version, '-SNAPSHOT')",latest)
        self.assertIn('needs: [identity, build, prepare, install-candidate]',latest)
        self.assertIn('wait-native-compatibility.py',latest)
        self.assertIn('--latest --artifacts target/distributions',latest)
        self.assertIn('JRELEASER_TAG_NAME: latest',latest)
        self.assertNotIn('secrets.',latest)
        self.assertNotIn('tap-token',latest)
        self.assertIn("--tag latest --revision '${{ needs.identity.outputs.revision }}' --published",workflow)

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
