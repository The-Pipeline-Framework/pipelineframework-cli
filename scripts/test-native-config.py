#!/usr/bin/env python3
"""Regression gates for native artifact validation and publication authority."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET
import json
import runpy
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
        coordinator=json.dumps({'statuses':[{'context':'tpf/system-tests','state':'success'}]}).encode()
        owner=json.dumps({'check_runs':[{'name':'verify','app':{'slug':'github-actions'},
            'status':'completed','conclusion':'failure'}]}).encode()
        with patch('sys.argv',['wait-native-compatibility.py','a'*40]), patch('subprocess.check_output',side_effect=[coordinator,owner]):
            with self.assertRaisesRegex(SystemExit,'Owner verification rejected'):
                runpy.run_path(str(ROOT/'scripts/wait-native-compatibility.py'),run_name='__main__')

if __name__=='__main__': unittest.main()
