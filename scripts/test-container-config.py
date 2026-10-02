#!/usr/bin/env python3
"""Regression guards for publication identity, trust boundaries and wrapper arguments."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class ContainerConfigurationTest(unittest.TestCase):
    def test_metadata_rejects_mismatched_release_and_short_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            (work / "pom.xml").write_text('<project xmlns="http://maven.apache.org/POM/4.0.0"><version>1.2.3</version></project>')
            command = ["python3", str(ROOT / "scripts/container-metadata.py"), "--output", str(work / "metadata.json")]
            for revision, tag in [("a" * 40, "v1.2.4"), ("abc123", "v1.2.3")]:
                self.assertNotEqual(0, subprocess.run(command + ["--revision", revision, "--release-tag", tag],
                                                     cwd=work, capture_output=True).returncode)
            subprocess.run(command + ["--revision", "a" * 40, "--release-tag", "v1.2.3"], cwd=work, check=True)
            metadata = json.loads((work / "metadata.json").read_text())
            self.assertIn("ghcr.io/the-pipeline-framework/tpf:1.2.3", metadata["tags"])
            self.assertIn("ghcr.io/the-pipeline-framework/tpf:sha-" + "a" * 40, metadata["tags"])
            self.assertNotIn("ghcr.io/the-pipeline-framework/tpf:main", metadata["tags"])
            (work / "pom.xml").write_text('<project xmlns="http://maven.apache.org/POM/4.0.0"><version>1.2.3-SNAPSHOT</version></project>')
            self.assertNotEqual(0, subprocess.run(command + ["--revision", "a" * 40, "--release-tag", "v1.2.3-SNAPSHOT"],
                                                 cwd=work, capture_output=True).returncode)
            (work / "pom.xml").write_text('<project xmlns="http://maven.apache.org/POM/4.0.0"/>')
            missing = subprocess.run(command + ["--revision", "a" * 40], cwd=work, capture_output=True, text=True)
            self.assertEqual(2, missing.returncode)
            self.assertIn("Unsupported Maven version", missing.stderr)
            self.assertNotIn("Traceback", missing.stderr)

    def test_publication_digest_comes_from_registry_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            engine = work / "docker"
            engine.write_text('#!/usr/bin/env python3\nimport json,os,sys\n'
                              'assert "image" not in sys.argv, "Must not inspect local RepoDigests"\n'
                              'if sys.argv[1:4] == ["buildx", "imagetools", "inspect"]:\n'
                              ' print(json.dumps({"digest": os.environ["FIXTURE_DIGEST"]}))\n')
            engine.chmod(0o755)
            metadata = work / "metadata.json"
            image = "ghcr.io/the-pipeline-framework/tpf"
            original = json.dumps({"image": image, "tags": [image + ":test"]})
            metadata.write_text(original)
            command = ["python3", str(ROOT / "scripts/publish-container.py"), "--engine", str(engine),
                       "--metadata", str(metadata)]
            environment = dict(os.environ, FIXTURE_DIGEST="sha256:" + "c" * 64)
            subprocess.run(command, env=environment, check=True)
            self.assertEqual(image + "@" + environment["FIXTURE_DIGEST"],
                             json.loads(metadata.read_text())["digestReference"])
            metadata.write_text(original)
            environment["FIXTURE_DIGEST"] = "bad-digest"
            invalid = subprocess.run(command, env=environment, capture_output=True)
            self.assertEqual(2, invalid.returncode)
            self.assertEqual(original, metadata.read_text())

    def test_publication_is_trusted_and_retests_published_digest(self):
        workflow = (ROOT / ".github/workflows/publish-container.yml").read_text()
        self.assertNotIn("pull_request", workflow)
        self.assertNotIn("workflow_run", workflow)
        self.assertNotIn("PAT", workflow)
        self.assertNotIn("MokapotLabs", workflow)
        verify, publish = workflow.split("\n  publish:\n")
        self.assertNotIn("packages: write", verify)
        self.assertNotIn("secrets.", verify)
        self.assertIn("git merge-base --is-ancestor HEAD origin/main", verify)
        self.assertIn("github.ref == 'refs/heads/main'", verify)
        self.assertIn("needs: verify", publish)
        self.assertIn("packages: write", publish)
        self.assertIn("secrets.GITHUB_TOKEN", publish)
        self.assertIn('docker pull "$IMAGE"', publish)
        self.assertIn('test-container-workflow.py --image "$IMAGE"', publish)
        self.assertIn("digestReference", publish)
        self.assertIn('export DOCKER_CONFIG="$(mktemp -d)"', publish)

    def test_wrapper_preserves_arguments_and_read_only_resolver_mounts(self):
        with tempfile.TemporaryDirectory(prefix="tpf wrapper ") as directory:
            work = Path(directory)
            engine = work / "engine"
            engine.write_text('#!/usr/bin/env python3\nimport json,sys\nprint(json.dumps(sys.argv[1:]))\n')
            engine.chmod(0o755)
            settings = work / "settings.xml"
            settings.write_text("<settings/>")
            oci = work / "config.json"
            oci.write_text("{}")
            environment = dict(os.environ, TPF_IMAGE="ghcr.io/the-pipeline-framework/tpf:1.2.3",
                               TPF_CONTAINER_ENGINE=str(engine), TPF_MAVEN_SETTINGS=str(settings),
                               TPF_OCI_CONFIG=str(oci), TPF_CREDENTIAL_DIR=str(work / "credentials"),
                               TPF_CLOUD_CREDENTIAL_ENV="TPF_CREDENTIAL_STAGING")
            result = subprocess.check_output(["sh", str(ROOT / "scripts/tpf-container"), "deploy", "staging",
                                              "--release", "release with spaces.json"], cwd=work,
                                             env=environment, text=True)
            arguments = json.loads(result)
            self.assertEqual([environment["TPF_IMAGE"], "deploy", "staging", "--release", "release with spaces.json"],
                             arguments[-5:])
            self.assertIn("TPF_CREDENTIAL_STAGING", arguments)
            self.assertIn(f"type=bind,src={settings},dst=/home/tpf/.m2/settings.xml,readonly", arguments)
            self.assertIn(f"type=bind,src={oci},dst=/home/tpf/.docker/config.json,readonly", arguments)
            self.assertTrue((work / "credentials").is_dir())
            self.assertIn(f"type=bind,src={work / 'credentials/maven'},dst=/home/tpf/.m2/repository", arguments)
            self.assertTrue((work / "credentials/maven").is_dir())
            podman = work / "podman"
            podman.write_text('#!/usr/bin/env python3\nimport json,sys\n'
                              'print("true" if sys.argv[1] == "info" else json.dumps(sys.argv[1:]))\n')
            podman.chmod(0o755)
            environment["TPF_CONTAINER_ENGINE"] = str(podman)
            result = subprocess.check_output(["sh", str(ROOT / "scripts/tpf-container"), "--version"],
                                             cwd=work, env=environment, text=True)
            arguments = json.loads(result)
            self.assertEqual(["run", "--userns", "keep-id"], arguments[:3])


if __name__ == "__main__":
    unittest.main()
