# The Pipeline Framework CLI

The `tpf` CLI verifies and deploys immutable TPF Releases. Maven and future build-tool integrations produce
`pipeline-release.json`; this repository consumes it without rebuilding or rewriting it.

```bash
tpf release verify
tpf deploy local
tpf deploy staging
```

By default, `tpf` looks for `./pipeline-release.json` or `./target/pipeline-release.json`.
If both exist, specify the intended file with `--release`.

Deployment configuration belongs in `tpf-deploy.yaml`. Release identity, artifact URIs and digests remain identical
when the same descriptor is promoted between environments.

Build with Java 21:

```bash
./mvnw verify
```

The `tpf-cli` module produces an executable `-all.jar` and ZIP/TAR distributions containing POSIX and Windows
launchers.

The ordinary test suite has no container dependency. Run the real OCI registry conformance test explicitly on a
machine with Podman:

```bash
TPF_RUN_PODMAN_OCI_TEST=true ./mvnw -pl tpf-release-resolver test
```
