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

For Cloud deployments, see [Cloud authentication](docs/cloud-authentication.md) for explicit
human device login, persistent credential mounts and non-interactive CI credential sources.
`deploy` never prompts for login and authentication never changes the Release.

The first supported installation is the public Java 21 container image at
`ghcr.io/the-pipeline-framework/tpf` (initially `linux/amd64`). See
[Install the CLI](https://pipelineframework.org/deploy/cli-installation) for Docker/Podman commands,
the shell wrapper, resolver mounts, container paths and digest-pinned CI examples.
The image becomes installable when the trusted publication workflow passes its anonymous-pull and
published-digest conformance checks. `main` is a development tag; use a version tag or digest for reproducibility.

Maven produces the Release; the CLI consumes it:

```bash
# Configure pipelineframework-release-maven-plugin:generate-release-descriptor in your POM (verify phase).
./mvnw verify -Dtpf.release.version=2026.10.02.1 -Dmaven.repo.local="$PWD/.m2/repository"
# Optional: publish the Maven artefacts referenced by the descriptor.
./mvnw deploy -Dtpf.release.version=2026.10.02.1 -Dmaven.repo.local="$PWD/.m2/repository"
# Preserve target/pipeline-release.json unchanged for the deployment job.
tpf release verify --release target/pipeline-release.json
tpf deploy staging --release target/pipeline-release.json
```

Maven is the Release producer, not a Cloud deployment client. There is no `tpf:deploy` Maven goal.
The Cloud target requires the private external Cloud API to be available, an existing Application and
Environment, and an authorised bearer credential. Its current success state is `REGISTERED`;
physical deployment, runtime verification and activation remain `NOT_REQUESTED`.

Build the CLI from source with Java 21:

```bash
./mvnw verify -Dmaven.repo.local="$PWD/.m2/repository"
```

The `tpf-cli` module produces an executable `-all.jar` and ZIP/TAR distributions containing POSIX and Windows
launchers for development. These build outputs are not a claim of supported native downloads.

The ordinary test suite has no container dependency. Run the real OCI registry conformance test explicitly on a
machine with Podman:

```bash
TPF_RUN_PODMAN_OCI_TEST=true ./mvnw -pl tpf-release-resolver test -Dmaven.repo.local="$PWD/.m2/repository"
```

## Container validation and publication

```sh
python3 scripts/test-publication-config.py
python3 scripts/test-container-config.py
docker build --build-arg VERSION=26.10.1-SNAPSHOT --build-arg REVISION="$(git rev-parse HEAD)" -t tpf-tested .
python3 scripts/test-container-workflow.py --image tpf-tested
```

Build after `mvn verify`. The test starts from only `pipeline-release.json` in each working directory, mounts
external resolver/deployment configuration read-only, starts with an empty persistent cache, and exercises
Maven and OCI authentication, JSON verification, exact-byte Cloud registration and missing Cloud credentials
inside the CLI image. A separate Python container supplies controlled protocol fixtures; this is not a live
private Cloud deployment test. The fixture container is removed after the test.

`publish-container.yml` runs only for trusted `main` pushes/manual runs and published non-prerelease tags whose
commits are merged into `main` and whose names match the non-snapshot Maven version. An unprivileged Java 21 job
verifies and tests the image. A separate job publishes that exact image using only repository `GITHUB_TOKEN`
with `packages: write`, reports `container-publication.json` and the image digest in its summary, then pulls
anonymously and repeats the test against the published digest. No MokapotLabs PAT belongs in this workflow.

Tags include `sha-<full commit>` and `<Maven version>-sha-<full commit>`. Main builds also update `main`;
release builds publish `<version>`. Pin CI to the reported digest. Commit tags identify source, but a rebuilt
image can change with its base image or snapshot dependencies; the digest pins the exact installed bytes.

GitHub initially creates container packages as private. On the first trusted publication, a package
administrator must link the `tpf` package to this repository (the source label and repository token provide
the normal association), set visibility to **public**, and rerun a failed anonymous-pull job. An existing
package must grant this repository write access. The workflow fails until an unauthenticated pull works.
See [GitHub Container registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry).

Native downloads and JReleaser are the next distribution slice. This plain-Java CLI needs its own
[Native Build Tools](https://graalvm.github.io/native-build-tools/latest/maven-plugin)/Mandrel configuration
and reachability checks. Before [JReleaser distribution packaging](https://jreleaser.org/guide/latest/reference/distributions.html),
a native-image conformance build must pass JSON serialisation, Maven Resolver, OCI and authentication paths.
A Quarkus container-native workflow is prior art, not conformance evidence for this CLI.
