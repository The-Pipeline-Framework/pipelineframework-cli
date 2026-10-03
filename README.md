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

## Install

Native releases run directly on macOS Apple Silicon and Linux x64/ARM64, without Java or Docker.
Homebrew is the recommended installation for published native releases:

```sh
brew install The-Pipeline-Framework/tap/tpf
tpf --version
tpf release verify --help
```

Check [GitHub Releases](https://github.com/The-Pipeline-Framework/pipelineframework-cli/releases) for an available
native version before installing. The tap is populated only after all platform builds and conformance checks pass;
a source checkout or a GHCR image is not a native release. Checksummed ZIP downloads provide a manual alternative.
The Linux baseline is Ubuntu 24.04 or a compatible glibc system. Intel macOS, Windows and Alpine are not supported
native targets. macOS downloads are initially unsigned; Homebrew is recommended.
See [Install the CLI](https://pipelineframework.org/deploy/cli-installation) for archive installation and first use.

The public `ghcr.io/the-pipeline-framework/tpf` container remains a secondary option. It supplies its own Java 25
runtime and currently targets `linux/amd64`. Containers need mounts to read host files; a directly installed `tpf`
uses the current directory and host paths normally. Use the full GHCR image name: Docker's short `tpf` name means
`tpf:latest` in its default registry. `main` is a development tag; pin a reported digest for reproducible CI.

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

## Build

Build the CLI from source with Java 25:

```bash
./mvnw verify -Dmaven.repo.local="$PWD/.m2/repository"
```

The `tpf-cli` module retains its executable `-all.jar` and JVM development archives. Native builds use the same reactor
and compiled source, with no Maven profile selection.

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
commits are merged into `main` and whose names match the non-snapshot Maven version. An unprivileged Java 25 job
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

Native downloads use this CLI's own
[Native Build Tools](https://graalvm.github.io/native-build-tools/latest/maven-plugin)/Mandrel configuration
and reviewed reachability metadata. [JReleaser distribution packaging](https://jreleaser.org/guide/latest/reference/distributions.html)
is gated by native conformance for JSON serialisation, Maven Resolver, OCI and authentication paths on every target.

## Native validation and release

Use the checksum-pinned Mandrel Java 25 toolchain:

```sh
export JAVA_HOME="$(python3 scripts/setup-mandrel.py "$PWD/target/toolchain")"
export PATH="$JAVA_HOME/bin:$PATH"
./mvnw clean verify -Dmaven.repo.local="$PWD/.m2/repository"
# Warm only this worktree's dependency cache for the standalone native plugin invocation.
./mvnw install -DskipTests -Dgpg.skip -Dmaven.repo.local="$PWD/.m2/repository"
./mvnw -pl tpf-cli package native:compile-no-fork -DskipTests -Dmaven.repo.local="$PWD/.m2/repository"
python3 scripts/test-container-workflow.py --executable tpf-cli/target/tpf --report target/native-conformance.json
```

The shared fixture suite also accepts `--java-jar` and `--image`. Native execution removes Java and Docker from the
client's search path. It proves JSON/YAML, Maven/OCI/file resolution, credential helpers, human device login and refresh,
CI service credentials, TLS verification, authentication failures and unchanged descriptor submission. Private Cloud
and identity APIs still need to exist for actual deployment.

Native Build Tools 1.1.13 compiles the plain-Java CLI using Mandrel 25.0.4.1-Final. Reviewed reflection/resource metadata
lives under `META-INF/native-image`; JVM fallback is disabled. JReleaser 1.26.0 packages tested platform-specific
`BINARY` archives and Homebrew formulae. Publication is not bound to Maven `verify` or `deploy`.

See [native publication setup](docs/native-publication.md) for the dedicated GitHub App, release checks and retry rules.
