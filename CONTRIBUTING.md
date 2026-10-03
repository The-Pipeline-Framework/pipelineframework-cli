# Contributing to the TPF CLI

This repository owns the CLI and its build-tool-neutral release resolution and deployment adapters.
User documentation is published in the [TPF Deploy documentation](https://pipelineframework.org/deploy/deployment-cli).
Update that canonical documentation alongside changes to user behaviour. This repository does not publish a docs site.

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


## Native publication

The native workflow builds macOS ARM64, Linux x64 and Linux ARM64 on matching runners. Build and package-definition
jobs have read-only repository permission and no publication secrets. Each archive includes its platform, source
revision, pinned Mandrel/Java version, checksums and conformance/timing report. Windows, Intel macOS and Alpine are
outside this distribution.

### Snapshot latest

The nightly schedule runs at 20:47 UTC, alongside the existing Maven Central snapshot schedule. Both workflows
validate independently; the native workflow does not require a stable version or Maven publication credentials.
Manual runs on main can also publish snapshots; main pushes and PRs validate without snapshot publication.

A `-SNAPSHOT` reactor version on trusted main is eligible for the moving `latest` GitHub prerelease after all native
conformance, candidate installation, owner verification and exact-source Compatibility Set gates pass. The same
checksummed archives keep their original version, commit and toolchain metadata; no release-like staging artifacts
are published. Public archive installations are then exercised on all three platforms.

Snapshots use only the CLI repository's `GITHUB_TOKEN`; Homebrew App provisioning does not block them. The standard
JReleaser Homebrew packager excludes snapshots, so Homebrew stays a stable-release channel. `latest` is deliberately
mutable: JReleaser replaces that snapshot prerelease on each passing nightly or manual main publication. Retries use the original
artifacts, and stale runs cannot replace a newer main commit. Download and preserve the ZIP, metadata and checksum
for repeatable CI; do not treat the moving URL as an immutable version.

### One-time Homebrew setup

The public `The-Pipeline-Framework/homebrew-tap` repository is the publication destination. Create a dedicated GitHub
App with **Contents: read and write**, and install it on **only homebrew-tap**. Set `HOMEBREW_APP_ID` as a repository
variable and `HOMEBREW_APP_PRIVATE_KEY` as a repository/environment secret on pipelineframework-cli. The trusted
`native-release` job mints a token restricted to the tap and revokes it when finished. No PAT is required.
The CLI repository's `GITHUB_TOKEN` publishes GitHub release assets, not cross-repository formula commits.

Ensure the tap's default branch is `main`. Configure the `native-release` GitHub environment to allow protected version
tags. Do not expose App credentials to build jobs or use a broadly installed Cloud/coordinator App for this purpose.

### Release

Prepare a non-SNAPSHOT reactor version using the normal version change process; merge it into main with the required
owner and exact-source Compatibility Set checks. Create the matching `v<version>` tag on that main commit. A tag
whose version differs from the root POM, or whose source is outside main, fails before publication.

All three platforms must pass JVM and native conformance, extracted-archive tests, generated Homebrew formula tests,
and installation tests before publishing. JReleaser uses the already-tested archives. Published assets and the real
Homebrew installation are exercised afterward on all three platforms. Release success requires those final checks.
The macOS binary is unsigned initially; Apple Developer signing/notarization is a later distribution change.

### Retry and evidence

Rerun failed publication jobs on the original workflow run; downloaded build artifacts retain the exact tested
identity and bytes. Never rerun a complete build to silently replace an existing release. Archive identity checks
reject source/version/platform mismatches, invalid executable permissions and failed conformance reports.
JReleaser refuses to overwrite an existing stable version release. If publication created the release before failing, preserve its
assets and resume only the unfinished steps with the original tested artifacts; do not enable overwrite. Investigate failing
post-publication checks before advertising the release as installable. Build reports retain startup timings and
binary/archive sizes; the release report records JReleaser output properties.

### Reachability metadata

The checked-in metadata was collected from the shared JVM fixture suite with the Java 25 tracing agent and reviewed
for concrete Picocli command classes, Jackson record construction, Maven Resolver service constructors, XML parsing,
HTTP and authentication paths. Platform-specific Apple security provider entries were removed. Fixture credentials,
absolute workstation paths and captured application data are not metadata. Native conformance, rather than successful
compilation alone, is the gate for metadata changes.

