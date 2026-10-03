# Native publication

The native workflow builds macOS ARM64, Linux x64 and Linux ARM64 on matching runners. Build and package-definition
jobs have read-only repository permission and no publication secrets. Each archive includes its platform, source
revision, pinned Mandrel/Java version, checksums and conformance/timing report. Windows, Intel macOS and Alpine are
outside this distribution.

## Snapshot latest

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

## One-time Homebrew setup

The public `The-Pipeline-Framework/homebrew-tap` repository is the publication destination. Create a dedicated GitHub
App with **Contents: read and write**, and install it on **only homebrew-tap**. Set `HOMEBREW_APP_ID` as a repository
variable and `HOMEBREW_APP_PRIVATE_KEY` as a repository/environment secret on pipelineframework-cli. The trusted
`native-release` job mints a token restricted to the tap and revokes it when finished. No PAT is required.
The CLI repository's `GITHUB_TOKEN` publishes GitHub release assets, not cross-repository formula commits.

Ensure the tap's default branch is `main`. Configure the `native-release` GitHub environment to allow protected version
tags. Do not expose App credentials to build jobs or use a broadly installed Cloud/coordinator App for this purpose.

## Release

Prepare a non-SNAPSHOT reactor version using the normal version change process; merge it into main with the required
owner and exact-source Compatibility Set checks. Create the matching `v<version>` tag on that main commit. A tag
whose version differs from the root POM, or whose source is outside main, fails before publication.

All three platforms must pass JVM and native conformance, extracted-archive tests, generated Homebrew formula tests,
and installation tests before publishing. JReleaser uses the already-tested archives. Published assets and the real
Homebrew installation are exercised afterward on all three platforms. Release success requires those final checks.
The macOS binary is unsigned initially; Apple Developer signing/notarization is a later distribution change.

## Retry and evidence

Rerun failed publication jobs on the original workflow run; downloaded build artifacts retain the exact tested
identity and bytes. Never rerun a complete build to silently replace an existing release. Archive identity checks
reject source/version/platform mismatches, invalid executable permissions and failed conformance reports.
JReleaser refuses to overwrite an existing stable version release. If publication created the release before failing, preserve its
assets and resume only the unfinished steps with the original tested artifacts; do not enable overwrite. Investigate failing
post-publication checks before advertising the release as installable. Build reports retain startup timings and
binary/archive sizes; the release report records JReleaser output properties.

## Reachability metadata

The checked-in metadata was collected from the shared JVM fixture suite with the Java 25 tracing agent and reviewed
for concrete Picocli command classes, Jackson record construction, Maven Resolver service constructors, XML parsing,
HTTP and authentication paths. Platform-specific Apple security provider entries were removed. Fixture credentials,
absolute workstation paths and captured application data are not metadata. Native conformance, rather than successful
compilation alone, is the gate for metadata changes.
