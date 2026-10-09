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

The native nightly schedule runs at 20:47 UTC. Maven Central snapshots publish on main pushes or manual runs;
the workflows validate independently, and native snapshots do not require Maven publication credentials.
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

The public `The-Pipeline-Framework/homebrew-tap` repository is the publication destination. Its dedicated App is
`tpf-homebrew-tap-app`. Registration, installation, and credentials are separate prerequisites:

1. Register a private App under the organisation in
   [organisation developer settings](https://github.com/organizations/The-Pipeline-Framework/settings/apps/new).
   Give it **Repository contents: read and write**. Metadata read access is implicit. Disable webhooks and leave
   other optional permissions unset; this App only commits formulae.
2. Install it on **selected repositories**, selecting **only homebrew-tap**. Registering an App does not install it.
   Do not reuse a broadly installed Cloud or coordinator App.
3. Open the App's General settings and generate a private key. Download the PEM file and store it securely;
   do not commit it. The App ID is different from the OAuth Client ID and the installation ID.
4. Configure the variable and secret on **pipelineframework-cli**, which runs publication, rather than the tap.
   Discover the actual App ID from the installed App instead of copying a placeholder:

```sh
TPF_TAP_APP_ID=$(gh api --paginate --slurp orgs/The-Pipeline-Framework/installations \
  --jq '[.[].installations[] | select(.app_slug == "tpf-homebrew-tap-app") | .app_id] | unique | .[]')
case "$TPF_TAP_APP_ID" in
  ''|*[!0-9]*) echo "Expected one installed App with a numeric App ID" >&2; exit 1 ;;
esac
gh variable set HOMEBREW_APP_ID --repo The-Pipeline-Framework/pipelineframework-cli \
  --body "$TPF_TAP_APP_ID"
# Replace this path with the downloaded PEM file; never paste the key into a command argument.
TPF_TAP_APP_KEY_PATH="/path/to/downloaded-private-key.pem"
gh secret set HOMEBREW_APP_PRIVATE_KEY --repo The-Pipeline-Framework/pipelineframework-cli \
  < "$TPF_TAP_APP_KEY_PATH"
```

The key can instead be an environment secret in `native-release`. The workflow needs `vars.HOMEBREW_APP_ID` and
`secrets.HOMEBREW_APP_PRIVATE_KEY`. Secret-name presence proves configuration, not that the PEM belongs to this App;
successful installation-token creation is the credential check. If the App ID is still the literal `<APP_ID>`,
replace it with the discovered numeric ID. GitHub cannot return an existing private key; generate a replacement if
it has been lost.

Ensure the tap's default branch is `main`. Configure the `native-release` GitHub environment to allow protected
version tags. The trusted publication job mints a short-lived token restricted to the tap and revokes it when finished.
Build and conformance jobs receive no App credentials. The CLI repository's `GITHUB_TOKEN` publishes release assets;
the App token commits cross-repository formulae. No PAT is required. The current stable publication job checks these
prerequisites before publishing; snapshot archives use only `GITHUB_TOKEN` and do not require the App.

For key rotation, generate the replacement, update the Actions secret, validate token creation during the trusted
publication flow, and only then revoke the old key. Do not trigger a stable release merely to test credentials.
See [GitHub App registration](https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/registering-a-github-app)
and [private-key management](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/managing-private-keys-for-github-apps).

### Automating Homebrew provisioning

Provisioning automation is the next maintainer slice; the current repository does not claim to provision its App.
Keep this work in organisation infrastructure/bootstrap ownership, not in each CLI build or release:

- Inventory and import the existing tap and App installation instead of creating replacements.
- Use Terraform for the tap's durable repository policy, `HOMEBREW_APP_ID` variable, release-environment policy, and
  the existing installation's repository binding. Enforce selected-repository access limited to the tap.
- `github_app_installation_repository` manages an **existing installation** and cannot authenticate through an
  installation token. It needs a separate authorised provisioning identity; the tap-writing App must not be given
  administration permissions so it can provision itself.
- For repeatable App registration, a GitHub App manifest can define contents-write permission, private visibility,
  and disabled webhooks. GitHub still requires owner approval in the browser and a secure callback/code exchange.
  Registration does not remove the installation step. Do not put the manifest conversion response or PEM in logs.
- Seed and rotate the PEM directly in GitHub Actions or an approved secret manager. Terraform's sensitive plaintext
  secret inputs still enter state. If Terraform manages the secret, use GitHub-public-key-encrypted input and record
  its encryption key ID; pin and check the provider schema before implementation.
- Acceptance must prove idempotence, no takeover of unrelated repositories, rejection of placeholder IDs or incorrect
  scope, secret-safe rotation, and trusted token validation against the tap without committing a formula or publishing
  a release. CLI CI continues to mint ephemeral installation tokens and publish tested assets.

This is partial automation with an explicit bootstrap, not a Terraform resource that creates the complete App and
its keys. See the provider's [installation binding](https://registry.terraform.io/providers/integrations/github/latest/docs/resources/app_installation_repository)
and [Actions secret](https://registry.terraform.io/providers/integrations/github/latest/docs/resources/actions_secret)
contracts, and GitHub's [manifest registration flow](https://docs.github.com/en/apps/sharing-github-apps/registering-a-github-app-from-a-manifest).

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
