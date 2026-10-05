# CLI repository instructions

## Development dependency policy

- Development on `main` follows the active TPF snapshot line, currently `26.10.1-SNAPSHOT`; do not pin another TPF component to `26.9.4` or an older snapshot line.
- Maven requires a version: do not use `LATEST`, `RELEASE`, or version ranges to stand in for Git `main`. Applications should select the snapshot product BOM; retained component properties are compatibility-test override points, not separate application version choices.
- `.mvn/maven.config` includes `-U` so cached snapshot metadata is refreshed. Every Maven invocation still needs the isolated local repository required below.
- Maven-producing repositories publish Central snapshots on pushes to `main` and manually for recovery. Nightly full-train testing remains separate; snapshot publication is not scheduled nightly. Publication is asynchronous, not an atomic cross-repository transaction; a green merge alone does not mean publication completed. Check the publisher before retrying dependent builds.
- Coordinated changes use compatibility sets before merge. Those runs continue to use exact immutable candidate/baseline versions, not floating snapshots. No composite source reactor or extra Maven profile is introduced.
- Freeze compatible published coordinates for a stable release. Never alter an already published release, connector contract identity, or pipeline release pin to make development float.

This repository owns build-tool-neutral release resolution and deployment adapters. Runtime owns release
production; compiler and contracts remain separately owned. Do not import sibling repository sources.

Run the canonical owner gate with an isolated cache:

```sh
./mvnw clean verify -Dmaven.repo.local="$PWD/.m2/repository"
python3 scripts/test-publication-config.py
```

PR candidates use GitHub Packages and exact source-SHA versions, not Central snapshots. Coordinated component
PRs use the same head-branch name under the same GitHub owner; the coordinator discovers the compatibility set.
New owners must be registered in the trusted coordinator before discovery can include them. Never merge or
publish snapshots one by one to unblock a coordinated feature.

Before enabling publication, establish `main` as the repository default branch. Configure `SYSTEM_TEST_APP_ID`
as a repository variable and `SYSTEM_TEST_APP_PRIVATE_KEY` as a secret for a dispatch-only App installed on
`pipelineframework`. Grant the coordinator read access to the candidate Maven packages. Owner test jobs receive
no publication or dispatch credentials.

The nightly/manual `publish-snapshot.yml` publishes only `main`. It needs repository secrets `CENTRAL_USERNAME`,
`CENTRAL_PASSWORD`, `GPG_PRIVATE_KEY`, and `GPG_PASSPHRASE`. Do not publish from a feature branch or trigger a release
without explicit authorisation. `central-publishing` is the sole allowed profile and must not alter the reactor.

When adding artifacts, update candidate collection, metadata, trusted validation and upload lists, the product
BOM/publication manifest and central component allowlist together. Follow the
[component onboarding checklist](https://github.com/The-Pipeline-Framework/pipelineframework/blob/main/docs/evolve/cross-repository-system-tests.md#adding-a-component-or-public-artifact).
Seed a new owner's exact baseline only through tested promotion; snapshots are not baseline identities.

Do not commit, push, merge or publish unless explicitly requested. Preserve unrelated changes.
