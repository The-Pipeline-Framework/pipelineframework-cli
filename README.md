# The Pipeline Framework CLI

`tpf` verifies and deploys an existing `pipeline-release.json`. Install it once, then run it directly from your
application directory. Native downloads need neither Java nor Docker.

## Install

Supported hosts are macOS Apple Silicon and Ubuntu 24.04 Linux x64/ARM64 or a compatible glibc system.
Check [available downloads](https://github.com/The-Pipeline-Framework/pipelineframework-cli/releases) before installing.

For a published stable release, use Homebrew:

```sh
brew install The-Pipeline-Framework/tap/tpf
tpf --version
```

For a development snapshot, download your platform's checksummed ZIP from the
[`latest` prerelease](https://github.com/The-Pipeline-Framework/pipelineframework-cli/releases/tag/latest).
Snapshots retain a version such as `26.10.1-SNAPSHOT`. The nightly channel is separate from stable releases and does
not update the stable Homebrew formula.

Follow [Install the TPF CLI](https://pipelineframework.org/deploy/cli-installation) for archive installation,
upgrades, platform limits and the secondary Docker/Podman option. macOS archives are initially unsigned.

## First use

```sh
cd /path/to/your/application
tpf --help
tpf release verify --help
tpf release verify --release target/pipeline-release.json
```

Without `--release`, the CLI discovers `pipeline-release.json` or `target/pipeline-release.json` in the current
directory. If both exist, select one explicitly. Native installation reads your host files directly; no mounts are needed.

Maven produces the descriptor during `verify`; optional standard `mvn deploy` publishes its Maven artefacts.
Preserve the descriptor for later CLI verification and deployment. See
[Maven Release production](https://pipelineframework.org/deploy/release-descriptors) for plugin configuration.

## Deploy

Configure a named environment in `tpf-deploy.yaml`, then run `tpf deploy <environment>` with the preserved descriptor.
Cloud commands require the existing private Cloud and identity APIs, an Application and Environment, and authorised
credentials. Human users sign in explicitly; CI uses service credentials without interactive login.

- [Configure verification and deployment](https://pipelineframework.org/deploy/deployment-cli)
- [Human Cloud authentication](https://pipelineframework.org/deploy/deployment-cli#human-cloud-deployment)
- [CI Cloud authentication](https://pipelineframework.org/deploy/deployment-cli#ci-cloud-deployment)

For source builds, validation and publication procedures, see [Contributing](CONTRIBUTING.md).
