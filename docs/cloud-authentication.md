# Cloud authentication

Authentication never changes a Release. `deploy` remains non-interactive: missing,
expired or rejected credentials produce an actionable authentication error (exit 5),
not a browser prompt. Application and environment must already exist in Cloud.

## Human authors

Obtain the public OAuth CLI client ID and HTTPS issuer from your Cloud operator.
This client has no embedded secret. Sign in explicitly:

```sh
tpf auth login --issuer https://auth.example.com --client-id client_public
tpf auth status
tpf auth logout
```

Login prints a verification URL and user code. Open that URL, approve access to
your organisation, and enter the code. Status checks local credentials and refreshes
them when needed; it is not a remote membership/permission check. Logout deletes
local credentials; it does not revoke access tokens already issued by the provider.

Set `TPF_CREDENTIAL_DIRECTORY` to a dedicated mounted user credential directory
(or pass `--credential-dir` to auth commands). The host default is
`$HOME/.tpf/credentials`. Container users must mount this directory persistently
and pass its **container** path to `TPF_CREDENTIAL_DIRECTORY` on every invocation.
The initial implementation requires a POSIX filesystem: directory 0700, files 0600,
no symlinked path components. Refresh rotation is serialized and saved atomically.
Invalid refresh grants remove unusable credentials; temporary outages preserve them.
Never commit, cache, upload or mount this directory as an application workspace.

`--profile cloud` is the default. Reference that profile, not its token, in
`tpf-deploy.yaml`:

```yaml
resolverProfiles:
  default:
    file: true
environments:
  development:
    resolverProfile: default
    target:
      type: tpf-cloud
      endpoint: https://cloud.example.com
      organization: org_canonical_cloud_id
      application: payments
      environment: development
      mode: CUSTOMER_MANAGED
      credential: oauth-session:cloud
```

```sh
tpf deploy development --release target/pipeline-release.json
```

Do not put access tokens, refresh tokens, API keys or client secrets in this file
or in a Release. Exact verified descriptor bytes are transmitted unchanged.

## CI service credentials

Use a separate organisation-scoped OAuth service application with `tpf:deploy`
access. Inject these through your CI secret store or workload-secret integration:

```text
TPF_OAUTH_CI_ISSUER       HTTPS issuer origin
TPF_OAUTH_CI_CLIENT_ID    organisation-scoped service client ID
TPF_OAUTH_CI_CLIENT_SECRET  injected secret; never a YAML value
```

Use `credential: oauth-client:ci` in the target configuration. A different source
name maps to `TPF_OAUTH_<UPPERCASE_NAME>_*` (hyphens become underscores).
Each credential resolution acquires a short-lived access token using
`client_credentials`; no human credential file is read or created. There is no
interactive fallback. CI must not depend on a developer's refresh credential mount.

`EnvironmentCredentialResolver` is the shared build-tool-neutral implementation.
A Maven adapter must use it with the same YAML/source references; this checkout
does not contain a deploy Maven plugin, so its adapter wiring is not verified here.

## Operator boundary

The private Cloud identity service verifies bearer credentials and resolves a
canonical Cloud tenant and a human or service principal. The public CLI contains
only OAuth protocol and Cloud API clients, not WorkOS management keys or Cloud
membership policy. A 403 can mean missing deploy permission or tenant authority;
logging in again cannot grant access you do not have. WorkOS client provisioning
and a real human/CI acceptance run remain separate from deterministic fixture tests.
