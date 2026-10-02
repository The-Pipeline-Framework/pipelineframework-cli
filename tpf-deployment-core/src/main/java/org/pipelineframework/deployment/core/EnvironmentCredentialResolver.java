package org.pipelineframework.deployment.core;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.pipelineframework.deployment.release.Credential;
import org.pipelineframework.deployment.release.CredentialReference;
import org.pipelineframework.deployment.release.CredentialResolver;

public final class EnvironmentCredentialResolver implements CredentialResolver {
    private final Map<String, String> environment;

    public EnvironmentCredentialResolver() {
        this(System.getenv());
    }

    EnvironmentCredentialResolver(Map<String, String> environment) {
        this.environment = Map.copyOf(environment);
    }

    @Override
    public Optional<Credential> resolve(CredentialReference reference) {
        if (reference.name().startsWith("oauth-session:")) {
            return new UserCredentialDirectory(UserCredentialDirectory.configured(environment))
                    .resolve(reference.name().substring("oauth-session:".length()));
        }
        if (reference.name().startsWith("oauth-client:")) {
            String profile = reference.name().substring("oauth-client:".length());
            if (!profile.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,63}")) throw new IllegalArgumentException("Invalid OAuth credential source");
            String prefix = "TPF_OAUTH_" + profile.toUpperCase(Locale.ROOT).replace('-', '_');
            String issuer = environment.get(prefix + "_ISSUER");
            String client = environment.get(prefix + "_CLIENT_ID");
            String secret = environment.get(prefix + "_CLIENT_SECRET");
            if (issuer == null || client == null || secret == null || issuer.isBlank() || client.isBlank() || secret.isBlank())
                throw new IllegalStateException("CI OAuth credential source requires injected issuer, client ID and client secret");
            var tokens = new OAuthClient(java.net.URI.create(issuer)).request("token", Map.of(
                    "grant_type", "client_credentials", "client_id", client, "client_secret", secret, "scope", "tpf:deploy"));
            if (!"Bearer".equalsIgnoreCase(tokens.path("token_type").asText()) || tokens.path("expires_in").asLong(0) <= 0)
                throw new OAuthClient.OAuthFailure("invalid_response");
            return Optional.of(new Credential.Bearer(OAuthClient.text(tokens, "access_token")));
        }
        String prefix = "TPF_CREDENTIAL_" + reference.name().toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]", "_");
        String token = environment.get(prefix);
        if (token != null && !token.isBlank()) return Optional.of(new Credential.Bearer(token));
        String username = environment.get(prefix + "_USERNAME");
        String password = environment.get(prefix + "_PASSWORD");
        if (username != null && password != null) return Optional.of(new Credential.Basic(username, password));
        return Optional.empty();
    }
}
