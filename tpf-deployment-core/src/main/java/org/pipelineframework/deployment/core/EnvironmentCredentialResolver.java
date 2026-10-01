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
        String prefix = "TPF_CREDENTIAL_" + reference.name().toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]", "_");
        String token = environment.get(prefix);
        if (token != null && !token.isBlank()) return Optional.of(new Credential.Bearer(token));
        String username = environment.get(prefix + "_USERNAME");
        String password = environment.get(prefix + "_PASSWORD");
        if (username != null && password != null) return Optional.of(new Credential.Basic(username, password));
        return Optional.empty();
    }
}
