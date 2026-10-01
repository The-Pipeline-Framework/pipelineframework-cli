package org.pipelineframework.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.pipelineframework.deployment.api.DeploymentException;
import org.pipelineframework.deployment.core.DeploymentConfiguration;
import org.pipelineframework.deployment.core.DeploymentConfigurationLoader;
import org.pipelineframework.deployment.release.ReleaseVerificationException;
import org.pipelineframework.deployment.release.ResolverProfile;

final class CliSupport {
    static final int INVALID_INPUT = 2;
    static final int INVALID_RELEASE = 3;
    static final int RESOLUTION_FAILURE = 4;
    static final int AUTH_FAILURE = 5;
    static final int DEPLOYMENT_FAILURE = 6;
    static final int RUNTIME_FAILURE = 7;
    static final ObjectMapper JSON = new ObjectMapper().findAndRegisterModules();

    private CliSupport() {}

    static DeploymentConfiguration configuration(Path path) throws Exception {
        if (!Files.isRegularFile(path)) throw new IllegalArgumentException("Deployment configuration does not exist: " + path);
        return new DeploymentConfigurationLoader().load(path);
    }

    static ResolverProfile verificationProfile(Path config, String profile) throws Exception {
        if (!Files.isRegularFile(config)) return ResolverProfile.localOnly();
        DeploymentConfiguration loaded = configuration(config);
        if (profile != null && !profile.isBlank()) {
            ResolverProfile selected = loaded.resolverProfiles().get(profile);
            if (selected == null) throw new IllegalArgumentException("Unknown resolver profile " + profile);
            return selected;
        }
        ResolverProfile selected = loaded.resolverProfiles().get("default");
        if (selected != null) return selected;
        if (loaded.resolverProfiles().size() == 1) return loaded.resolverProfiles().values().iterator().next();
        throw new IllegalArgumentException("Use --resolver-profile when configuration has no default profile");
    }

    static void requireOutput(String output) {
        if (!output.equals("human") && !output.equals("json")) {
            throw new IllegalArgumentException("--output must be human or json");
        }
    }

    static int failure(Throwable failure, PrintWriter error) {
        Throwable cause = root(failure);
        error.println("tpf: " + cause.getMessage());
        if (cause instanceof ReleaseVerificationException release) {
            return release.category() == ReleaseVerificationException.Category.RESOLUTION
                ? RESOLUTION_FAILURE : INVALID_RELEASE;
        }
        if (cause instanceof DeploymentException deployment) {
            return switch (deployment.failureClass()) {
                case AUTHENTICATION -> AUTH_FAILURE;
                case TARGET -> DEPLOYMENT_FAILURE;
                case RUNTIME -> RUNTIME_FAILURE;
            };
        }
        return INVALID_INPUT;
    }

    static void json(PrintWriter output, Map<String, ?> value) throws Exception {
        output.println(JSON.writeValueAsString(value));
    }

    private static Throwable root(Throwable failure) {
        Throwable result = failure;
        while (result.getCause() != null && !(result instanceof ReleaseVerificationException)
            && !(result instanceof DeploymentException)) {
            result = result.getCause();
        }
        return result;
    }
}
