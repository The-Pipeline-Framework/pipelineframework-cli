package org.pipelineframework.deployment.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import org.pipelineframework.deployment.api.DeploymentRequest;
import org.pipelineframework.deployment.api.DeploymentResult;
import org.pipelineframework.deployment.api.DeploymentServices;
import org.pipelineframework.deployment.api.DeploymentTargetProvider;
import org.pipelineframework.deployment.release.CredentialResolver;
import org.pipelineframework.deployment.release.ReleaseVerifier;
import org.pipelineframework.deployment.release.VerifiedRelease;

public final class DeploymentEngine {
    private final ObjectMapper mapper;
    private final Map<String, DeploymentTargetProvider<?>> providers;
    private final CredentialResolver credentials;

    public DeploymentEngine(
        ObjectMapper mapper,
        Map<String, DeploymentTargetProvider<?>> providers,
        CredentialResolver credentials
    ) {
        this.mapper = mapper;
        this.providers = Map.copyOf(providers);
        this.credentials = credentials;
    }

    public VerifiedRelease verify(Path release, Path workspace, org.pipelineframework.deployment.release.ResolverProfile profile)
        throws IOException {
        Files.createDirectories(workspace);
        Path destination = Files.createTempDirectory(workspace, "verify-");
        return new ReleaseVerifier().verify(release, destination, profile, credentials);
    }

    public DeploymentResult deploy(
        String environmentName,
        Path release,
        Path workspace,
        DeploymentConfiguration configuration
    ) throws Exception {
        EnvironmentConfiguration environment = configuration.environments().get(environmentName);
        if (environment == null) throw new IllegalArgumentException("Unknown environment " + environmentName);
        var profile = configuration.resolverProfiles().get(environment.resolverProfile());
        VerifiedRelease verified = verify(release, workspace.resolve("verification"), profile);
        DeploymentTargetProvider<?> provider = providers.get(environment.targetType());
        if (provider == null) throw new IllegalArgumentException("Unknown deployment target type " + environment.targetType());
        var target = create(provider, environment, new DeploymentServices(credentials));
        String key = verified.loaded().descriptorDigest() + ":" + environmentName;
        var plan = target.plan(verified, new DeploymentRequest(
                environmentName, UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8)).toString()));
        var registration = target.register(plan);
        var physical = target.materialize(plan, registration);
        var runtime = target.verifyRuntime(plan, physical);
        return target.activate(plan, runtime);
    }

    private <C> org.pipelineframework.deployment.api.DeploymentTarget create(
        DeploymentTargetProvider<C> provider,
        EnvironmentConfiguration environment,
        DeploymentServices services
    ) {
        C targetConfiguration = mapper.convertValue(environment.targetConfiguration(), provider.configurationType());
        return provider.create(targetConfiguration, services);
    }
}
