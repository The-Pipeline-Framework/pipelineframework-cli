package org.pipelineframework.deployment.core;

import java.util.Map;
import org.pipelineframework.deployment.release.ResolverProfile;

public record DeploymentConfiguration(
    Map<String, ResolverProfile> resolverProfiles,
    Map<String, EnvironmentConfiguration> environments
) {
    public DeploymentConfiguration {
        resolverProfiles = Map.copyOf(resolverProfiles);
        environments = Map.copyOf(environments);
    }
}
