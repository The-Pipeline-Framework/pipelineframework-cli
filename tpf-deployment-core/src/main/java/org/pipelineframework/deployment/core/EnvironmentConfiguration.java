package org.pipelineframework.deployment.core;

import com.fasterxml.jackson.databind.JsonNode;

public record EnvironmentConfiguration(String resolverProfile, String targetType, JsonNode targetConfiguration) {
    public EnvironmentConfiguration {
        if (resolverProfile == null || resolverProfile.isBlank() || targetType == null || targetType.isBlank()
            || targetConfiguration == null) {
            throw new IllegalArgumentException("Environment resolverProfile and target are required");
        }
    }
}
