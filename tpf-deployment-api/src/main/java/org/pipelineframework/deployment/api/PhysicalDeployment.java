package org.pipelineframework.deployment.api;

public record PhysicalDeployment(String identifier, StageState state) {
    public PhysicalDeployment {
        identifier = identifier == null ? "" : identifier;
        if (state == null) throw new IllegalArgumentException("Physical deployment state is required");
    }
}
