package org.pipelineframework.deployment.api;

public record ReleaseRegistration(String identifier, StageState state) {
    public ReleaseRegistration {
        identifier = identifier == null ? "" : identifier;
        if (state == null) throw new IllegalArgumentException("Registration state is required");
    }
}
