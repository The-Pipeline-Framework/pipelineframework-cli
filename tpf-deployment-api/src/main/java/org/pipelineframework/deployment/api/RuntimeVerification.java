package org.pipelineframework.deployment.api;

public record RuntimeVerification(StageState state, String details) {
    public RuntimeVerification {
        if (state == null) throw new IllegalArgumentException("Runtime verification state is required");
        details = details == null ? "" : details;
    }
}
