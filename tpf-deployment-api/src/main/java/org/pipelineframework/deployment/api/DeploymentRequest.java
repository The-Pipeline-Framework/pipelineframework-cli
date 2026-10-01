package org.pipelineframework.deployment.api;

public record DeploymentRequest(String environment, String idempotencyKey) {
    public DeploymentRequest {
        if (environment == null || environment.isBlank() || idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("Deployment environment and idempotency key are required");
        }
    }
}
