package org.pipelineframework.deployment.api;

public record DeploymentResult(
    String deploymentId,
    DeploymentStatus status,
    StageState registration,
    StageState physicalDeployment,
    StageState runtimeVerification,
    StageState activation,
    String message
) {
    public DeploymentResult {
        deploymentId = deploymentId == null ? "" : deploymentId;
        message = message == null ? "" : message;
        if (status == null || registration == null || physicalDeployment == null
            || runtimeVerification == null || activation == null) {
            throw new IllegalArgumentException("Deployment result states are required");
        }
    }
}
