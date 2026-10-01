package org.pipelineframework.deployment.api;

import org.pipelineframework.deployment.release.VerifiedRelease;

public record DeploymentPlan(VerifiedRelease release, DeploymentRequest request, String targetType) {
    public DeploymentPlan {
        if (release == null || request == null || targetType == null || targetType.isBlank()) {
            throw new IllegalArgumentException("Deployment plan fields are required");
        }
    }
}
