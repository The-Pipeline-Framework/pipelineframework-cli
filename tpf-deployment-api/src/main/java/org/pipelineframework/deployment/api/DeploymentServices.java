package org.pipelineframework.deployment.api;

import org.pipelineframework.deployment.release.CredentialResolver;

public record DeploymentServices(CredentialResolver credentials) {
    public DeploymentServices {
        if (credentials == null) {
            throw new IllegalArgumentException("Credential resolver is required");
        }
    }
}
