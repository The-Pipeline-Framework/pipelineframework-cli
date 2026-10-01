package org.pipelineframework.deployment.target.cloud;

import java.net.URI;

public record CloudTargetConfiguration(
    URI endpoint,
    String organization,
    String application,
    String environment,
    String mode,
    String credential
) {
    public CloudTargetConfiguration {
        if (endpoint == null || organization == null || organization.isBlank() || application == null
            || application.isBlank() || environment == null || environment.isBlank()
            || credential == null || credential.isBlank()) {
            throw new IllegalArgumentException("TPF Cloud endpoint, identity and credential are required");
        }
        mode = mode == null || mode.isBlank() ? "CUSTOMER_MANAGED" : mode;
        if (!mode.equals("CUSTOMER_MANAGED")) {
            throw new IllegalArgumentException("The initial TPF Cloud target supports CUSTOMER_MANAGED only");
        }
    }
}
