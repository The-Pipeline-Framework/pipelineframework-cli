package org.pipelineframework.deployment.target.cloud;

import org.pipelineframework.deployment.api.DeploymentServices;
import org.pipelineframework.deployment.api.DeploymentTarget;
import org.pipelineframework.deployment.api.DeploymentTargetProvider;

public final class CloudTargetProvider implements DeploymentTargetProvider<CloudTargetConfiguration> {
    @Override public String type() { return "tpf-cloud"; }
    @Override public Class<CloudTargetConfiguration> configurationType() { return CloudTargetConfiguration.class; }
    @Override public DeploymentTarget create(CloudTargetConfiguration configuration, DeploymentServices services) {
        return new CloudTarget(configuration, services);
    }
}
