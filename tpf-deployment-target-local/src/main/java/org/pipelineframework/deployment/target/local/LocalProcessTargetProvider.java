package org.pipelineframework.deployment.target.local;

import org.pipelineframework.deployment.api.DeploymentServices;
import org.pipelineframework.deployment.api.DeploymentTarget;
import org.pipelineframework.deployment.api.DeploymentTargetProvider;

public final class LocalProcessTargetProvider implements DeploymentTargetProvider<LocalProcessTargetConfiguration> {
    @Override public String type() { return "local-process"; }
    @Override public Class<LocalProcessTargetConfiguration> configurationType() { return LocalProcessTargetConfiguration.class; }
    @Override public DeploymentTarget create(LocalProcessTargetConfiguration configuration, DeploymentServices services) {
        return new LocalProcessTarget(configuration);
    }
}
