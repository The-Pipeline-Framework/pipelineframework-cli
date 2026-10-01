package org.pipelineframework.deployment.api;

public interface DeploymentTargetProvider<C> {
    String type();
    Class<C> configurationType();
    DeploymentTarget create(C configuration, DeploymentServices services);
}
