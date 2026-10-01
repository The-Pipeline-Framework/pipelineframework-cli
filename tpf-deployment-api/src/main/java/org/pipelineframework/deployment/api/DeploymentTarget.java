package org.pipelineframework.deployment.api;

import org.pipelineframework.deployment.release.VerifiedRelease;

public interface DeploymentTarget {
    DeploymentPlan plan(VerifiedRelease release, DeploymentRequest request) throws DeploymentException;
    ReleaseRegistration register(DeploymentPlan plan) throws DeploymentException;
    PhysicalDeployment materialize(DeploymentPlan plan, ReleaseRegistration registration) throws DeploymentException;
    RuntimeVerification verifyRuntime(DeploymentPlan plan, PhysicalDeployment deployment) throws DeploymentException;
    DeploymentResult activate(DeploymentPlan plan, RuntimeVerification verification) throws DeploymentException;
}
