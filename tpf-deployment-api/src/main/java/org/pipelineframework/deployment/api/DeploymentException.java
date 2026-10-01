package org.pipelineframework.deployment.api;

public final class DeploymentException extends Exception {
    private final FailureClass failureClass;

    public enum FailureClass {
        AUTHENTICATION,
        TARGET,
        RUNTIME
    }

    public DeploymentException(FailureClass failureClass, String message) {
        super(message);
        this.failureClass = failureClass;
    }

    public DeploymentException(FailureClass failureClass, String message, Throwable cause) {
        super(message, cause);
        this.failureClass = failureClass;
    }

    public FailureClass failureClass() {
        return failureClass;
    }
}
