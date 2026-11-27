package org.pipelineframework.deployment.target.cloud;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import org.pipelineframework.deployment.api.DeploymentException;
import org.pipelineframework.deployment.api.DeploymentPlan;
import org.pipelineframework.deployment.api.DeploymentRequest;
import org.pipelineframework.deployment.api.DeploymentResult;
import org.pipelineframework.deployment.api.DeploymentServices;
import org.pipelineframework.deployment.api.DeploymentStatus;
import org.pipelineframework.deployment.api.DeploymentTarget;
import org.pipelineframework.deployment.api.PhysicalDeployment;
import org.pipelineframework.deployment.api.ReleaseRegistration;
import org.pipelineframework.deployment.api.RuntimeVerification;
import org.pipelineframework.deployment.api.StageState;
import org.pipelineframework.deployment.release.Credential;
import org.pipelineframework.deployment.release.CredentialReference;
import org.pipelineframework.deployment.release.VerifiedRelease;

final class CloudTarget implements DeploymentTarget {
    private static final ObjectMapper JSON = new ObjectMapper();
    static final String RELEASE_MEDIA_TYPE = "application/vnd.tpf.pipeline-release+json";
    static final String DEPLOYMENT_PATH = "/api/v1/organizations/%s/applications/%s/environments/%s/deployments";
    static final String RESUME_PATH = DEPLOYMENT_PATH + "/%s/resume";
    static final String STATUS_REGISTERED = "REGISTERED";
    static final String STATUS_ACTIVATED = "ACTIVATED";
    static final String STATUS_WAITING_FOR_WORKER = "WAITING_FOR_WORKER";
    static final String STATUS_WAITING_FOR_ROUTING = "WAITING_FOR_ROUTING";
    static final String STATUS_PENDING_ATTACHMENT = "PENDING_ATTACHMENT";
    private static final Set<String> HOSTED_POLLING_STATUSES = Set.of(
        STATUS_WAITING_FOR_WORKER, STATUS_WAITING_FOR_ROUTING, STATUS_PENDING_ATTACHMENT);
    private static final Set<String> HOSTED_INITIAL_STATUSES = Set.of(
        STATUS_ACTIVATED, STATUS_WAITING_FOR_WORKER, STATUS_WAITING_FOR_ROUTING,
        STATUS_PENDING_ATTACHMENT, STATUS_REGISTERED);

    @FunctionalInterface
    interface Sleeper {
        void sleep(Duration delay) throws InterruptedException;
    }

    private final CloudTargetConfiguration configuration;
    private final DeploymentServices services;
    private final HttpClient httpClient;
    private final Clock clock;
    private final Sleeper sleeper;
    private String deploymentId = "";
    private String remoteStatus = "";
    private String activationId = "";

    CloudTarget(CloudTargetConfiguration configuration, DeploymentServices services) {
        this(configuration, services, HttpClient.newHttpClient(), Clock.systemUTC(),
            delay -> Thread.sleep(Math.max(1L, delay.toMillis())));
    }

    CloudTarget(
        CloudTargetConfiguration configuration,
        DeploymentServices services,
        HttpClient httpClient,
        Clock clock,
        Sleeper sleeper
    ) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.services = Objects.requireNonNull(services, "services");
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    @Override
    public DeploymentPlan plan(VerifiedRelease release, DeploymentRequest request) {
        return new DeploymentPlan(release, request, "tpf-cloud");
    }

    @Override
    public ReleaseRegistration register(DeploymentPlan plan) throws DeploymentException {
        Credential.Bearer bearer = resolveBearer(services.credentials(), configuration.credential());
        String path = DEPLOYMENT_PATH.formatted(
            CloudTargetConfiguration.encodePathSegment(configuration.organization()),
            CloudTargetConfiguration.encodePathSegment(configuration.application()),
            CloudTargetConfiguration.encodePathSegment(configuration.environment()));
        HttpRequest request = HttpRequest.newBuilder(configuration.endpointUri(path))
            .timeout(Duration.ofSeconds(60))
            .header("Authorization", "Bearer " + bearer.token())
            .header("Content-Type", RELEASE_MEDIA_TYPE)
            .header("Idempotency-Key", plan.request().idempotencyKey())
            .header("X-TPF-Deployment-Mode", configuration.mode())
            .POST(HttpRequest.BodyPublishers.ofByteArray(plan.release().loaded().originalBytes()))
            .build();
        JsonNode body = sendJsonRequest(request, "deployment registration");
        String receivedDeploymentId = requiredText(body, "deploymentId");
        String status = requiredText(body, "status");
        if (configuration.isHosted()) {
            rejectWorkerFailureIfPresent(body, receivedDeploymentId, status);
            if (!HOSTED_INITIAL_STATUSES.contains(status)) {
                throw new DeploymentException(
                    DeploymentException.FailureClass.TARGET, "Unexpected TPF Cloud deployment status " + status);
            }
        } else if (!STATUS_REGISTERED.equals(status)) {
            throw new DeploymentException(
                DeploymentException.FailureClass.TARGET, "Unexpected TPF Cloud deployment status " + status);
        }
        this.deploymentId = receivedDeploymentId;
        this.remoteStatus = status;
        this.activationId = optionalText(body, "activationId");
        return new ReleaseRegistration(receivedDeploymentId, StageState.COMPLETED);
    }

    @Override
    public PhysicalDeployment materialize(DeploymentPlan plan, ReleaseRegistration registration) throws DeploymentException {
        if (!configuration.isHosted() || STATUS_REGISTERED.equals(remoteStatus)) {
            return new PhysicalDeployment(registration.identifier(), StageState.NOT_REQUESTED);
        }
        if (STATUS_ACTIVATED.equals(remoteStatus)) {
            return new PhysicalDeployment(registration.identifier(), StageState.COMPLETED);
        }
        Instant deadline = clock.instant().plusSeconds(configuration.timeoutSeconds());
        Duration pollInterval = Duration.ofSeconds(configuration.pollIntervalSeconds());
        while (!STATUS_ACTIVATED.equals(remoteStatus)) {
            Instant now = clock.instant();
            if (!now.isBefore(deadline)) {
                throw new DeploymentException(
                    DeploymentException.FailureClass.TARGET,
                    "Timed out after " + configuration.timeoutSeconds()
                        + "s waiting for TPF Cloud hosted deployment " + deploymentId
                        + " to reach ACTIVATED (last status: " + remoteStatus + ")");
            }
            Duration remaining = Duration.between(now, deadline);
            Duration wait = remaining.compareTo(pollInterval) < 0 ? remaining : pollInterval;
            try {
                sleeper.sleep(wait);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DeploymentException(
                    DeploymentException.FailureClass.TARGET,
                    "Cloud deployment polling interrupted for " + deploymentId, e);
            }
            resumeDeployment();
        }
        return new PhysicalDeployment(registration.identifier(), StageState.COMPLETED);
    }

    @Override
    public RuntimeVerification verifyRuntime(DeploymentPlan plan, PhysicalDeployment deployment) {
        if (!configuration.isHosted()) {
            return new RuntimeVerification(StageState.NOT_REQUESTED, "Customer-managed physical deployment");
        }
        if (STATUS_ACTIVATED.equals(remoteStatus)) {
            return new RuntimeVerification(StageState.COMPLETED, "TPF Cloud hosted deployment verified and routed");
        }
        return new RuntimeVerification(StageState.NOT_REQUESTED, "Hosted release registered with TPF Cloud");
    }

    @Override
    public DeploymentResult activate(DeploymentPlan plan, RuntimeVerification verification) {
        if (configuration.isHosted() && STATUS_ACTIVATED.equals(remoteStatus)) {
            String message = activationId.isBlank()
                ? "Release deployed and activated on TPF Cloud"
                : "Release deployed and activated on TPF Cloud (activation " + activationId + ")";
            return new DeploymentResult(
                deploymentId, DeploymentStatus.ACTIVATED,
                StageState.COMPLETED, StageState.COMPLETED, StageState.COMPLETED, StageState.COMPLETED,
                message);
        }
        if (configuration.isHosted()) {
            return new DeploymentResult(
                deploymentId, DeploymentStatus.REGISTERED,
                StageState.COMPLETED, StageState.NOT_REQUESTED, StageState.NOT_REQUESTED, StageState.NOT_REQUESTED,
                "Release and hosted Deployment registered with TPF Cloud");
        }
        return new DeploymentResult(
            deploymentId, DeploymentStatus.REGISTERED,
            StageState.COMPLETED, StageState.NOT_REQUESTED, StageState.NOT_REQUESTED, StageState.NOT_REQUESTED,
            "Release and customer-managed Deployment registered with TPF Cloud");
    }

    private void resumeDeployment() throws DeploymentException {
        Credential.Bearer bearer = resolveBearer(services.credentials(), configuration.credential());
        String path = RESUME_PATH.formatted(
            CloudTargetConfiguration.encodePathSegment(configuration.organization()),
            CloudTargetConfiguration.encodePathSegment(configuration.application()),
            CloudTargetConfiguration.encodePathSegment(configuration.environment()),
            CloudTargetConfiguration.encodePathSegment(deploymentId));
        HttpRequest request = HttpRequest.newBuilder(configuration.endpointUri(path))
            .timeout(Duration.ofSeconds(60))
            .header("Authorization", "Bearer " + bearer.token())
            .header("Accept", "application/json")
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();
        JsonNode body = sendJsonRequest(request, "deployment resume");
        String returnedId = optionalText(body, "deploymentId");
        if (!returnedId.isBlank() && !returnedId.equals(deploymentId)) {
            throw new DeploymentException(
                DeploymentException.FailureClass.TARGET,
                "TPF Cloud resume response returned mismatched deploymentId " + returnedId);
        }
        String status = requiredText(body, "status");
        rejectWorkerFailureIfPresent(body, deploymentId, status);
        if (!STATUS_ACTIVATED.equals(status) && !HOSTED_POLLING_STATUSES.contains(status)) {
            throw new DeploymentException(
                DeploymentException.FailureClass.TARGET, "Unexpected TPF Cloud deployment status " + status);
        }
        this.remoteStatus = status;
        String returnedActivation = optionalText(body, "activationId");
        if (!returnedActivation.isBlank()) {
            this.activationId = returnedActivation;
        }
    }

    private JsonNode sendJsonRequest(HttpRequest request, String operation) throws DeploymentException {
        try {
            HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() == 401 || response.statusCode() == 403) {
                throw new DeploymentException(
                    DeploymentException.FailureClass.AUTHENTICATION,
                    "TPF Cloud rejected deployment access; run tpf auth login or check the CI credential, organisation and deploy permission");
            }
            if (response.statusCode() == 409) {
                throw new DeploymentException(
                    DeploymentException.FailureClass.TARGET,
                    "TPF Cloud rejected " + operation + " with HTTP 409 (immutable Release or deployment conflict)");
            }
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new DeploymentException(
                    DeploymentException.FailureClass.TARGET, "TPF Cloud returned HTTP " + response.statusCode());
            }
            return JSON.readTree(response.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DeploymentException(DeploymentException.FailureClass.TARGET, "Cloud request interrupted", e);
        } catch (IOException e) {
            throw new DeploymentException(DeploymentException.FailureClass.TARGET, "Cloud request failed", e);
        }
    }

    static Credential.Bearer resolveBearer(
        org.pipelineframework.deployment.release.CredentialResolver resolver,
        String credentialName
    ) throws DeploymentException {
        Credential credential;
        try {
            credential = resolver.resolve(new CredentialReference(credentialName))
                .orElseThrow(() -> new DeploymentException(
                    DeploymentException.FailureClass.AUTHENTICATION,
                    "Cloud credentials are unavailable; run tpf auth login for a human session or configure the CI OAuth or env: credential source"));
        } catch (DeploymentException e) {
            throw e;
        } catch (RuntimeException failure) {
            throw new DeploymentException(
                DeploymentException.FailureClass.AUTHENTICATION,
                "Cloud credential acquisition failed; retry, run tpf auth login, or check the injected CI credential source");
        }
        if (!(credential instanceof Credential.Bearer bearer)) {
            throw new DeploymentException(
                DeploymentException.FailureClass.AUTHENTICATION, "TPF Cloud requires a bearer credential");
        }
        return bearer;
    }

    private static void rejectWorkerFailureIfPresent(JsonNode body, String deploymentId, String status)
        throws DeploymentException {
        JsonNode failure = failureNode(body);
        if (failure == null) {
            return;
        }
        String errorCode = optionalText(failure, "errorCode");
        String stackStatus = optionalText(failure, "stackStatus");
        String reason = optionalText(failure, "reason");
        String code = !errorCode.isBlank() ? errorCode : stackStatus;
        StringBuilder message = new StringBuilder("TPF Cloud hosted deployment ")
            .append(deploymentId)
            .append(" failed during ")
            .append(status);
        if (!code.isBlank()) {
            message.append(" [").append(code).append("]");
        }
        if (!reason.isBlank()) {
            message.append(": ").append(reason);
        } else {
            message.append(": worker provisioning failed");
        }
        throw new DeploymentException(DeploymentException.FailureClass.TARGET, message.toString());
    }

    private static JsonNode failureNode(JsonNode body) {
        JsonNode failure = body.get("failure");
        if (failure != null && failure.isObject()) {
            return failure;
        }
        JsonNode workerFailure = body.get("workerFailure");
        if (workerFailure != null && workerFailure.isObject()) {
            return workerFailure;
        }
        return null;
    }

    static String requiredText(JsonNode node, String field) throws DeploymentException {
        String value = node.path(field).asText();
        if (value.isBlank()) {
            throw new DeploymentException(DeploymentException.FailureClass.TARGET, "Cloud response is missing " + field);
        }
        return value;
    }

    static String optionalText(JsonNode node, String field) {
        JsonNode child = node.get(field);
        if (child == null || child.isNull() || !child.isTextual()) {
            return "";
        }
        return child.asText().trim();
    }
}
