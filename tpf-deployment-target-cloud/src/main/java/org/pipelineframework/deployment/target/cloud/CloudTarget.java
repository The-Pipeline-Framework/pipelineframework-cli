package org.pipelineframework.deployment.target.cloud;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
    private final CloudTargetConfiguration configuration;
    private final DeploymentServices services;
    private String deploymentId = "";

    CloudTarget(CloudTargetConfiguration configuration, DeploymentServices services) {
        this.configuration = configuration;
        this.services = services;
    }

    @Override
    public DeploymentPlan plan(VerifiedRelease release, DeploymentRequest request) {
        return new DeploymentPlan(release, request, "tpf-cloud");
    }

    @Override
    public ReleaseRegistration register(DeploymentPlan plan) throws DeploymentException {
        Credential credential = services.credentials().resolve(new CredentialReference(configuration.credential()))
            .orElseThrow(() -> new DeploymentException(
                DeploymentException.FailureClass.AUTHENTICATION,
                "Credential " + configuration.credential() + " is unavailable"));
        if (!(credential instanceof Credential.Bearer bearer)) {
            throw new DeploymentException(
                DeploymentException.FailureClass.AUTHENTICATION, "TPF Cloud requires a bearer credential");
        }
        String path = DEPLOYMENT_PATH.formatted(
            encode(configuration.organization()), encode(configuration.application()), encode(configuration.environment()));
        HttpRequest request = HttpRequest.newBuilder(deploymentUri(path))
            .timeout(Duration.ofSeconds(60))
            .header("Authorization", "Bearer " + bearer.token())
            .header("Content-Type", RELEASE_MEDIA_TYPE)
            .header("Idempotency-Key", plan.request().idempotencyKey())
            .header("X-TPF-Deployment-Mode", configuration.mode())
            .POST(HttpRequest.BodyPublishers.ofByteArray(plan.release().loaded().originalBytes()))
            .build();
        try {
            HttpResponse<byte[]> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() == 401 || response.statusCode() == 403) {
                throw new DeploymentException(
                    DeploymentException.FailureClass.AUTHENTICATION, "TPF Cloud rejected the credential");
            }
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new DeploymentException(
                    DeploymentException.FailureClass.TARGET, "TPF Cloud returned HTTP " + response.statusCode());
            }
            JsonNode body = JSON.readTree(response.body());
            String deploymentId = requiredText(body, "deploymentId");
            String status = requiredText(body, "status");
            if (!status.equals("REGISTERED")) {
                throw new DeploymentException(
                    DeploymentException.FailureClass.TARGET, "Unexpected TPF Cloud deployment status " + status);
            }
            this.deploymentId = deploymentId;
            return new ReleaseRegistration(deploymentId, StageState.COMPLETED);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DeploymentException(DeploymentException.FailureClass.TARGET, "Cloud request interrupted", e);
        } catch (IOException e) {
            throw new DeploymentException(DeploymentException.FailureClass.TARGET, "Cloud request failed", e);
        }
    }

    @Override
    public PhysicalDeployment materialize(DeploymentPlan plan, ReleaseRegistration registration) {
        return new PhysicalDeployment(registration.identifier(), StageState.NOT_REQUESTED);
    }

    @Override
    public RuntimeVerification verifyRuntime(DeploymentPlan plan, PhysicalDeployment deployment) {
        return new RuntimeVerification(StageState.NOT_REQUESTED, "Customer-managed physical deployment");
    }

    @Override
    public DeploymentResult activate(DeploymentPlan plan, RuntimeVerification verification) {
        return new DeploymentResult(
            deploymentId, DeploymentStatus.REGISTERED,
            StageState.COMPLETED, StageState.NOT_REQUESTED, StageState.NOT_REQUESTED, StageState.NOT_REQUESTED,
            "Release and customer-managed Deployment registered with TPF Cloud");
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private URI deploymentUri(String path) {
        URI endpoint = configuration.endpoint();
        String basePath = endpoint.getRawPath();
        if (basePath == null || basePath.isBlank() || basePath.equals("/")) {
            basePath = "";
        } else if (basePath.endsWith("/")) {
            basePath = basePath.substring(0, basePath.length() - 1);
        }
        String query = endpoint.getRawQuery() == null ? "" : "?" + endpoint.getRawQuery();
        return URI.create(endpoint.getScheme() + "://" + endpoint.getRawAuthority() + basePath + path + query);
    }

    private static String requiredText(JsonNode node, String field) throws DeploymentException {
        String value = node.path(field).asText();
        if (value.isBlank()) {
            throw new DeploymentException(DeploymentException.FailureClass.TARGET, "Cloud response is missing " + field);
        }
        return value;
    }
}
