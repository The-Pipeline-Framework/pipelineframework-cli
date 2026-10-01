package org.pipelineframework.deployment.target.cloud;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.deployment.api.DeploymentRequest;
import org.pipelineframework.deployment.api.DeploymentServices;
import org.pipelineframework.deployment.api.DeploymentStatus;
import org.pipelineframework.deployment.api.StageState;
import org.pipelineframework.deployment.release.Credential;
import org.pipelineframework.deployment.release.LoadedRelease;
import org.pipelineframework.deployment.release.VerifiedRelease;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;
import org.pipelineframework.orchestrator.release.ResolvedPipelineRelease;

class CloudTargetTest {
    @TempDir Path temporaryDirectory;

    @Test
    void clientContractMatchesPublishedOpenApiFixture() throws Exception {
        String contract = Files.readString(Path.of("..", "api", "tpf-cloud-deployment-v1.openapi.yaml"));

        assertEquals(true, contract.contains(CloudTarget.DEPLOYMENT_PATH
            .formatted("{organization}", "{application}", "{environment}")));
        assertEquals(true, contract.contains(CloudTarget.RELEASE_MEDIA_TYPE + ":"));
        assertEquals(true, contract.contains("REGISTERED"));
    }

    @Test
    void transmitsExactDescriptorBytesAndStopsAtRegistered() throws Exception {
        byte[] descriptorBytes = "{\"schemaVersion\":1}\n".getBytes();
        AtomicReference<byte[]> received = new AtomicReference<>();
        AtomicReference<String> idempotency = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/organizations/example/applications/payments/environments/staging/deployments", exchange -> {
            received.set(exchange.getRequestBody().readAllBytes());
            idempotency.set(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
            assertEquals("Bearer secret", exchange.getRequestHeaders().getFirst("Authorization"));
            assertEquals("CUSTOMER_MANAGED", exchange.getRequestHeaders().getFirst("X-TPF-Deployment-Mode"));
            byte[] response = "{\"deploymentId\":\"deployment-1\",\"status\":\"REGISTERED\"}".getBytes();
            exchange.sendResponseHeaders(201, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            var configuration = new CloudTargetConfiguration(
                URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
                "example", "payments", "staging", "CUSTOMER_MANAGED", "cloud");
            CloudTarget target = (CloudTarget) new CloudTargetProvider().create(
                configuration,
                new DeploymentServices(reference -> java.util.Optional.of(new Credential.Bearer("secret"))));
            var plan = target.plan(verified(descriptorBytes), new DeploymentRequest("staging", "key-1"));
            var registration = target.register(plan);
            var physical = target.materialize(plan, registration);
            var runtime = target.verifyRuntime(plan, physical);
            var result = target.activate(plan, runtime);

            assertArrayEquals(descriptorBytes, received.get());
            assertEquals("key-1", idempotency.get());
            assertEquals("deployment-1", result.deploymentId());
            assertEquals(DeploymentStatus.REGISTERED, result.status());
            assertEquals(StageState.NOT_REQUESTED, result.physicalDeployment());
            assertEquals(StageState.NOT_REQUESTED, result.runtimeVerification());
            assertEquals(StageState.NOT_REQUESTED, result.activation());
        } finally {
            server.stop(0);
        }
    }

    private VerifiedRelease verified(byte[] bytes) {
        PipelineReleaseDescriptor descriptor = new PipelineReleaseDescriptor(
            PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION,
            "orders", "sha256:" + "a".repeat(64), "release-1", "carrier", List.of());
        PipelineContractDescriptor contract = new PipelineContractDescriptor(
            PipelineContractDescriptor.CURRENT_SCHEMA_VERSION,
            "orders", "sha256:" + "a".repeat(64), "a".repeat(64), "COMPUTE", "REST", "app", false, "monolith",
            List.of(), new PipelineBundleCapabilities(false, List.of()));
        LoadedRelease loaded = new LoadedRelease(bytes, "sha256:" + "b".repeat(64), descriptor);
        return new VerifiedRelease(loaded, new ResolvedPipelineRelease(
            descriptor, contract, List.of(), temporaryDirectory.resolve("truth")));
    }
}
