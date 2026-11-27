package org.pipelineframework.deployment.target.cloud;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.deployment.api.DeploymentException;
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

        assertTrue(contract.contains(CloudTarget.DEPLOYMENT_PATH
            .formatted("{organization}", "{application}", "{environment}")));
        assertTrue(contract.contains(CloudTarget.RESUME_PATH
            .formatted("{organization}", "{application}", "{environment}", "{deploymentId}")));
        assertTrue(contract.contains(CloudArtifactUploader.PRESIGN_PATH
            .formatted("{organization}", "{application}", "{environment}")));
        assertTrue(contract.contains(CloudTarget.RELEASE_MEDIA_TYPE + ":"));
        assertTrue(contract.contains("REGISTERED"));
        assertTrue(contract.contains("TPF_HOSTED"));
        assertTrue(contract.contains("ACTIVATED"));
    }

    @Test
    void transmitsExactDescriptorBytesAndStopsAtRegistered() throws Exception {
        byte[] descriptorBytes = "{\"schemaVersion\":1}\n".getBytes(StandardCharsets.UTF_8);
        AtomicReference<byte[]> received = new AtomicReference<>();
        AtomicReference<String> idempotency = new AtomicReference<>();
        AtomicReference<String> query = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/cloud/api/v1/organizations/example/applications/payments/environments/staging/deployments", exchange -> {
            received.set(exchange.getRequestBody().readAllBytes());
            idempotency.set(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
            query.set(exchange.getRequestURI().getRawQuery());
            assertEquals("Bearer secret", exchange.getRequestHeaders().getFirst("Authorization"));
            assertEquals("CUSTOMER_MANAGED", exchange.getRequestHeaders().getFirst("X-TPF-Deployment-Mode"));
            byte[] response = "{\"deploymentId\":\"deployment-1\",\"status\":\"REGISTERED\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(201, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            var configuration = new CloudTargetConfiguration(
                URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/cloud?tenant=example"),
                "example", "payments", "staging", "CUSTOMER_MANAGED", "cloud");
            CloudTarget target = (CloudTarget) new CloudTargetProvider().create(
                configuration,
                new DeploymentServices(reference -> Optional.of(new Credential.Bearer("secret"))));
            var plan = target.plan(verified(descriptorBytes), new DeploymentRequest("staging", "key-1"));
            var registration = target.register(plan);
            var physical = target.materialize(plan, registration);
            var runtime = target.verifyRuntime(plan, physical);
            var result = target.activate(plan, runtime);

            assertArrayEquals(descriptorBytes, received.get());
            assertEquals("key-1", idempotency.get());
            assertEquals("tenant=example", query.get());
            assertEquals("deployment-1", result.deploymentId());
            assertEquals(DeploymentStatus.REGISTERED, result.status());
            assertEquals(StageState.NOT_REQUESTED, result.physicalDeployment());
            assertEquals(StageState.NOT_REQUESTED, result.runtimeVerification());
            assertEquals(StageState.NOT_REQUESTED, result.activation());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void hostedImmediateActivationCompletesAllStagesWithoutPolling() throws Exception {
        byte[] descriptorBytes = "{\"schemaVersion\":2,\"releaseVersion\":\"1.0.0\"}\n".getBytes(StandardCharsets.UTF_8);
        AtomicReference<byte[]> received = new AtomicReference<>();
        AtomicInteger resumeCalls = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/organizations/acme/applications/orders/environments/prod/deployments", exchange -> {
            if (exchange.getRequestURI().getPath().endsWith("/resume")) {
                resumeCalls.incrementAndGet();
                exchange.sendResponseHeaders(500, -1);
                exchange.close();
                return;
            }
            received.set(exchange.getRequestBody().readAllBytes());
            assertEquals("TPF_HOSTED", exchange.getRequestHeaders().getFirst("X-TPF-Deployment-Mode"));
            assertEquals("Bearer tpf_tok_live", exchange.getRequestHeaders().getFirst("Authorization"));
            byte[] response = "{\"deploymentId\":\"dep-hosted-1\",\"status\":\"ACTIVATED\",\"activationId\":\"act-99\"}"
                .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            var configuration = new CloudTargetConfiguration(
                URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
                "acme", "orders", "prod", "TPF_HOSTED", "env:TPF_CLOUD_TOKEN", 2, 30);
            var clock = new MutableClock(Instant.parse("2026-11-27T00:00:00Z"));
            CloudTarget target = new CloudTarget(
                configuration,
                new DeploymentServices(ref -> Optional.of(new Credential.Bearer("tpf_tok_live"))),
                HttpClient.newHttpClient(),
                clock,
                clock::advance);

            var plan = target.plan(verified(descriptorBytes), new DeploymentRequest("prod", "key-hosted-1"));
            var registration = target.register(plan);
            var physical = target.materialize(plan, registration);
            var runtime = target.verifyRuntime(plan, physical);
            var result = target.activate(plan, runtime);

            assertArrayEquals(descriptorBytes, received.get());
            assertEquals(0, resumeCalls.get());
            assertEquals("dep-hosted-1", result.deploymentId());
            assertEquals(DeploymentStatus.ACTIVATED, result.status());
            assertEquals(StageState.COMPLETED, result.registration());
            assertEquals(StageState.COMPLETED, result.physicalDeployment());
            assertEquals(StageState.COMPLETED, result.runtimeVerification());
            assertEquals(StageState.COMPLETED, result.activation());
            assertTrue(result.message().contains("act-99"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void hostedMultiStepResumePollingAdvancesFromWaitingForWorkerToActivated() throws Exception {
        byte[] descriptorBytes = "{\"schemaVersion\":2,\"releaseVersion\":\"2.0.0\"}\n".getBytes(StandardCharsets.UTF_8);
        AtomicInteger resumeAttempts = new AtomicInteger();
        AtomicInteger sleepCount = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/organizations/acme/applications/orders/environments/staging/deployments/dep-2/resume", exchange -> {
            assertEquals("POST", exchange.getRequestMethod());
            assertEquals("Bearer tpf_tok_poll", exchange.getRequestHeaders().getFirst("Authorization"));
            int attempt = resumeAttempts.incrementAndGet();
            int code;
            String json;
            if (attempt == 1) {
                code = 202;
                json = "{\"deploymentId\":\"dep-2\",\"status\":\"WAITING_FOR_WORKER\"}";
            } else if (attempt == 2) {
                code = 202;
                json = "{\"deploymentId\":\"dep-2\",\"status\":\"WAITING_FOR_ROUTING\"}";
            } else {
                code = 200;
                json = "{\"deploymentId\":\"dep-2\",\"status\":\"ACTIVATED\",\"activationId\":\"act-2\"}";
            }
            byte[] response = json.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(code, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.createContext("/api/v1/organizations/acme/applications/orders/environments/staging/deployments", exchange -> {
            assertEquals("TPF_HOSTED", exchange.getRequestHeaders().getFirst("X-TPF-Deployment-Mode"));
            byte[] response = "{\"deploymentId\":\"dep-2\",\"status\":\"WAITING_FOR_WORKER\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(202, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            var configuration = new CloudTargetConfiguration(
                URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
                "acme", "orders", "staging", "TPF_HOSTED", "env:TPF_CLOUD_TOKEN", 5, 60);
            var clock = new MutableClock(Instant.parse("2026-11-27T00:00:00Z"));
            CloudTarget target = new CloudTarget(
                configuration,
                new DeploymentServices(ref -> Optional.of(new Credential.Bearer("tpf_tok_poll"))),
                HttpClient.newHttpClient(),
                clock,
                delay -> {
                    sleepCount.incrementAndGet();
                    assertEquals(Duration.ofSeconds(5), delay);
                    clock.advance(delay);
                });

            var plan = target.plan(verified(descriptorBytes), new DeploymentRequest("staging", "key-poll"));
            var registration = target.register(plan);
            var physical = target.materialize(plan, registration);
            var runtime = target.verifyRuntime(plan, physical);
            var result = target.activate(plan, runtime);

            assertEquals(3, resumeAttempts.get());
            assertEquals(3, sleepCount.get());
            assertEquals("dep-2", result.deploymentId());
            assertEquals(DeploymentStatus.ACTIVATED, result.status());
            assertEquals(StageState.COMPLETED, result.registration());
            assertEquals(StageState.COMPLETED, result.physicalDeployment());
            assertEquals(StageState.COMPLETED, result.runtimeVerification());
            assertEquals(StageState.COMPLETED, result.activation());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void hostedFailsFastOnWorkerFailureDuringRegisterAndResume() throws Exception {
        byte[] descriptorBytes = "{\"schemaVersion\":2}\n".getBytes(StandardCharsets.UTF_8);
        AtomicInteger resumeAttempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        // Case 1: workerFailure on initial POST
        server.createContext("/init-fail/api/v1/organizations/acme/applications/orders/environments/staging/deployments", exchange -> {
            byte[] response = ("{\"deploymentId\":\"dep-fail-1\",\"status\":\"WAITING_FOR_WORKER\","
                + "\"workerFailure\":{\"status\":\"FAILED\",\"errorCode\":\"ARTIFACT_MISSING\","
                + "\"reason\":\"Worker archive s3://bucket/missing.zip was not found\"}}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(202, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        // Case 2: failure (Cloud API Progress.failure field) on first resume
        server.createContext("/resume-fail/api/v1/organizations/acme/applications/orders/environments/staging/deployments/dep-fail-2/resume", exchange -> {
            resumeAttempts.incrementAndGet();
            byte[] response = ("{\"deploymentId\":\"dep-fail-2\",\"status\":\"WAITING_FOR_WORKER\","
                + "\"failure\":{\"status\":\"FAILED\",\"errorCode\":\"ROLLBACK_COMPLETE\","
                + "\"reason\":\"CloudFormation stack rolled back: invalid Lambda handler\"}}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(202, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.createContext("/resume-fail/api/v1/organizations/acme/applications/orders/environments/staging/deployments", exchange -> {
            byte[] response = "{\"deploymentId\":\"dep-fail-2\",\"status\":\"WAITING_FOR_WORKER\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(202, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            var services = new DeploymentServices(ref -> Optional.of(new Credential.Bearer("tpf_tok_secret")));
            var clock = new MutableClock(Instant.parse("2026-11-27T00:00:00Z"));

            CloudTarget initTarget = new CloudTarget(
                new CloudTargetConfiguration(
                    URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/init-fail"),
                    "acme", "orders", "staging", "TPF_HOSTED", "cloud", 5, 60),
                services, HttpClient.newHttpClient(), clock, clock::advance);
            var initPlan = initTarget.plan(verified(descriptorBytes), new DeploymentRequest("staging", "k1"));
            DeploymentException initError = assertThrows(DeploymentException.class, () -> initTarget.register(initPlan));
            assertEquals(DeploymentException.FailureClass.TARGET, initError.failureClass());
            assertTrue(initError.getMessage().contains("ARTIFACT_MISSING"));
            assertTrue(initError.getMessage().contains("was not found"));
            assertFalse(initError.getMessage().contains("tpf_tok_secret"));

            CloudTarget resumeTarget = new CloudTarget(
                new CloudTargetConfiguration(
                    URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/resume-fail"),
                    "acme", "orders", "staging", "TPF_HOSTED", "cloud", 5, 60),
                services, HttpClient.newHttpClient(), clock, clock::advance);
            var resumePlan = resumeTarget.plan(verified(descriptorBytes), new DeploymentRequest("staging", "k2"));
            var registration = resumeTarget.register(resumePlan);
            DeploymentException resumeError = assertThrows(
                DeploymentException.class, () -> resumeTarget.materialize(resumePlan, registration));
            assertEquals(DeploymentException.FailureClass.TARGET, resumeError.failureClass());
            assertEquals(1, resumeAttempts.get());
            assertTrue(resumeError.getMessage().contains("ROLLBACK_COMPLETE"));
            assertTrue(resumeError.getMessage().contains("invalid Lambda handler"));
            assertFalse(resumeError.getMessage().contains("tpf_tok_secret"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void hostedTimesOutWhenDeploymentRemainsWaiting() throws Exception {
        byte[] descriptorBytes = "{\"schemaVersion\":2}\n".getBytes(StandardCharsets.UTF_8);
        AtomicInteger resumeAttempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/organizations/acme/applications/orders/environments/staging/deployments/dep-slow/resume", exchange -> {
            resumeAttempts.incrementAndGet();
            byte[] response = "{\"deploymentId\":\"dep-slow\",\"status\":\"WAITING_FOR_WORKER\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(202, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.createContext("/api/v1/organizations/acme/applications/orders/environments/staging/deployments", exchange -> {
            byte[] response = "{\"deploymentId\":\"dep-slow\",\"status\":\"WAITING_FOR_WORKER\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(202, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            var clock = new MutableClock(Instant.parse("2026-11-27T00:00:00Z"));
            CloudTarget target = new CloudTarget(
                new CloudTargetConfiguration(
                    URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
                    "acme", "orders", "staging", "TPF_HOSTED", "cloud", 4, 10),
                new DeploymentServices(ref -> Optional.of(new Credential.Bearer("secret"))),
                HttpClient.newHttpClient(),
                clock,
                clock::advance);
            var plan = target.plan(verified(descriptorBytes), new DeploymentRequest("staging", "k-timeout"));
            var registration = target.register(plan);
            DeploymentException error = assertThrows(
                DeploymentException.class, () -> target.materialize(plan, registration));
            assertEquals(DeploymentException.FailureClass.TARGET, error.failureClass());
            assertTrue(error.getMessage().contains("Timed out after 10s"));
            assertTrue(error.getMessage().contains("WAITING_FOR_WORKER"));
            assertEquals(3, resumeAttempts.get()); // waits: 4s, 4s, 2s -> 10s reached
        } finally {
            server.stop(0);
        }
    }

    @Test
    void hostedAuthErrorsDoNotLeakBearerToken() throws Exception {
        byte[] descriptorBytes = "{\"schemaVersion\":2}\n".getBytes(StandardCharsets.UTF_8);
        String secretToken = "tpf_tok_super_secret_bearer_value";
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/unauth/api/v1/organizations/acme/applications/orders/environments/staging/deployments", exchange -> {
            byte[] response = ("unauthorized " + secretToken).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(401, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.createContext("/forbid/api/v1/organizations/acme/applications/orders/environments/staging/deployments", exchange -> {
            byte[] response = ("forbidden " + secretToken).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(403, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            var services = new DeploymentServices(ref -> Optional.of(new Credential.Bearer(secretToken)));
            for (String prefix : List.of("/unauth", "/forbid")) {
                CloudTarget target = new CloudTarget(
                    new CloudTargetConfiguration(
                        URI.create("http://127.0.0.1:" + server.getAddress().getPort() + prefix),
                        "acme", "orders", "staging", "TPF_HOSTED", "cloud"),
                    services);
                var plan = target.plan(verified(descriptorBytes), new DeploymentRequest("staging", "k-auth"));
                DeploymentException error = assertThrows(DeploymentException.class, () -> target.register(plan));
                assertEquals(DeploymentException.FailureClass.AUTHENTICATION, error.failureClass());
                assertFalse(error.getMessage().contains(secretToken));
            }
        } finally {
            server.stop(0);
        }
    }

    @Test
    void hostedAcceptsLegacyRegisteredStatusWhenNonRegional() throws Exception {
        byte[] descriptorBytes = "{\"schemaVersion\":2}\n".getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/organizations/acme/applications/orders/environments/staging/deployments", exchange -> {
            byte[] response = "{\"deploymentId\":\"dep-legacy\",\"status\":\"REGISTERED\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(201, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            CloudTarget target = new CloudTarget(
                new CloudTargetConfiguration(
                    URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
                    "acme", "orders", "staging", "TPF_HOSTED", "cloud"),
                new DeploymentServices(ref -> Optional.of(new Credential.Bearer("secret"))));
            var plan = target.plan(verified(descriptorBytes), new DeploymentRequest("staging", "k-legacy"));
            var result = target.activate(plan, target.verifyRuntime(plan, target.materialize(plan, target.register(plan))));
            assertEquals(DeploymentStatus.REGISTERED, result.status());
            assertEquals(StageState.COMPLETED, result.registration());
            assertEquals(StageState.NOT_REQUESTED, result.activation());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void configurationValidatesModePollSettingsAndJacksonDefaults() {
        var mapper = new ObjectMapper();
        CloudTargetConfiguration omitted = mapper.convertValue(Map.of(
            "endpoint", "https://api.example.test",
            "organization", "acme",
            "application", "orders",
            "environment", "staging",
            "credential", "env:TPF_CLOUD_TOKEN"), CloudTargetConfiguration.class);
        assertEquals("CUSTOMER_MANAGED", omitted.mode());
        assertFalse(omitted.isHosted());
        assertEquals(CloudTargetConfiguration.DEFAULT_POLL_INTERVAL_SECONDS, omitted.pollIntervalSeconds());
        assertEquals(CloudTargetConfiguration.DEFAULT_TIMEOUT_SECONDS, omitted.timeoutSeconds());

        CloudTargetConfiguration hosted = mapper.convertValue(Map.of(
            "endpoint", "https://api.example.test",
            "organization", "acme",
            "application", "orders",
            "environment", "prod",
            "mode", "TPF_HOSTED",
            "credential", "env:TPF_CLOUD_TOKEN",
            "pollIntervalSeconds", 10,
            "timeoutSeconds", 600), CloudTargetConfiguration.class);
        assertEquals("TPF_HOSTED", hosted.mode());
        assertTrue(hosted.isHosted());
        assertEquals(10, hosted.pollIntervalSeconds());
        assertEquals(600, hosted.timeoutSeconds());

        assertThrows(IllegalArgumentException.class, () -> new CloudTargetConfiguration(
            URI.create("https://api.example.test"), "acme", "orders", "prod", "UNKNOWN", "cloud"));
        assertThrows(IllegalArgumentException.class, () -> new CloudTargetConfiguration(
            URI.create("https://api.example.test"), "acme", "orders", "prod", "TPF_HOSTED", "cloud", 0, 300));
        assertThrows(IllegalArgumentException.class, () -> new CloudTargetConfiguration(
            URI.create("https://api.example.test"), "acme", "orders", "prod", "TPF_HOSTED", "cloud", 5, 0));
    }

    @Test
    void requiresHttpsExceptForLoopbackHttp() {
        assertDoesNotThrow(() -> configuration(URI.create("https://api.example.test/cloud?tenant=example")));
        assertDoesNotThrow(() -> configuration(URI.create("http://localhost:8080")));
        assertDoesNotThrow(() -> configuration(URI.create("http://127.42.0.1:8080")));
        assertDoesNotThrow(() -> configuration(URI.create("http://[::1]:8080")));
        assertThrows(IllegalArgumentException.class,
            () -> configuration(URI.create("http://api.example.test")));
        assertThrows(IllegalArgumentException.class,
            () -> configuration(URI.create("ftp://127.0.0.1/releases")));
    }

    private static CloudTargetConfiguration configuration(URI endpoint) {
        return new CloudTargetConfiguration(
            endpoint, "example", "payments", "staging", "CUSTOMER_MANAGED", "cloud");
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

    private static final class MutableClock extends Clock {
        private Instant current;

        MutableClock(Instant start) {
            this.current = start;
        }

        void advance(Duration delta) {
            this.current = this.current.plus(delta);
        }

        @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
        @Override public Instant instant() { return current; }
    }
}
