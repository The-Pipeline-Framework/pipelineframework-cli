package org.pipelineframework.cli;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.deployment.api.DeploymentException;
import org.pipelineframework.deployment.core.EnvironmentCredentialResolver;
import org.pipelineframework.deployment.target.cloud.CloudArtifactUploader;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.release.producer.DefaultPipelineReleaseProducer;
import org.pipelineframework.release.producer.ReleaseArtifactInput;
import org.pipelineframework.release.producer.ReleaseProductionRequest;
import picocli.CommandLine;

class ArtifactAndHostedDeployCommandTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    @TempDir Path temporaryDirectory;

    @Test
    void artifactUploadCommandUploadsViaPresignAndNeverLogsSecrets() throws Exception {
        Path workerZip = createZip(temporaryDirectory.resolve("function.zip"), "handler.py", "def handle(e): return e\n");
        byte[] zipBytes = Files.readAllBytes(workerZip);
        byte[] sha256 = MessageDigest.getInstance("SHA-256").digest(zipBytes);
        String expectedDigest = "sha256:" + HexFormat.of().formatHex(sha256);
        String expectedChecksum = Base64.getEncoder().encodeToString(sha256);
        String secretToken = "tpf_tok_cli_upload_secret_98765";
        String secretSignature = "X-Amz-Signature=super-secret-s3-sig-4567";

        AtomicInteger presignCalls = new AtomicInteger();
        AtomicInteger putCalls = new AtomicInteger();
        AtomicReference<byte[]> putBody = new AtomicReference<>();

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        int port = server.getAddress().getPort();
        String uploadUrl = "http://127.0.0.1:" + port + "/s3/upload?" + secretSignature;
        server.createContext("/api/v1/organizations/acme/applications/orders/environments/staging/artifacts/presign", exchange -> {
            assertEquals("Bearer " + secretToken, exchange.getRequestHeaders().getFirst("Authorization"));
            exchange.getRequestBody().readAllBytes();
            int call = presignCalls.incrementAndGet();
            String body;
            int status;
            if (call == 1) {
                status = 201;
                body = JSON.writeValueAsString(Map.of(
                    "status", "UPLOAD_REQUIRED",
                    "uploadUrl", uploadUrl,
                    "method", "PUT",
                    "headers", Map.of(
                        "Content-Type", "application/zip",
                        "If-None-Match", "*",
                        "x-amz-checksum-sha256", expectedChecksum),
                    "expiresAt", "2026-11-27T02:00:00Z",
                    "artifactUri", "s3://tpf-bucket/tpf/releases/p1/abc/workers/work-and-recovery/function.zip",
                    "bucket", "tpf-bucket",
                    "key", "tpf/releases/p1/abc/workers/work-and-recovery/function.zip",
                    "artifactDigest", expectedDigest,
                    "contentLength", zipBytes.length));
            } else {
                status = 200;
                body = JSON.writeValueAsString(Map.of(
                    "status", "ALREADY_UPLOADED",
                    "artifactUri", "s3://tpf-bucket/tpf/releases/p1/abc/workers/work-and-recovery/function.zip?versionId=v1",
                    "bucket", "tpf-bucket",
                    "key", "tpf/releases/p1/abc/workers/work-and-recovery/function.zip",
                    "versionId", "v1",
                    "artifactDigest", expectedDigest,
                    "contentLength", zipBytes.length));
            }
            byte[] response = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.createContext("/s3/upload", exchange -> {
            putCalls.incrementAndGet();
            assertEquals("application/zip", exchange.getRequestHeaders().getFirst("Content-Type"));
            assertEquals("*", exchange.getRequestHeaders().getFirst("If-None-Match"));
            assertEquals(expectedChecksum, exchange.getRequestHeaders().getFirst("x-amz-checksum-sha256"));
            putBody.set(exchange.getRequestBody().readAllBytes());
            exchange.getResponseHeaders().add("x-amz-version-id", "v1");
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        try {
            Path config = writeDeployConfig(port, "TPF_HOSTED", 1, 30);
            var resolver = new EnvironmentCredentialResolver(Map.of("TPF_CLOUD_TOKEN", secretToken));

            // 1. JSON output on first upload (UPLOAD_REQUIRED -> UPLOADED)
            StringWriter out1 = new StringWriter();
            StringWriter err1 = new StringWriter();
            int exit1 = new CommandLine(new ArtifactUploadCommand(resolver))
                .setOut(new PrintWriter(out1, true))
                .setErr(new PrintWriter(err1, true))
                .execute("staging", "--file", workerZip.toString(), "--artifact-id", "work-and-recovery",
                    "--config", config.toString(), "--output", "json");
            assertEquals(0, exit1, err1.toString());
            assertEquals("", err1.toString());
            assertArrayEquals(zipBytes, putBody.get());
            assertEquals(1, putCalls.get());
            assertFalse(out1.toString().contains(secretToken));
            assertFalse(out1.toString().contains(secretSignature));
            JsonNode json = JSON.readTree(out1.toString());
            assertEquals("artifact.upload", json.path("operation").asText());
            assertEquals("UPLOADED", json.path("status").asText());
            assertEquals("work-and-recovery", json.path("artifactId").asText());
            assertEquals("s3://tpf-bucket/tpf/releases/p1/abc/workers/work-and-recovery/function.zip",
                json.path("artifactUri").asText());
            assertEquals(expectedDigest, json.path("artifactDigest").asText());
            assertEquals("v1", json.path("versionId").asText());

            // 2. Human output on replay (ALREADY_UPLOADED -> skips PUT)
            StringWriter out2 = new StringWriter();
            StringWriter err2 = new StringWriter();
            int exit2 = new CommandLine(new ArtifactUploadCommand(resolver))
                .setOut(new PrintWriter(out2, true))
                .setErr(new PrintWriter(err2, true))
                .execute("staging", "--file", workerZip.toString(), "--artifact-id", "work-and-recovery",
                    "--config", config.toString(), "--output", "human");
            assertEquals(0, exit2, err2.toString());
            assertEquals(1, putCalls.get()); // No second PUT!
            assertTrue(out2.toString().startsWith("ALREADY_UPLOADED work-and-recovery -> s3://"));
            assertFalse(out2.toString().contains(secretToken));
            assertFalse(out2.toString().contains(secretSignature));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void deployWithArtifactOptionUploadsBeforeDeployPollsToActivatedAndNeverMutatesDescriptor() throws Exception {
        Path workerZip = createZip(temporaryDirectory.resolve("worker.zip"), "worker.txt", "immutable-worker-content");
        Path descriptorPath = produceRelease(workerZip);
        byte[] originalDescriptorBytes = Files.readAllBytes(descriptorPath);
        String workerDigest = CloudArtifactUploader.digestFile(workerZip).artifactDigest();
        String secretToken = "tpf_tok_hosted_deploy_secret";

        List<String> callOrder = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<byte[]> postedDescriptorBytes = new AtomicReference<>();

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        int port = server.getAddress().getPort();
        server.createContext("/api/v1/organizations/acme/applications/orders/environments/staging/artifacts/presign", exchange -> {
            callOrder.add("presign");
            exchange.getRequestBody().readAllBytes();
            String body = JSON.writeValueAsString(Map.of(
                "status", "UPLOAD_REQUIRED",
                "uploadUrl", "http://127.0.0.1:" + port + "/s3/worker-put",
                "method", "PUT",
                "headers", Map.of("Content-Type", "application/zip", "If-None-Match", "*"),
                "expiresAt", "2026-11-27T02:00:00Z",
                "artifactUri", "s3://tpf-bucket/tpf/releases/p1/hash/workers/work-and-recovery/function.zip",
                "artifactDigest", workerDigest,
                "contentLength", Files.size(workerZip)));
            byte[] response = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(201, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.createContext("/s3/worker-put", exchange -> {
            callOrder.add("s3-put");
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.createContext("/api/v1/organizations/acme/applications/orders/environments/staging/deployments/dep-hosted-42/resume", exchange -> {
            callOrder.add("resume");
            exchange.getRequestBody().readAllBytes();
            byte[] response = "{\"deploymentId\":\"dep-hosted-42\",\"status\":\"ACTIVATED\",\"activationId\":\"act-42\"}"
                .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.createContext("/api/v1/organizations/acme/applications/orders/environments/staging/deployments", exchange -> {
            callOrder.add("deploy");
            assertEquals("TPF_HOSTED", exchange.getRequestHeaders().getFirst("X-TPF-Deployment-Mode"));
            postedDescriptorBytes.set(exchange.getRequestBody().readAllBytes());
            byte[] response = "{\"deploymentId\":\"dep-hosted-42\",\"status\":\"WAITING_FOR_WORKER\"}"
                .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(202, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            Path config = writeDeployConfig(port, "TPF_HOSTED", 1, 30);
            var resolver = new EnvironmentCredentialResolver(Map.of("TPF_CLOUD_TOKEN", secretToken));

            StringWriter out = new StringWriter();
            StringWriter err = new StringWriter();
            int exit = new CommandLine(new DeployCommand(resolver))
                .setOut(new PrintWriter(out, true))
                .setErr(new PrintWriter(err, true))
                .execute("staging",
                    "--release", descriptorPath.toString(),
                    "--config", config.toString(),
                    "--artifact", "work-and-recovery=" + workerZip,
                    "--output", "json");

            assertEquals(0, exit, err.toString());
            assertEquals(List.of("presign", "s3-put", "deploy", "resume"), callOrder);
            // HP-2 invariant: immutable descriptor on disk and on wire is bit-for-bit unmodified!
            assertArrayEquals(originalDescriptorBytes, Files.readAllBytes(descriptorPath));
            assertArrayEquals(originalDescriptorBytes, postedDescriptorBytes.get());

            JsonNode result = JSON.readTree(out.toString());
            assertEquals("ACTIVATED", result.path("status").asText());
            assertEquals("dep-hosted-42", result.path("deploymentId").asText());
            assertEquals("COMPLETED", result.path("registration").asText());
            assertEquals("COMPLETED", result.path("physicalDeployment").asText());
            assertEquals("COMPLETED", result.path("runtimeVerification").asText());
            assertEquals("COMPLETED", result.path("activation").asText());
            assertFalse(out.toString().contains(secretToken));

            // Tampered artifact file fails fast BEFORE calling presign or deploy and never mutates descriptor!
            Path wrongZip = createZip(temporaryDirectory.resolve("tampered.zip"), "worker.txt", "different-content");
            callOrder.clear();
            StringWriter badOut = new StringWriter();
            StringWriter badErr = new StringWriter();
            int badExit = new CommandLine(new DeployCommand(resolver))
                .setOut(new PrintWriter(badOut, true))
                .setErr(new PrintWriter(badErr, true))
                .execute("staging",
                    "--release", descriptorPath.toString(),
                    "--config", config.toString(),
                    "--artifact", "work-and-recovery=" + wrongZip,
                    "--output", "json");
            assertEquals(CliSupport.DEPLOYMENT_FAILURE, badExit);
            assertTrue(callOrder.isEmpty(), "Must fail before hitting presign or deploy");
            assertTrue(badErr.toString().contains("does not match immutable pipeline-release.json digest"));
            assertArrayEquals(originalDescriptorBytes, Files.readAllBytes(descriptorPath));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void verifyUploadedArtifactMatchesDescriptorChecksS3UriAndDigest() throws Exception {
        String digest = "sha256:" + "a".repeat(64);
        var declaredUnversioned = new PipelineReleaseArtifactDescriptor(
            "work-and-recovery", "lambda-zip",
            "s3://tpf-bucket/tpf/releases/p1/hash/workers/work-and-recovery/function.zip",
            digest, List.of(), List.of());
        var matchingExact = new CloudArtifactUploader.UploadResult(
            "UPLOADED", "work-and-recovery",
            "s3://tpf-bucket/tpf/releases/p1/hash/workers/work-and-recovery/function.zip",
            "tpf-bucket", "key", "", digest, 100);
        var matchingVersioned = new CloudArtifactUploader.UploadResult(
            "ALREADY_UPLOADED", "work-and-recovery",
            "s3://tpf-bucket/tpf/releases/p1/hash/workers/work-and-recovery/function.zip?versionId=v1",
            "tpf-bucket", "key", "v1", digest, 100);
        var wrongBucket = new CloudArtifactUploader.UploadResult(
            "UPLOADED", "work-and-recovery",
            "s3://other-bucket/tpf/releases/p1/hash/workers/work-and-recovery/function.zip",
            "other-bucket", "key", "", digest, 100);

        DeployCommand.verifyUploadedArtifactMatchesDescriptor(matchingExact, declaredUnversioned);
        DeployCommand.verifyUploadedArtifactMatchesDescriptor(matchingVersioned, declaredUnversioned);
        DeploymentException uriMismatch = assertThrows(
            DeploymentException.class,
            () -> DeployCommand.verifyUploadedArtifactMatchesDescriptor(wrongBucket, declaredUnversioned));
        assertEquals(DeploymentException.FailureClass.TARGET, uriMismatch.failureClass());
        assertTrue(uriMismatch.getMessage().contains("does not match immutable pipeline-release.json URI"));
    }

    private Path writeDeployConfig(int port, String mode, int pollIntervalSeconds, int timeoutSeconds) throws IOException {
        Path config = temporaryDirectory.resolve("tpf-deploy.yaml");
        Files.writeString(config, """
            resolverProfiles:
              default:
                file: true
            environments:
              staging:
                resolverProfile: default
                target:
                  type: tpf-cloud
                  endpoint: http://127.0.0.1:%d
                  organization: acme
                  application: orders
                  environment: staging
                  mode: %s
                  credential: env:TPF_CLOUD_TOKEN
                  pollIntervalSeconds: %d
                  timeoutSeconds: %d
            """.formatted(port, mode, pollIntervalSeconds, timeoutSeconds));
        return config;
    }

    private Path produceRelease(Path workerZip) throws IOException {
        Path metadataDir = temporaryDirectory.resolve("metadata");
        Files.createDirectories(metadataDir);
        PipelineBundleStepDescriptor step = new PipelineBundleStepDescriptor(
            0, "ProcessOrder", "SERVICE", "ONE_TO_ONE", "OrderInput", "OrderOutput",
            "com.example.ProcessOrderService", "com.example.ProcessOrderClient", Map.of(), Map.of());
        PipelineContractDescriptor contract = new PipelineContractDescriptor(
            PipelineContractDescriptor.CURRENT_SCHEMA_VERSION,
            "orders", "sha256:" + "a".repeat(64), "a".repeat(64),
            "FUNCTION", "REST", "orders-app", false, "modular",
            List.of(step), new PipelineBundleCapabilities(false, List.of()));
        Files.write(metadataDir.resolve("pipeline-contract.json"),
            PipelineJson.mapper().writeValueAsBytes(contract));

        Path releaseDir = temporaryDirectory.resolve("release-out");
        Files.createDirectories(releaseDir);
        Path descriptorPath = releaseDir.resolve("pipeline-release.json");

        var producer = new DefaultPipelineReleaseProducer();
        var artifacts = producer.materialize(List.of(
            new ReleaseArtifactInput("compiled-truth", "compiled-truth", metadataDir,
                descriptorPath.toUri().toASCIIString(), List.of(), List.of()),
            new ReleaseArtifactInput("work-and-recovery", "lambda-zip", workerZip,
                workerZip.toAbsolutePath().normalize().toUri().toASCIIString(), List.of("ProcessOrder"), List.of())),
            "compiled-truth", metadataDir, releaseDir);
        producer.write(descriptorPath, producer.produce(new ReleaseProductionRequest(
            contract, "1.0.0", "compiled-truth", metadataDir, artifacts, true)));
        return descriptorPath;
    }

    private static Path createZip(Path target, String entryName, String content) throws IOException {
        try (ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(target))) {
            out.putNextEntry(new ZipEntry(entryName));
            out.write(content.getBytes(StandardCharsets.UTF_8));
            out.closeEntry();
        }
        return target;
    }
}
