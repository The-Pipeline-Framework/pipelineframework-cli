package org.pipelineframework.deployment.target.cloud;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.deployment.api.DeploymentException;
import org.pipelineframework.deployment.release.Credential;

class CloudArtifactUploaderTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    @TempDir Path temporaryDirectory;

    @Test
    void uploadsExactBytesAndPresignedHeadersOn201UploadRequired() throws Exception {
        byte[] zipBytes = "PK\u0003\u0004synthetic-worker-archive-bytes".getBytes(StandardCharsets.UTF_8);
        Path archive = temporaryDirectory.resolve("worker.zip");
        Files.write(archive, zipBytes);
        byte[] sha256Bytes = MessageDigest.getInstance("SHA-256").digest(zipBytes);
        String expectedDigest = "sha256:" + HexFormat.of().formatHex(sha256Bytes);
        String expectedChecksumBase64 = Base64.getEncoder().encodeToString(sha256Bytes);

        AtomicReference<JsonNode> presignRequest = new AtomicReference<>();
        AtomicReference<byte[]> uploadedBytes = new AtomicReference<>();
        AtomicReference<String> putContentType = new AtomicReference<>();
        AtomicReference<String> putIfNoneMatch = new AtomicReference<>();
        AtomicReference<String> putChecksum = new AtomicReference<>();
        AtomicInteger putCalls = new AtomicInteger();

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        int port = server.getAddress().getPort();
        String secretUploadUrl = "http://127.0.0.1:" + port + "/s3-upload/object?X-Amz-Signature=super-secret-sig-999";

        server.createContext("/api/v1/organizations/acme/applications/orders/environments/staging/artifacts/presign", exchange -> {
            assertEquals("POST", exchange.getRequestMethod());
            assertEquals("Bearer tpf_tok_upload_secret", exchange.getRequestHeaders().getFirst("Authorization"));
            presignRequest.set(JSON.readTree(exchange.getRequestBody().readAllBytes()));
            String body = JSON.writeValueAsString(Map.of(
                "status", "UPLOAD_REQUIRED",
                "uploadUrl", secretUploadUrl,
                "method", "PUT",
                "headers", Map.of(
                    "Content-Type", "application/zip",
                    "If-None-Match", "*",
                    "x-amz-checksum-sha256", expectedChecksumBase64,
                    "Host", "should-be-ignored-safely",
                    "Content-Length", String.valueOf(zipBytes.length)),
                "expiresAt", "2026-11-27T02:00:00Z",
                "artifactUri", "s3://tpf-bucket/tpf/releases/p1/abc/workers/work-and-recovery/function.zip",
                "bucket", "tpf-bucket",
                "key", "tpf/releases/p1/abc/workers/work-and-recovery/function.zip",
                "artifactDigest", expectedDigest,
                "contentLength", zipBytes.length));
            byte[] response = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(201, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.createContext("/s3-upload/object", exchange -> {
            assertEquals("PUT", exchange.getRequestMethod());
            putCalls.incrementAndGet();
            putContentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            putIfNoneMatch.set(exchange.getRequestHeaders().getFirst("If-None-Match"));
            putChecksum.set(exchange.getRequestHeaders().getFirst("x-amz-checksum-sha256"));
            uploadedBytes.set(exchange.getRequestBody().readAllBytes());
            exchange.getResponseHeaders().add("x-amz-version-id", "ver-42");
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        try {
            var config = new CloudTargetConfiguration(
                URI.create("http://127.0.0.1:" + port),
                "acme", "orders", "staging", "TPF_HOSTED", "env:TPF_CLOUD_TOKEN");
            var uploader = new CloudArtifactUploader(
                config, ref -> Optional.of(new Credential.Bearer("tpf_tok_upload_secret")));

            var result = uploader.upload("work-and-recovery", archive);

            assertEquals("work-and-recovery", presignRequest.get().path("artifactId").asText());
            assertEquals(expectedDigest, presignRequest.get().path("artifactDigest").asText());
            assertEquals(zipBytes.length, presignRequest.get().path("contentLength").asLong());
            assertEquals(1, putCalls.get());
            assertArrayEquals(zipBytes, uploadedBytes.get());
            assertEquals("application/zip", putContentType.get());
            assertEquals("*", putIfNoneMatch.get());
            assertEquals(expectedChecksumBase64, putChecksum.get());
            assertEquals(CloudArtifactUploader.STATUS_UPLOADED, result.status());
            assertEquals("s3://tpf-bucket/tpf/releases/p1/abc/workers/work-and-recovery/function.zip", result.artifactUri());
            assertEquals("ver-42", result.versionId());
            assertEquals(expectedDigest, result.artifactDigest());
            assertEquals(zipBytes.length, result.contentLength());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void skipsUploadOn200AlreadyUploadedReplay() throws Exception {
        byte[] zipBytes = "already-uploaded-worker-zip".getBytes(StandardCharsets.UTF_8);
        Path archive = temporaryDirectory.resolve("worker.zip");
        Files.write(archive, zipBytes);
        String expectedDigest = CloudArtifactUploader.digestFile(archive).artifactDigest();
        AtomicInteger putCalls = new AtomicInteger();

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/organizations/acme/applications/orders/environments/staging/artifacts/presign", exchange -> {
            exchange.getRequestBody().readAllBytes();
            String body = JSON.writeValueAsString(Map.of(
                "status", "ALREADY_UPLOADED",
                "artifactUri", "s3://tpf-bucket/tpf/releases/p1/key/function.zip?versionId=v1",
                "bucket", "tpf-bucket",
                "key", "tpf/releases/p1/key/function.zip",
                "versionId", "v1",
                "artifactDigest", expectedDigest,
                "contentLength", zipBytes.length));
            byte[] response = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.createContext("/s3-upload", exchange -> {
            putCalls.incrementAndGet();
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        server.start();
        try {
            var config = new CloudTargetConfiguration(
                URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
                "acme", "orders", "staging", "TPF_HOSTED", "env:TPF_CLOUD_TOKEN");
            var uploader = new CloudArtifactUploader(
                config, ref -> Optional.of(new Credential.Bearer("tpf_tok_replay")));

            var result = uploader.upload("work-and-recovery", archive);
            assertEquals(0, putCalls.get());
            assertEquals(CloudArtifactUploader.STATUS_ALREADY_UPLOADED, result.status());
            assertEquals("s3://tpf-bucket/tpf/releases/p1/key/function.zip?versionId=v1", result.artifactUri());
            assertEquals("v1", result.versionId());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void handlesConcurrent412PreconditionFailedByRecheckingPresign() throws Exception {
        byte[] zipBytes = "raced-worker-zip".getBytes(StandardCharsets.UTF_8);
        Path archive = temporaryDirectory.resolve("worker.zip");
        Files.write(archive, zipBytes);
        String expectedDigest = CloudArtifactUploader.digestFile(archive).artifactDigest();
        AtomicInteger presignCalls = new AtomicInteger();

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        int port = server.getAddress().getPort();
        server.createContext("/api/v1/organizations/acme/applications/orders/environments/staging/artifacts/presign", exchange -> {
            exchange.getRequestBody().readAllBytes();
            int call = presignCalls.incrementAndGet();
            String body;
            int status;
            if (call == 1) {
                status = 201;
                body = JSON.writeValueAsString(Map.of(
                    "status", "UPLOAD_REQUIRED",
                    "uploadUrl", "http://127.0.0.1:" + port + "/s3-race",
                    "method", "PUT",
                    "headers", Map.of("If-None-Match", "*"),
                    "expiresAt", "2026-11-27T02:00:00Z",
                    "artifactUri", "s3://tpf-bucket/key/function.zip",
                    "artifactDigest", expectedDigest,
                    "contentLength", zipBytes.length));
            } else {
                status = 200;
                body = JSON.writeValueAsString(Map.of(
                    "status", "ALREADY_UPLOADED",
                    "artifactUri", "s3://tpf-bucket/key/function.zip?versionId=winner-v1",
                    "versionId", "winner-v1",
                    "artifactDigest", expectedDigest,
                    "contentLength", zipBytes.length));
            }
            byte[] response = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.createContext("/s3-race", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(412, -1);
            exchange.close();
        });
        server.start();
        try {
            var config = new CloudTargetConfiguration(
                URI.create("http://127.0.0.1:" + port),
                "acme", "orders", "staging", "TPF_HOSTED", "env:TPF_CLOUD_TOKEN");
            var uploader = new CloudArtifactUploader(
                config, ref -> Optional.of(new Credential.Bearer("tpf_tok_race")));
            var result = uploader.upload("work-and-recovery", archive);
            assertEquals(2, presignCalls.get());
            assertEquals(CloudArtifactUploader.STATUS_ALREADY_UPLOADED, result.status());
            assertEquals("winner-v1", result.versionId());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void handles409ConflictAnd401And403AndRedactsUploadUrlOnFailure() throws Exception {
        byte[] zipBytes = "conflict-and-auth-test-zip".getBytes(StandardCharsets.UTF_8);
        Path archive = temporaryDirectory.resolve("worker.zip");
        Files.write(archive, zipBytes);
        String expectedDigest = CloudArtifactUploader.digestFile(archive).artifactDigest();
        String secretToken = "tpf_tok_never_print_this";
        String secretSig = "X-Amz-Signature=super-secret-presign-sig-xyz";

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        int port = server.getAddress().getPort();
        server.createContext("/conflict/api/v1/organizations/acme/applications/orders/environments/staging/artifacts/presign", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(409, -1);
            exchange.close();
        });
        server.createContext("/unauth/api/v1/organizations/acme/applications/orders/environments/staging/artifacts/presign", exchange -> {
            exchange.getRequestBody().readAllBytes();
            byte[] response = secretToken.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(401, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.createContext("/forbid/api/v1/organizations/acme/applications/orders/environments/staging/artifacts/presign", exchange -> {
            exchange.getRequestBody().readAllBytes();
            byte[] response = secretToken.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(403, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.createContext("/put-fail/api/v1/organizations/acme/applications/orders/environments/staging/artifacts/presign", exchange -> {
            exchange.getRequestBody().readAllBytes();
            String body = JSON.writeValueAsString(Map.of(
                "status", "UPLOAD_REQUIRED",
                "uploadUrl", "http://127.0.0.1:" + port + "/s3-fail?" + secretSig,
                "method", "PUT",
                "headers", Map.of("Content-Type", "application/zip"),
                "expiresAt", "2026-11-27T02:00:00Z",
                "artifactUri", "s3://tpf-bucket/key/function.zip",
                "artifactDigest", expectedDigest,
                "contentLength", zipBytes.length));
            byte[] response = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(201, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.createContext("/s3-fail", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        server.start();
        try {
            var resolver = (org.pipelineframework.deployment.release.CredentialResolver)
                ref -> Optional.of(new Credential.Bearer(secretToken));

            var conflictUploader = new CloudArtifactUploader(
                new CloudTargetConfiguration(URI.create("http://127.0.0.1:" + port + "/conflict"),
                    "acme", "orders", "staging", "TPF_HOSTED", "env:TPF_CLOUD_TOKEN"),
                resolver);
            DeploymentException conflict = assertThrows(
                DeploymentException.class, () -> conflictUploader.upload("work-and-recovery", archive));
            assertEquals(DeploymentException.FailureClass.TARGET, conflict.failureClass());
            assertTrue(conflict.getMessage().contains("409"));

            for (String prefix : new String[] {"/unauth", "/forbid"}) {
                var authUploader = new CloudArtifactUploader(
                    new CloudTargetConfiguration(URI.create("http://127.0.0.1:" + port + prefix),
                        "acme", "orders", "staging", "TPF_HOSTED", "env:TPF_CLOUD_TOKEN"),
                    resolver);
                DeploymentException authError = assertThrows(
                    DeploymentException.class, () -> authUploader.upload("work-and-recovery", archive));
                assertEquals(DeploymentException.FailureClass.AUTHENTICATION, authError.failureClass());
                assertFalse(authError.getMessage().contains(secretToken));
            }

            var putFailUploader = new CloudArtifactUploader(
                new CloudTargetConfiguration(URI.create("http://127.0.0.1:" + port + "/put-fail"),
                    "acme", "orders", "staging", "TPF_HOSTED", "env:TPF_CLOUD_TOKEN"),
                resolver);
            DeploymentException putError = assertThrows(
                DeploymentException.class, () -> putFailUploader.upload("work-and-recovery", archive));
            assertEquals(DeploymentException.FailureClass.TARGET, putError.failureClass());
            assertTrue(putError.getMessage().contains("500"));
            assertFalse(putError.getMessage().contains(secretSig));
            assertFalse(putError.getMessage().contains(secretToken));
        } finally {
            server.stop(0);
        }
    }
}
