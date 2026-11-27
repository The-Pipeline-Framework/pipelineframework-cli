package org.pipelineframework.deployment.target.cloud;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import org.pipelineframework.deployment.api.DeploymentException;
import org.pipelineframework.deployment.release.Credential;
import org.pipelineframework.deployment.release.CredentialResolver;

/** Presigned immutable artifact intake client for TPF Cloud Regional Hosted environments. */
public final class CloudArtifactUploader {
    private static final ObjectMapper JSON = new ObjectMapper();
    static final String PRESIGN_PATH =
        "/api/v1/organizations/%s/applications/%s/environments/%s/artifacts/presign";
    public static final String STATUS_UPLOADED = "UPLOADED";
    public static final String STATUS_ALREADY_UPLOADED = "ALREADY_UPLOADED";
    private static final String STATUS_UPLOAD_REQUIRED = "UPLOAD_REQUIRED";
    private static final Pattern ARTIFACT_ID_PATTERN = Pattern.compile("^[A-Za-z0-9._-]{1,128}$");
    private static final Set<String> RESTRICTED_HTTP_CLIENT_HEADERS =
        Set.of("connection", "content-length", "expect", "host", "upgrade");
    private static final int STREAM_BUFFER_SIZE = 64 * 1024;

    private final CloudTargetConfiguration configuration;
    private final CredentialResolver credentials;
    private final HttpClient httpClient;

    public record FileDigest(String artifactDigest, long contentLength) {
        public FileDigest {
            if (artifactDigest == null || !artifactDigest.matches("^sha256:[0-9a-f]{64}$")) {
                throw new IllegalArgumentException("artifactDigest must use sha256:<64 lowercase hex>");
            }
            if (contentLength < 1L) {
                throw new IllegalArgumentException("Artifact file must be non-empty");
            }
        }

        String sha256Base64() {
            byte[] raw = HexFormat.of().parseHex(artifactDigest.substring("sha256:".length()));
            return Base64.getEncoder().encodeToString(raw);
        }
    }

    public record UploadResult(
        String status,
        String artifactId,
        String artifactUri,
        String bucket,
        String key,
        String versionId,
        String artifactDigest,
        long contentLength
    ) {
        public UploadResult {
            status = status == null ? "" : status;
            artifactId = artifactId == null ? "" : artifactId;
            artifactUri = artifactUri == null ? "" : artifactUri;
            bucket = bucket == null ? "" : bucket;
            key = key == null ? "" : key;
            versionId = versionId == null ? "" : versionId;
            artifactDigest = artifactDigest == null ? "" : artifactDigest;
        }
    }

    private record StagedArtifact(Path snapshotFile, FileDigest digest) {}

    public CloudArtifactUploader(CloudTargetConfiguration configuration, CredentialResolver credentials) {
        this(configuration, credentials, HttpClient.newHttpClient());
    }

    CloudArtifactUploader(
        CloudTargetConfiguration configuration,
        CredentialResolver credentials,
        HttpClient httpClient
    ) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.credentials = Objects.requireNonNull(credentials, "credentials");
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
    }

    public static String validateArtifactId(String artifactId) {
        if (artifactId == null || !ARTIFACT_ID_PATTERN.matcher(artifactId).matches()
            || artifactId.contains("..") || ".".equals(artifactId)) {
            throw new IllegalArgumentException(
                "artifactId must match ^[A-Za-z0-9._-]{1,128}$ without path traversal");
        }
        return artifactId;
    }

    public static FileDigest digestFile(Path file) throws IOException {
        Objects.requireNonNull(file, "file");
        Path normalized = file.toAbsolutePath().normalize();
        if (!Files.isRegularFile(normalized)) {
            throw new IllegalArgumentException("Artifact file does not exist: " + file);
        }
        MessageDigest sha256 = sha256Digest();
        long bytesRead = 0L;
        byte[] buffer = new byte[STREAM_BUFFER_SIZE];
        try (InputStream input = Files.newInputStream(normalized)) {
            int read;
            while ((read = input.read(buffer)) != -1) {
                sha256.update(buffer, 0, read);
                bytesRead += read;
            }
        }
        if (bytesRead < 1L) {
            throw new IllegalArgumentException("Artifact file is empty: " + file);
        }
        long expectedSize = Files.size(normalized);
        if (bytesRead != expectedSize) {
            throw new IOException("Artifact file size changed while computing digest: " + file);
        }
        String digest = "sha256:" + HexFormat.of().formatHex(sha256.digest());
        return new FileDigest(digest, bytesRead);
    }

    public UploadResult upload(String artifactId, Path file) throws DeploymentException, IOException {
        String safeArtifactId = validateArtifactId(artifactId);
        Path normalizedFile = Objects.requireNonNull(file, "file").toAbsolutePath().normalize();
        // Stage an immutable temporary snapshot while hashing so TOCTOU file replacement cannot alter uploaded bytes.
        StagedArtifact staged = stageArtifact(normalizedFile);
        try {
            return uploadWithDigest(safeArtifactId, staged.snapshotFile(), staged.digest(), false);
        } finally {
            Files.deleteIfExists(staged.snapshotFile());
        }
    }

    private static StagedArtifact stageArtifact(Path file) throws IOException {
        if (!Files.isRegularFile(file)) {
            throw new IllegalArgumentException("Artifact file does not exist: " + file);
        }
        Path snapshot = Files.createTempFile("tpf-artifact-", ".upload");
        boolean keepSnapshot = false;
        try {
            MessageDigest sha256 = sha256Digest();
            long bytesRead = 0L;
            byte[] buffer = new byte[STREAM_BUFFER_SIZE];
            try (InputStream input = Files.newInputStream(file);
                 OutputStream output = Files.newOutputStream(snapshot)) {
                int read;
                while ((read = input.read(buffer)) != -1) {
                    sha256.update(buffer, 0, read);
                    output.write(buffer, 0, read);
                    bytesRead += read;
                }
            }
            if (bytesRead < 1L) {
                throw new IllegalArgumentException("Artifact file is empty: " + file);
            }
            String digest = "sha256:" + HexFormat.of().formatHex(sha256.digest());
            keepSnapshot = true;
            return new StagedArtifact(snapshot, new FileDigest(digest, bytesRead));
        } finally {
            if (!keepSnapshot) {
                Files.deleteIfExists(snapshot);
            }
        }
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest algorithm is unavailable", e);
        }
    }

    private UploadResult uploadWithDigest(
        String artifactId,
        Path snapshotFile,
        FileDigest fileDigest,
        boolean retriedAfterPreconditionFailed
    ) throws DeploymentException {
        JsonNode presignBody = requestPresign(artifactId, fileDigest);
        String status = CloudTarget.requiredText(presignBody, "status");
        String artifactUri = CloudTarget.requiredText(presignBody, "artifactUri");
        String returnedDigest = CloudTarget.requiredText(presignBody, "artifactDigest");
        if (!fileDigest.artifactDigest().equals(returnedDigest)) {
            throw new DeploymentException(
                DeploymentException.FailureClass.TARGET,
                "TPF Cloud presign response digest does not match local artifact digest");
        }
        String bucket = CloudTarget.optionalText(presignBody, "bucket");
        String key = CloudTarget.optionalText(presignBody, "key");
        String versionId = CloudTarget.optionalText(presignBody, "versionId");

        if (STATUS_ALREADY_UPLOADED.equals(status)) {
            return new UploadResult(
                STATUS_ALREADY_UPLOADED,
                artifactId,
                artifactUri,
                bucket,
                key,
                versionId,
                fileDigest.artifactDigest(),
                fileDigest.contentLength());
        }
        if (!STATUS_UPLOAD_REQUIRED.equals(status)) {
            throw new DeploymentException(
                DeploymentException.FailureClass.TARGET, "Unexpected TPF Cloud presign status " + status);
        }

        String method = CloudTarget.optionalText(presignBody, "method");
        if (!method.isBlank() && !"PUT".equalsIgnoreCase(method)) {
            throw new DeploymentException(
                DeploymentException.FailureClass.TARGET, "Unsupported TPF Cloud presign upload method " + method);
        }
        URI uploadUri = parseUploadUri(CloudTarget.requiredText(presignBody, "uploadUrl"));
        Map<String, String> headers = parseUploadHeaders(presignBody.get("headers"));
        verifyPresignedChecksumHeader(headers, fileDigest);

        HttpResponse<byte[]> putResponse = executePresignedPut(uploadUri, headers, snapshotFile);
        if (putResponse.statusCode() == 412 && !retriedAfterPreconditionFailed) {
            // If-None-Match: * raced with a concurrent upload for the exact digest; re-check presign once.
            return uploadWithDigest(artifactId, snapshotFile, fileDigest, true);
        }
        if (putResponse.statusCode() < 200 || putResponse.statusCode() >= 300) {
            throw new DeploymentException(
                DeploymentException.FailureClass.TARGET,
                "Presigned artifact upload failed with HTTP " + putResponse.statusCode());
        }
        String uploadedVersionId = putResponse.headers().firstValue("x-amz-version-id")
            .map(String::trim)
            .filter(value -> !value.isBlank() && !"null".equals(value))
            .orElse(versionId);
        return new UploadResult(
            STATUS_UPLOADED,
            artifactId,
            artifactUri,
            bucket,
            key,
            uploadedVersionId,
            fileDigest.artifactDigest(),
            fileDigest.contentLength());
    }

    private static void verifyPresignedChecksumHeader(Map<String, String> headers, FileDigest fileDigest)
        throws DeploymentException {
        String expectedBase64 = fileDigest.sha256Base64();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if ("x-amz-checksum-sha256".equalsIgnoreCase(entry.getKey())) {
                String provided = entry.getValue() == null ? "" : entry.getValue().trim();
                if (!expectedBase64.equals(provided)) {
                    throw new DeploymentException(
                        DeploymentException.FailureClass.TARGET,
                        "TPF Cloud presign response x-amz-checksum-sha256 does not match local artifact digest");
                }
            }
        }
    }

    private JsonNode requestPresign(String artifactId, FileDigest fileDigest) throws DeploymentException {
        Credential.Bearer bearer = CloudTarget.resolveBearer(credentials, configuration.credential());
        String path = PRESIGN_PATH.formatted(
            CloudTargetConfiguration.encodePathSegment(configuration.organization()),
            CloudTargetConfiguration.encodePathSegment(configuration.application()),
            CloudTargetConfiguration.encodePathSegment(configuration.environment()));
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("artifactId", artifactId);
        payload.put("artifactDigest", fileDigest.artifactDigest());
        payload.put("contentLength", fileDigest.contentLength());
        byte[] bodyBytes;
        try {
            bodyBytes = JSON.writeValueAsBytes(payload);
        } catch (IOException e) {
            throw new DeploymentException(
                DeploymentException.FailureClass.TARGET, "Failed to encode artifact presign request", e);
        }
        HttpRequest request = HttpRequest.newBuilder(configuration.endpointUri(path))
            .timeout(Duration.ofSeconds(60))
            .header("Authorization", "Bearer " + bearer.token())
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
            .build();
        try {
            HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() == 401 || response.statusCode() == 403) {
                throw new DeploymentException(
                    DeploymentException.FailureClass.AUTHENTICATION,
                    "TPF Cloud rejected artifact upload access; run tpf auth login or check the CI credential, organisation and deploy permission");
            }
            if (response.statusCode() == 409) {
                throw new DeploymentException(
                    DeploymentException.FailureClass.TARGET,
                    "TPF Cloud rejected artifact presign with HTTP 409 (immutable artifact digest or size conflict)");
            }
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new DeploymentException(
                    DeploymentException.FailureClass.TARGET,
                    "TPF Cloud artifact presign returned HTTP " + response.statusCode());
            }
            return JSON.readTree(response.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DeploymentException(
                DeploymentException.FailureClass.TARGET, "Cloud artifact presign request interrupted", e);
        } catch (IOException e) {
            throw new DeploymentException(
                DeploymentException.FailureClass.TARGET, "Cloud artifact presign request failed", e);
        }
    }

    private HttpResponse<byte[]> executePresignedPut(URI uploadUri, Map<String, String> headers, Path snapshotFile)
        throws DeploymentException {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder(uploadUri)
                .timeout(Duration.ofMinutes(5))
                .PUT(HttpRequest.BodyPublishers.ofFile(snapshotFile));
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                String name = entry.getKey();
                if (RESTRICTED_HTTP_CLIENT_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
                    continue;
                }
                builder.header(name, entry.getValue());
            }
            return httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DeploymentException(
                DeploymentException.FailureClass.TARGET, "Presigned artifact upload interrupted");
        } catch (IOException | IllegalArgumentException e) {
            // Never propagate the raw exception or URI because presigned URLs carry bearer-equivalent signatures.
            throw new DeploymentException(
                DeploymentException.FailureClass.TARGET,
                "Presigned artifact upload failed (" + e.getClass().getSimpleName() + ")");
        }
    }

    private static URI parseUploadUri(String rawUploadUrl) throws DeploymentException {
        try {
            URI uri = URI.create(rawUploadUrl);
            CloudTargetConfiguration.validateEndpointUri(uri, "Presigned upload URL");
            return uri;
        } catch (IllegalArgumentException e) {
            // Never include rawUploadUrl in the exception message.
            throw new DeploymentException(
                DeploymentException.FailureClass.TARGET,
                "TPF Cloud returned an invalid or non-HTTPS presigned upload URL");
        }
    }

    private static Map<String, String> parseUploadHeaders(JsonNode headersNode) throws DeploymentException {
        Map<String, String> headers = new LinkedHashMap<>();
        if (headersNode == null || headersNode.isNull()) {
            return headers;
        }
        if (!headersNode.isObject()) {
            throw new DeploymentException(
                DeploymentException.FailureClass.TARGET, "TPF Cloud presign response headers must be a JSON object");
        }
        Iterator<Map.Entry<String, JsonNode>> fields = headersNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            if (!entry.getValue().isTextual()) {
                throw new DeploymentException(
                    DeploymentException.FailureClass.TARGET,
                    "TPF Cloud presign response header value must be text for " + entry.getKey());
            }
            headers.put(entry.getKey(), entry.getValue().asText());
        }
        return headers;
    }
}
