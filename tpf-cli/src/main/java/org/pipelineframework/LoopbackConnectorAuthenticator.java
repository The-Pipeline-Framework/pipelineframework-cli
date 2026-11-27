package org.pipelineframework;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.orchestrator.TransitionWorkerSignature;

/**
 * Mutual per-session HMAC-SHA256 challenge/response authenticator for local Tier-2
 * Polyglot Connector listeners over plaintext loopback HTTP (ADR-0092 §4).
 *
 * <p>Prevents scoped connector credentials from ever being transmitted to an unauthenticated
 * loopback listener in {@code tpf dev}. Fails closed on any transport error, timeout, non-200
 * status, oversized body, challenge mismatch, or invalid HMAC proof.</p>
 */
public final class LoopbackConnectorAuthenticator {

    public static final String CHALLENGE_PATH = "/tpf/connector/auth/challenge";
    public static final String PROOF_DOMAIN = "tpf-connector-listener-auth-v1\n";

    private static final int CHALLENGE_BYTES = 32;
    private static final int MAX_RESPONSE_BYTES = 8 * 1024;
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final ObjectMapper JSON = PipelineJson.mapper();

    private LoopbackConnectorAuthenticator() {
    }

    public static boolean isPlaintextLoopbackHttp(URI uri) {
        if (uri == null || uri.getScheme() == null || uri.getHost() == null) {
            return false;
        }
        if (!"http".equalsIgnoreCase(uri.getScheme())) {
            return false;
        }
        String host = uri.getHost().toLowerCase(Locale.ROOT);
        return "127.0.0.1".equals(host)
            || "localhost".equals(host)
            || "::1".equals(host)
            || "[::1]".equals(host);
    }

    public static String computeExpectedProof(String sharedSecret, String challenge) {
        Objects.requireNonNull(sharedSecret, "sharedSecret must not be null");
        Objects.requireNonNull(challenge, "challenge must not be null");
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(sharedSecret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] digest = mac.doFinal((PROOF_DOMAIN + challenge).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 is unavailable", e);
        }
    }

    public static CompletionStage<Void> authenticateAsync(
        HttpClient httpClient,
        URI baseUri,
        String sharedSecret,
        Duration timeout
    ) {
        Objects.requireNonNull(httpClient, "httpClient must not be null");
        Objects.requireNonNull(baseUri, "baseUri must not be null");
        Objects.requireNonNull(sharedSecret, "sharedSecret must not be null");
        Objects.requireNonNull(timeout, "timeout must not be null");
        if (sharedSecret.isBlank()) {
            return CompletableFuture.failedStage(new ConnectionResolutionException(
                "Tier-2 loopback connector listener authentication requires a non-blank shared secret"));
        }
        if (!isPlaintextLoopbackHttp(baseUri)) {
            if ("https".equalsIgnoreCase(baseUri.getScheme())) {
                return CompletableFuture.completedStage(null);
            }
            return CompletableFuture.failedStage(new ConnectionResolutionException(
                "Refusing plaintext non-loopback Tier-2 connector host: " + baseUri));
        }

        byte[] challengeBytes = new byte[CHALLENGE_BYTES];
        RANDOM.nextBytes(challengeBytes);
        String challenge = HexFormat.of().formatHex(challengeBytes);
        byte[] requestBody = ("{\"challenge\":\"" + challenge + "\"}").getBytes(StandardCharsets.UTF_8);

        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            sharedSecret, "POST", CHALLENGE_PATH, timestamp, nonce, requestBody);

        String base = baseUri.toString();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        HttpRequest request = HttpRequest.newBuilder(URI.create(base + CHALLENGE_PATH))
            .timeout(timeout)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header(TransitionWorkerSignature.TIMESTAMP_HEADER, timestamp)
            .header(TransitionWorkerSignature.NONCE_HEADER, nonce)
            .header(TransitionWorkerSignature.SIGNATURE_HEADER, signature)
            .POST(HttpRequest.BodyPublishers.ofByteArray(requestBody))
            .build();

        CompletableFuture<Void> verified = new CompletableFuture<>();
        httpClient.sendAsync(request, boundedByteArrayHandler(MAX_RESPONSE_BYTES))
            .whenComplete((response, failure) -> {
                if (failure != null) {
                    verified.completeExceptionally(new ConnectionResolutionException(
                        "Tier-2 loopback connector host failed listener authentication at " + baseUri + CHALLENGE_PATH,
                        failure));
                    return;
                }
                try {
                    verifyChallengeResponse(baseUri, sharedSecret, challenge, response);
                    verified.complete(null);
                } catch (RuntimeException invalid) {
                    verified.completeExceptionally(invalid);
                }
            });
        return verified;
    }

    private static void verifyChallengeResponse(
        URI baseUri,
        String sharedSecret,
        String expectedChallenge,
        HttpResponse<byte[]> response
    ) {
        if (response.statusCode() != 200) {
            throw new ConnectionResolutionException(
                "Tier-2 loopback connector host rejected listener authentication at " + baseUri + CHALLENGE_PATH
                    + " (HTTP " + response.statusCode() + ")");
        }
        JsonNode root;
        try {
            root = JSON.reader()
                .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .with(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .readTree(response.body());
        } catch (IOException malformed) {
            throw new ConnectionResolutionException(
                "Tier-2 loopback connector host returned malformed JSON for listener authentication", malformed);
        }
        if (root == null || !root.isObject()) {
            throw new ConnectionResolutionException(
                "Tier-2 loopback connector host returned non-object JSON for listener authentication");
        }
        String status = root.path("status").asText("");
        String echoedChallenge = root.path("challenge").asText("");
        String actualProof = root.path("proof").asText("");
        if (!"OK".equals(status) || !expectedChallenge.equals(echoedChallenge) || actualProof.isBlank()) {
            throw new ConnectionResolutionException(
                "Tier-2 loopback connector host returned invalid listener authentication challenge response");
        }
        String expectedProof = computeExpectedProof(sharedSecret, expectedChallenge);
        boolean matches = MessageDigest.isEqual(
            expectedProof.getBytes(StandardCharsets.UTF_8),
            actualProof.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.UTF_8));
        if (!matches) {
            throw new ConnectionResolutionException(
                "Tier-2 loopback connector host failed HMAC proof verification; refusing to transmit scoped secrets");
        }
    }

    private static HttpResponse.BodyHandler<byte[]> boundedByteArrayHandler(int maxBytes) {
        return responseInfo -> new HttpResponse.BodySubscriber<>() {
            private final CompletableFuture<byte[]> body = new CompletableFuture<>();
            private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            private Flow.Subscription subscription;
            private int totalBytes;
            private boolean exceeded;

            @Override
            public void onSubscribe(Flow.Subscription sub) {
                this.subscription = sub;
                sub.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(List<ByteBuffer> items) {
                if (exceeded) {
                    return;
                }
                for (ByteBuffer item : items) {
                    int remaining = item.remaining();
                    if (remaining > maxBytes - totalBytes) {
                        exceeded = true;
                        if (subscription != null) {
                            subscription.cancel();
                        }
                        body.completeExceptionally(new IOException(
                            "Listener authentication response exceeded " + maxBytes + " bytes"));
                        return;
                    }
                    byte[] chunk = new byte[remaining];
                    item.get(chunk);
                    buffer.write(chunk, 0, remaining);
                    totalBytes += remaining;
                }
            }

            @Override
            public void onError(Throwable throwable) {
                body.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                if (!exceeded) {
                    body.complete(buffer.toByteArray());
                }
            }

            @Override
            public CompletionStage<byte[]> getBody() {
                return body;
            }
        };
    }
}
