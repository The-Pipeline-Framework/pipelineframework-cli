package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.orchestrator.TransitionWorkerSignature;

class LoopbackConnectorAuthenticatorTest {

    private static final String SECRET = "loopback-auth-test-secret";
    private static final ObjectMapper JSON = PipelineJson.mapper();
    private static final HttpClient CLIENT = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(Duration.ofSeconds(2))
        .followRedirects(HttpClient.Redirect.NEVER)
        .build();

    @Test
    void detectsPlaintextLoopbackHttpOrigins() {
        assertTrue(LoopbackConnectorAuthenticator.isPlaintextLoopbackHttp(URI.create("http://127.0.0.1:4100")));
        assertTrue(LoopbackConnectorAuthenticator.isPlaintextLoopbackHttp(URI.create("http://localhost:4100")));
        assertTrue(LoopbackConnectorAuthenticator.isPlaintextLoopbackHttp(URI.create("http://[::1]:4100")));
        assertFalse(LoopbackConnectorAuthenticator.isPlaintextLoopbackHttp(URI.create("https://127.0.0.1:4100")));
        assertFalse(LoopbackConnectorAuthenticator.isPlaintextLoopbackHttp(URI.create("http://10.0.0.1:4100")));
        assertFalse(LoopbackConnectorAuthenticator.isPlaintextLoopbackHttp(URI.create("https://api.example.com")));
    }

    @Test
    void acceptsValidMutualHmacChallengeProof() throws Exception {
        AtomicReference<String> seenSignature = new AtomicReference<>("");
        HttpServer server = startServer(exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            String ts = exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.TIMESTAMP_HEADER);
            String nonce = exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.NONCE_HEADER);
            String sig = exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.SIGNATURE_HEADER);
            seenSignature.set(sig == null ? "" : sig);
            String expectedReqSig = TransitionWorkerSignature.sign(
                SECRET, "POST", LoopbackConnectorAuthenticator.CHALLENGE_PATH, ts, nonce, body);
            assertEquals(expectedReqSig, sig);
            String challenge = JSON.readTree(body).path("challenge").asText();
            String proof = LoopbackConnectorAuthenticator.computeExpectedProof(SECRET, challenge);
            respond(exchange, 200, "{\"status\":\"OK\",\"challenge\":\"" + challenge + "\",\"proof\":\"" + proof + "\"}");
        });
        try {
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            assertDoesNotThrow(() -> LoopbackConnectorAuthenticator
                .authenticateAsync(CLIENT, uri, SECRET, Duration.ofSeconds(2))
                .toCompletableFuture()
                .join());
            assertFalse(seenSignature.get().isBlank());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectsWrongProofWrongChallengeAndMissingEndpoint() throws Exception {
        HttpServer wrongProof = startServer(exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            String challenge = JSON.readTree(body).path("challenge").asText();
            String bogus = LoopbackConnectorAuthenticator.computeExpectedProof("wrong-secret", challenge);
            respond(exchange, 200, "{\"status\":\"OK\",\"challenge\":\"" + challenge + "\",\"proof\":\"" + bogus + "\"}");
        });
        HttpServer wrongChallenge = startServer(exchange -> {
            exchange.getRequestBody().readAllBytes();
            String proof = LoopbackConnectorAuthenticator.computeExpectedProof(SECRET, "different-challenge");
            respond(exchange, 200, "{\"status\":\"OK\",\"challenge\":\"different-challenge\",\"proof\":\"" + proof + "\"}");
        });
        HttpServer missingEndpoint = startServer(exchange -> {
            exchange.getRequestBody().readAllBytes();
            respond(exchange, 404, "{\"error\":\"not found\"}");
        });
        HttpServer oversized = startServer(exchange -> {
            exchange.getRequestBody().readAllBytes();
            respond(exchange, 200, "{\"status\":\"OK\",\"padding\":\"" + "x".repeat(16_384) + "\"}");
        });
        try {
            for (HttpServer bad : new HttpServer[] { wrongProof, wrongChallenge, missingEndpoint, oversized }) {
                URI uri = URI.create("http://127.0.0.1:" + bad.getAddress().getPort());
                CompletionException failure = assertThrows(CompletionException.class, () ->
                    LoopbackConnectorAuthenticator.authenticateAsync(CLIENT, uri, SECRET, Duration.ofSeconds(2))
                        .toCompletableFuture()
                        .join());
                assertTrue(failure.getCause() instanceof ConnectionResolutionException,
                    "Expected ConnectionResolutionException, got " + failure.getCause());
            }
        } finally {
            wrongProof.stop(0);
            wrongChallenge.stop(0);
            missingEndpoint.stop(0);
            oversized.stop(0);
        }
    }

    @Test
    void rejectsSlowListenerAndStalledBodyOnTimeoutAndRejectsNonLoopbackHttp() throws Exception {
        HttpServer slow = startServer(exchange -> {
            exchange.getRequestBody().readAllBytes();
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            respond(exchange, 200, "{\"status\":\"OK\"}");
        });
        HttpServer stalledBody = startServer(exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            // Send 200 headers immediately, then withhold the promised body bytes.
            exchange.sendResponseHeaders(200, 128);
            try {
                Thread.sleep(2_000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            exchange.close();
        });
        try {
            URI slowUri = URI.create("http://127.0.0.1:" + slow.getAddress().getPort());
            CompletionException timeout = assertThrows(CompletionException.class, () ->
                LoopbackConnectorAuthenticator.authenticateAsync(CLIENT, slowUri, SECRET, Duration.ofMillis(100))
                    .toCompletableFuture()
                    .join());
            assertTrue(timeout.getCause() instanceof ConnectionResolutionException);

            URI stalledUri = URI.create("http://127.0.0.1:" + stalledBody.getAddress().getPort());
            CompletionException bodyTimeout = assertThrows(CompletionException.class, () ->
                LoopbackConnectorAuthenticator.authenticateAsync(CLIENT, stalledUri, SECRET, Duration.ofMillis(150))
                    .toCompletableFuture()
                    .join());
            assertTrue(bodyTimeout.getCause() instanceof ConnectionResolutionException);

            CompletionException nonLoopback = assertThrows(CompletionException.class, () ->
                LoopbackConnectorAuthenticator.authenticateAsync(
                        CLIENT, URI.create("http://10.0.0.1:4100"), SECRET, Duration.ofSeconds(1))
                    .toCompletableFuture()
                    .join());
            assertTrue(nonLoopback.getCause() instanceof ConnectionResolutionException);

            assertDoesNotThrow(() -> LoopbackConnectorAuthenticator.authenticateAsync(
                    CLIENT, URI.create("https://connector.example.com"), SECRET, Duration.ofSeconds(1))
                .toCompletableFuture()
                .join());
        } finally {
            slow.stop(0);
            stalledBody.stop(0);
        }
    }

    private interface Handler {
        void handle(HttpExchange exchange) throws IOException;
    }

    private static HttpServer startServer(Handler handler) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext(LoopbackConnectorAuthenticator.CHALLENGE_PATH, handler::handle);
        server.start();
        return server;
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
