package org.pipelineframework.cli;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.TpfLocalDevRuntime;
import org.pipelineframework.config.pipeline.PipelineJson;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end verification of Tier-1 bundled {@code object.ingest} and {@code mcp.client}
 * connectors in {@code tpf dev}, covering multi-binding root isolation, host-owned bearer
 * credential isolation from foreign function steps, {@code McpJsonSchema} argument validation,
 * atomic object publication with persisted command-effect replay across restarts, and
 * path-traversal rejection.
 */
class Tier1McpAndObjectIngestConnectorTest {

    private static final ObjectMapper JSON = PipelineJson.mapper().copy()
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN);
    private static final String SHARED_SECRET = "tpf-tier1-shared-secret";
    private static final String MCP_BEARER_SECRET = "host-only-mcp-secret-token-98765";

    @Test
    void wiresObjectIngestForeignFunctionAndMcpClientWithCredentialIsolationAndRestartReplay(
        @TempDir Path temp
    ) throws Exception {
        Path inboxRoot = Files.createDirectories(temp.resolve("inbox"));
        Path outboxRoot = Files.createDirectories(temp.resolve("outbox"));
        Path dataDir = temp.resolve("dev-state");
        Files.writeString(inboxRoot.resolve("cust-42.txt"), "cust-42", StandardCharsets.UTF_8);

        AtomicInteger functionCalls = new AtomicInteger();
        AtomicReference<String> functionAuthHeader = new AtomicReference<>("");
        AtomicReference<String> functionRawRequest = new AtomicReference<>("");
        HttpServer functionHost = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        functionHost.createContext("/tpf/functions/prepare-enrichment", ex -> {
            functionCalls.incrementAndGet();
            String auth = ex.getRequestHeaders().getFirst("Authorization");
            functionAuthHeader.set(auth == null ? "" : auth);
            String reqBody = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            functionRawRequest.set(reqBody);
            JsonNode envelope = JSON.readTree(reqBody);
            String customerId = envelope.path("payload").path("content").asText().trim();
            byte[] resp = JSON.writeValueAsBytes(Map.of(
                "status", "OK",
                "outputTypeId", "EnrichmentQuery",
                "payload", Map.of("customerId", customerId)));
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(200, resp.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(resp);
            }
        });
        functionHost.start();

        AtomicInteger mcpCalls = new AtomicInteger();
        AtomicReference<String> mcpAuthHeader = new AtomicReference<>("");
        AtomicReference<String> mcpToolName = new AtomicReference<>("");
        HttpServer mcpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        mcpServer.createContext("/mcp", ex -> {
            mcpCalls.incrementAndGet();
            String auth = ex.getRequestHeaders().getFirst("Authorization");
            mcpAuthHeader.set(auth == null ? "" : auth);
            JsonNode rpc = JSON.readTree(ex.getRequestBody().readAllBytes());
            String method = rpc.path("method").asText();
            JsonNode params = rpc.path("params");
            mcpToolName.set(params.path("name").asText());
            String customerId = params.path("arguments").path("customerId").asText();
            assertEquals("tools/call", method);
            byte[] resp = JSON.writeValueAsBytes(Map.of(
                "jsonrpc", "2.0",
                "id", rpc.path("id").asText(),
                "result", Map.of(
                    "isError", false,
                    "structuredContent", Map.of(
                        "objectKey", "enriched/" + customerId + ".json",
                        "content", "{\"customerId\":\"" + customerId + "\",\"tier\":\"enterprise\"}",
                        "contentType", "application/json"))));
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(200, resp.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(resp);
            }
        });
        mcpServer.start();

        try {
            URI functionHostUri = URI.create("http://127.0.0.1:" + functionHost.getAddress().getPort());
            String mcpEndpoint = "http://127.0.0.1:" + mcpServer.getAddress().getPort() + "/mcp";
            Path yaml = temp.resolve("tier1-pipeline.yaml");
            Files.writeString(yaml, """
                version: 3
                appName: ObjectMcpEnrichmentFlow
                basePackage: com.example.tier1
                transport: LOCAL
                platform: COMPUTE
                connectors:
                  inbox-files:
                    provider: object.ingest
                    version: 1
                    config:
                      root: %s
                      maxBytes: 1048576
                  outbox-files:
                    provider: object.ingest
                    version: 1
                    config:
                      root: %s
                      maxBytes: 1048576
                  crm-mcp:
                    provider: mcp.client
                    version: 1
                    config:
                      endpointUri: %s
                      connection: crm-mcp-conn
                      bearerTokenSecret: crm-mcp-token
                      operations:
                        customer.enrich:
                          toolName: enrich_customer
                          inputSchema:
                            type: object
                            required:
                              - customerId
                            properties:
                              customerId:
                                type: string
                                minLength: 3
                types:
                  ObjectReadRequest:
                    fields:
                      - name: objectKey
                        type: string
                  IngestedObject:
                    fields:
                      - name: objectKey
                        type: string
                      - name: content
                        type: string
                      - name: sha256
                        type: string
                      - name: sizeBytes
                        type: int64
                  EnrichmentQuery:
                    fields:
                      - name: customerId
                        type: string
                  PublishObjectRequest:
                    fields:
                      - name: objectKey
                        type: string
                      - name: content
                        type: string
                      - name: contentType
                        type: string
                  PublishedObjectReceipt:
                    fields:
                      - name: objectKey
                        type: string
                      - name: sha256
                        type: string
                      - name: sizeBytes
                        type: int64
                steps:
                  - name: Read Source Object
                    kind: query
                    using: inbox-files
                    operation: object.read
                    operationVersion: 1
                    cardinality: ONE_TO_ONE
                    input: ObjectReadRequest
                    output: IngestedObject
                  - name: Prepare Enrichment
                    cardinality: ONE_TO_ONE
                    input: IngestedObject
                    output: EnrichmentQuery
                  - name: Enrich Customer Via Mcp
                    kind: query
                    using: crm-mcp
                    operation: customer.enrich
                    operationVersion: 1
                    cardinality: ONE_TO_ONE
                    input: EnrichmentQuery
                    output: PublishObjectRequest
                  - name: Publish Enriched Object
                    kind: command
                    using: outbox-files
                    operation: object.write
                    operationVersion: 1
                    cardinality: ONE_TO_ONE
                    input: PublishObjectRequest
                    output: PublishedObjectReceipt
                """.formatted(inboxRoot, outboxRoot, mcpEndpoint));

            HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .build();
            Map<String, String> connectorSecrets = Map.of("crm-mcp-token", MCP_BEARER_SECRET);

            String firstExecutionId;
            try (TpfLocalDevRuntime dev1 = new TpfLocalDevRuntime(
                yaml, functionHostUri, Optional.of(functionHostUri),
                SHARED_SECRET, dataDir, 1, connectorSecrets, true)) {
                HttpServer server1 = dev1.startHttp(0, System.nanoTime(), new PrintWriter(new StringWriter(), true));
                URI devUri1 = URI.create("http://127.0.0.1:" + server1.getAddress().getPort());

                JsonNode run1 = postJson(client, devUri1.resolve("/runs"), """
                    {"tenantId":"t1","executionKey":"tier1-run-1","payload":{"objectKey":"cust-42.txt"}}""");
                assertEquals("SUCCEEDED", run1.path("status").asText(), run1.toString());
                firstExecutionId = run1.path("executionId").asText();
                String expectedContent = "{\"customerId\":\"cust-42\",\"tier\":\"enterprise\"}";
                assertEquals("enriched/cust-42.json", run1.path("result").path("objectKey").asText());
                assertEquals(expectedContent.getBytes(StandardCharsets.UTF_8).length,
                    run1.path("result").path("sizeBytes").asLong());
                assertFalse(run1.path("result").path("sha256").asText().isBlank());

                // Multi-binding isolation: output written ONLY to outboxRoot, never inboxRoot
                Path published = outboxRoot.resolve("enriched/cust-42.json");
                assertTrue(Files.isRegularFile(published), "Expected atomic output in outboxRoot");
                assertFalse(Files.exists(inboxRoot.resolve("enriched/cust-42.json")));
                assertEquals(expectedContent, Files.readString(published, StandardCharsets.UTF_8));

                // Credential isolation: host sent bearer token to MCP server, NEVER to foreign function
                assertEquals("Bearer " + MCP_BEARER_SECRET, mcpAuthHeader.get());
                assertEquals("enrich_customer", mcpToolName.get());
                assertEquals(1, functionCalls.get());
                assertEquals(1, mcpCalls.get());
                assertTrue(functionAuthHeader.get().isEmpty(), "Foreign function must never receive Authorization header");
                assertFalse(functionRawRequest.get().contains(MCP_BEARER_SECRET),
                    "Foreign function request must never contain host-owned connector secret");

                // Path-traversal attempt on inbox-files fails terminally without escaping root
                JsonNode escapeRun = postJson(client, devUri1.resolve("/runs"), """
                    {"tenantId":"t1","executionKey":"tier1-escape-1","payload":{"objectKey":"../../etc/passwd"}}""");
                assertEquals("FAILED", escapeRun.path("status").asText(), escapeRun.toString());

                // McpJsonSchema rejects short customerId ("x" < minLength 3) BEFORE invoking MCP HTTP server
                Files.writeString(inboxRoot.resolve("short.txt"), "x", StandardCharsets.UTF_8);
                int mcpCallsBeforeInvalid = mcpCalls.get();
                JsonNode invalidMcpRun = postJson(client, devUri1.resolve("/runs"), """
                    {"tenantId":"t1","executionKey":"tier1-invalid-mcp-1","payload":{"objectKey":"short.txt"}}""");
                assertEquals("FAILED", invalidMcpRun.path("status").asText(), invalidMcpRun.toString());
                assertEquals(mcpCallsBeforeInvalid, mcpCalls.get(),
                    "McpJsonSchema must reject invalid arguments before sending HTTP request");
            }

            // Hard restart on same dataDir: re-submitting same executionKey replays persisted result cleanly
            try (TpfLocalDevRuntime dev2 = new TpfLocalDevRuntime(
                yaml, functionHostUri, Optional.of(functionHostUri),
                SHARED_SECRET, dataDir, 1, connectorSecrets, true)) {
                HttpServer server2 = dev2.startHttp(0, System.nanoTime(), new PrintWriter(new StringWriter(), true));
                URI devUri2 = URI.create("http://127.0.0.1:" + server2.getAddress().getPort());

                JsonNode replayed = postJson(client, devUri2.resolve("/runs"), """
                    {"tenantId":"t1","executionKey":"tier1-run-1","payload":{"objectKey":"cust-42.txt"}}""");
                assertEquals(firstExecutionId, replayed.path("executionId").asText());
                assertEquals("SUCCEEDED", replayed.path("status").asText());
                assertEquals(1, mcpCalls.get(), "Replayed execution must not re-invoke MCP server");
            }
        } finally {
            functionHost.stop(0);
            mcpServer.stop(0);
        }
    }

    private static JsonNode postJson(HttpClient client, URI uri, String body) throws IOException, InterruptedException {
        HttpResponse<String> resp = client.send(
            HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build(),
            HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        assertEquals(200, resp.statusCode(), "Unexpected HTTP " + resp.statusCode() + ": " + resp.body());
        return JSON.readTree(resp.body());
    }
}
