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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.TpfLocalDevRuntime;
import org.pipelineframework.config.pipeline.PipelineJson;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end verification of {@code tpf generate} (TS &amp; Python) and crash-safe {@code tpf dev}
 * wired against a live generated Node TypeScript Foreign Function host and bundled {@code http.client}
 * Connector with persisted command effects and query captures across restarts.
 */
class PolyglotCliEndToEndTest {

    private static final ObjectMapper JSON = PipelineJson.mapper().copy()
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN);
    private static final String SHARED_SECRET = "tpf-cli-e2e-shared-secret";

    private static final String AWAIT_PIPELINE_YAML = """
        version: 3
        appName: OrderApprovalFlow
        basePackage: com.example.orders
        transport: LOCAL
        platform: COMPUTE
        types:
          OrderRequest:
            fields:
              - name: orderSeq
                type: int64
              - name: amount
                type: decimal
              - name: mode
                type: string
          ValidatedOrder:
            fields:
              - name: orderSeq
                type: int64
              - name: amount
                type: decimal
          ApprovalDecision:
            fields:
              - name: orderSeq
                type: int64
              - name: amount
                type: decimal
              - name: approver
                type: string
          OrderReceipt:
            fields:
              - name: orderSeq
                type: int64
              - name: settledAmount
                type: decimal
              - name: approver
                type: string
        steps:
          - name: Validate Order
            cardinality: ONE_TO_ONE
            input: OrderRequest
            output: ValidatedOrder
          - name: Await Approval
            cardinality: ONE_TO_ONE
            input: ValidatedOrder
            output: ApprovalDecision
          - name: Finalize Order
            cardinality: ONE_TO_ONE
            input: ApprovalDecision
            output: OrderReceipt
        """;

    @Test
    void generateCommandEmitsClassFreeContractsAndStubsForTypeScriptAndPython(@TempDir Path temp) throws Exception {
        Path yaml = temp.resolve("pipeline.yaml");
        Files.writeString(yaml, AWAIT_PIPELINE_YAML);
        Path tsOut = temp.resolve("out-ts");
        Path pyOut = temp.resolve("out-py");

        StringWriter tsStdout = new StringWriter();
        StringWriter tsStderr = new StringWriter();
        int tsCode = new CommandLine(new TpfCli())
            .setOut(new PrintWriter(tsStdout, true))
            .setErr(new PrintWriter(tsStderr, true))
            .execute("generate", "--pipeline", yaml.toString(), "--lang", "ts", "--out", tsOut.toString(), "--output", "json");
        assertEquals(0, tsCode, tsStderr.toString());
        JsonNode tsJson = JSON.readTree(tsStdout.toString());
        assertEquals("GENERATED", tsJson.path("status").asText());
        assertEquals("OrderApprovalFlow", tsJson.path("pipelineId").asText());
        assertEquals("typescript", tsJson.path("target").asText());
        assertTrue(Files.isRegularFile(tsOut.resolve("types.ts")));
        assertTrue(Files.isRegularFile(tsOut.resolve("host.ts")));
        assertTrue(Files.isRegularFile(tsOut.resolve("META-INF/pipeline/pipeline-contract.json")));

        StringWriter pyStdout = new StringWriter();
        StringWriter pyStderr = new StringWriter();
        int pyCode = new CommandLine(new TpfCli())
            .setOut(new PrintWriter(pyStdout, true))
            .setErr(new PrintWriter(pyStderr, true))
            .execute("generate", "--pipeline", yaml.toString(), "--lang", "py", "--out", pyOut.toString(), "--output", "json");
        assertEquals(0, pyCode, pyStderr.toString());
        JsonNode pyJson = JSON.readTree(pyStdout.toString());
        assertEquals("python", pyJson.path("target").asText());
        assertEquals(tsJson.path("contractVersion").asText(), pyJson.path("contractVersion").asText());
        assertTrue(Files.isRegularFile(pyOut.resolve("types.py")));
        assertTrue(Files.isRegularFile(pyOut.resolve("host.py")));
    }

    @Test
    void localDevEngineSurvivesRestartAcrossDurableAwaitAndClassifiesFailures(@TempDir Path temp) throws Exception {
        requireNodeWithTypeStripping();

        Path yaml = temp.resolve("pipeline.yaml");
        Files.writeString(yaml, AWAIT_PIPELINE_YAML);
        Path tsOut = temp.resolve("gen-ts");
        Path dataDir = temp.resolve("dev-state");

        int genCode = new CommandLine(new TpfCli())
            .execute("generate", "--pipeline", yaml.toString(), "--lang", "ts", "--out", tsOut.toString());
        assertEquals(0, genCode);

        Path runner = tsOut.resolve("app.ts");
        Path countsFile = temp.resolve("counts.json");
        Files.writeString(runner, """
            import fs from 'node:fs';
            import { startAppHost } from './host.ts';
            import { TpfBusinessRejectionError, TpfTransientError } from './types.ts';
            import type { FunctionHandlers } from './types.ts';

            const countsPath = process.env.COUNTS_FILE!;
            const counts = { validate: 0, finalize: 0, transientOnce: 0 };
            const save = () => fs.writeFileSync(countsPath, JSON.stringify(counts));
            save();

            const handlers: FunctionHandlers = {
              async validateOrder(input) {
                counts.validate += 1;
                save();
                if (input.mode === 'REJECT') {
                  throw new TpfBusinessRejectionError('INVALID_ORDER', 'rejected by rule');
                }
                if (input.mode === 'TRANSIENT_ONCE' && counts.transientOnce === 0) {
                  counts.transientOnce += 1;
                  save();
                  throw new TpfTransientError('BUSY', 'try again');
                }
                return { orderSeq: input.orderSeq + 1n, amount: input.amount };
              },
              async awaitApproval() {
                throw new Error('Await is owned by TPF engine');
              },
              async finalizeOrder(input) {
                counts.finalize += 1;
                save();
                return { orderSeq: input.orderSeq, settledAmount: input.amount, approver: input.approver };
              },
            };

            const running = await startAppHost(handlers, { sharedSecret: process.env.TPF_SECRET! });
            console.log('READY ' + running.baseUrl);
            process.on('SIGTERM', async () => { await running.close(); process.exit(0); });
            """);

        ProcessBuilder pb = new ProcessBuilder(List.of("node", runner.toString()))
            .directory(tsOut.toFile())
            .redirectErrorStream(true);
        pb.environment().put("TPF_SECRET", SHARED_SECRET);
        pb.environment().put("COUNTS_FILE", countsFile.toString());
        Process nodeProc = pb.start();
        try {
            String hostUrl = awaitReadyLine(nodeProc, Duration.ofSeconds(10));
            URI functionHostUri = URI.create(hostUrl);
            HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .build();

            String executionId;
            String interactionId;
            String correlationId;
            try (TpfLocalDevRuntime dev1 = new TpfLocalDevRuntime(
                yaml, functionHostUri, Optional.of(functionHostUri),
                SHARED_SECRET, dataDir, 2, Map.of(), true)) {
                HttpServer devServer1 = dev1.startHttp(0, System.nanoTime(), new PrintWriter(new StringWriter(), true));
                URI devUri1 = URI.create("http://127.0.0.1:" + devServer1.getAddress().getPort());

                // 1. Submit run -> Step 0 runs in Node, Step 1 suspends in durable Await on disk
                JsonNode run1 = postJson(client, devUri1.resolve("/runs"), """
                    {"tenantId":"t1","executionKey":"exec-await-1","payload":{"orderSeq":9007199254740993,"amount":12345.6789,"mode":"OK"}}""");
                assertEquals("WAITING_EXTERNAL", run1.path("status").asText(), run1.toString());
                executionId = run1.path("executionId").asText();
                interactionId = run1.path("interactionId").asText();
                correlationId = run1.path("correlationId").asText();
                assertFalse(interactionId.isBlank());
                // Verify Step 0's high-precision int64 (> Number.MAX_SAFE_INTEGER) & decimal output persisted in Await request payload.
                assertEquals("9007199254740994", run1.path("awaitRequestPayload").path("orderSeq").asText());
                assertEquals("12345.6789", run1.path("awaitRequestPayload").path("amount").asText());
            }

            // 2. Hard restart: brand-new TpfLocalDevRuntime #2 on the same dataDir
            try (TpfLocalDevRuntime dev2 = new TpfLocalDevRuntime(
                yaml, functionHostUri, Optional.of(functionHostUri),
                SHARED_SECRET, dataDir, 2, Map.of(), true)) {
                HttpServer devServer2 = dev2.startHttp(0, System.nanoTime(), new PrintWriter(new StringWriter(), true));
                URI devUri2 = URI.create("http://127.0.0.1:" + devServer2.getAddress().getPort());

                // Reloaded state from disk preserves high-precision Step 0 output across restart
                JsonNode reloaded = getJson(client, devUri2.resolve("/runs/t1/" + executionId));
                assertEquals("WAITING_EXTERNAL", reloaded.path("status").asText());
                assertEquals(interactionId, reloaded.path("interactionId").asText());
                assertEquals("9007199254740994", reloaded.path("awaitRequestPayload").path("orderSeq").asText());
                assertEquals("12345.6789", reloaded.path("awaitRequestPayload").path("amount").asText());

                // GET /sweep must be rejected with HTTP 405
                HttpResponse<String> getSweep = client.send(
                    HttpRequest.newBuilder(devUri2.resolve("/sweep")).GET().build(),
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                assertEquals(405, getSweep.statusCode());

                // Complete Await using correlationId ONLY (no interactionId) -> resumes ONLY Step 2!
                JsonNode completed = postJson(client, devUri2.resolve("/awaits/complete"),
                    "{\"tenantId\":\"t1\",\"correlationId\":\"" + correlationId
                        + "\",\"payload\":{\"orderSeq\":" + reloaded.path("awaitRequestPayload").path("orderSeq").asText()
                        + ",\"amount\":" + reloaded.path("awaitRequestPayload").path("amount").asText()
                        + ",\"approver\":\"alice\"}}");
                assertEquals("SUCCEEDED", completed.path("status").asText(), completed.toString());
                assertEquals("9007199254740994", completed.path("result").path("orderSeq").asText());
                assertEquals("12345.6789", completed.path("result").path("settledAmount").asText());
                assertEquals("alice", completed.path("result").path("approver").asText());

                JsonNode countsAfterAwait = JSON.readTree(Files.readString(countsFile));
                assertEquals(1, countsAfterAwait.path("validate").asInt(), "Step 0 must not re-run after restart");
                assertEquals(1, countsAfterAwait.path("finalize").asInt(), "Step 2 must run once");

                // 3. BUSINESS_REJECTION -> immediate terminal FAILED with step index & commandId, 0 retries
                JsonNode rejected = postJson(client, devUri2.resolve("/runs"), """
                    {"tenantId":"t1","executionKey":"exec-reject-1","payload":{"orderSeq":10,"amount":1.00,"mode":"REJECT"}}""");
                assertEquals("FAILED", rejected.path("status").asText(), rejected.toString());
                assertEquals(0, rejected.path("failedStepIndex").asInt());
                assertEquals("cmd-validate-order", rejected.path("failedCommandId").asText());

                // 4. TRANSIENT -> WAIT_RETRY -> automatically retried by background timer without client /sweep!
                JsonNode retryRun = postJson(client, devUri2.resolve("/runs"), """
                    {"tenantId":"t1","executionKey":"exec-retry-1","payload":{"orderSeq":20,"amount":2.50,"mode":"TRANSIENT_ONCE"}}""");
                assertEquals("WAIT_RETRY", retryRun.path("status").asText(), retryRun.toString());
                String retryExecId = retryRun.path("executionId").asText();
                JsonNode afterAutoRetry = null;
                long deadline = System.currentTimeMillis() + 5_000L;
                while (System.currentTimeMillis() < deadline) {
                    afterAutoRetry = getJson(client, devUri2.resolve("/runs/t1/" + retryExecId));
                    if ("WAITING_EXTERNAL".equals(afterAutoRetry.path("status").asText())) {
                        break;
                    }
                    Thread.sleep(50);
                }
                assertNotNull(afterAutoRetry);
                assertEquals("WAITING_EXTERNAL", afterAutoRetry.path("status").asText(), afterAutoRetry.toString());
            }
        } finally {
            nodeProc.destroy();
            if (!nodeProc.waitFor(5, TimeUnit.SECONDS)) {
                nodeProc.destroyForcibly();
            }
        }
    }

    @Test
    void localDevWiresBundledHttpClientConnectorAndPersistsCommandEffectsAndQueryCapturesAcrossRestart(
        @TempDir Path temp
    ) throws Exception {
        AtomicInteger lookupCalls = new AtomicInteger();
        AtomicInteger chargeCalls = new AtomicInteger();
        AtomicReference<String> seenAuth = new AtomicReference<>("");
        AtomicReference<String> seenIdemKey = new AtomicReference<>("");

        HttpServer upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        upstream.createContext("/api/customers/", ex -> {
            lookupCalls.incrementAndGet();
            seenAuth.set(ex.getRequestHeaders().getFirst("Authorization"));
            String path = ex.getRequestURI().getPath();
            String customerId = path.substring("/api/customers/".length());
            byte[] resp = ("{\"customerId\":\"" + customerId + "\",\"segment\":\"gold\"}")
                .getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(200, resp.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(resp);
            }
        });
        upstream.createContext("/api/charges", ex -> {
            chargeCalls.incrementAndGet();
            seenIdemKey.set(ex.getRequestHeaders().getFirst("Idempotency-Key"));
            byte[] resp = "{\"chargeId\":\"ch-9001\",\"status\":\"CAPTURED\"}"
                .getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(201, resp.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(resp);
            }
        });
        upstream.start();
        try {
            String upstreamBase = "http://127.0.0.1:" + upstream.getAddress().getPort() + "/api";
            Path yaml = temp.resolve("http-pipeline.yaml");
            Files.writeString(yaml, """
                version: 3
                appName: BillingHttpFlow
                basePackage: com.example.billing
                transport: LOCAL
                platform: COMPUTE
                connectors:
                  billing-http:
                    provider: http.client
                    version: 1
                    config:
                      baseUri: %s
                      connection: billing-api
                      operations:
                        customer.lookup:
                          method: GET
                          path: /customers/{customerId}
                          queryParams:
                            - tier
                          successStatus: "200"
                        payment.charge:
                          method: POST
                          path: /charges
                          idempotencyHeader: Idempotency-Key
                          successStatus: "201"
                types:
                  CustomerLookup:
                    fields:
                      - name: customerId
                        type: string
                      - name: tier
                        type: string
                  CustomerProfile:
                    fields:
                      - name: customerId
                        type: string
                      - name: segment
                        type: string
                  ChargeReceipt:
                    fields:
                      - name: chargeId
                        type: string
                      - name: status
                        type: string
                steps:
                  - name: Lookup Customer
                    kind: query
                    using: billing-http
                    operation: customer.lookup
                    operationVersion: 1
                    cardinality: ONE_TO_ONE
                    input: CustomerLookup
                    output: CustomerProfile
                  - name: Charge Customer
                    kind: command
                    using: billing-http
                    operation: payment.charge
                    operationVersion: 1
                    cardinality: ONE_TO_ONE
                    input: CustomerProfile
                    output: ChargeReceipt
                """.formatted(upstreamBase));

            Path dataDir = temp.resolve("http-dev-state");
            URI unusedFunctionHost = URI.create("http://127.0.0.1:1");
            HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .build();

            try (TpfLocalDevRuntime dev1 = new TpfLocalDevRuntime(
                yaml, unusedFunctionHost, Optional.of(unusedFunctionHost),
                SHARED_SECRET, dataDir, 1, Map.of("billing-token", "secret-host-bearer"), true)) {
                HttpServer server1 = dev1.startHttp(0, System.nanoTime(), new PrintWriter(new StringWriter(), true));
                URI devUri1 = URI.create("http://127.0.0.1:" + server1.getAddress().getPort());

                JsonNode firstRun = postJson(client, devUri1.resolve("/runs"), """
                    {"tenantId":"t1","executionKey":"bill-1","payload":{"customerId":"cust-42","tier":"vip"}}""");
                assertEquals("SUCCEEDED", firstRun.path("status").asText(), firstRun.toString());
                assertEquals("ch-9001", firstRun.path("result").path("chargeId").asText());
                assertEquals("CAPTURED", firstRun.path("result").path("status").asText());
                assertFalse(seenIdemKey.get().isBlank());
                assertEquals(1, lookupCalls.get());
                assertEquals(1, chargeCalls.get());
            }

            assertTrue(Files.isRegularFile(dataDir.resolve("command-effects.json")));
            assertTrue(Files.isRegularFile(dataDir.resolve("query-captures.json")));

            // Restart tpf dev on the same dataDir and submit a NEW execution with the same deterministic
            // command payload -> FileCommandEffectStore returns RETURN_RECORDED with 0 duplicate POST /api/charges!
            try (TpfLocalDevRuntime dev2 = new TpfLocalDevRuntime(
                yaml, unusedFunctionHost, Optional.of(unusedFunctionHost),
                SHARED_SECRET, dataDir, 1, Map.of("billing-token", "secret-host-bearer"), true)) {
                HttpServer server2 = dev2.startHttp(0, System.nanoTime(), new PrintWriter(new StringWriter(), true));
                URI devUri2 = URI.create("http://127.0.0.1:" + server2.getAddress().getPort());

                JsonNode secondRun = postJson(client, devUri2.resolve("/runs"), """
                    {"tenantId":"t1","executionKey":"bill-2","payload":{"customerId":"cust-42","tier":"vip"}}""");
                assertEquals("SUCCEEDED", secondRun.path("status").asText(), secondRun.toString());
                assertEquals("ch-9001", secondRun.path("result").path("chargeId").asText());
                assertEquals(1, chargeCalls.get(),
                    "Persisted CommandEffectStore must return recorded outcome across restarts without re-dispatching");
            }
        } finally {
            upstream.stop(0);
        }
    }

    private static void requireNodeWithTypeStripping() {
        boolean isCi = "true".equalsIgnoreCase(System.getenv("CI"));
        try {
            Process p = new ProcessBuilder("node", "-v").redirectErrorStream(true).start();
            String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            boolean ok = p.waitFor(5, TimeUnit.SECONDS) && p.exitValue() == 0 && supportsTypeStripping(output);
            String message = "Node.js >= 22.6 is required to run TypeScript host end-to-end tests (found: " + output + ")";
            if (isCi) {
                assertTrue(ok, message);
            } else {
                Assumptions.assumeTrue(ok, message);
            }
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            String message = "Node.js >= 22.6 is required on PATH to run TypeScript host end-to-end tests: " + e.getMessage();
            if (isCi) {
                fail(message);
            } else {
                Assumptions.assumeTrue(false, message);
            }
        }
    }

    private static boolean supportsTypeStripping(String versionOutput) {
        String normalized = versionOutput.startsWith("v") ? versionOutput.substring(1) : versionOutput;
        String[] parts = normalized.split("\\.");
        if (parts.length < 2) {
            return false;
        }
        try {
            int major = Integer.parseInt(parts[0]);
            int minor = Integer.parseInt(parts[1]);
            return major > 22 || (major == 22 && minor >= 6);
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static String awaitReadyLine(Process proc, Duration timeout) throws Exception {
        ExecutorService readerExec = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "node-ready-reader");
            t.setDaemon(true);
            return t;
        });
        try {
            Future<String> future = readerExec.submit(() -> {
                StringBuilder seen = new StringBuilder();
                try (var reader = proc.inputReader(StandardCharsets.UTF_8)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        seen.append(line).append('\n');
                        if (line.startsWith("READY ")) {
                            return line.substring("READY ".length()).trim();
                        }
                    }
                }
                throw new IllegalStateException("Generated TS host exited before READY. Output:\n" + seen);
            });
            try {
                return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
                proc.destroyForcibly();
                fail("Timed out waiting for generated TS host READY line after " + timeout);
                return "";
            } catch (ExecutionException e) {
                throw new AssertionError("Generated TS host failed to start", e.getCause());
            }
        } finally {
            readerExec.shutdownNow();
        }
    }

    private static JsonNode postJson(HttpClient client, URI uri, String body) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(uri)
            .timeout(Duration.ofSeconds(15))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        assertEquals(200, resp.statusCode(), resp.body());
        return JSON.readTree(resp.body());
    }

    private static JsonNode getJson(HttpClient client, URI uri) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(uri)
            .timeout(Duration.ofSeconds(15))
            .GET()
            .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        assertEquals(200, resp.statusCode(), resp.body());
        return JSON.readTree(resp.body());
    }
}
