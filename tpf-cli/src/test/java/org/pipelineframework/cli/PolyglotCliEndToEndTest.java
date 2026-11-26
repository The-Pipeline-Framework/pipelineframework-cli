package org.pipelineframework.cli;

import java.io.PrintWriter;
import java.io.StringWriter;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.TpfLocalDevRuntime;
import org.pipelineframework.config.pipeline.PipelineJson;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end verification of {@code tpf generate} (TS &amp; Python) and crash-safe {@code tpf dev}
 * wired against a live generated Node TypeScript Foreign Function &amp; Tier-2 Custom Connector host.
 */
class PolyglotCliEndToEndTest {

    private static final ObjectMapper JSON = PipelineJson.mapper();
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
        Path yaml = temp.resolve("pipeline.yaml");
        Files.writeString(yaml, AWAIT_PIPELINE_YAML);
        Path tsOut = temp.resolve("gen-ts");
        Path dataDir = temp.resolve("dev-state");

        int genCode = new CommandLine(new TpfCli())
            .execute("generate", "--pipeline", yaml.toString(), "--lang", "ts", "--out", tsOut.toString());
        assertEquals(0, genCode);

        // Write a real Node app using the generated host.ts & types.ts
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
        HttpServer devServer1 = null;
        HttpServer devServer2 = null;
        try {
            String hostUrl = null;
            try (var reader = nodeProc.inputReader(StandardCharsets.UTF_8)) {
                long deadline = System.currentTimeMillis() + 10_000;
                while (System.currentTimeMillis() < deadline) {
                    String line = reader.readLine();
                    if (line == null) break;
                    if (line.startsWith("READY ")) {
                        hostUrl = line.substring("READY ".length()).trim();
                        break;
                    }
                }
            }
            assertNotNull(hostUrl, "Generated TS host failed to start");
            URI functionHostUri = URI.create(hostUrl);

            // Instance #1 of TpfLocalDevRuntime
            TpfLocalDevRuntime dev1 = new TpfLocalDevRuntime(
                yaml, functionHostUri, Optional.of(functionHostUri),
                SHARED_SECRET, dataDir, 2, Map.of(), true);
            devServer1 = dev1.startHttp(0, System.nanoTime(), new PrintWriter(new StringWriter(), true));
            URI devUri1 = URI.create("http://127.0.0.1:" + devServer1.getAddress().getPort());
            HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .build();

            // 1. Submit run -> Step 0 runs in Node, Step 1 suspends in durable Await on disk
            JsonNode run1 = postJson(client, devUri1.resolve("/runs"), """
                {"tenantId":"t1","executionKey":"exec-await-1","payload":{"orderSeq":9007199254740993,"amount":99999999999999999999.123456789,"mode":"OK"}}""");
            assertEquals("WAITING_EXTERNAL", run1.path("status").asText(), run1.toString());
            String executionId = run1.path("executionId").asText();
            String interactionId = run1.path("interactionId").asText();
            String correlationId = run1.path("correlationId").asText();
            assertFalse(interactionId.isBlank());

            // 2. Simulate hard restart: stop devServer1 and spin up brand-new TpfLocalDevRuntime #2 on same dataDir
            devServer1.stop(0);
            devServer1 = null;

            TpfLocalDevRuntime dev2 = new TpfLocalDevRuntime(
                yaml, functionHostUri, Optional.of(functionHostUri),
                SHARED_SECRET, dataDir, 2, Map.of(), true);
            devServer2 = dev2.startHttp(0, System.nanoTime(), new PrintWriter(new StringWriter(), true));
            URI devUri2 = URI.create("http://127.0.0.1:" + devServer2.getAddress().getPort());

            // Reloaded state from disk matches WAITING_EXTERNAL
            JsonNode reloaded = getJson(client, devUri2.resolve("/runs/t1/" + executionId));
            assertEquals("WAITING_EXTERNAL", reloaded.path("status").asText());
            assertEquals(interactionId, reloaded.path("interactionId").asText());

            // Complete Await on instance #2 -> resumes ONLY Step 2 (finalizeOrder); Step 0 is NOT re-executed!
            JsonNode completed = postJson(client, devUri2.resolve("/awaits/complete"),
                "{\"tenantId\":\"t1\",\"interactionId\":\"" + interactionId
                    + "\",\"correlationId\":\"" + correlationId
                    + "\",\"payload\":{\"orderSeq\":9007199254740994,\"amount\":99999999999999999999.123456789,\"approver\":\"alice\"}}");
            assertEquals("SUCCEEDED", completed.path("status").asText(), completed.toString());
            assertEquals("9007199254740994", completed.path("result").path("orderSeq").asText());
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

            // 4. TRANSIENT -> WAIT_RETRY -> sweep retries and reaches WAITING_EXTERNAL
            JsonNode retryRun = postJson(client, devUri2.resolve("/runs"), """
                {"tenantId":"t1","executionKey":"exec-retry-1","payload":{"orderSeq":20,"amount":2.50,"mode":"TRANSIENT_ONCE"}}""");
            assertEquals("WAIT_RETRY", retryRun.path("status").asText(), retryRun.toString());
            String retryExecId = retryRun.path("executionId").asText();
            Thread.sleep(25);
            postJson(client, devUri2.resolve("/sweep"),
                "{\"nowEpochMs\":" + (System.currentTimeMillis() + 60_000L) + "}");
            JsonNode afterSweep = getJson(client, devUri2.resolve("/runs/t1/" + retryExecId));
            assertEquals("WAITING_EXTERNAL", afterSweep.path("status").asText(), afterSweep.toString());
        } finally {
            if (devServer1 != null) devServer1.stop(0);
            if (devServer2 != null) devServer2.stop(0);
            nodeProc.destroy();
            if (!nodeProc.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                nodeProc.destroyForcibly();
            }
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
