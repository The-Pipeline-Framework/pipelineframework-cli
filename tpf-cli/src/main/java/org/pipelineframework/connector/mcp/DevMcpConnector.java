package org.pipelineframework.connector.mcp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.connector.CommandCapabilities;
import org.pipelineframework.connector.CommandConfirmation;
import org.pipelineframework.connector.CommandDispatchIdentity;
import org.pipelineframework.connector.CommandExecutionPosture;
import org.pipelineframework.connector.CommandInvocation;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandOperation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.ResolvedConnection;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/**
 * Tier-1 portable MCP Connector ({@code mcp.client} v1) bundled directly into the native {@code tpf}
 * CLI using class-free {@link JsonNode} boundaries, {@link McpJsonSchema} argument validation,
 * host-owned {@link ConnectionResolver} credentials, and JDK {@link HttpClient} JSON-RPC 2.0
 * Streamable HTTP transport (MCP 2025-06-18 lifecycle with {@code initialize},
 * {@code notifications/initialized}, session/protocol headers, JSON &amp; SSE response decoding,
 * and bounded response reception; zero reflection / GraalVM Native Image compatible).
 */
public final class DevMcpConnector implements ConnectorProvider<DevMcpConnector.Config> {

    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("mcp.client");
    public static final String MCP_PROTOCOL_VERSION = "2025-06-18";
    public static final String PROTOCOL_VERSION_HEADER = "MCP-Protocol-Version";
    public static final String SESSION_ID_HEADER = "Mcp-Session-Id";
    private static final String ACCEPT_HEADER = "application/json, text/event-stream";
    private static final ObjectMapper JSON = CanonicalTypeCatalogue.exactJsonMapper();
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(15);

    /** Deployment configuration referencing a host-resolved MCP Streamable HTTP connection. */
    public record Config(ConnectionRef connection) {
        public Config {
            Objects.requireNonNull(connection, "connection must not be null");
        }
    }

    /** Host-owned MCP HTTP connection with optional bearer credential (never exposed to foreign functions). */
    public record Connection(
        HttpClient httpClient,
        URI endpointUri,
        Optional<String> bearerToken,
        int maxResponseBytes
    ) implements ResolvedConnection {
        public Connection {
            Objects.requireNonNull(httpClient, "httpClient must not be null");
            Objects.requireNonNull(endpointUri, "endpointUri must not be null");
            bearerToken = Objects.requireNonNull(bearerToken, "bearerToken must not be null")
                .filter(v -> !v.isBlank());
            if (maxResponseBytes <= 0) {
                throw new IllegalArgumentException("maxResponseBytes must be positive");
            }
        }
    }

    /** Pinned operation binding for one MCP tool invoked as a TPF Query or Command. */
    public record OperationSpec(
        String operationId,
        ConnectorOperationKind kind,
        int majorVersion,
        String mcpToolName,
        String inputType,
        String outputType,
        Optional<McpJsonSchema> inputSchema
    ) {
        public OperationSpec {
            if (operationId == null || operationId.isBlank()) {
                throw new IllegalArgumentException("operationId must not be blank");
            }
            Objects.requireNonNull(kind, "kind must not be null");
            if (mcpToolName == null || mcpToolName.isBlank()) {
                throw new IllegalArgumentException("mcpToolName must not be blank");
            }
            if (inputType == null || inputType.isBlank()) {
                throw new IllegalArgumentException("inputType must not be blank");
            }
            if (outputType == null || outputType.isBlank()) {
                throw new IllegalArgumentException("outputType must not be blank");
            }
            inputSchema = Objects.requireNonNull(inputSchema, "inputSchema must not be null");
        }
    }

    /** Empty per-operation configuration record required by TPF's native command contract. */
    public record OperationConfig() {
    }

    private static final ConnectorConfigSchema<Config> CONFIG_SCHEMA =
        ConnectorConfigSchema.record(Config.class, "mcp.client.provider", 1);
    private static final ConnectorConfigSchema<OperationConfig> OPERATION_CONFIG_SCHEMA =
        ConnectorConfigSchema.record(OperationConfig.class, "mcp.client.operation", 1);

    private final CanonicalTypeCatalogue catalogue;
    private final List<OperationSpec> specs;
    private final List<ConnectorOperation> operations;
    private final AtomicReference<Optional<ActiveBinding>> activeBinding = new AtomicReference<>(Optional.empty());
    private final Object sessionLock = new Object();
    private volatile Optional<McpSessionState> sessionState = Optional.empty();

    public DevMcpConnector(CanonicalTypeCatalogue catalogue, List<OperationSpec> specs) {
        this.catalogue = Objects.requireNonNull(catalogue, "catalogue must not be null");
        this.specs = List.copyOf(Objects.requireNonNull(specs, "specs must not be null"));
        List<ConnectorOperation> ops = new ArrayList<>(this.specs.size());
        for (OperationSpec spec : this.specs) {
            ops.add(spec.kind() == ConnectorOperationKind.QUERY
                ? new McpQueryOperation(spec)
                : new McpCommandOperation(spec));
        }
        this.operations = List.copyOf(ops);
    }

    /** Creates an isolated provider instance sharing the compiled specs for a separate binding. */
    public DevMcpConnector copy() {
        return new DevMcpConnector(catalogue, specs);
    }

    @Override
    public ConnectorProviderId id() {
        return PROVIDER_ID;
    }

    @Override
    public ConnectorProviderVersion version() {
        return new ConnectorProviderVersion(1, 0);
    }

    @Override
    public Collection<? extends ConnectorOperation> operations() {
        return operations;
    }

    @Override
    public Optional<ConnectorConfigSchema<Config>> configurationSchema() {
        return Optional.of(CONFIG_SCHEMA);
    }

    @Override
    public CompletionStage<Void> start(ConnectorRuntimeContext context, Config configuration) {
        synchronized (sessionLock) {
            sessionState = Optional.empty();
        }
        activeBinding.set(Optional.of(new ActiveBinding(context, configuration)));
        return CompletableFuture.completedStage(null);
    }

    @Override
    public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        synchronized (sessionLock) {
            sessionState = Optional.empty();
        }
        activeBinding.set(Optional.empty());
        return CompletableFuture.completedStage(null);
    }

    private CompletionStage<Connection> resolveConnection(ConnectorExecutionContext execCtx) {
        ActiveBinding binding = activeBinding.get().orElseThrow(() ->
            new IllegalStateException("MCP connector binding is not active"));
        ConnectionResolver resolver = binding.context().connectionResolver().orElseThrow(() ->
            new ConnectionResolutionException("No host ConnectionResolver configured for mcp.client"));
        return resolver.resolve(new ConnectionResolutionRequest<>(
            binding.config().connection(), Connection.class, execCtx));
    }

    /**
     * Ensures the MCP Streamable HTTP lifecycle ({@code initialize} followed by
     * {@code notifications/initialized}) has completed for this binding and retains the negotiated
     * protocol version and optional {@code Mcp-Session-Id}.
     */
    private InitOutcome ensureInitialized(Connection conn) {
        Optional<McpSessionState> existing = sessionState;
        if (existing.isPresent() && existing.orElseThrow().endpointUri().equals(conn.endpointUri())) {
            return new InitOutcome(existing, Optional.empty());
        }
        synchronized (sessionLock) {
            existing = sessionState;
            if (existing.isPresent() && existing.orElseThrow().endpointUri().equals(conn.endpointUri())) {
                return new InitOutcome(existing, Optional.empty());
            }
            try {
                String initRequestId = "init-" + UUID.randomUUID();
                Map<String, Object> initParams = new LinkedHashMap<>();
                initParams.put("protocolVersion", MCP_PROTOCOL_VERSION);
                initParams.put("capabilities", Map.of());
                initParams.put("clientInfo", Map.of("name", "tpf-cli", "version", "1.0"));

                Map<String, Object> initRpc = new LinkedHashMap<>();
                initRpc.put("jsonrpc", "2.0");
                initRpc.put("id", initRequestId);
                initRpc.put("method", "initialize");
                initRpc.put("params", initParams);

                HttpResponse<byte[]> initResp = sendBoundedPost(
                    conn, JSON.writeValueAsBytes(initRpc), Optional.empty(), Optional.empty());
                Optional<ToolCallOutcome> statusErr = checkHttpStatus(initResp.statusCode(), false);
                if (statusErr.isPresent()) {
                    return new InitOutcome(Optional.empty(), statusErr);
                }
                JsonNode initRoot = decodeRpcResponse(initResp, initRequestId);
                if (initRoot.hasNonNull("error")) {
                    return new InitOutcome(Optional.empty(), Optional.of(rpcErrorOutcome(initRoot.get("error"))));
                }
                String negotiatedVersion = initRoot.path("result").path("protocolVersion").asText("").trim();
                if (negotiatedVersion.isEmpty()) {
                    negotiatedVersion = MCP_PROTOCOL_VERSION;
                }
                Optional<String> returnedSessionId = initResp.headers()
                    .firstValue(SESSION_ID_HEADER)
                    .map(String::trim)
                    .filter(v -> !v.isEmpty());
                McpSessionState established = new McpSessionState(
                    conn.endpointUri(), negotiatedVersion, returnedSessionId);

                Map<String, Object> initializedNotification = new LinkedHashMap<>();
                initializedNotification.put("jsonrpc", "2.0");
                initializedNotification.put("method", "notifications/initialized");
                HttpResponse<byte[]> notifResp = sendBoundedPost(
                    conn, JSON.writeValueAsBytes(initializedNotification), Optional.of(established), Optional.empty());
                Optional<ToolCallOutcome> notifErr = checkHttpStatus(notifResp.statusCode(), true);
                if (notifErr.isPresent()) {
                    return new InitOutcome(Optional.empty(), notifErr);
                }

                sessionState = Optional.of(established);
                return new InitOutcome(Optional.of(established), Optional.empty());
            } catch (ResponseTooLargeException tooLarge) {
                return new InitOutcome(Optional.empty(), Optional.of(new ToolCallOutcome(
                    CallStatus.TERMINAL_ERROR, Optional.empty(),
                    "mcp-response-too-large", tooLarge.getMessage())));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return new InitOutcome(Optional.empty(), Optional.of(new ToolCallOutcome(
                    CallStatus.RETRYABLE_ERROR, Optional.empty(),
                    "mcp-init-interrupted", "Interrupted initializing MCP session")));
            } catch (IOException transport) {
                return new InitOutcome(Optional.empty(), Optional.of(new ToolCallOutcome(
                    CallStatus.RETRYABLE_ERROR, Optional.empty(),
                    "mcp-init-transport-error", "MCP initialize transport failure: " + transport.getMessage())));
            }
        }
    }

    private ToolCallOutcome invokeTool(
        Connection conn,
        OperationSpec spec,
        JsonNode inputNode,
        Optional<String> idempotencyKey
    ) {
        try {
            if (spec.inputSchema().isPresent()) {
                spec.inputSchema().orElseThrow().validateArguments(inputNode);
            } else {
                catalogue.validateAndCanonicalize(spec.inputType(), JSON.writeValueAsString(inputNode));
            }
        } catch (RuntimeException | IOException invalid) {
            return new ToolCallOutcome(CallStatus.TERMINAL_ERROR, Optional.empty(),
                "mcp-invalid-arguments", "Invalid MCP tool arguments: " + invalid.getMessage());
        }

        InitOutcome init = ensureInitialized(conn);
        if (init.error().isPresent()) {
            return init.error().orElseThrow();
        }
        McpSessionState session = init.session().orElseThrow();

        try {
            String requestId = UUID.randomUUID().toString();
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("name", spec.mcpToolName());
            params.put("arguments", inputNode);
            idempotencyKey.ifPresent(key -> params.put("_meta", Map.of("providerIdempotencyKey", key)));

            Map<String, Object> rpc = new LinkedHashMap<>();
            rpc.put("jsonrpc", "2.0");
            rpc.put("id", requestId);
            rpc.put("method", "tools/call");
            rpc.put("params", params);
            byte[] body = JSON.writeValueAsBytes(rpc);

            HttpResponse<byte[]> resp = sendBoundedPost(conn, body, Optional.of(session), idempotencyKey);
            int status = resp.statusCode();
            if (status == 404 && session.sessionId().isPresent()) {
                synchronized (sessionLock) {
                    sessionState = Optional.empty();
                }
            }
            Optional<ToolCallOutcome> statusErr = checkHttpStatus(status, false);
            if (statusErr.isPresent()) {
                return statusErr.orElseThrow();
            }
            JsonNode root = decodeRpcResponse(resp, requestId);
            if (root.hasNonNull("error")) {
                return rpcErrorOutcome(root.get("error"));
            }
            JsonNode result = root.path("result");
            if (result.path("isError").asBoolean(false)) {
                return new ToolCallOutcome(CallStatus.TERMINAL_ERROR, Optional.empty(),
                    "mcp-tool-error", result.path("content").toString());
            }
            JsonNode structured = result.get("structuredContent");
            if (structured == null || structured.isNull()) {
                return new ToolCallOutcome(CallStatus.TERMINAL_ERROR, Optional.empty(),
                    "mcp-missing-structured-content",
                    "MCP tool '" + spec.mcpToolName() + "' returned no structuredContent");
            }
            String canonicalOut = catalogue.validateAndCanonicalize(
                spec.outputType(), JSON.writeValueAsString(structured));
            return new ToolCallOutcome(
                CallStatus.OK,
                Optional.of(JSON.readTree(canonicalOut.getBytes(StandardCharsets.UTF_8))),
                "", "");
        } catch (ResponseTooLargeException tooLarge) {
            return new ToolCallOutcome(CallStatus.TERMINAL_ERROR, Optional.empty(),
                "mcp-response-too-large", tooLarge.getMessage());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new ToolCallOutcome(CallStatus.AMBIGUOUS, Optional.empty(),
                "mcp-interrupted", "Interrupted invoking MCP tool " + spec.mcpToolName());
        } catch (IOException transport) {
            return new ToolCallOutcome(CallStatus.AMBIGUOUS, Optional.empty(),
                "mcp-transport-error", "MCP transport failure: " + transport.getMessage());
        } catch (RuntimeException schemaErr) {
            return new ToolCallOutcome(CallStatus.TERMINAL_ERROR, Optional.empty(),
                "mcp-schema-violation", "MCP output violated canonical schema: " + schemaErr.getMessage());
        }
    }

    private static HttpResponse<byte[]> sendBoundedPost(
        Connection conn,
        byte[] body,
        Optional<McpSessionState> session,
        Optional<String> idempotencyKey
    ) throws IOException, InterruptedException {
        HttpRequest.Builder req = HttpRequest.newBuilder(conn.endpointUri())
            .timeout(REQUEST_TIMEOUT)
            .header("Content-Type", "application/json")
            .header("Accept", ACCEPT_HEADER)
            .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        conn.bearerToken().ifPresent(token -> req.header("Authorization", "Bearer " + token));
        session.ifPresent(state -> {
            req.header(PROTOCOL_VERSION_HEADER, state.protocolVersion());
            state.sessionId().ifPresent(id -> req.header(SESSION_ID_HEADER, id));
        });
        idempotencyKey.ifPresent(key -> req.header("Idempotency-Key", key));
        try {
            return conn.httpClient().send(
                req.build(),
                responseInfo -> new BoundedBytesBodySubscriber(conn.maxResponseBytes()));
        } catch (IOException failure) {
            ResponseTooLargeException tooLarge = findCause(failure, ResponseTooLargeException.class);
            if (tooLarge != null) {
                throw tooLarge;
            }
            throw failure;
        }
    }

    private static Optional<ToolCallOutcome> checkHttpStatus(int status, boolean allowAcceptedNoContent) {
        if (status == 401 || status == 403) {
            return Optional.of(new ToolCallOutcome(CallStatus.AUTH_REQUIRED, Optional.empty(),
                "mcp-unauthorized", "MCP server rejected credentials (HTTP " + status + ")"));
        }
        if (status == 429 || status >= 500) {
            return Optional.of(new ToolCallOutcome(CallStatus.RETRYABLE_ERROR, Optional.empty(),
                "mcp-http-" + status, "Transient MCP server failure (HTTP " + status + ")"));
        }
        if (status == 200 || (allowAcceptedNoContent && (status == 202 || status == 204))) {
            return Optional.empty();
        }
        return Optional.of(new ToolCallOutcome(CallStatus.TERMINAL_ERROR, Optional.empty(),
            "mcp-http-" + status, "Unexpected MCP HTTP status " + status));
    }

    private static ToolCallOutcome rpcErrorOutcome(JsonNode err) {
        String rawCode = err.path("code").asText("error").toLowerCase(Locale.ROOT)
            .replaceAll("[^a-z0-9-]", "");
        return new ToolCallOutcome(CallStatus.TERMINAL_ERROR, Optional.empty(),
            "mcp-rpc-" + (rawCode.isEmpty() ? "error" : rawCode),
            err.path("message").asText("MCP JSON-RPC error"));
    }

    private static JsonNode decodeRpcResponse(HttpResponse<byte[]> resp, String expectedRequestId) throws IOException {
        String contentType = resp.headers().firstValue("Content-Type").orElse("").toLowerCase(Locale.ROOT);
        if (contentType.startsWith("text/event-stream")) {
            return decodeSseRpcResponse(resp.body(), expectedRequestId);
        }
        JsonNode root = JSON.readTree(resp.body());
        if (root == null || !root.isObject()) {
            throw new IOException("Expected JSON-RPC object response from MCP server");
        }
        return root;
    }

    private static JsonNode decodeSseRpcResponse(byte[] body, String expectedRequestId) throws IOException {
        String text = new String(body, StandardCharsets.UTF_8);
        String[] lines = text.split("\r?\n", -1);
        StringBuilder dataBuf = new StringBuilder();
        JsonNode fallbackResponse = null;

        for (int i = 0; i <= lines.length; i++) {
            String line = i < lines.length ? lines[i] : "";
            if (line.isEmpty()) {
                if (dataBuf.length() > 0) {
                    String payload = dataBuf.toString();
                    dataBuf.setLength(0);
                    JsonNode candidate = JSON.readTree(payload);
                    if (candidate != null && candidate.isObject()
                        && (candidate.has("result") || candidate.has("error"))) {
                        if (expectedRequestId.equals(candidate.path("id").asText(""))) {
                            return candidate;
                        }
                        if (fallbackResponse == null && !candidate.has("method")) {
                            fallbackResponse = candidate;
                        }
                    }
                }
                continue;
            }
            if (line.startsWith(":")) {
                continue;
            }
            if (line.startsWith("data:")) {
                String value = line.substring("data:".length());
                if (value.startsWith(" ")) {
                    value = value.substring(1);
                }
                if (dataBuf.length() > 0) {
                    dataBuf.append('\n');
                }
                dataBuf.append(value);
            }
        }
        if (fallbackResponse != null) {
            return fallbackResponse;
        }
        throw new IOException("MCP SSE response stream contained no JSON-RPC response for id " + expectedRequestId);
    }

    private static <T extends Throwable> T findCause(Throwable root, Class<T> type) {
        Throwable cur = root;
        while (cur != null) {
            if (type.isInstance(cur)) {
                return type.cast(cur);
            }
            cur = cur.getCause();
        }
        return null;
    }

    private static final class ResponseTooLargeException extends IOException {
        private ResponseTooLargeException(int maxBytes) {
            super("MCP response exceeded maxResponseBytes (" + maxBytes + ")");
        }
    }

    /** Bounded {@link HttpResponse.BodySubscriber} cancelling reception immediately when {@code maxBytes} is exceeded. */
    private static final class BoundedBytesBodySubscriber implements HttpResponse.BodySubscriber<byte[]> {
        private final int maxBytes;
        private final CompletableFuture<byte[]> future = new CompletableFuture<>();
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private volatile Flow.Subscription subscription;
        private int totalBytes;

        private BoundedBytesBodySubscriber(int maxBytes) {
            this.maxBytes = maxBytes;
        }

        @Override
        public CompletionStage<byte[]> getBody() {
            return future;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            if (future.isDone()) {
                return;
            }
            for (ByteBuffer buf : items) {
                int remaining = buf.remaining();
                if (totalBytes + remaining > maxBytes) {
                    Flow.Subscription sub = this.subscription;
                    if (sub != null) {
                        sub.cancel();
                    }
                    future.completeExceptionally(new ResponseTooLargeException(maxBytes));
                    return;
                }
                byte[] chunk = new byte[remaining];
                buf.get(chunk);
                out.write(chunk, 0, remaining);
                totalBytes += remaining;
            }
        }

        @Override
        public void onError(Throwable throwable) {
            future.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            if (!future.isDone()) {
                future.complete(out.toByteArray());
            }
        }
    }

    private enum CallStatus {
        OK, AUTH_REQUIRED, RETRYABLE_ERROR, TERMINAL_ERROR, AMBIGUOUS
    }

    private record ToolCallOutcome(
        CallStatus status,
        Optional<JsonNode> payload,
        String code,
        String message
    ) {
    }

    private record McpSessionState(
        URI endpointUri,
        String protocolVersion,
        Optional<String> sessionId
    ) {
    }

    private record InitOutcome(
        Optional<McpSessionState> session,
        Optional<ToolCallOutcome> error
    ) {
    }

    private record ActiveBinding(ConnectorRuntimeContext context, Config config) {
    }

    private final class McpQueryOperation implements QueryOperation<JsonNode, ConnectorConfigurationDocument, JsonNode> {
        private final OperationSpec spec;

        private McpQueryOperation(OperationSpec spec) {
            this.spec = spec;
        }

        @Override public String id() { return spec.operationId(); }
        @Override public int majorVersion() { return spec.majorVersion(); }
        @Override public QueryCapabilities capabilities() { return QueryCapabilities.conservative(); }

        @Override
        public CompletionStage<QueryOutcome<JsonNode>> query(
            QueryInvocation<JsonNode, ConnectorConfigurationDocument, JsonNode> invocation
        ) {
            ActiveBinding binding = activeBinding.get().orElseThrow(() ->
                new IllegalStateException("MCP connector binding is not active"));
            return resolveConnection(invocation.executionContext()).thenApplyAsync(conn -> {
                ToolCallOutcome outcome = invokeTool(conn, spec, invocation.input(), Optional.empty());
                return switch (outcome.status()) {
                    case OK -> new QueryOutcome.Found<>(outcome.payload().orElseThrow());
                    case AUTH_REQUIRED -> new QueryOutcome.AuthenticationRequired<>(outcome.code());
                    case RETRYABLE_ERROR, AMBIGUOUS -> new QueryOutcome.TemporarilyUnavailable<>(outcome.code());
                    case TERMINAL_ERROR -> new QueryOutcome.TerminalFailure<>(outcome.code());
                };
            }, binding.context().executor());
        }
    }

    private final class McpCommandOperation implements CommandOperation<JsonNode, OperationConfig, JsonNode> {
        private final OperationSpec spec;

        private McpCommandOperation(OperationSpec spec) {
            this.spec = spec;
        }

        @Override public String id() { return spec.operationId(); }
        @Override public int majorVersion() { return spec.majorVersion(); }
        @Override public Optional<ConnectorConfigSchema<OperationConfig>> configurationSchema() {
            return Optional.of(OPERATION_CONFIG_SCHEMA);
        }

        @Override
        public CommandCapabilities capabilities() {
            return new CommandCapabilities(
                true, true, false,
                CommandExecutionPosture.UNSPECIFIED,
                CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED,
                false, Set.of());
        }

        @Override
        public CompletionStage<CommandOutcome<JsonNode>> dispatch(
            CommandInvocation<JsonNode, OperationConfig> invocation
        ) {
            ActiveBinding binding = activeBinding.get().orElseThrow(() ->
                new IllegalStateException("MCP connector binding is not active"));
            Optional<String> idemKey = invocation.dispatchIdentity()
                .map(CommandDispatchIdentity::providerIdempotencyKey);
            return resolveConnection(invocation.executionContext()).thenApplyAsync(conn -> {
                ToolCallOutcome outcome = invokeTool(conn, spec, invocation.input(), idemKey);
                return switch (outcome.status()) {
                    case OK -> new CommandOutcome.Succeeded<>(
                        outcome.payload().orElseThrow(),
                        new CommandConfirmation(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false),
                        List.of());
                    case AUTH_REQUIRED -> new CommandOutcome.UserActionRequired<>(
                        outcome.code(), outcome.message(), List.of());
                    case RETRYABLE_ERROR -> new CommandOutcome.RetryableFailure<>(outcome.code(), List.of());
                    case AMBIGUOUS -> new CommandOutcome.Ambiguous<>(outcome.code(), List.of());
                    case TERMINAL_ERROR -> new CommandOutcome.TerminalFailure<>(outcome.code(), List.of());
                };
            }, binding.context().executor());
        }
    }
}
