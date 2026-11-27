package org.pipelineframework.connector.mcp;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
 * Streamable HTTP transport (zero reflection / GraalVM Native Image compatible).
 */
public final class DevMcpConnector implements ConnectorProvider<DevMcpConnector.Config> {

    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("mcp.client");
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
        activeBinding.set(Optional.of(new ActiveBinding(context, configuration)));
        return CompletableFuture.completedStage(null);
    }

    @Override
    public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
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

        try {
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("name", spec.mcpToolName());
            params.put("arguments", inputNode);
            idempotencyKey.ifPresent(key -> params.put("_meta", Map.of("providerIdempotencyKey", key)));

            Map<String, Object> rpc = new LinkedHashMap<>();
            rpc.put("jsonrpc", "2.0");
            rpc.put("id", UUID.randomUUID().toString());
            rpc.put("method", "tools/call");
            rpc.put("params", params);
            byte[] body = JSON.writeValueAsBytes(rpc);

            HttpRequest.Builder req = HttpRequest.newBuilder(conn.endpointUri())
                .timeout(REQUEST_TIMEOUT)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
            conn.bearerToken().ifPresent(token -> req.header("Authorization", "Bearer " + token));
            idempotencyKey.ifPresent(key -> req.header("Idempotency-Key", key));

            HttpResponse<byte[]> resp = conn.httpClient().send(req.build(), HttpResponse.BodyHandlers.ofByteArray());
            int status = resp.statusCode();
            if (status == 401 || status == 403) {
                return new ToolCallOutcome(CallStatus.AUTH_REQUIRED, Optional.empty(),
                    "mcp-unauthorized", "MCP server rejected credentials (HTTP " + status + ")");
            }
            if (status == 429 || status >= 500) {
                return new ToolCallOutcome(CallStatus.RETRYABLE_ERROR, Optional.empty(),
                    "mcp-http-" + status, "Transient MCP server failure (HTTP " + status + ")");
            }
            if (status != 200) {
                return new ToolCallOutcome(CallStatus.TERMINAL_ERROR, Optional.empty(),
                    "mcp-http-" + status, "Unexpected MCP HTTP status " + status);
            }
            if (resp.body().length > conn.maxResponseBytes()) {
                return new ToolCallOutcome(CallStatus.TERMINAL_ERROR, Optional.empty(),
                    "mcp-response-too-large", "MCP response exceeded maxResponseBytes");
            }
            JsonNode root = JSON.readTree(resp.body());
            if (root.hasNonNull("error")) {
                JsonNode err = root.get("error");
                String rawCode = err.path("code").asText("error").toLowerCase(java.util.Locale.ROOT)
                    .replaceAll("[^a-z0-9-]", "");
                return new ToolCallOutcome(CallStatus.TERMINAL_ERROR, Optional.empty(),
                    "mcp-rpc-" + (rawCode.isEmpty() ? "error" : rawCode),
                    err.path("message").asText("MCP JSON-RPC error"));
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
