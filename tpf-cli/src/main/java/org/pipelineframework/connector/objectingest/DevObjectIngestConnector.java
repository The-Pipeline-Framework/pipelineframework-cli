package org.pipelineframework.connector.objectingest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.config.boundary.PipelineObjectFilterConfig;
import org.pipelineframework.config.boundary.PipelineObjectIdentityConfig;
import org.pipelineframework.config.boundary.PipelineObjectNamingConfig;
import org.pipelineframework.config.boundary.PipelineObjectPayloadConfig;
import org.pipelineframework.config.boundary.PipelineObjectPollConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishPayloadConfig;
import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.connector.CommandCapabilities;
import org.pipelineframework.connector.CommandConfirmation;
import org.pipelineframework.connector.CommandDispatchIdentity;
import org.pipelineframework.connector.CommandExecutionPosture;
import org.pipelineframework.connector.CommandInvocation;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandOperation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
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
import org.pipelineframework.objectingest.ObjectSourceItem;
import org.pipelineframework.objectpublish.ObjectWriteCloseRequest;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.objectpublish.ObjectWriteSession;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/**
 * Tier-1 portable Object Ingest &amp; Publish Connector ({@code object.ingest} v1) bundled directly
 * into the native {@code tpf} CLI over class-free {@link JsonNode} boundaries, delegating file
 * discovery, bounded reads, path-traversal enforcement, and atomic writes to TPF's
 * {@link FilesystemObjectSourceProvider} and {@link FilesystemObjectTargetProvider}.
 */
public final class DevObjectIngestConnector implements ConnectorProvider<DevObjectIngestConnector.Config> {

    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("object.ingest");
    private static final ObjectMapper JSON = CanonicalTypeCatalogue.exactJsonMapper();

    /** Sandboxed filesystem root configuration for local object-ingest/publish operations. */
    public record Config(String root, long maxBytes) {
        public Config {
            if (root == null || root.isBlank()) {
                throw new IllegalArgumentException("object.ingest config.root must not be blank");
            }
            if (maxBytes <= 0) {
                throw new IllegalArgumentException("object.ingest config.maxBytes must be positive");
            }
        }
    }

    /** Pinned operation binding for one object-ingest Query or object-publish Command. */
    public record OperationSpec(
        String operationId,
        ConnectorOperationKind kind,
        int majorVersion,
        String inputType,
        String outputType
    ) {
        public OperationSpec {
            if (operationId == null || operationId.isBlank()) {
                throw new IllegalArgumentException("operationId must not be blank");
            }
            Objects.requireNonNull(kind, "kind must not be null");
            if (inputType == null || inputType.isBlank()) {
                throw new IllegalArgumentException("inputType must not be blank");
            }
            if (outputType == null || outputType.isBlank()) {
                throw new IllegalArgumentException("outputType must not be blank");
            }
        }
    }

    /** Empty per-operation configuration record required by TPF's native command contract. */
    public record OperationConfig() {
    }

    private static final ConnectorConfigSchema<Config> CONFIG_SCHEMA =
        ConnectorConfigSchema.record(Config.class, "object.ingest.provider", 1);
    private static final ConnectorConfigSchema<OperationConfig> OPERATION_CONFIG_SCHEMA =
        ConnectorConfigSchema.record(OperationConfig.class, "object.ingest.operation", 1);

    private final CanonicalTypeCatalogue catalogue;
    private final List<OperationSpec> specs;
    private final FilesystemObjectSourceProvider sourceProvider;
    private final FilesystemObjectTargetProvider targetProvider;
    private final List<ConnectorOperation> operations;
    private final AtomicReference<Optional<Config>> activeConfig = new AtomicReference<>(Optional.empty());

    public DevObjectIngestConnector(CanonicalTypeCatalogue catalogue, List<OperationSpec> specs) {
        this.catalogue = Objects.requireNonNull(catalogue, "catalogue must not be null");
        this.specs = List.copyOf(Objects.requireNonNull(specs, "specs must not be null"));
        this.sourceProvider = new FilesystemObjectSourceProvider();
        this.targetProvider = new FilesystemObjectTargetProvider();
        List<ConnectorOperation> ops = new ArrayList<>(this.specs.size());
        for (OperationSpec spec : this.specs) {
            ops.add(spec.kind() == ConnectorOperationKind.QUERY
                ? new ObjectReadQueryOperation(spec)
                : new ObjectWriteCommandOperation(spec));
        }
        this.operations = List.copyOf(ops);
    }

    /** Creates an isolated provider instance sharing the compiled specs for a separate binding. */
    public DevObjectIngestConnector copy() {
        return new DevObjectIngestConnector(catalogue, specs);
    }

    @Override public ConnectorProviderId id() { return PROVIDER_ID; }
    @Override public ConnectorProviderVersion version() { return new ConnectorProviderVersion(1, 0); }
    @Override public Collection<? extends ConnectorOperation> operations() { return operations; }
    @Override public Optional<ConnectorConfigSchema<Config>> configurationSchema() { return Optional.of(CONFIG_SCHEMA); }

    @Override
    public CompletionStage<Void> start(ConnectorRuntimeContext context, Config configuration) {
        activeConfig.set(Optional.of(configuration));
        return CompletableFuture.completedStage(null);
    }

    @Override
    public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        activeConfig.set(Optional.empty());
        return CompletableFuture.completedStage(null);
    }

    private Config requireConfig() {
        return activeConfig.get().orElseThrow(() ->
            new IllegalStateException("object.ingest connector binding is not active"));
    }

    private static PipelineObjectSourceConfig sourceConfig(String name, String root) {
        return new PipelineObjectSourceConfig(
            name,
            "object",
            "filesystem",
            Map.of("root", Path.of(root).toAbsolutePath().normalize().toString()),
            PipelineObjectFilterConfig.defaults(),
            PipelineObjectPollConfig.defaults(),
            PipelineObjectIdentityConfig.defaults(),
            PipelineObjectPayloadConfig.reference());
    }

    private static PipelineObjectPublishConfig publishConfig(String name, String root) {
        return new PipelineObjectPublishConfig(
            name,
            "object",
            "filesystem",
            Map.of("root", Path.of(root).toAbsolutePath().normalize().toString()),
            PipelineObjectNamingConfig.defaults(),
            PipelineObjectPublishPayloadConfig.defaults());
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private final class ObjectReadQueryOperation implements QueryOperation<JsonNode, ConnectorConfigurationDocument, JsonNode> {
        private final OperationSpec spec;

        private ObjectReadQueryOperation(OperationSpec spec) {
            this.spec = spec;
        }

        @Override public String id() { return spec.operationId(); }
        @Override public int majorVersion() { return spec.majorVersion(); }
        @Override public QueryCapabilities capabilities() { return QueryCapabilities.conservative(); }

        @Override
        public CompletionStage<QueryOutcome<JsonNode>> query(
            QueryInvocation<JsonNode, ConnectorConfigurationDocument, JsonNode> invocation
        ) {
            return CompletableFuture.supplyAsync(() -> {
                Config cfg = requireConfig();
                JsonNode input = invocation.input();
                String key = input.path("objectKey").asText("").trim();
                if (key.isEmpty()) {
                    return new QueryOutcome.TerminalFailure<>("object-invalid-key");
                }
                PipelineObjectSourceConfig src = sourceConfig(spec.operationId(), cfg.root());
                try {
                    if (key.contains("..") || Path.of(key).isAbsolute()) {
                        return new QueryOutcome.TerminalFailure<>("object-path-escape");
                    }
                    List<ObjectSourceItem> listed = sourceProvider.list(src, 10_000);
                    Optional<ObjectSourceItem> match = listed.stream()
                        .filter(item -> key.equals(item.key()))
                        .findFirst();
                    if (match.isEmpty()) {
                        return new QueryOutcome.NotFound<>("object-not-found");
                    }
                    ObjectSourceItem item = match.orElseThrow();
                    String content = sourceProvider.readText(src, item, cfg.maxBytes()).orElse("");
                    Map<String, Object> result = new LinkedHashMap<>();
                    result.put("objectKey", item.key());
                    result.put("content", content);
                    result.put("sha256", item.etag() == null ? "" : item.etag());
                    result.put("sizeBytes", item.sizeBytes());
                    String canonical = catalogue.validateAndCanonicalize(
                        spec.outputType(), JSON.writeValueAsString(result));
                    return new QueryOutcome.Found<>(JSON.readTree(canonical));
                } catch (SecurityException escape) {
                    return new QueryOutcome.TerminalFailure<>("object-path-escape");
                } catch (RuntimeException | IOException failure) {
                    return new QueryOutcome.TerminalFailure<>("object-read-failed");
                }
            });
        }
    }

    private final class ObjectWriteCommandOperation implements CommandOperation<JsonNode, OperationConfig, JsonNode> {
        private final OperationSpec spec;

        private ObjectWriteCommandOperation(OperationSpec spec) {
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
            Config cfg = requireConfig();
            JsonNode input = invocation.input();
            String key = input.path("objectKey").asText("").trim();
            if (key.isEmpty()) {
                return CompletableFuture.completedStage(
                    new CommandOutcome.TerminalFailure<>("object-invalid-key", List.of()));
            }
            if (key.contains("..") || Path.of(key).isAbsolute()) {
                return CompletableFuture.completedStage(
                    new CommandOutcome.TerminalFailure<>("object-path-escape", List.of()));
            }
            byte[] payloadBytes = input.path("content").asText("").getBytes(StandardCharsets.UTF_8);
            if (payloadBytes.length > cfg.maxBytes()) {
                return CompletableFuture.completedStage(
                    new CommandOutcome.TerminalFailure<>("object-too-large", List.of()));
            }
            String checksum = sha256Hex(payloadBytes);
            String contentType = input.path("contentType").asText("text/plain; charset=utf-8");
            PipelineObjectPublishConfig target = publishConfig(spec.operationId(), cfg.root());
            Optional<String> idemKey = invocation.dispatchIdentity()
                .map(CommandDispatchIdentity::providerIdempotencyKey);
            Map<String, String> metadata = idemKey
                .map(idem -> Map.of("providerIdempotencyKey", idem))
                .orElseGet(Map::of);
            ObjectWriteOpenRequest openReq = new ObjectWriteOpenRequest(
                spec.operationId(), target, key, contentType, metadata, idemKey.orElse(""));
            return targetProvider.open(openReq)
                .thenCompose(session -> writeAndClose(session, payloadBytes, checksum))
                .thenApply(writeResult -> {
                    try {
                        Map<String, Object> out = new LinkedHashMap<>();
                        out.put("objectKey", writeResult.reference().key());
                        out.put("sha256", writeResult.checksum());
                        out.put("sizeBytes", writeResult.bytes());
                        String canonical = catalogue.validateAndCanonicalize(
                            spec.outputType(), JSON.writeValueAsString(out));
                        return (CommandOutcome<JsonNode>) new CommandOutcome.Succeeded<>(
                            JSON.readTree(canonical),
                            new CommandConfirmation(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false),
                            List.of());
                    } catch (IOException | RuntimeException failure) {
                        return new CommandOutcome.TerminalFailure<JsonNode>("object-schema-violation", List.of());
                    }
                })
                .exceptionally(failure -> {
                    Throwable cause = failure.getCause() != null ? failure.getCause() : failure;
                    String code = cause instanceof SecurityException ? "object-path-escape" : "object-write-failed";
                    return new CommandOutcome.TerminalFailure<>(code, List.of());
                });
        }

        private CompletionStage<ObjectWriteResult> writeAndClose(
            ObjectWriteSession session, byte[] payloadBytes, String checksum
        ) {
            return session.write(ByteBuffer.wrap(payloadBytes))
                .thenCompose(ignored -> session.close(new ObjectWriteCloseRequest(
                    payloadBytes.length, checksum, Map.of())));
        }
    }
}
