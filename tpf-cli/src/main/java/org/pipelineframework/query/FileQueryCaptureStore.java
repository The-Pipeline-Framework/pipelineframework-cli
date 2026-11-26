package org.pipelineframework.query;

import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pipelineframework.cli.LocalFileDevStores;
import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * Crash-safe, zero-JNI disk-backed {@link QueryCaptureStore} for local {@code tpf dev}.
 * Persists {@link QueryCaptureRecord} observations via fsync + atomic rename so replays across
 * {@code SIGKILL} and restarts reuse captured external observations without re-invoking upstream
 * Query connectors, and never publishes uncommitted mutations if persistence fails.
 */
public final class FileQueryCaptureStore implements QueryCaptureStore {

    private static final ObjectMapper JSON = PipelineJson.mapper();

    private final Object lock = new Object();
    private final LocalFileDevStores.AtomicJsonFile file;
    private final Map<String, QueryCaptureRecord> records = new LinkedHashMap<>();

    public FileQueryCaptureStore(Path dataDir) {
        this(dataDir, () -> true);
    }

    public FileQueryCaptureStore(Path dataDir, BooleanSupplier isOpen) {
        this.file = new LocalFileDevStores.AtomicJsonFile(
            Objects.requireNonNull(dataDir, "dataDir must not be null").resolve("query-captures.json"),
            isOpen);
        load();
    }

    @Override
    public String providerName() {
        return "local-file";
    }

    @Override
    public CompletionStage<Optional<QueryCaptureRecord>> get(String captureKey) {
        synchronized (lock) {
            return CompletableFuture.completedFuture(Optional.ofNullable(records.get(captureKey)));
        }
    }

    @Override
    public CompletionStage<QueryCaptureRecord> putIfAbsent(QueryCaptureRecord record) {
        Objects.requireNonNull(record, "record must not be null");
        synchronized (lock) {
            QueryCaptureRecord existing = records.get(record.captureKey());
            if (existing != null) {
                return CompletableFuture.completedFuture(existing);
            }
            Map<String, QueryCaptureRecord> next = new LinkedHashMap<>(records);
            next.put(record.captureKey(), record);
            persistSnapshot(next);
            records.put(record.captureKey(), record);
            return CompletableFuture.completedFuture(record);
        }
    }

    @Override
    public CompletionStage<Boolean> remove(String captureKey) {
        synchronized (lock) {
            if (!records.containsKey(captureKey)) {
                return CompletableFuture.completedFuture(false);
            }
            Map<String, QueryCaptureRecord> next = new LinkedHashMap<>(records);
            next.remove(captureKey);
            persistSnapshot(next);
            records.remove(captureKey);
            return CompletableFuture.completedFuture(true);
        }
    }

    @Override
    public CompletionStage<Void> clear() {
        synchronized (lock) {
            if (records.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            persistSnapshot(Map.of());
            records.clear();
            return CompletableFuture.completedFuture(null);
        }
    }

    private void load() {
        Optional<JsonNode> root = file.readIfPresent();
        if (root.isEmpty() || !root.orElseThrow().isObject()) {
            return;
        }
        root.orElseThrow().fields().forEachRemaining(entry -> {
            JsonNode n = entry.getValue();
            QueryCaptureRecord record = new QueryCaptureRecord(
                n.path("tenantId").asText(),
                n.path("executionId").asText(),
                n.path("stepIndex").asInt(),
                n.path("queryId").asText(),
                n.path("queryVersion").asText(),
                n.path("captureKey").asText(),
                n.path("inputJson").asText(),
                n.path("outputJson").isNull() || n.path("outputJson").isMissingNode() ? null : n.path("outputJson").asText(),
                n.path("outputType").asText(),
                Instant.ofEpochMilli(n.path("capturedAtEpochMs").asLong()),
                QueryCaptureStatus.valueOf(n.path("status").asText("FOUND")),
                n.path("outcomeCode").isNull() || n.path("outcomeCode").isMissingNode() ? null : n.path("outcomeCode").asText());
            records.put(entry.getKey(), record);
        });
    }

    private void persistSnapshot(Map<String, QueryCaptureRecord> snapshot) {
        ObjectNode root = JSON.createObjectNode();
        for (Map.Entry<String, QueryCaptureRecord> entry : snapshot.entrySet()) {
            QueryCaptureRecord r = entry.getValue();
            ObjectNode n = JSON.createObjectNode();
            n.put("tenantId", r.tenantId());
            n.put("executionId", r.executionId());
            n.put("stepIndex", r.stepIndex());
            n.put("queryId", r.queryId());
            n.put("queryVersion", r.queryVersion());
            n.put("captureKey", r.captureKey());
            n.put("inputJson", r.inputJson());
            if (r.outputJson() == null) {
                n.putNull("outputJson");
            } else {
                n.put("outputJson", r.outputJson());
            }
            n.put("outputType", r.outputType());
            n.put("capturedAtEpochMs", r.capturedAt().toEpochMilli());
            n.put("status", r.status().name());
            if (r.outcomeCode() == null) {
                n.putNull("outcomeCode");
            } else {
                n.put("outcomeCode", r.outcomeCode());
            }
            root.set(entry.getKey(), n);
        }
        file.writeAtomically(root);
    }
}
