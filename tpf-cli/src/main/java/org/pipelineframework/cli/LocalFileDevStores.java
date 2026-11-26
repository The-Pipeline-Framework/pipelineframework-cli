package org.pipelineframework.cli;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCompletionAdmission;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionCursor;
import org.pipelineframework.awaitable.AwaitCompletionPage;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitCreateResult;
import org.pipelineframework.awaitable.AwaitInteractionCursor;
import org.pipelineframework.awaitable.AwaitInteractionNotFoundException;
import org.pipelineframework.awaitable.AwaitInteractionPage;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitInteractionTerminalException;
import org.pipelineframework.awaitable.AwaitUnitCreateCommand;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitReleaseAcknowledgement;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ForeignDispatchIntent;
import org.pipelineframework.orchestrator.ForeignDispatchIntentStore;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;

/**
 * Crash-safe, zero-JNI disk-backed local dev stores ({@link ExecutionStateStore},
 * {@link AwaitUnitStore}, {@link AwaitInteractionStore}, {@link ForeignDispatchIntentStore})
 * persisted via fsynced atomic JSON snapshots ({@link FileChannel#force(boolean)} +
 * {@link StandardCopyOption#ATOMIC_MOVE}) to survive {@code SIGKILL} in GraalVM native-image
 * without Docker or LocalStack.
 */
public final class LocalFileDevStores {

    private static final ObjectMapper JSON = PipelineJson.mapper();

    public final FileExecutionStateStore executionStore;
    public final FileAwaitUnitStore unitStore;
    public final FileAwaitInteractionStore interactionStore;
    public final FileForeignDispatchIntentStore dispatchIntentStore;

    public LocalFileDevStores(Path dataDir, JsonTransitionPayloadCodec payloadCodec) {
        Objects.requireNonNull(dataDir, "dataDir must not be null");
        Objects.requireNonNull(payloadCodec, "payloadCodec must not be null");
        this.executionStore = new FileExecutionStateStore(
            new AtomicJsonFile(dataDir.resolve("executions.json")), payloadCodec);
        this.unitStore = new FileAwaitUnitStore(
            new AtomicJsonFile(dataDir.resolve("await-units.json")));
        this.interactionStore = new FileAwaitInteractionStore(
            new AtomicJsonFile(dataDir.resolve("await-interactions.json")), payloadCodec);
        this.dispatchIntentStore = new FileForeignDispatchIntentStore(
            new AtomicJsonFile(dataDir.resolve("dispatch-intents.json")));
    }

    private static Optional<ObjectNode> encodePayload(JsonTransitionPayloadCodec codec, Object value) {
        if (value == null) {
            return Optional.empty();
        }
        SerializedTransitionPayload wire = value instanceof SerializedTransitionPayload s ? s : codec.encode(value);
        ObjectNode node = JSON.createObjectNode();
        node.put("wrapped", value instanceof SerializedTransitionPayload);
        node.put("typeId", wire.payloadTypeId());
        node.put("encoding", wire.payloadEncoding());
        node.put("payload", wire.payload());
        return Optional.of(node);
    }

    private static Optional<Object> decodePayload(JsonTransitionPayloadCodec codec, JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return Optional.empty();
        }
        SerializedTransitionPayload wire = new SerializedTransitionPayload(
            node.path("typeId").asText(),
            node.path("encoding").asText(),
            node.path("payload").asText());
        return Optional.of(node.path("wrapped").asBoolean(false) ? wire : codec.decode(wire));
    }

    private static Optional<String> optString(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return Optional.empty();
        }
        String text = value.asText();
        return text.isBlank() ? Optional.empty() : Optional.of(text);
    }

    // --- Crash-safe atomic JSON file helper ---

    static final class AtomicJsonFile {
        private final Path target;

        AtomicJsonFile(Path target) {
            this.target = target;
        }

        Optional<JsonNode> readIfPresent() {
            if (!Files.exists(target)) {
                return Optional.empty();
            }
            try {
                byte[] bytes = Files.readAllBytes(target);
                if (bytes.length == 0) {
                    return Optional.empty();
                }
                return Optional.of(JSON.readTree(bytes));
            } catch (IOException e) {
                throw new UncheckedIOException("Failed reading local dev state " + target, e);
            }
        }

        void writeAtomically(Object value) {
            try {
                Path parent = target.toAbsolutePath().getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                byte[] bytes = JSON.writeValueAsBytes(value);
                Path tmp = target.resolveSibling(target.getFileName() + ".tmp." + System.nanoTime());
                try (FileChannel channel = FileChannel.open(
                    tmp,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                    ByteBuffer buf = ByteBuffer.wrap(bytes);
                    while (buf.hasRemaining()) {
                        channel.write(buf);
                    }
                    channel.force(true);
                }
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed persisting local dev state " + target, e);
            }
        }
    }

    // --- ForeignDispatchIntentStore ---

    public static final class FileForeignDispatchIntentStore implements ForeignDispatchIntentStore {
        private final Object lock = new Object();
        private final AtomicJsonFile file;
        private final Map<String, ObjectNode> entries = new LinkedHashMap<>();

        FileForeignDispatchIntentStore(AtomicJsonFile file) {
            this.file = file;
            load();
        }

        @Override
        public ReservationOutcome reserve(ForeignDispatchIntent intent) {
            Objects.requireNonNull(intent, "intent must not be null");
            synchronized (lock) {
                String key = intent.storageKey();
                if (entries.containsKey(key)) {
                    return ReservationOutcome.ALREADY_DISPATCHED_UNCERTAIN;
                }
                ObjectNode record = JSON.createObjectNode();
                record.put("key", key);
                record.put("tenantId", intent.tenantId());
                record.put("executionId", intent.executionId());
                record.put("executionVersion", intent.executionVersion());
                record.put("attempt", intent.attempt());
                record.put("transitionKey", intent.transitionKey());
                record.put("stepIndex", intent.stepIndex());
                record.put("stepId", intent.stepId());
                record.put("targetUri", intent.targetUri());
                record.put("requestDigest", intent.requestDigest());
                record.put("createdAtEpochMs", intent.createdAtEpochMs());
                record.putArray("settlements");
                entries.put(key, record);
                flush();
                return ReservationOutcome.RESERVED;
            }
        }

        @Override
        public void recordSettlement(ForeignDispatchIntent intent, SettlementStatus status, long settledAtEpochMs) {
            Objects.requireNonNull(intent, "intent must not be null");
            Objects.requireNonNull(status, "status must not be null");
            synchronized (lock) {
                ObjectNode existing = entries.get(intent.storageKey());
                if (existing == null) {
                    return;
                }
                ArrayNode settlements = (ArrayNode) existing.get("settlements");
                ObjectNode event = settlements.addObject();
                event.put("status", status.name());
                event.put("settledAtEpochMs", settledAtEpochMs);
                flush();
            }
        }

        private void flush() {
            ObjectNode root = JSON.createObjectNode();
            ArrayNode list = root.putArray("intents");
            entries.values().forEach(list::add);
            file.writeAtomically(root);
        }

        private void load() {
            file.readIfPresent().ifPresent(root -> {
                for (JsonNode n : root.path("intents")) {
                    if (n.isObject() && n.hasNonNull("key")) {
                        entries.put(n.get("key").asText(), (ObjectNode) n);
                    }
                }
            });
        }
    }

    // --- ExecutionStateStore ---

    public static final class FileExecutionStateStore implements ExecutionStateStore {
        private final Object lock = new Object();
        private final AtomicJsonFile file;
        private final JsonTransitionPayloadCodec codec;
        private final Map<String, ExecutionRecord<Object, Object>> executions = new LinkedHashMap<>();
        private final Map<String, String> idByKey = new LinkedHashMap<>();

        FileExecutionStateStore(AtomicJsonFile file, JsonTransitionPayloadCodec codec) {
            this.file = file;
            this.codec = codec;
            load();
        }

        @Override public String providerName() { return "local-file"; }
        @Override public Optional<String> startupValidationError() { return Optional.empty(); }
        @Override public boolean supportsLeaseRenewal() { return true; }

        @Override
        public Uni<CreateExecutionResult> createOrGetExecution(ExecutionCreateCommand command) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    String scopedKey = command.tenantId() + "\0" + command.executionKey();
                    String existingId = idByKey.get(scopedKey);
                    if (existingId != null) {
                        ExecutionRecord<Object, Object> existing = executions.get(scope(command.tenantId(), existingId));
                        if (existing != null) {
                            return new CreateExecutionResult(existing, true);
                        }
                    }
                    String execId = UUID.randomUUID().toString();
                    ExecutionRecord<Object, Object> created = new ExecutionRecord<>(
                        command.tenantId(), execId, command.executionKey(),
                        command.pipelineId(), command.contractVersion(), command.releaseVersion(),
                        command.resultShape(), ExecutionStatus.QUEUED, 0L, 0, 0,
                        null, 0L, command.nowEpochMs(), null,
                        command.inputPayload(), null, null, null, null,
                        command.nowEpochMs(), command.nowEpochMs(), command.ttlEpochS(),
                        0L, 0, null, null, -1, Optional.empty(), Optional.empty(), Optional.empty(),
                        Optional.empty(), Optional.of("initial-v1"), Optional.empty());
                    executions.put(scope(command.tenantId(), execId), created);
                    idByKey.put(scopedKey, execId);
                    flush();
                    return new CreateExecutionResult(created, false);
                }
            });
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> getExecution(String tenantId, String executionId) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    return Optional.ofNullable(executions.get(scope(tenantId, executionId)));
                }
            });
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> getExecutionByKey(String tenantId, String executionKey) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    String id = idByKey.get(tenantId + "\0" + executionKey);
                    return id == null ? Optional.empty() : Optional.ofNullable(executions.get(scope(tenantId, id)));
                }
            });
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> claimLease(
            String tenantId, String executionId, String leaseOwner, long nowEpochMs, long leaseMs) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    String key = scope(tenantId, executionId);
                    ExecutionRecord<Object, Object> cur = executions.get(key);
                    if (cur == null) return Optional.empty();
                    boolean claimable = cur.status() == ExecutionStatus.QUEUED
                        || (cur.status() == ExecutionStatus.WAIT_RETRY && cur.nextDueEpochMs() <= nowEpochMs)
                        || (cur.status() == ExecutionStatus.RUNNING && cur.leaseExpiresEpochMs() <= nowEpochMs);
                    if (!claimable) return Optional.empty();
                    ExecutionRecord<Object, Object> updated = new ExecutionRecord<>(
                        cur.tenantId(), cur.executionId(), cur.executionKey(),
                        cur.pipelineId(), cur.contractVersion(), cur.releaseVersion(),
                        cur.resultShape(), ExecutionStatus.RUNNING, cur.version() + 1L,
                        cur.currentStepIndex(), cur.attempt(),
                        leaseOwner, nowEpochMs + leaseMs, 0L, cur.lastTransitionKey(),
                        cur.inputPayload(), cur.awaitUnitId(), cur.resultPayload(),
                        cur.errorCode(), cur.errorMessage(),
                        cur.createdAtEpochMs(), nowEpochMs, cur.ttlEpochS(),
                        cur.firstCircuitDeferredAtEpochMs(), cur.circuitDeferralCount(), cur.circuitIdentity(),
                        cur.redriveIntent(), cur.failedStepIndex(), cur.failedCommandId(),
                        cur.redriveTargetCommandId(), cur.redriveReason(), cur.pagingState(),
                        cur.awaitOccurrenceCause(), cur.dispatchPin());
                    executions.put(key, updated);
                    flush();
                    return Optional.of(updated);
                }
            });
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> renewLease(
            String tenantId, String executionId, long expectedVersion, String leaseOwner, long nowEpochMs, long leaseMs) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    String key = scope(tenantId, executionId);
                    ExecutionRecord<Object, Object> cur = executions.get(key);
                    if (cur == null || cur.version() != expectedVersion || cur.status() != ExecutionStatus.RUNNING
                        || !Objects.equals(cur.leaseOwner(), leaseOwner)) {
                        return Optional.empty();
                    }
                    ExecutionRecord<Object, Object> updated = new ExecutionRecord<>(
                        cur.tenantId(), cur.executionId(), cur.executionKey(),
                        cur.pipelineId(), cur.contractVersion(), cur.releaseVersion(),
                        cur.resultShape(), ExecutionStatus.RUNNING, cur.version() + 1L,
                        cur.currentStepIndex(), cur.attempt(),
                        leaseOwner, nowEpochMs + leaseMs, 0L, cur.lastTransitionKey(),
                        cur.inputPayload(), cur.awaitUnitId(), cur.resultPayload(),
                        cur.errorCode(), cur.errorMessage(),
                        cur.createdAtEpochMs(), nowEpochMs, cur.ttlEpochS(),
                        cur.firstCircuitDeferredAtEpochMs(), cur.circuitDeferralCount(), cur.circuitIdentity(),
                        cur.redriveIntent(), cur.failedStepIndex(), cur.failedCommandId(),
                        cur.redriveTargetCommandId(), cur.redriveReason(), cur.pagingState(),
                        cur.awaitOccurrenceCause(), cur.dispatchPin());
                    executions.put(key, updated);
                    flush();
                    return Optional.of(updated);
                }
            });
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> markSucceeded(
            String tenantId, String executionId, long expectedVersion, String transitionKey,
            Object resultPayload, long nowEpochMs) {
            return updateMatching(tenantId, executionId, expectedVersion, cur -> new ExecutionRecord<>(
                cur.tenantId(), cur.executionId(), cur.executionKey(),
                cur.pipelineId(), cur.contractVersion(), cur.releaseVersion(),
                cur.resultShape(), ExecutionStatus.SUCCEEDED, cur.version() + 1L,
                cur.currentStepIndex(), cur.attempt(), null, 0L, 0L, transitionKey,
                cur.inputPayload(), cur.awaitUnitId(), resultPayload, null, null,
                cur.createdAtEpochMs(), nowEpochMs, cur.ttlEpochS(),
                0L, 0, null, null, -1, Optional.empty(), Optional.empty(), Optional.empty(),
                cur.pagingState(), cur.awaitOccurrenceCause(), cur.dispatchPin()));
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> markWaitingExternal(
            String tenantId, String executionId, long expectedVersion, String transitionKey,
            String awaitUnitId, int stepIndex, long nowEpochMs) {
            return updateMatching(tenantId, executionId, expectedVersion, cur -> new ExecutionRecord<>(
                cur.tenantId(), cur.executionId(), cur.executionKey(),
                cur.pipelineId(), cur.contractVersion(), cur.releaseVersion(),
                cur.resultShape(), ExecutionStatus.WAITING_EXTERNAL, cur.version() + 1L,
                stepIndex, cur.attempt(), null, 0L, 0L, transitionKey,
                cur.inputPayload(), awaitUnitId, null, null, null,
                cur.createdAtEpochMs(), nowEpochMs, cur.ttlEpochS(),
                0L, 0, null, null, -1, Optional.empty(), Optional.empty(), Optional.empty(),
                cur.pagingState(), cur.awaitOccurrenceCause(), cur.dispatchPin()));
        }

        @Override
        public Uni<Optional<AwaitUnitReleaseAcknowledgement>> getAwaitUnitReleaseAcknowledgement(
            String tenantId, String executionId, String awaitUnitId) {
            return Uni.createFrom().item(Optional.empty());
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> markAwaitCompleted(
            String tenantId, String executionId, String awaitUnitId, int nextStepIndex, long nowEpochMs) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    String key = scope(tenantId, executionId);
                    ExecutionRecord<Object, Object> cur = executions.get(key);
                    if (cur == null || cur.status() != ExecutionStatus.WAITING_EXTERNAL
                        || !Objects.equals(cur.awaitUnitId(), awaitUnitId)) {
                        return Optional.empty();
                    }
                    ExecutionRecord<Object, Object> updated = new ExecutionRecord<>(
                        cur.tenantId(), cur.executionId(), cur.executionKey(),
                        cur.pipelineId(), cur.contractVersion(), cur.releaseVersion(),
                        cur.resultShape(), ExecutionStatus.QUEUED, cur.version() + 1L,
                        nextStepIndex, 0, null, 0L, nowEpochMs, cur.lastTransitionKey(),
                        cur.inputPayload(), cur.awaitUnitId(), null, null, null,
                        cur.createdAtEpochMs(), nowEpochMs, cur.ttlEpochS(),
                        0L, 0, null, null, -1, Optional.empty(), Optional.empty(), Optional.empty(),
                        cur.pagingState(), cur.awaitOccurrenceCause(), cur.dispatchPin());
                    executions.put(key, updated);
                    flush();
                    return Optional.of(updated);
                }
            });
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> markAwaitItemContinuationsCompleted(
            String tenantId, String executionId, String awaitUnitId, int nextStepIndex,
            Object resultPayload, long nowEpochMs) {
            return Uni.createFrom().item(Optional.empty());
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> scheduleRetry(
            String tenantId, String executionId, long expectedVersion, int nextAttempt, long nextDueEpochMs,
            String transitionKey, String errorCode, String errorMessage, long nowEpochMs) {
            return updateMatching(tenantId, executionId, expectedVersion, cur -> new ExecutionRecord<>(
                cur.tenantId(), cur.executionId(), cur.executionKey(),
                cur.pipelineId(), cur.contractVersion(), cur.releaseVersion(),
                cur.resultShape(), ExecutionStatus.WAIT_RETRY, cur.version() + 1L,
                cur.currentStepIndex(), nextAttempt, null, 0L, nextDueEpochMs, transitionKey,
                cur.inputPayload(), cur.awaitUnitId(), null, errorCode, errorMessage,
                cur.createdAtEpochMs(), nowEpochMs, cur.ttlEpochS(),
                cur.firstCircuitDeferredAtEpochMs(), cur.circuitDeferralCount(), cur.circuitIdentity(),
                cur.redriveIntent(), cur.failedStepIndex(), cur.failedCommandId(),
                cur.redriveTargetCommandId(), cur.redriveReason(), cur.pagingState(),
                cur.awaitOccurrenceCause(), cur.dispatchPin()));
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> markRemoteOutcomeUnknown(
            String tenantId, String executionId, long expectedVersion, String transitionKey,
            String errorCode, String errorMessage, long nowEpochMs) {
            return updateMatching(tenantId, executionId, expectedVersion, cur -> new ExecutionRecord<>(
                cur.tenantId(), cur.executionId(), cur.executionKey(),
                cur.pipelineId(), cur.contractVersion(), cur.releaseVersion(),
                cur.resultShape(), ExecutionStatus.REMOTE_OUTCOME_UNKNOWN, cur.version() + 1L,
                cur.currentStepIndex(), cur.attempt(), null, 0L, 0L, transitionKey,
                cur.inputPayload(), cur.awaitUnitId(), null, errorCode, errorMessage,
                cur.createdAtEpochMs(), nowEpochMs, cur.ttlEpochS(),
                0L, 0, null, null, cur.failedStepIndex(), cur.failedCommandId(),
                cur.redriveTargetCommandId(), cur.redriveReason(), cur.pagingState(),
                cur.awaitOccurrenceCause(), cur.dispatchPin()));
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> markTerminalFailure(
            String tenantId, String executionId, long expectedVersion, ExecutionStatus terminalStatus,
            String transitionKey, String errorCode, String errorMessage, long nowEpochMs) {
            return markTerminalFailure(tenantId, executionId, expectedVersion, terminalStatus,
                transitionKey, errorCode, errorMessage, -1, Optional.empty(), nowEpochMs);
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> markTerminalFailure(
            String tenantId, String executionId, long expectedVersion, ExecutionStatus terminalStatus,
            String transitionKey, String errorCode, String errorMessage, int failedStepIndex,
            Optional<String> failedCommandId, long nowEpochMs) {
            return updateMatching(tenantId, executionId, expectedVersion, cur -> new ExecutionRecord<>(
                cur.tenantId(), cur.executionId(), cur.executionKey(),
                cur.pipelineId(), cur.contractVersion(), cur.releaseVersion(),
                cur.resultShape(), terminalStatus, cur.version() + 1L,
                cur.currentStepIndex(), cur.attempt(), null, 0L, 0L, transitionKey,
                cur.inputPayload(), cur.awaitUnitId(), null, errorCode, errorMessage,
                cur.createdAtEpochMs(), nowEpochMs, cur.ttlEpochS(),
                0L, 0, null, null, failedStepIndex, failedCommandId,
                Optional.empty(), Optional.empty(), cur.pagingState(),
                cur.awaitOccurrenceCause(), cur.dispatchPin()));
        }

        @Override
        public Uni<List<ExecutionRecord<Object, Object>>> findDueExecutions(long nowEpochMs, int limit) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    return executions.values().stream()
                        .filter(r -> r.status() == ExecutionStatus.QUEUED
                            || (r.status() == ExecutionStatus.WAIT_RETRY && r.nextDueEpochMs() <= nowEpochMs)
                            || (r.status() == ExecutionStatus.RUNNING && r.leaseExpiresEpochMs() <= nowEpochMs))
                        .limit(Math.max(1, limit))
                        .toList();
                }
            });
        }

        @Override
        public Uni<Optional<ExecutionRecord<Object, Object>>> redriveTerminalExecution(
            String tenantId, String executionId, long expectedVersion, boolean allowFailed,
            String reason, long nowEpochMs) {
            return Uni.createFrom().item(Optional.empty());
        }

        private Uni<Optional<ExecutionRecord<Object, Object>>> updateMatching(
            String tenantId, String executionId, long expectedVersion,
            java.util.function.UnaryOperator<ExecutionRecord<Object, Object>> mutator) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    String key = scope(tenantId, executionId);
                    ExecutionRecord<Object, Object> cur = executions.get(key);
                    if (cur == null || cur.version() != expectedVersion) {
                        return Optional.empty();
                    }
                    ExecutionRecord<Object, Object> updated = mutator.apply(cur);
                    executions.put(key, updated);
                    flush();
                    return Optional.of(updated);
                }
            });
        }

        private void flush() {
            ObjectNode root = JSON.createObjectNode();
            ArrayNode list = root.putArray("executions");
            for (var r : executions.values()) {
                ObjectNode n = list.addObject();
                n.put("tenantId", r.tenantId());
                n.put("executionId", r.executionId());
                n.put("executionKey", r.executionKey());
                n.put("pipelineId", r.pipelineId());
                n.put("contractVersion", r.contractVersion());
                n.put("releaseVersion", r.releaseVersion());
                n.put("resultShape", r.resultShape().name());
                n.put("status", r.status().name());
                n.put("version", r.version());
                n.put("currentStepIndex", r.currentStepIndex());
                n.put("attempt", r.attempt());
                if (r.leaseOwner() != null) n.put("leaseOwner", r.leaseOwner());
                n.put("leaseExpiresEpochMs", r.leaseExpiresEpochMs());
                n.put("nextDueEpochMs", r.nextDueEpochMs());
                if (r.lastTransitionKey() != null) n.put("lastTransitionKey", r.lastTransitionKey());
                encodePayload(codec, r.inputPayload()).ifPresent(v -> n.set("inputPayload", v));
                if (r.awaitUnitId() != null) n.put("awaitUnitId", r.awaitUnitId());
                encodePayload(codec, r.resultPayload()).ifPresent(v -> n.set("resultPayload", v));
                if (r.errorCode() != null) n.put("errorCode", r.errorCode());
                if (r.errorMessage() != null) n.put("errorMessage", r.errorMessage());
                n.put("createdAtEpochMs", r.createdAtEpochMs());
                n.put("updatedAtEpochMs", r.updatedAtEpochMs());
                n.put("ttlEpochS", r.ttlEpochS());
                n.put("failedStepIndex", r.failedStepIndex());
                r.failedCommandId().ifPresent(v -> n.put("failedCommandId", v));
                r.awaitOccurrenceCause().ifPresent(v -> n.put("awaitOccurrenceCause", v));
            }
            file.writeAtomically(root);
        }

        private void load() {
            file.readIfPresent().ifPresent(root -> {
                for (JsonNode n : root.path("executions")) {
                    ExecutionRecord<Object, Object> r = new ExecutionRecord<>(
                        n.path("tenantId").asText(),
                        n.path("executionId").asText(),
                        n.path("executionKey").asText(),
                        n.path("pipelineId").asText(),
                        n.path("contractVersion").asText(),
                        n.path("releaseVersion").asText(),
                        ExecutionResultShape.valueOf(n.path("resultShape").asText("SINGLE")),
                        ExecutionStatus.valueOf(n.path("status").asText()),
                        n.path("version").asLong(),
                        n.path("currentStepIndex").asInt(),
                        n.path("attempt").asInt(),
                        optString(n, "leaseOwner").orElse(null),
                        n.path("leaseExpiresEpochMs").asLong(),
                        n.path("nextDueEpochMs").asLong(),
                        optString(n, "lastTransitionKey").orElse(null),
                        decodePayload(codec, n.get("inputPayload")).orElse(null),
                        optString(n, "awaitUnitId").orElse(null),
                        decodePayload(codec, n.get("resultPayload")).orElse(null),
                        optString(n, "errorCode").orElse(null),
                        optString(n, "errorMessage").orElse(null),
                        n.path("createdAtEpochMs").asLong(),
                        n.path("updatedAtEpochMs").asLong(),
                        n.path("ttlEpochS").asLong(),
                        0L, 0, null, null,
                        n.path("failedStepIndex").asInt(-1),
                        optString(n, "failedCommandId"),
                        Optional.empty(), Optional.empty(), Optional.empty(),
                        optString(n, "awaitOccurrenceCause"),
                        Optional.empty());
                    executions.put(scope(r.tenantId(), r.executionId()), r);
                    idByKey.put(r.tenantId() + "\0" + r.executionKey(), r.executionId());
                }
            });
        }

        private static String scope(String tenantId, String executionId) {
            return tenantId + "\0" + executionId;
        }
    }

    // --- AwaitUnitStore ---

    public static final class FileAwaitUnitStore implements AwaitUnitStore {
        private final Object lock = new Object();
        private final AtomicJsonFile file;
        private final Map<String, AwaitUnitRecord> units = new LinkedHashMap<>();

        FileAwaitUnitStore(AtomicJsonFile file) {
            this.file = file;
            load();
        }

        @Override public String providerName() { return "local-file"; }

        @Override
        public Uni<AwaitUnitRecord> createOrGet(AwaitUnitCreateCommand cmd) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    String key = cmd.tenantId() + "\0" + cmd.unitId();
                    AwaitUnitRecord existing = units.get(key);
                    if (existing != null) return existing;
                    AwaitUnitRecord created = new AwaitUnitRecord(
                        cmd.tenantId(), cmd.unitId(), cmd.executionId(), cmd.stepId(),
                        cmd.stepIndex(), cmd.cardinality(), 0L, AwaitUnitStatus.WAITING_EXTERNAL,
                        null, null, 0, Set.of(), false, cmd.nowEpochMs(), cmd.nowEpochMs(), cmd.ttlEpochS());
                    units.put(key, created);
                    flush();
                    return created;
                }
            });
        }

        @Override
        public Uni<Optional<AwaitUnitRecord>> get(String tenantId, String unitId) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    return Optional.ofNullable(units.get(tenantId + "\0" + unitId));
                }
            });
        }

        @Override
        public Uni<AwaitUnitRecord> importRecord(AwaitUnitRecord record) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    units.putIfAbsent(record.tenantId() + "\0" + record.unitId(), record);
                    flush();
                    return units.get(record.tenantId() + "\0" + record.unitId());
                }
            });
        }

        @Override
        public Uni<Optional<AwaitUnitRecord>> attachPrimaryInteraction(
            String tenantId, String unitId, String interactionId, long nowEpochMs) {
            return update(tenantId, unitId, u -> new AwaitUnitRecord(
                u.tenantId(), u.unitId(), u.executionId(), u.stepId(), u.stepIndex(), u.cardinality(),
                u.version() + 1L, u.status(), interactionId, u.expectedItemCount(),
                u.completedItemCount(), u.completedItemKeys(), u.dispatchComplete(),
                u.createdAtEpochMs(), nowEpochMs, u.ttlEpochS()));
        }

        @Override
        public Uni<Optional<AwaitUnitRecord>> markDispatchComplete(
            String tenantId, String unitId, int expectedItemCount, long nowEpochMs) {
            return update(tenantId, unitId, u -> new AwaitUnitRecord(
                u.tenantId(), u.unitId(), u.executionId(), u.stepId(), u.stepIndex(), u.cardinality(),
                u.version() + 1L, u.status(), u.primaryInteractionId(), expectedItemCount,
                u.completedItemCount(), u.completedItemKeys(), true,
                u.createdAtEpochMs(), nowEpochMs, u.ttlEpochS()));
        }

        @Override
        public Uni<Optional<AwaitUnitRecord>> markCompleted(String tenantId, String unitId, long nowEpochMs) {
            return update(tenantId, unitId, u -> new AwaitUnitRecord(
                u.tenantId(), u.unitId(), u.executionId(), u.stepId(), u.stepIndex(), u.cardinality(),
                u.version() + 1L, AwaitUnitStatus.COMPLETED, u.primaryInteractionId(), u.expectedItemCount(),
                u.completedItemCount() + 1, u.completedItemKeys(), u.dispatchComplete(),
                u.createdAtEpochMs(), nowEpochMs, u.ttlEpochS()));
        }

        @Override
        public Uni<Optional<AwaitUnitRecord>> recordItemCompleted(
            String tenantId, String unitId, String itemKey, long nowEpochMs) {
            return update(tenantId, unitId, u -> {
                Set<String> keys = new HashSet<>(u.completedItemKeys());
                keys.add(itemKey);
                return new AwaitUnitRecord(
                    u.tenantId(), u.unitId(), u.executionId(), u.stepId(), u.stepIndex(), u.cardinality(),
                    u.version() + 1L, u.status(), u.primaryInteractionId(), u.expectedItemCount(),
                    keys.size(), Set.copyOf(keys), u.dispatchComplete(),
                    u.createdAtEpochMs(), nowEpochMs, u.ttlEpochS());
            });
        }

        @Override
        public Uni<Optional<AwaitUnitRecord>> recordItemContinuationCompleted(
            String tenantId, String unitId, String itemKey, long nowEpochMs) {
            return recordItemCompleted(tenantId, unitId, itemKey, nowEpochMs);
        }

        @Override
        public Uni<Optional<AwaitUnitRecord>> markTerminal(
            String tenantId, String unitId, AwaitUnitStatus status, long nowEpochMs) {
            return update(tenantId, unitId, u -> new AwaitUnitRecord(
                u.tenantId(), u.unitId(), u.executionId(), u.stepId(), u.stepIndex(), u.cardinality(),
                u.version() + 1L, status, u.primaryInteractionId(), u.expectedItemCount(),
                u.completedItemCount(), u.completedItemKeys(), u.dispatchComplete(),
                u.createdAtEpochMs(), nowEpochMs, u.ttlEpochS()));
        }

        private Uni<Optional<AwaitUnitRecord>> update(
            String tenantId, String unitId, java.util.function.UnaryOperator<AwaitUnitRecord> fn) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    String key = tenantId + "\0" + unitId;
                    AwaitUnitRecord cur = units.get(key);
                    if (cur == null) return Optional.empty();
                    AwaitUnitRecord next = fn.apply(cur);
                    units.put(key, next);
                    flush();
                    return Optional.of(next);
                }
            });
        }

        private void flush() {
            ObjectNode root = JSON.createObjectNode();
            ArrayNode list = root.putArray("units");
            for (var u : units.values()) {
                ObjectNode n = list.addObject();
                n.put("tenantId", u.tenantId());
                n.put("unitId", u.unitId());
                n.put("executionId", u.executionId());
                n.put("stepId", u.stepId());
                n.put("stepIndex", u.stepIndex());
                n.put("cardinality", u.cardinality());
                n.put("version", u.version());
                n.put("status", u.status().name());
                if (u.primaryInteractionId() != null) n.put("primaryInteractionId", u.primaryInteractionId());
                if (u.expectedItemCount() != null) n.put("expectedItemCount", u.expectedItemCount());
                n.put("completedItemCount", u.completedItemCount());
                ArrayNode ks = n.putArray("completedItemKeys");
                u.completedItemKeys().forEach(ks::add);
                n.put("dispatchComplete", u.dispatchComplete());
                n.put("createdAtEpochMs", u.createdAtEpochMs());
                n.put("updatedAtEpochMs", u.updatedAtEpochMs());
                n.put("ttlEpochS", u.ttlEpochS());
            }
            file.writeAtomically(root);
        }

        private void load() {
            file.readIfPresent().ifPresent(root -> {
                for (JsonNode n : root.path("units")) {
                    Set<String> keys = new HashSet<>();
                    n.path("completedItemKeys").forEach(k -> keys.add(k.asText()));
                    AwaitUnitRecord u = new AwaitUnitRecord(
                        n.path("tenantId").asText(),
                        n.path("unitId").asText(),
                        n.path("executionId").asText(),
                        n.path("stepId").asText(),
                        n.path("stepIndex").asInt(),
                        n.path("cardinality").asText(),
                        n.path("version").asLong(),
                        AwaitUnitStatus.valueOf(n.path("status").asText()),
                        optString(n, "primaryInteractionId").orElse(null),
                        n.hasNonNull("expectedItemCount") ? n.get("expectedItemCount").asInt() : null,
                        n.path("completedItemCount").asInt(),
                        Set.copyOf(keys),
                        n.path("dispatchComplete").asBoolean(),
                        n.path("createdAtEpochMs").asLong(),
                        n.path("updatedAtEpochMs").asLong(),
                        n.path("ttlEpochS").asLong());
                    units.put(u.tenantId() + "\0" + u.unitId(), u);
                }
            });
        }
    }

    // --- AwaitInteractionStore ---

    public static final class FileAwaitInteractionStore implements AwaitInteractionStore {
        private final Object lock = new Object();
        private final AtomicJsonFile file;
        private final JsonTransitionPayloadCodec codec;
        private final Map<String, AwaitInteractionRecord> interactions = new LinkedHashMap<>();
        private final Map<String, String> idByIdem = new HashMap<>();
        private final Map<String, String> idByCorr = new HashMap<>();

        FileAwaitInteractionStore(AtomicJsonFile file, JsonTransitionPayloadCodec codec) {
            this.file = file;
            this.codec = codec;
            load();
        }

        @Override public String providerName() { return "local-file"; }

        @Override
        public Uni<AwaitCreateResult> createOrGet(AwaitCreateCommand cmd) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    String idemKey = cmd.tenantId() + "\0" + cmd.stepId() + "\0" + cmd.idempotencyKey();
                    String existingId = idByIdem.get(idemKey);
                    if (existingId != null) {
                        AwaitInteractionRecord existing = interactions.get(cmd.tenantId() + "\0" + existingId);
                        if (existing != null) return new AwaitCreateResult(existing, true);
                    }
                    String id = UUID.randomUUID().toString();
                    AwaitInteractionRecord created = new AwaitInteractionRecord(
                        cmd.tenantId(), cmd.executionId(), cmd.stepId(), cmd.stepIndex(),
                        cmd.outputType(), id, cmd.correlationId(), cmd.causationId(),
                        cmd.idempotencyKey(), 0L, AwaitInteractionStatus.WAITING,
                        cmd.requestPayload(), null, cmd.unitId(), cmd.itemIndex(), null,
                        cmd.assignee(), cmd.group(), cmd.transportType(), cmd.transportMetadata(),
                        cmd.deadlineEpochMs(), cmd.nowEpochMs(), cmd.nowEpochMs(), cmd.ttlEpochS(),
                        cmd.transportOutputType());
                    interactions.put(cmd.tenantId() + "\0" + id, created);
                    idByIdem.put(idemKey, id);
                    if (cmd.correlationId() != null) {
                        idByCorr.put(cmd.tenantId() + "\0" + cmd.correlationId(), id);
                    }
                    flush();
                    return new AwaitCreateResult(created, false);
                }
            });
        }

        @Override
        public Uni<Optional<AwaitInteractionRecord>> get(String tenantId, String interactionId) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    return Optional.ofNullable(interactions.get(tenantId + "\0" + interactionId));
                }
            });
        }

        @Override
        public Uni<AwaitInteractionRecord> importRecord(AwaitInteractionRecord record) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    interactions.putIfAbsent(record.tenantId() + "\0" + record.interactionId(), record);
                    flush();
                    return interactions.get(record.tenantId() + "\0" + record.interactionId());
                }
            });
        }

        @Override
        public Uni<Optional<AwaitInteractionRecord>> findByCorrelation(String tenantId, String correlationId) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    String id = idByCorr.get(tenantId + "\0" + correlationId);
                    return id == null ? Optional.empty() : Optional.ofNullable(interactions.get(tenantId + "\0" + id));
                }
            });
        }

        @Override
        public Uni<List<AwaitInteractionRecord>> findByUnit(String tenantId, String unitId) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    return interactions.values().stream()
                        .filter(i -> tenantId.equals(i.tenantId()) && unitId.equals(i.unitId()))
                        .toList();
                }
            });
        }

        @Override
        public Uni<AwaitInteractionPage> findByUnitPage(
            String tenantId, String unitId, int limit, Optional<AwaitInteractionCursor> cursor) {
            return findByUnit(tenantId, unitId).map(list -> new AwaitInteractionPage(list, Optional.empty()));
        }

        @Override
        public Uni<Optional<AwaitInteractionRecord>> markDispatching(
            String tenantId, String interactionId, long expectedVersion, long nowEpochMs) {
            return get(tenantId, interactionId);
        }

        @Override
        public Uni<Optional<AwaitInteractionRecord>> markDispatching(
            String tenantId, String interactionId, long expectedVersion,
            Map<String, Object> metadata, long nowEpochMs) {
            return get(tenantId, interactionId);
        }

        @Override
        public Uni<Optional<AwaitInteractionRecord>> markDispatched(
            String tenantId, String interactionId, long expectedVersion,
            Map<String, Object> metadata, long nowEpochMs) {
            return get(tenantId, interactionId);
        }

        @Override
        public Uni<AwaitCompletionResult> complete(AwaitCompletionCommand cmd) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    String id = cmd.interactionId();
                    if ((id == null || id.isBlank()) && cmd.correlationId() != null) {
                        id = idByCorr.get(cmd.tenantId() + "\0" + cmd.correlationId());
                    }
                    AwaitInteractionRecord cur = id == null ? null : interactions.get(cmd.tenantId() + "\0" + id);
                    if (cur == null) {
                        throw new AwaitInteractionNotFoundException("Await interaction not found");
                    }
                    if (cur.status() == AwaitInteractionStatus.COMPLETED) {
                        return new AwaitCompletionResult(cur, true);
                    }
                    if (cur.status().terminal()) {
                        throw new AwaitInteractionTerminalException("Await interaction already terminal: " + cur.status());
                    }
                    AwaitInteractionRecord completed = new AwaitInteractionRecord(
                        cur.tenantId(), cur.executionId(), cur.stepId(), cur.stepIndex(),
                        cur.outputType(), cur.interactionId(), cur.correlationId(), cur.causationId(),
                        cur.idempotencyKey(), cur.version() + 1L, AwaitInteractionStatus.COMPLETED,
                        cur.requestPayload(), cmd.responsePayload(), cur.unitId(), cur.itemIndex(),
                        cmd.actor(), cur.assignee(), cur.group(), cur.transportType(), cur.transportMetadata(),
                        cur.deadlineEpochMs(), cur.createdAtEpochMs(), cmd.nowEpochMs(), cur.ttlEpochS(),
                        cur.transportOutputType());
                    interactions.put(cur.tenantId() + "\0" + cur.interactionId(), completed);
                    flush();
                    return new AwaitCompletionResult(completed, false);
                }
            });
        }

        @Override
        public Uni<Optional<AwaitCompletionAdmission>> getCompletionAdmission(String tenantId, String interactionId) {
            return Uni.createFrom().item(Optional.empty());
        }

        @Override
        public Uni<AwaitCompletionPage> findOpenCompletions(
            String tenantId, int limit, Optional<AwaitCompletionCursor> cursor) {
            return Uni.createFrom().item(new AwaitCompletionPage(List.of(), Optional.empty(), 0));
        }

        @Override
        public Uni<Void> retireCompletionAdmission(AwaitCompletionAdmission admission) {
            return Uni.createFrom().voidItem();
        }

        @Override
        public Uni<Optional<AwaitInteractionRecord>> fail(
            String tenantId, String interactionId, long expectedVersion, String reason, long nowEpochMs) {
            return transitionTerminal(tenantId, interactionId, expectedVersion, AwaitInteractionStatus.FAILED, nowEpochMs);
        }

        @Override
        public Uni<Optional<AwaitInteractionRecord>> cancel(
            String tenantId, String interactionId, long expectedVersion, String reason, long nowEpochMs) {
            return transitionTerminal(tenantId, interactionId, expectedVersion, AwaitInteractionStatus.CANCELLED, nowEpochMs);
        }

        @Override
        public Uni<Optional<AwaitInteractionRecord>> markTimedOut(
            String tenantId, String interactionId, long expectedVersion, long nowEpochMs) {
            return transitionTerminal(tenantId, interactionId, expectedVersion, AwaitInteractionStatus.TIMED_OUT, nowEpochMs);
        }

        @Override
        public Uni<List<AwaitInteractionRecord>> findTimedOut(long nowEpochMs, int limit) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    return interactions.values().stream()
                        .filter(i -> !i.status().terminal() && i.deadlineEpochMs() <= nowEpochMs)
                        .sorted(Comparator.comparingLong(AwaitInteractionRecord::deadlineEpochMs))
                        .limit(Math.max(1, limit))
                        .toList();
                }
            });
        }

        @Override
        public Uni<List<AwaitInteractionRecord>> findDueTimeoutConsequences(long nowEpochMs, int limit) {
            return Uni.createFrom().item(List.of());
        }

        @Override
        public Uni<Void> completeTimeoutConsequences(AwaitInteractionRecord interaction) {
            return Uni.createFrom().voidItem();
        }

        @Override
        public Uni<List<AwaitInteractionRecord>> queryPending(
            String tenantId, String assignee, String group, String stepId, int limit) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    return interactions.values().stream()
                        .filter(i -> tenantId.equals(i.tenantId()) && !i.status().terminal())
                        .limit(Math.max(1, limit))
                        .toList();
                }
            });
        }

        private Uni<Optional<AwaitInteractionRecord>> transitionTerminal(
            String tenantId, String interactionId, long expectedVersion,
            AwaitInteractionStatus status, long nowEpochMs) {
            return Uni.createFrom().item(() -> {
                synchronized (lock) {
                    String key = tenantId + "\0" + interactionId;
                    AwaitInteractionRecord cur = interactions.get(key);
                    if (cur == null || cur.version() != expectedVersion || cur.status().terminal()) {
                        return Optional.empty();
                    }
                    AwaitInteractionRecord next = new AwaitInteractionRecord(
                        cur.tenantId(), cur.executionId(), cur.stepId(), cur.stepIndex(),
                        cur.outputType(), cur.interactionId(), cur.correlationId(), cur.causationId(),
                        cur.idempotencyKey(), cur.version() + 1L, status,
                        cur.requestPayload(), cur.responsePayload(), cur.unitId(), cur.itemIndex(),
                        cur.actor(), cur.assignee(), cur.group(), cur.transportType(), cur.transportMetadata(),
                        cur.deadlineEpochMs(), cur.createdAtEpochMs(), nowEpochMs, cur.ttlEpochS(),
                        cur.transportOutputType());
                    interactions.put(key, next);
                    flush();
                    return Optional.of(next);
                }
            });
        }

        private void flush() {
            ObjectNode root = JSON.createObjectNode();
            ArrayNode list = root.putArray("interactions");
            for (var i : interactions.values()) {
                ObjectNode n = list.addObject();
                n.put("tenantId", i.tenantId());
                n.put("executionId", i.executionId());
                n.put("stepId", i.stepId());
                n.put("stepIndex", i.stepIndex());
                n.put("outputType", i.outputType());
                n.put("interactionId", i.interactionId());
                if (i.correlationId() != null) n.put("correlationId", i.correlationId());
                if (i.causationId() != null) n.put("causationId", i.causationId());
                if (i.idempotencyKey() != null) n.put("idempotencyKey", i.idempotencyKey());
                n.put("version", i.version());
                n.put("status", i.status().name());
                encodePayload(codec, i.requestPayload()).ifPresent(v -> n.set("requestPayload", v));
                encodePayload(codec, i.responsePayload()).ifPresent(v -> n.set("responsePayload", v));
                if (i.unitId() != null) n.put("unitId", i.unitId());
                if (i.itemIndex() != null) n.put("itemIndex", i.itemIndex());
                if (i.actor() != null) n.put("actor", i.actor());
                if (i.assignee() != null) n.put("assignee", i.assignee());
                if (i.group() != null) n.put("group", i.group());
                if (i.transportType() != null) n.put("transportType", i.transportType());
                n.put("deadlineEpochMs", i.deadlineEpochMs());
                n.put("createdAtEpochMs", i.createdAtEpochMs());
                n.put("updatedAtEpochMs", i.updatedAtEpochMs());
                n.put("ttlEpochS", i.ttlEpochS());
                if (i.transportOutputType() != null) n.put("transportOutputType", i.transportOutputType());
            }
            file.writeAtomically(root);
        }

        private void load() {
            file.readIfPresent().ifPresent(root -> {
                for (JsonNode n : root.path("interactions")) {
                    AwaitInteractionRecord i = new AwaitInteractionRecord(
                        n.path("tenantId").asText(),
                        n.path("executionId").asText(),
                        n.path("stepId").asText(),
                        n.path("stepIndex").asInt(),
                        n.path("outputType").asText(),
                        n.path("interactionId").asText(),
                        optString(n, "correlationId").orElse(null),
                        optString(n, "causationId").orElse(null),
                        optString(n, "idempotencyKey").orElse(null),
                        n.path("version").asLong(),
                        AwaitInteractionStatus.valueOf(n.path("status").asText()),
                        decodePayload(codec, n.get("requestPayload")).orElse(null),
                        decodePayload(codec, n.get("responsePayload")).orElse(null),
                        optString(n, "unitId").orElse(null),
                        n.hasNonNull("itemIndex") ? n.get("itemIndex").asInt() : null,
                        optString(n, "actor").orElse(null),
                        optString(n, "assignee").orElse(null),
                        optString(n, "group").orElse(null),
                        optString(n, "transportType").orElse(null),
                        Map.of("transport", "callback"),
                        n.path("deadlineEpochMs").asLong(),
                        n.path("createdAtEpochMs").asLong(),
                        n.path("updatedAtEpochMs").asLong(),
                        n.path("ttlEpochS").asLong(),
                        optString(n, "transportOutputType").orElse(null));
                    interactions.put(i.tenantId() + "\0" + i.interactionId(), i);
                    if (i.idempotencyKey() != null) {
                        idByIdem.put(i.tenantId() + "\0" + i.stepId() + "\0" + i.idempotencyKey(), i.interactionId());
                    }
                    if (i.correlationId() != null) {
                        idByCorr.put(i.tenantId() + "\0" + i.correlationId(), i.interactionId());
                    }
                }
            });
        }
    }
}
