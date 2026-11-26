package org.pipelineframework.command;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.cli.LocalFileDevStores;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.CommandRecoveryBinding;

/**
 * Crash-safe, zero-JNI disk-backed {@link CommandEffectStore} for local {@code tpf dev}.
 * Reuses TPF's {@link CommandEffectRecordCodec} (schema v3, full attempt history,
 * {@link CommandOutcomeSnapshot}, {@link CommandRecoveryBinding}, and
 * {@link CommandReconciliationReceipt}) persisted via fsync + atomic rename, and rolls back
 * in-memory state if the atomic disk write fails.
 */
public final class FileCommandEffectStore implements CommandEffectStore {

    private static final ObjectMapper JSON = PipelineJson.mapper();

    private final Object lock = new Object();
    private final LocalFileDevStores.AtomicJsonFile file;
    private final CommandEffectRecordCodec codec;
    private final Map<String, PersistedEntry> entries = new LinkedHashMap<>();

    public FileCommandEffectStore(Path dataDir) {
        this(dataDir, () -> true);
    }

    public FileCommandEffectStore(Path dataDir, BooleanSupplier isOpen) {
        this.file = new LocalFileDevStores.AtomicJsonFile(
            Objects.requireNonNull(dataDir, "dataDir must not be null").resolve("command-effects.json"),
            isOpen);
        this.codec = new CommandEffectRecordCodec(JSON, FileCommandEffectStore.class.getClassLoader());
        load();
    }

    @Override
    public Uni<Optional<CommandEffectRecord>> find(String tenantId, String commandId) {
        synchronized (lock) {
            PersistedEntry entry = entries.get(key(tenantId, commandId));
            return Uni.createFrom().item(Optional.ofNullable(entry).map(PersistedEntry::record));
        }
    }

    @Override
    public boolean supportsRecovery() {
        return true;
    }

    @Override
    public boolean supportsRetryAttempts() {
        return true;
    }

    @Override
    public boolean supportsAttempt(CommandAttemptPurpose purpose) {
        return purpose == CommandAttemptPurpose.RETRY || purpose == CommandAttemptPurpose.REISSUE;
    }

    @Override
    public boolean supportsNativeOutcomeSnapshots() {
        return true;
    }

    @Override
    public Uni<CommandEffectRecord> createPending(CommandRequest<?> request, long nowEpochMs) {
        return createPendingBound(request, Optional.empty(), nowEpochMs);
    }

    @Override
    public Uni<CommandEffectRecord> createPending(
        CommandRequest<?> request,
        CommandRecoveryBinding binding,
        long nowEpochMs
    ) {
        return createPendingBound(request, Optional.of(Objects.requireNonNull(binding, "binding must not be null")), nowEpochMs);
    }

    private Uni<CommandEffectRecord> createPendingBound(
        CommandRequest<?> request,
        Optional<CommandRecoveryBinding> binding,
        long nowEpochMs
    ) {
        synchronized (lock) {
            CommandEffectRecord pending = new CommandEffectRecord(
                request.executionContext().tenantId(),
                request.executionContext().executionId(),
                request.descriptor().stepId(),
                request.descriptor().command(),
                request.commandId(),
                CommandEffectStatus.PENDING,
                request.input(),
                null,
                null,
                null,
                Optional.empty(),
                List.of(new CommandEffectAttemptRecord(
                    request.attemptId(),
                    request.occurrenceId(),
                    1,
                    request.executionContext().executionId(),
                    CommandAttemptPurpose.INITIAL,
                    CommandEffectStatus.PENDING,
                    Optional.empty(),
                    null,
                    null,
                    Optional.empty(),
                    Optional.empty(),
                    nowEpochMs,
                    nowEpochMs)),
                nowEpochMs,
                nowEpochMs);
            pending = binding.map(pending::bindRecovery).orElse(pending);
            String inputType = request.descriptor().inputType();
            String outputType = request.descriptor().outputType();
            if (binding.isPresent()) {
                codec.validateRecovery(pending, inputType, outputType);
            }
            String k = key(pending.tenantId(), pending.commandId());
            if (entries.containsKey(k)) {
                return Uni.createFrom().failure(new IllegalStateException(
                    "Command effect record already exists for commandId " + pending.commandId()));
            }
            PersistedEntry candidate = new PersistedEntry(pending, inputType, outputType);
            Map<String, PersistedEntry> nextEntries = new LinkedHashMap<>(entries);
            nextEntries.put(k, candidate);
            persistSnapshot(nextEntries);
            entries.put(k, candidate);
            return Uni.createFrom().item(pending);
        }
    }

    @Override
    public Uni<CommandEffectRecord> createRetryAttempt(CommandRequest<?> request, long nowEpochMs) {
        return createAttempt(request, CommandAttemptAdmission.retry(), nowEpochMs);
    }

    @Override
    public Uni<CommandEffectRecord> createAttempt(
        CommandRequest<?> request,
        CommandAttemptAdmission admission,
        long nowEpochMs
    ) {
        return update(request.executionContext().tenantId(), request.commandId(),
            existing -> existing.appendAttempt(request, admission, nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> createAttempt(
        CommandRequest<?> request,
        CommandAttemptAdmission admission,
        CommandRecoveryBinding binding,
        long nowEpochMs
    ) {
        return update(request.executionContext().tenantId(), request.commandId(),
            existing -> {
                CommandEffectRecord next = existing.appendAttempt(request, admission, nowEpochMs).bindRecovery(binding);
                codec.validateRecovery(next, request.descriptor().inputType(), request.descriptor().outputType());
                return next;
            });
    }

    @Override
    public Uni<CommandEffectRecord> claimPendingDispatch(CommandRecoveryBinding expected, long nowEpochMs) {
        return update(expected.tenantId(), expected.commandId(), current -> {
            codec.validateRecovery(current, expected.inputType(), expected.outputType());
            return current.claimPendingDispatch(expected, nowEpochMs);
        });
    }

    @Override
    public Uni<CommandEffectRecord> reconcileSucceeded(
        CommandRecoveryBinding expected,
        CommandEffectStatus expectedStatus,
        Object output,
        CommandOutcomeSnapshot outcome,
        CommandReconciliationReceipt receipt,
        long nowEpochMs
    ) {
        return update(expected.tenantId(), expected.commandId(), current -> {
            CommandEffectRecord next = current.reconcileSucceeded(
                expected, expectedStatus, output, outcome, receipt, nowEpochMs);
            codec.validateRecovery(next, expected.inputType(), expected.outputType());
            return next;
        });
    }

    @Override
    public Uni<CommandEffectRecord> markDispatching(String tenantId, String commandId, long nowEpochMs) {
        return update(tenantId, commandId,
            record -> record.dispatching(record.currentAttempt().attemptId(), nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markDispatching(String tenantId, String commandId, String attemptId, long nowEpochMs) {
        return update(tenantId, commandId, record -> record.dispatching(attemptId, nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markSucceeded(String tenantId, String commandId, Object output, long nowEpochMs) {
        return update(tenantId, commandId, record -> {
            CommandEffectRecord dispatching = legacyCompletionSource(record, nowEpochMs);
            return dispatching.succeeded(dispatching.currentAttempt().attemptId(), output, nowEpochMs);
        });
    }

    @Override
    public Uni<CommandEffectRecord> markSucceeded(
        String tenantId, String commandId, String attemptId, Object output, long nowEpochMs
    ) {
        return update(tenantId, commandId, record -> record.succeeded(attemptId, output, nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markSucceeded(
        String tenantId, String commandId, Object output, CommandOutcomeSnapshot outcome, long nowEpochMs
    ) {
        return update(tenantId, commandId, record -> {
            CommandEffectRecord dispatching = legacyCompletionSource(record, nowEpochMs);
            return dispatching.succeeded(dispatching.currentAttempt().attemptId(), output, outcome, nowEpochMs);
        });
    }

    @Override
    public Uni<CommandEffectRecord> markSucceeded(
        String tenantId, String commandId, String attemptId, Object output,
        CommandOutcomeSnapshot outcome, long nowEpochMs
    ) {
        return update(tenantId, commandId, record -> record.succeeded(attemptId, output, outcome, nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markFailed(String tenantId, String commandId, Throwable failure, long nowEpochMs) {
        return update(tenantId, commandId, record -> {
            CommandEffectRecord dispatching = legacyCompletionSource(record, nowEpochMs);
            return dispatching.failed(dispatching.currentAttempt().attemptId(), failure, nowEpochMs);
        });
    }

    @Override
    public Uni<CommandEffectRecord> markFailed(
        String tenantId, String commandId, String attemptId, Throwable failure, long nowEpochMs
    ) {
        return update(tenantId, commandId, record -> record.failed(attemptId, failure, nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markDlq(String tenantId, String commandId, Throwable failure, long nowEpochMs) {
        return update(tenantId, commandId, record -> {
            CommandEffectRecord dispatching = legacyCompletionSource(record, nowEpochMs);
            return dispatching.dlq(dispatching.currentAttempt().attemptId(), failure, nowEpochMs);
        });
    }

    @Override
    public Uni<CommandEffectRecord> markDlq(
        String tenantId, String commandId, String attemptId, Throwable failure, long nowEpochMs
    ) {
        return update(tenantId, commandId, record -> record.dlq(attemptId, failure, nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markOutcome(
        String tenantId, String commandId, CommandEffectStatus status, Throwable failure,
        CommandOutcomeSnapshot outcome, long nowEpochMs
    ) {
        return update(tenantId, commandId, record -> {
            CommandEffectRecord dispatching = legacyCompletionSource(record, nowEpochMs);
            return dispatching.failedWithStatus(
                dispatching.currentAttempt().attemptId(), status, failure, outcome, nowEpochMs);
        });
    }

    @Override
    public Uni<CommandEffectRecord> markOutcome(
        String tenantId, String commandId, String attemptId, CommandEffectStatus status,
        Throwable failure, CommandOutcomeSnapshot outcome, long nowEpochMs
    ) {
        return update(tenantId, commandId,
            record -> record.failedWithStatus(attemptId, status, failure, outcome, nowEpochMs));
    }

    private Uni<CommandEffectRecord> update(
        String tenantId,
        String commandId,
        Function<CommandEffectRecord, CommandEffectRecord> updater
    ) {
        synchronized (lock) {
            String k = key(tenantId, commandId);
            PersistedEntry existing = entries.get(k);
            if (existing == null) {
                return Uni.createFrom().failure(new IllegalStateException(
                    "No command effect record found for commandId " + commandId));
            }
            CommandEffectRecord next = updater.apply(existing.record());
            PersistedEntry candidate = new PersistedEntry(next, existing.inputType(), existing.outputType());
            Map<String, PersistedEntry> nextEntries = new LinkedHashMap<>(entries);
            nextEntries.put(k, candidate);
            persistSnapshot(nextEntries);
            entries.put(k, candidate);
            return Uni.createFrom().item(next);
        }
    }

    private void load() {
        Optional<JsonNode> root = file.readIfPresent();
        if (root.isEmpty() || !root.orElseThrow().isObject()) {
            return;
        }
        root.orElseThrow().fields().forEachRemaining(entry -> {
            String encoded = entry.getValue().asText();
            CommandEffectRecordCodec.DecodedSnapshot decoded = codec.decode(encoded);
            entries.put(entry.getKey(), new PersistedEntry(
                decoded.record(), decoded.inputDeclaredType(), decoded.outputDeclaredType()));
        });
    }

    private void persistSnapshot(Map<String, PersistedEntry> snapshot) {
        ObjectNode root = JSON.createObjectNode();
        for (Map.Entry<String, PersistedEntry> entry : snapshot.entrySet()) {
            PersistedEntry value = entry.getValue();
            root.put(entry.getKey(), codec.encode(value.record(), value.inputType(), value.outputType()));
        }
        file.writeAtomically(root);
    }

    private static String key(String tenantId, String commandId) {
        return tenantId + ":" + commandId;
    }

    private static CommandEffectRecord legacyCompletionSource(CommandEffectRecord record, long nowEpochMs) {
        return record.status() == CommandEffectStatus.PENDING
            ? record.dispatching(record.currentAttempt().attemptId(), nowEpochMs)
            : record;
    }

    private record PersistedEntry(CommandEffectRecord record, String inputType, String outputType) {
    }
}
