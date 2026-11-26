package org.pipelineframework.cli;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitInteractionPage;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.query.QueryCaptureRecord;
import org.pipelineframework.query.QueryCaptureStatus;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Focused regression tests for {@link LocalFileDevStores} durability, locking, atomic rollback,
 * indexing, pagination, filtering, and metadata preservation.
 */
class LocalFileDevStoresTest {

    private static final JsonTransitionPayloadCodec CODEC = new JsonTransitionPayloadCodec();

    @Test
    void rejectsConcurrentWritersOnSameDataDirectoryUntilClosed(@TempDir Path temp) {
        try (LocalFileDevStores first = new LocalFileDevStores(temp, CODEC)) {
            IllegalStateException conflict = assertThrows(
                IllegalStateException.class,
                () -> new LocalFileDevStores(temp, CODEC));
            assertTrue(conflict.getMessage().contains("already using data directory"), conflict.getMessage());
        }
        try (LocalFileDevStores reopened = new LocalFileDevStores(temp, CODEC)) {
            assertNotNull(reopened.executionStore);
        }
    }

    @Test
    void doesNotPublishMutationWhenSnapshotWriteFails(@TempDir Path temp) throws IOException {
        try (LocalFileDevStores stores = new LocalFileDevStores(temp, CODEC)) {
            // Replace target file with a non-empty directory so ATOMIC_MOVE fails on every platform.
            Path blockingDir = temp.resolve("executions.json");
            Files.createDirectories(blockingDir.resolve("blocker"));

            ExecutionCreateCommand cmd = new ExecutionCreateCommand(
                "t1", "exec-key-fail", "pipe-1", "v1", "1.0.0",
                "payload", ExecutionResultShape.SINGLE, 1000L, 2000L);
            assertThrows(UncheckedIOException.class,
                () -> stores.executionStore.createOrGetExecution(cmd).await().indefinitely());

            assertTrue(stores.executionStore.getExecutionByKey("t1", "exec-key-fail")
                .await().indefinitely().isEmpty(),
                "Failed snapshot write must not leave uncommitted execution visible in memory");
        }
    }

    @Test
    void indexesImportedInteractionsPaginatesAppliesFiltersAndPreservesTransportMetadata(@TempDir Path temp) {
        Map<String, Object> customMeta = Map.of("transport", "webhook", "endpoint", "https://hooks.example/cb");
        try (LocalFileDevStores stores = new LocalFileDevStores(temp, CODEC)) {
            AwaitInteractionRecord imported = new AwaitInteractionRecord(
                "t1", "exec-1", "await-approval", 1, "Decision",
                "int-a", "corr-a", "cause-a", "idem-a", 0L, AwaitInteractionStatus.WAITING,
                "req", null, "unit-1", 0, null, "alice", "finance",
                "callback", customMeta, 9_999_999L, 100L, 100L, 9_999L, "Decision");
            stores.interactionStore.importRecord(imported).await().indefinitely();

            // Imported record is immediately indexed by correlationId and idempotencyKey.
            assertEquals("int-a", stores.interactionStore.findByCorrelation("t1", "corr-a")
                .await().indefinitely().orElseThrow().interactionId());
            var dedup = stores.interactionStore.createOrGet(new AwaitCreateCommand(
                "t1", "exec-1", "await-approval", 1, "Decision", "Decision",
                "cause-a", "idem-a", "corr-a", "req", "alice", "finance",
                "callback", "unit-1", 0, customMeta, 101L, 9_999_999L, 9_999L, Optional.empty()))
                .await().indefinitely();
            assertTrue(dedup.duplicate());
            assertEquals("int-a", dedup.record().interactionId());

            // Add a second interaction for Bob on the same unit.
            AwaitInteractionRecord bob = new AwaitInteractionRecord(
                "t1", "exec-2", "await-review", 2, "Decision",
                "int-b", "corr-b", "cause-b", "idem-b", 0L, AwaitInteractionStatus.WAITING,
                "req2", null, "unit-1", 1, null, "bob", "legal",
                "callback", Map.of("transport", "slack"), 9_999_999L, 102L, 102L, 9_999L, "Decision");
            stores.interactionStore.importRecord(bob).await().indefinitely();

            // Pagination applies limit & cursor deterministically.
            AwaitInteractionPage page1 = stores.interactionStore
                .findByUnitPage("t1", "unit-1", 1, Optional.empty()).await().indefinitely();
            assertEquals(1, page1.interactions().size());
            assertEquals("int-a", page1.interactions().getFirst().interactionId());
            assertTrue(page1.nextCursor().isPresent());

            AwaitInteractionPage page2 = stores.interactionStore
                .findByUnitPage("t1", "unit-1", 1, page1.nextCursor()).await().indefinitely();
            assertEquals(1, page2.interactions().size());
            assertEquals("int-b", page2.interactions().getFirst().interactionId());
            assertTrue(page2.nextCursor().isEmpty());

            // Pending-interaction filters apply BEFORE limit.
            List<AwaitInteractionRecord> onlyBob = stores.interactionStore
                .queryPending("t1", "bob", "legal", "await-review", 1).await().indefinitely();
            assertEquals(1, onlyBob.size());
            assertEquals("int-b", onlyBob.getFirst().interactionId());

            List<AwaitInteractionRecord> onlyAlice = stores.interactionStore
                .queryPending("t1", "alice", null, null, 1).await().indefinitely();
            assertEquals(1, onlyAlice.size());
            assertEquals("int-a", onlyAlice.getFirst().interactionId());
        }

        // Reload from disk and verify custom transportMetadata survived restart.
        try (LocalFileDevStores reloaded = new LocalFileDevStores(temp, CODEC)) {
            AwaitInteractionRecord restored = reloaded.interactionStore.get("t1", "int-a")
                .await().indefinitely().orElseThrow();
            assertEquals("webhook", restored.transportMetadata().get("transport"));
            assertEquals("https://hooks.example/cb", restored.transportMetadata().get("endpoint"));
        }
    }

    @Test
    void persistsQueryCapturesAcrossRestart(@TempDir Path temp) throws Exception {
        QueryCaptureRecord capture = new QueryCaptureRecord(
            "t1", "exec-q1", 1, "customer.lookup", "1", "cap-key-1",
            "{\"customerId\":\"c1\"}", "{\"segment\":\"gold\"}", "CustomerProfile",
            Instant.ofEpochMilli(123456789L), QueryCaptureStatus.FOUND, "ok");
        try (LocalFileDevStores first = new LocalFileDevStores(temp, CODEC)) {
            first.queryCaptureStore.putIfAbsent(capture).toCompletableFuture().join();
        }
        try (LocalFileDevStores second = new LocalFileDevStores(temp, CODEC)) {
            QueryCaptureRecord restored = second.queryCaptureStore.get("cap-key-1")
                .toCompletableFuture().join().orElseThrow();
            assertEquals("{\"segment\":\"gold\"}", restored.outputJson());
            assertEquals("CustomerProfile", restored.outputType());
        }
    }
}
