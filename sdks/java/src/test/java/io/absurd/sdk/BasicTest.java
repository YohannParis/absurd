package io.absurd.sdk;

import io.absurd.sdk.internal.TaskContextImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests covering spawn, worker execution, complete, fail, cancel, and step idempotency.
 */
class BasicTest {

    @AfterEach
    void cleanup() throws Exception {
        TestSetup.clearFakeNow();
        TestSetup.cleanupQueue();
    }

    @Test
    void spawnedTaskAppearsAsPending() throws Exception {
        Absurd absurd = TestSetup.createAbsurd();
        SpawnResult result = absurd.spawn("any-task", Map.of());

        assertNotNull(result.getTaskId(), "taskId must be non-null");
        assertNotNull(result.getRunId(),  "runId must be non-null");

        String state;
        try (var conn = TestSetup.getConnection();
             var stmt = conn.prepareStatement(
                 "SELECT state FROM absurd.t_" + TestSetup.DEFAULT_QUEUE + " WHERE task_id = ?::uuid")) {
            stmt.setString(1, result.getTaskId());
            try (var rs = stmt.executeQuery()) {
                state = rs.next() ? rs.getString(1) : null;
            }
        }
        assertEquals("pending", state);
    }

    @Test
    void workerCompletesTask() throws Exception {
        Absurd absurd = TestSetup.createAbsurd();
        absurd.register(simpleDef("sum", (input, ctx) -> Map.of("answer", 42)));

        SpawnResult result = absurd.spawn("sum", Map.of("x", 1));
        Worker worker = absurd.startWorker(fastWorker());

        TaskResultSnapshot snap = absurd.awaitTaskResult(result.getTaskId(), 10_000);
        worker.stop();

        assertEquals(TaskState.COMPLETED, snap.getState());
        assertNotNull(snap.getResult());
    }

    @Test
    void workerFailsTaskOnHandlerException() throws Exception {
        Absurd absurd = TestSetup.createAbsurd();
        absurd.register(simpleDef("boom", (input, ctx) -> {
            throw new RuntimeException("kaboom");
        }));

        SpawnResult result = absurd.spawn("boom", null);
        Worker worker = absurd.startWorker(fastWorker());

        TaskResultSnapshot snap = absurd.awaitTaskResult(result.getTaskId(), 10_000);
        worker.stop();

        assertEquals(TaskState.FAILED, snap.getState());
    }

    @Test
    void cancelPendingTask() throws AbsurdException {
        Absurd absurd = TestSetup.createAbsurd();
        SpawnResult result = absurd.spawn("any-task", Map.of());

        absurd.cancelTask(result.getTaskId());

        TaskResultSnapshot snap = absurd.fetchTaskResult(result.getTaskId());
        assertNotNull(snap);
        assertEquals(TaskState.CANCELLED, snap.getState());
    }

    @Test
    void fetchTaskResultReturnsPendingBeforeWorkerRuns() throws AbsurdException {
        Absurd absurd = TestSetup.createAbsurd();
        SpawnResult result = absurd.spawn("any-task", Map.of());

        TaskResultSnapshot snap = absurd.fetchTaskResult(result.getTaskId());
        assertNotNull(snap);
        // Without a worker, the task is pending
        assertEquals(TaskState.PENDING, snap.getState());
    }

    @Test
    void stepCheckpointSurvivesRetry() throws Exception {
        AtomicInteger stepBodyCalls = new AtomicInteger(0);
        AtomicInteger handlerCalls  = new AtomicInteger(0);

        Absurd absurd = TestSetup.createAbsurd();
        absurd.register(simpleDef("checkpoint-task", (input, ctx) -> {
            int attempt = handlerCalls.incrementAndGet();

            // This step body must run exactly once across all attempts.
            ((TaskContextImpl) ctx).step("expensive-op", "compute", () -> {
                stepBodyCalls.incrementAndGet();
                return "done";
            });

            // Fail intentionally on the first attempt to trigger auto-retry.
            if (attempt == 1) {
                throw new RuntimeException("intentional failure — expect retry");
            }
            return Map.of("ok", true);
        }));

        SpawnResult result = absurd.spawn("checkpoint-task", null);
        Worker worker = absurd.startWorker(fastWorker());

        TaskResultSnapshot snap = absurd.awaitTaskResult(result.getTaskId(), 15_000);
        worker.stop();

        assertEquals(TaskState.COMPLETED, snap.getState(), "Task must complete on second attempt");
        assertEquals(2, handlerCalls.get(), "Handler must run twice (first fail, then succeed)");
        assertEquals(1, stepBodyCalls.get(), "Step body must run exactly once (cached on retry)");
    }

    // ---- helpers ----

    private static WorkerOptions fastWorker() {
        return WorkerOptions.builder()
            .queueName(TestSetup.DEFAULT_QUEUE)
            .pollIntervalMs(100)
            .pollTimeoutMs(5_000)
            .build();
    }

    private static TaskDefinition<Object, Object> simpleDef(String name, TaskHandler<Object, Object> h) {
        return new TaskDefinition<>() {
            @Override public String getName()               { return name; }
            @Override public TaskHandler<Object, Object> getHandler() { return h; }
            @Override public RetryStrategy getRetryStrategy() {
                return new RetryStrategy(3, 100, 1.0);
            }
            @Override public CancellationPolicy getCancellationPolicy() {
                return new CancellationPolicy(30_000, true);
            }
        };
    }
}
