package io.absurd.sdk;

import io.absurd.sdk.internal.TaskContextImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests covering emitEvent, awaitEvent, and cross-task event coordination.
 */
class EventTest {

    @AfterEach
    void cleanup() throws Exception {
        TestSetup.clearFakeNow();
        TestSetup.cleanupQueue();
    }

    @Test
    void emitEventPersistsPayloadInQueue() throws Exception {
        Absurd absurd = TestSetup.createAbsurd();
        absurd.emitEvent("order-placed", Map.of("id", 1));

        String payload = TestSetup.queryScalar(
            "SELECT payload::text FROM absurd.e_" + TestSetup.DEFAULT_QUEUE
                + " WHERE event_name = 'order-placed'",
            String.class
        );
        assertNotNull(payload, "Event payload must be stored");
        assertTrue(payload.contains("1"), "Stored payload must contain the emitted value");
    }

    @Test
    void emitEventIsIdempotentFirstWriteWins() throws Exception {
        Absurd absurd = TestSetup.createAbsurd();
        absurd.emitEvent("my-event", Map.of("version", 1));
        absurd.emitEvent("my-event", Map.of("version", 2)); // must be ignored

        String payload = TestSetup.queryScalar(
            "SELECT payload::text FROM absurd.e_" + TestSetup.DEFAULT_QUEUE
                + " WHERE event_name = 'my-event'",
            String.class
        );
        assertTrue(payload.contains("1"), "First emission must win");
        assertFalse(payload.contains("2"), "Second emission must be ignored");
    }

    @Test
    void taskReturnsImmediatelyWhenEventAlreadyEmitted() throws Exception {
        Absurd absurd = TestSetup.createAbsurd();

        // Emit the event BEFORE spawning the task
        absurd.emitEvent("pre-emitted", Map.of("val", 99));

        AtomicReference<String> received = new AtomicReference<>();
        absurd.register(simpleDef("waiter", (input, ctx) -> {
            String payload = ((TaskContextImpl) ctx).awaitEvent("pre-emitted", Duration.ofMinutes(1));
            received.set(payload);
            return Map.of("got", payload);
        }));

        SpawnResult result = absurd.spawn("waiter", null);
        Worker worker = absurd.startWorker(fastWorker());

        TaskResultSnapshot snap = absurd.awaitTaskResult(result.getTaskId(), 10_000);
        worker.stop();

        assertEquals(TaskState.COMPLETED, snap.getState(),
            "Task must complete immediately — event was pre-emitted");
        assertNotNull(received.get(), "Event payload must have been received");
    }

    @Test
    void taskSuspendsAndResumesWhenEventArrives() throws Exception {
        Absurd absurd = TestSetup.createAbsurd();

        AtomicReference<String> received = new AtomicReference<>();
        CountDownLatch handlerStarted = new CountDownLatch(1);

        absurd.register(simpleDef("event-waiter", (input, ctx) -> {
            handlerStarted.countDown();
            String payload = ((TaskContextImpl) ctx).awaitEvent("door-opened", Duration.ofMinutes(5));
            received.set(payload);
            return Map.of("payload", payload);
        }));

        SpawnResult result = absurd.spawn("event-waiter", null);
        Worker worker = absurd.startWorker(fastWorker());

        assertTrue(handlerStarted.await(5, TimeUnit.SECONDS), "Handler must start");

        // Wait for the task to enter sleeping state (waiting for event)
        waitForState(absurd, result.getTaskId(), TaskState.SLEEPING, 5_000);

        // Emit the event to wake up the task
        absurd.emitEvent("door-opened", Map.of("who", "alice"));

        TaskResultSnapshot snap = absurd.awaitTaskResult(result.getTaskId(), 10_000);
        worker.stop();

        assertEquals(TaskState.COMPLETED, snap.getState());
        assertNotNull(received.get(), "Received payload must be non-null");
        assertTrue(received.get().contains("alice"), "Received payload must contain emitted data");
    }

    @Test
    void awaitEventWithTimeoutReturnsNullOnExpiry() throws Exception {
        TestSetup.setFakeNow(java.time.Instant.parse("2025-06-01T00:00:00Z"));

        Absurd absurd = TestSetup.createAbsurd();
        AtomicReference<String> received = new AtomicReference<>("sentinel");

        absurd.register(simpleDef("timeout-waiter", (input, ctx) -> {
            // 1-second timeout; event will never arrive
            String payload = ((TaskContextImpl) ctx).awaitEvent("never-happens", Duration.ofSeconds(1));
            received.set(payload);
            return Map.of("timedOut", payload == null);
        }));

        SpawnResult result = absurd.spawn("timeout-waiter", null);
        Worker worker = absurd.startWorker(fastWorker());

        // Let the task suspend waiting for the event
        waitForState(absurd, result.getTaskId(), TaskState.SLEEPING, 5_000);

        // Advance fake time past the 1-second timeout
        TestSetup.setFakeNow(java.time.Instant.parse("2025-06-01T00:01:00Z"));

        TaskResultSnapshot snap = absurd.awaitTaskResult(result.getTaskId(), 10_000);
        worker.stop();

        assertEquals(TaskState.COMPLETED, snap.getState());
        assertNull(received.get(), "Payload must be null on timeout");
    }

    @Test
    void crossTaskEventCoordination() throws Exception {
        Absurd absurd = TestSetup.createAbsurd();
        CountDownLatch waiterStarted = new CountDownLatch(1);
        AtomicReference<String> waiterReceived = new AtomicReference<>();

        // Task 1: waits for an event emitted by task 2
        absurd.register(simpleDef("coordinator", (input, ctx) -> {
            waiterStarted.countDown();
            String payload = ((TaskContextImpl) ctx).awaitEvent("signal", Duration.ofMinutes(1));
            waiterReceived.set(payload);
            return Map.of("received", payload);
        }));

        // Task 2: emits the event
        absurd.register(simpleDef("emitter", (input, ctx) -> {
            ((TaskContextImpl) ctx).emitEvent("signal", "{\"msg\":\"hello\"}");
            return Map.of("emitted", true);
        }));

        SpawnResult waiter  = absurd.spawn("coordinator", null);
        Worker worker = absurd.startWorker(fastWorker());

        assertTrue(waiterStarted.await(5, TimeUnit.SECONDS));
        waitForState(absurd, waiter.getTaskId(), TaskState.SLEEPING, 5_000);

        // Spawn the emitter after the waiter is already sleeping
        absurd.spawn("emitter", null);

        TaskResultSnapshot snap = absurd.awaitTaskResult(waiter.getTaskId(), 10_000);
        worker.stop();

        assertEquals(TaskState.COMPLETED, snap.getState());
        assertNotNull(waiterReceived.get());
    }

    // ---- helpers ----

    private static WorkerOptions fastWorker() {
        return WorkerOptions.builder()
            .queueName(TestSetup.DEFAULT_QUEUE)
            .pollIntervalMs(100)
            .pollTimeoutMs(5_000)
            .build();
    }

    private static void waitForState(Absurd absurd, String taskId, TaskState target, long timeoutMs)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            TaskResultSnapshot snap = absurd.fetchTaskResult(taskId);
            if (snap != null && snap.getState() == target) return;
            Thread.sleep(100);
        }
        TaskResultSnapshot snap = absurd.fetchTaskResult(taskId);
        String actual = snap != null ? snap.getState().name() : "null";
        fail("Timed out waiting for state " + target + "; last state: " + actual);
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
