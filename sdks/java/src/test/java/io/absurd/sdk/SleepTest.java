package io.absurd.sdk;

import io.absurd.sdk.internal.TaskContextImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests covering sleepFor, sleepUntil, and fake_now time control.
 */
class SleepTest {

    private static final Instant T0      = Instant.parse("2025-06-01T00:00:00Z");
    private static final Instant T0_PLUS_2H = T0.plus(Duration.ofHours(2));

    @AfterEach
    void cleanup() throws Exception {
        TestSetup.clearFakeNow();
        TestSetup.cleanupQueue();
    }

    @Test
    void sleepUntilSuspendsAndResumesAfterFakeTimeAdvances() throws Exception {
        TestSetup.setFakeNow(T0);

        Instant wakeAt = T0.plus(Duration.ofHours(1));
        CountDownLatch firstRun  = new CountDownLatch(1);
        CountDownLatch secondRun = new CountDownLatch(1);
        AtomicInteger runCount   = new AtomicInteger(0);

        Absurd absurd = TestSetup.createAbsurd();
        absurd.register(simpleDef("sleeper", (input, ctx) -> {
            int n = runCount.incrementAndGet();
            if (n == 1) firstRun.countDown();
            else        secondRun.countDown();

            ((TaskContextImpl) ctx).sleepUntil(wakeAt);
            // Reached only after the sleep checkpoint is found (post-resume).
            return "awoke";
        }));

        SpawnResult result = absurd.spawn("sleeper", null);
        Worker worker = absurd.startWorker(fastWorker());

        // Wait for the first run to start and reach sleepUntil
        assertTrue(firstRun.await(5, TimeUnit.SECONDS), "First run did not start");

        // Wait until the task transitions to sleeping state in the DB
        waitForState(absurd, result.getTaskId(), TaskState.SLEEPING, 5_000);

        // Advance fake time past the wake point
        TestSetup.setFakeNow(T0_PLUS_2H);

        // Wait for the second run (post-resume)
        assertTrue(secondRun.await(10, TimeUnit.SECONDS), "Second run did not start after time advance");

        TaskResultSnapshot snap = absurd.awaitTaskResult(result.getTaskId(), 10_000);
        worker.stop();

        assertEquals(TaskState.COMPLETED, snap.getState());
        assertEquals(2, runCount.get(), "Handler must run exactly twice");
    }

    @Test
    void sleepForSuspendsAndResumesAfterFakeTimeAdvances() throws Exception {
        TestSetup.setFakeNow(T0);

        CountDownLatch firstRun  = new CountDownLatch(1);
        CountDownLatch secondRun = new CountDownLatch(1);
        AtomicInteger runCount   = new AtomicInteger(0);

        Absurd absurd = TestSetup.createAbsurd();
        absurd.register(simpleDef("sleeper", (input, ctx) -> {
            int n = runCount.incrementAndGet();
            if (n == 1) firstRun.countDown();
            else        secondRun.countDown();

            ((TaskContextImpl) ctx).sleepFor(Duration.ofHours(1));
            return "done";
        }));

        SpawnResult result = absurd.spawn("sleeper", null);
        Worker worker = absurd.startWorker(fastWorker());

        assertTrue(firstRun.await(5, TimeUnit.SECONDS), "First run did not start");
        waitForState(absurd, result.getTaskId(), TaskState.SLEEPING, 5_000);

        TestSetup.setFakeNow(T0_PLUS_2H);

        assertTrue(secondRun.await(10, TimeUnit.SECONDS), "Second run did not start");

        TaskResultSnapshot snap = absurd.awaitTaskResult(result.getTaskId(), 10_000);
        worker.stop();

        assertEquals(TaskState.COMPLETED, snap.getState());
    }

    @Test
    void multipleSleepsExecuteSequentially() throws Exception {
        TestSetup.setFakeNow(T0);

        AtomicInteger step = new AtomicInteger(0);

        Absurd absurd = TestSetup.createAbsurd();
        absurd.register(simpleDef("multi-sleep", (input, ctx) -> {
            TaskContextImpl c = (TaskContextImpl) ctx;
            c.sleepUntil(T0.plus(Duration.ofMinutes(30)));
            step.set(1);
            c.sleepUntil(T0.plus(Duration.ofHours(2)));
            step.set(2);
            return "done";
        }));

        SpawnResult result = absurd.spawn("multi-sleep", null);
        Worker worker = absurd.startWorker(fastWorker());

        // After first sleep
        waitForState(absurd, result.getTaskId(), TaskState.SLEEPING, 5_000);
        TestSetup.setFakeNow(T0.plus(Duration.ofHours(1)));

        // Task resumes, reaches second sleep
        waitForState(absurd, result.getTaskId(), TaskState.SLEEPING, 10_000);
        TestSetup.setFakeNow(T0.plus(Duration.ofHours(3)));

        TaskResultSnapshot snap = absurd.awaitTaskResult(result.getTaskId(), 10_000);
        worker.stop();

        assertEquals(TaskState.COMPLETED, snap.getState());
        assertEquals(2, step.get(), "Both sleep barriers must be crossed");
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
        // Final check — will surface the actual state in the assertion message
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
