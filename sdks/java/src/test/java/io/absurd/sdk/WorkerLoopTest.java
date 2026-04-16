package io.absurd.sdk;

import io.absurd.sdk.internal.DbClient;
import io.absurd.sdk.internal.SuspendTask;
import io.absurd.sdk.internal.WorkerLoop;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class WorkerLoopTest {

    private static Absurd buildAbsurd(TaskDefinition<?, ?> def) {
        return Absurd.builder()
            .connectionString("jdbc:postgresql://localhost:5432/test")
            .registerTask(def)
            .build();
    }

    private static DbClient stubDbClient(List<DbClient.ClaimedTask> initialTasks,
                                         AtomicReference<String> completedRunId,
                                         AtomicReference<String> failedRunId) {
        DataSource ds = new DataSource() {
            @Override public Connection getConnection() { throw new UnsupportedOperationException(); }
            @Override public Connection getConnection(String u, String p) { throw new UnsupportedOperationException(); }
            @Override public <T> T unwrap(Class<T> iface) { throw new UnsupportedOperationException(); }
            @Override public boolean isWrapperFor(Class<?> iface) { return false; }
            @Override public java.io.PrintWriter getLogWriter() { return null; }
            @Override public void setLogWriter(java.io.PrintWriter pw) {}
            @Override public void setLoginTimeout(int s) {}
            @Override public int getLoginTimeout() { return 0; }
            @Override public java.util.logging.Logger getParentLogger() { return null; }
        };

        return new DbClient(ds) {
            private boolean claimed = false;

            @Override
            public List<ClaimedTask> claimTasks(String queue, String workerId, int claimTimeout, int batchSize) {
                if (!claimed) {
                    claimed = true;
                    return new ArrayList<>(initialTasks);
                }
                return List.of();
            }

            @Override
            public void completeTask(String queue, String runId, String output) {
                completedRunId.set(runId);
            }

            @Override
            public void failTask(String queue, String runId, String error) {
                failedRunId.set(runId);
            }
        };
    }

    @Test
    void workerStartsAndStops() throws InterruptedException {
        TaskDefinition<Object, Object> def = new SimpleTaskDef("noop", (input, ctx) -> "{}");
        Absurd absurd = buildAbsurd(def);

        AtomicReference<String> completed = new AtomicReference<>();
        AtomicReference<String> failed = new AtomicReference<>();
        DbClient client = stubDbClient(List.of(), completed, failed);

        WorkerOptions opts = WorkerOptions.builder().queueName("q").build();
        WorkerLoop worker = new WorkerLoop(absurd, opts, client);

        worker.start();
        assertTrue(worker.isRunning());

        worker.stop();
        assertFalse(worker.isRunning());
    }

    @Test
    void executesTaskAndCompletes() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        TaskDefinition<Object, Object> def = new SimpleTaskDef("my-task", (input, ctx) -> {
            latch.countDown();
            return "{}";
        });
        Absurd absurd = buildAbsurd(def);

        AtomicReference<String> completed = new AtomicReference<>();
        AtomicReference<String> failed = new AtomicReference<>();
        List<DbClient.ClaimedTask> tasks = List.of(
            new DbClient.ClaimedTask("run-1", "my-task", "\"hello\"", 1)
        );
        DbClient client = stubDbClient(tasks, completed, failed);

        WorkerOptions opts = WorkerOptions.builder().queueName("q").pollIntervalMs(50).build();
        WorkerLoop worker = new WorkerLoop(absurd, opts, client);

        worker.start();
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Handler was not called");
        Thread.sleep(200); // let completeTask be called
        worker.stop();

        assertEquals("run-1", completed.get());
        assertNull(failed.get());
    }

    @Test
    void failsTaskOnUnknownTaskName() throws InterruptedException {
        TaskDefinition<Object, Object> def = new SimpleTaskDef("known", (input, ctx) -> "{}");
        Absurd absurd = buildAbsurd(def);

        AtomicReference<String> completed = new AtomicReference<>();
        AtomicReference<String> failed = new AtomicReference<>();
        List<DbClient.ClaimedTask> tasks = List.of(
            new DbClient.ClaimedTask("run-2", "unknown-task", "{}", 1)
        );
        DbClient client = stubDbClient(tasks, completed, failed);

        WorkerOptions opts = WorkerOptions.builder().queueName("q").pollIntervalMs(50).build();
        WorkerLoop worker = new WorkerLoop(absurd, opts, client);

        worker.start();
        Thread.sleep(500);
        worker.stop();

        assertEquals("run-2", failed.get());
        assertNull(completed.get());
    }

    @Test
    void returnsOnSuspendTask() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        TaskDefinition<Object, Object> def = new SimpleTaskDef("suspend-task", (input, ctx) -> {
            latch.countDown();
            throw new SuspendTask("sleeping");
        });
        Absurd absurd = buildAbsurd(def);

        AtomicReference<String> completed = new AtomicReference<>();
        AtomicReference<String> failed = new AtomicReference<>();
        List<DbClient.ClaimedTask> tasks = List.of(
            new DbClient.ClaimedTask("run-3", "suspend-task", "{}", 1)
        );
        DbClient client = stubDbClient(tasks, completed, failed);

        WorkerOptions opts = WorkerOptions.builder().queueName("q").pollIntervalMs(50).build();
        WorkerLoop worker = new WorkerLoop(absurd, opts, client);

        worker.start();
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Handler was not called");
        Thread.sleep(200);
        worker.stop();

        assertNull(completed.get());
        assertNull(failed.get());
    }

    @Test
    void failsTaskOnHandlerException() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        TaskDefinition<Object, Object> def = new SimpleTaskDef("boom", (input, ctx) -> {
            latch.countDown();
            throw new RuntimeException("handler blew up");
        });
        Absurd absurd = buildAbsurd(def);

        AtomicReference<String> completed = new AtomicReference<>();
        AtomicReference<String> failed = new AtomicReference<>();
        List<DbClient.ClaimedTask> tasks = List.of(
            new DbClient.ClaimedTask("run-4", "boom", "{}", 1)
        );
        DbClient client = stubDbClient(tasks, completed, failed);

        WorkerOptions opts = WorkerOptions.builder().queueName("q").pollIntervalMs(50).build();
        WorkerLoop worker = new WorkerLoop(absurd, opts, client);

        worker.start();
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Handler was not called");
        Thread.sleep(200);
        worker.stop();

        assertEquals("run-4", failed.get());
        assertNull(completed.get());
    }

    private static class SimpleTaskDef implements TaskDefinition<Object, Object> {
        private final String name;
        private final TaskHandler<Object, Object> handler;

        SimpleTaskDef(String name, TaskHandler<Object, Object> handler) {
            this.name = name;
            this.handler = handler;
        }

        @Override public String getName() { return name; }
        @Override public TaskHandler<Object, Object> getHandler() { return handler; }
        @Override public RetryStrategy getRetryStrategy() { return new RetryStrategy(3, 1000, 2.0); }
        @Override public CancellationPolicy getCancellationPolicy() { return new CancellationPolicy(30000, true); }
    }
}
