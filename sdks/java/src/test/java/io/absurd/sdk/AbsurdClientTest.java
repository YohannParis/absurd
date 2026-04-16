package io.absurd.sdk;

import io.absurd.sdk.internal.DbClient;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class AbsurdClientTest {

    private static DataSource mockDataSource() {
        return new DataSource() {
            @Override public Connection getConnection() { throw new UnsupportedOperationException(); }
            @Override public Connection getConnection(String u, String p) { throw new UnsupportedOperationException(); }
            @Override public <T> T unwrap(Class<T> iface) { throw new UnsupportedOperationException(); }
            @Override public boolean isWrapperFor(Class<?> iface) { return false; }
            @Override public PrintWriter getLogWriter() { return null; }
            @Override public void setLogWriter(PrintWriter pw) {}
            @Override public void setLoginTimeout(int s) {}
            @Override public int getLoginTimeout() { return 0; }
            @Override public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
                throw new SQLFeatureNotSupportedException();
            }
        };
    }

    private static Absurd buildWithStub(StubDbClient stub, TaskDefinition<?, ?> def) {
        Absurd absurd = Absurd.builder()
            .dataSource(mockDataSource())
            .queueName("test-queue")
            .registerTask(def)
            .build();
        absurd.dbClient = stub;
        return absurd;
    }

    // ---- Builder tests ----

    @Test
    void builderRequiresConnectionSource() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            Absurd.builder().build()
        );
        assertTrue(ex.getMessage().contains("Either dataSource or connectionString must be specified"));
    }

    @Test
    void builderAcceptsDataSource() {
        Absurd absurd = Absurd.builder()
            .dataSource(mockDataSource())
            .build();
        assertNotNull(absurd);
    }

    @Test
    void builderAcceptsConnectionString() {
        Absurd absurd = Absurd.builder()
            .connectionString("jdbc:postgresql://localhost:5432/test")
            .build();
        assertNotNull(absurd);
    }

    @Test
    void builderAcceptsUrlAlias() {
        Absurd absurd = Absurd.builder()
            .url("jdbc:postgresql://localhost:5432/test")
            .build();
        assertNotNull(absurd);
    }

    @Test
    void defaultQueueNameIsDefault() {
        Absurd absurd = Absurd.builder().dataSource(mockDataSource()).build();
        assertEquals("default", absurd.getDefaultQueueName());
    }

    @Test
    void builderSetsQueueName() {
        Absurd absurd = Absurd.builder()
            .dataSource(mockDataSource())
            .queueName("my-queue")
            .build();
        assertEquals("my-queue", absurd.getDefaultQueueName());
    }

    @Test
    void registerAddsTaskDefinition() {
        TaskDefinition<Object, Object> def = simpleDef("task-a");
        Absurd absurd = Absurd.builder()
            .dataSource(mockDataSource())
            .registerTask(def)
            .build();
        assertTrue(absurd.getTaskDefinitions().containsKey("task-a"));
    }

    @Test
    void registerMethodOnInstanceAddsDefinition() {
        Absurd absurd = Absurd.builder().dataSource(mockDataSource()).build();
        assertFalse(absurd.getTaskDefinitions().containsKey("task-b"));
        absurd.register(simpleDef("task-b"));
        assertTrue(absurd.getTaskDefinitions().containsKey("task-b"));
    }

    // ---- Queue management tests ----

    @Test
    void createQueueWithNameCallsDbClient() throws AbsurdException {
        AtomicReference<String> created = new AtomicReference<>();
        StubDbClient stub = new StubDbClient() {
            @Override public void createQueue(String q) {
                created.set(q);
            }
        };

        Absurd absurd = buildWithStub(stub, simpleDef("t"));
        absurd.createQueue("my-queue");
        assertEquals("my-queue", created.get());
    }

    @Test
    void dropQueueCallsDbClient() throws AbsurdException {
        AtomicReference<String> dropped = new AtomicReference<>();
        StubDbClient stub = new StubDbClient() {
            @Override public void dropQueue(String q) { dropped.set(q); }
        };

        Absurd absurd = buildWithStub(stub, simpleDef("t"));
        absurd.dropQueue("gone-queue");
        assertEquals("gone-queue", dropped.get());
    }

    @Test
    void dropQueueWithNoArgUsesDefaultQueue() throws AbsurdException {
        AtomicReference<String> dropped = new AtomicReference<>();
        StubDbClient stub = new StubDbClient() {
            @Override public void dropQueue(String q) { dropped.set(q); }
        };

        Absurd absurd = buildWithStub(stub, simpleDef("t"));
        absurd.dropQueue();
        assertEquals("test-queue", dropped.get());
    }

    @Test
    void listQueuesReturnsDbClientResult() throws AbsurdException {
        StubDbClient stub = new StubDbClient() {
            @Override public List<String> listQueues() {
                return List.of("alpha", "beta");
            }
        };

        Absurd absurd = buildWithStub(stub, simpleDef("t"));
        List<String> queues = absurd.listQueues();
        assertEquals(List.of("alpha", "beta"), queues);
    }

    // ---- Spawn tests ----

    @Test
    void spawnWithNameAndParamsUsesDefaultQueue() throws AbsurdException {
        AtomicReference<String> capturedQueue = new AtomicReference<>();
        AtomicReference<String> capturedTask = new AtomicReference<>();
        AtomicReference<String> capturedInput = new AtomicReference<>();
        StubDbClient stub = new StubDbClient() {
            @Override public DbClient.SpawnRecord spawnTask(String q, String name, String paramsJson, String optsJson) {
                capturedQueue.set(q);
                capturedTask.set(name);
                capturedInput.set(paramsJson);
                return new DbClient.SpawnRecord("task-999", "run-999");
            }
        };

        Absurd absurd = buildWithStub(stub, simpleDef("t"));
        SpawnResult result = absurd.spawn("my-task", Map.of("key", "val"));

        assertEquals("test-queue", capturedQueue.get());
        assertEquals("my-task", capturedTask.get());
        assertTrue(capturedInput.get().contains("key"));
        assertEquals("task-999", result.getTaskId());
        assertEquals("run-999", result.getRunId());
        assertEquals("test-queue", result.getQueueName());
    }

    @Test
    void spawnWithOptsOverridesQueue() throws AbsurdException {
        AtomicReference<String> capturedQueue = new AtomicReference<>();
        StubDbClient stub = new StubDbClient() {
            @Override public DbClient.SpawnRecord spawnTask(String q, String name, String paramsJson, String optsJson) {
                capturedQueue.set(q);
                return new DbClient.SpawnRecord("t-1", "run-1");
            }
        };

        Absurd absurd = buildWithStub(stub, simpleDef("t"));
        SpawnOptions opts = SpawnOptions.builder()
            .queueName("other-queue")
            .taskName("x")
            .build();
        absurd.spawn("my-task", "payload", opts);
        assertEquals("other-queue", capturedQueue.get());
    }

    @Test
    void spawnViaSpawnOptionsUsesExplicitQueue() throws AbsurdException {
        AtomicReference<String> capturedQueue = new AtomicReference<>();
        StubDbClient stub = new StubDbClient() {
            @Override public DbClient.SpawnRecord spawnTask(String q, String name, String paramsJson, String optsJson) {
                capturedQueue.set(q);
                return new DbClient.SpawnRecord("t-2", "run-2");
            }
        };

        Absurd absurd = buildWithStub(stub, simpleDef("t"));
        SpawnOptions opts = SpawnOptions.builder()
            .queueName("explicit-queue")
            .taskName("task-x")
            .input("{}")
            .build();
        absurd.spawn(opts);
        assertEquals("explicit-queue", capturedQueue.get());
    }

    // ---- Event tests ----

    @Test
    void emitEventUsesDefaultQueue() throws AbsurdException {
        AtomicReference<String> capturedQueue = new AtomicReference<>();
        AtomicReference<String> capturedEvent = new AtomicReference<>();
        StubDbClient stub = new StubDbClient() {
            @Override public void emitEvent(String q, String event, String payload) {
                capturedQueue.set(q);
                capturedEvent.set(event);
            }
        };

        Absurd absurd = buildWithStub(stub, simpleDef("t"));
        absurd.emitEvent("order-placed", Map.of("id", 42));
        assertEquals("test-queue", capturedQueue.get());
        assertEquals("order-placed", capturedEvent.get());
    }

    @Test
    void emitEventWithExplicitQueue() throws AbsurdException {
        AtomicReference<String> capturedQueue = new AtomicReference<>();
        StubDbClient stub = new StubDbClient() {
            @Override public void emitEvent(String q, String event, String payload) {
                capturedQueue.set(q);
            }
        };

        Absurd absurd = buildWithStub(stub, simpleDef("t"));
        absurd.emitEvent("orders", "order-placed", null);
        assertEquals("orders", capturedQueue.get());
    }

    // ---- Result tests ----

    @Test
    void fetchTaskResultReturnsNullWhenNotFound() throws AbsurdException {
        StubDbClient stub = new StubDbClient() {
            @Override public DbClient.TaskResultRecord getTaskResultRecord(String q, String taskId) {
                return null;
            }
        };

        Absurd absurd = buildWithStub(stub, simpleDef("t"));
        assertNull(absurd.fetchTaskResult("task-id-1"));
    }

    @Test
    void fetchTaskResultMapsRecord() throws AbsurdException {
        StubDbClient stub = new StubDbClient() {
            @Override public DbClient.TaskResultRecord getTaskResultRecord(String q, String taskId) {
                return new DbClient.TaskResultRecord("task-id-1", TaskState.COMPLETED, "\"done\"", null);
            }
        };

        Absurd absurd = buildWithStub(stub, simpleDef("t"));
        TaskResultSnapshot snap = absurd.fetchTaskResult("task-id-1");

        assertNotNull(snap);
        assertEquals(TaskState.COMPLETED, snap.getState());
        assertEquals("\"done\"", snap.getResult());
        assertNull(snap.getError());
    }

    // ---- Admin tests ----

    @Test
    void cancelTaskCallsDbClientWithDefaultQueue() throws AbsurdException {
        AtomicReference<String> capturedQueue = new AtomicReference<>();
        AtomicReference<String> capturedId = new AtomicReference<>();
        StubDbClient stub = new StubDbClient() {
            @Override public void cancelTask(String q, String taskId) {
                capturedQueue.set(q);
                capturedId.set(taskId);
            }
        };

        Absurd absurd = buildWithStub(stub, simpleDef("t"));
        absurd.cancelTask("task-abc");
        assertEquals("test-queue", capturedQueue.get());
        assertEquals("task-abc", capturedId.get());
    }

    @Test
    void retryTaskCallsDbClient() throws AbsurdException {
        AtomicReference<String> capturedId = new AtomicReference<>();
        AtomicReference<Boolean> capturedSpawnNew = new AtomicReference<>();
        StubDbClient stub = new StubDbClient() {
            @Override public void retryTask(String q, String taskId, boolean spawnNew) {
                capturedId.set(taskId);
                capturedSpawnNew.set(spawnNew);
            }
        };

        Absurd absurd = buildWithStub(stub, simpleDef("t"));
        absurd.retryTask("task-xyz", true);
        assertEquals("task-xyz", capturedId.get());
        assertTrue(capturedSpawnNew.get());
    }

    // ---- Worker tests ----

    @Test
    void startWorkerStartsAndReturnsWorker() throws InterruptedException {
        StubDbClient stub = new StubDbClient() {
            @Override public List<DbClient.ClaimedTask> claimTasks(String q, String wid, int t, int b) {
                return List.of();
            }
        };

        Absurd absurd = buildWithStub(stub, simpleDef("t"));
        Worker w = absurd.startWorker();
        assertTrue(w.isRunning());
        w.stop();
        assertFalse(w.isRunning());
    }

    @Test
    void closeStopsActiveWorker() throws InterruptedException {
        StubDbClient stub = new StubDbClient() {
            @Override public List<DbClient.ClaimedTask> claimTasks(String q, String wid, int t, int b) {
                return List.of();
            }
        };

        Absurd absurd = buildWithStub(stub, simpleDef("t"));
        Worker w = absurd.startWorker();
        assertTrue(w.isRunning());
        absurd.close();
        assertFalse(w.isRunning());
    }

    // ---- Helpers ----

    private static TaskDefinition<Object, Object> simpleDef(String name) {
        return new TaskDefinition<>() {
            @Override public String getName() { return name; }
            @Override public TaskHandler<Object, Object> getHandler() { return (i, c) -> null; }
            @Override public RetryStrategy getRetryStrategy() { return new RetryStrategy(3, 1000, 2.0); }
            @Override public CancellationPolicy getCancellationPolicy() { return new CancellationPolicy(30000, true); }
        };
    }

    private static class Map {
        static java.util.Map<String, Object> of(String k, Object v) {
            return java.util.Map.of(k, v);
        }
    }

    /** Stub DbClient that throws UnsupportedOperationException by default. */
    static class StubDbClient extends DbClient {
        StubDbClient() {
            super(mockDataSource());
        }

        @Override public List<DbClient.ClaimedTask> claimTasks(String q, String wid, int t, int b) {
            throw new UnsupportedOperationException("claimTasks not stubbed");
        }
        @Override public DbClient.SpawnRecord spawnTask(String q, String name, String paramsJson, String optsJson) {
            throw new UnsupportedOperationException("spawnTask not stubbed");
        }
        @Override public void completeTask(String q, String runId, String output) {
            throw new UnsupportedOperationException("completeTask not stubbed");
        }
        @Override public void failTask(String q, String runId, String error) {
            throw new UnsupportedOperationException("failTask not stubbed");
        }
        @Override public void cancelTask(String q, String taskId) {
            throw new UnsupportedOperationException("cancelTask not stubbed");
        }
        @Override public TaskState getTaskState(String q, String runId) {
            throw new UnsupportedOperationException("getTaskState not stubbed");
        }
        @Override public String getTaskResult(String q, String runId) {
            throw new UnsupportedOperationException("getTaskResult not stubbed");
        }
        @Override public void createQueue(String q) {
            throw new UnsupportedOperationException("createQueue not stubbed");
        }
        @Override public void heartbeatTask(String q, String runId, int extensionSecs) {
            throw new UnsupportedOperationException("heartbeatTask not stubbed");
        }
        @Override public void dropQueue(String q) {
            throw new UnsupportedOperationException("dropQueue not stubbed");
        }
        @Override public List<String> listQueues() {
            throw new UnsupportedOperationException("listQueues not stubbed");
        }
        @Override public void emitEvent(String q, String event, String payload) {
            throw new UnsupportedOperationException("emitEvent not stubbed");
        }
        @Override public DbClient.TaskResultRecord getTaskResultRecord(String q, String taskId) {
            throw new UnsupportedOperationException("getTaskResultRecord not stubbed");
        }
        @Override public void retryTask(String q, String taskId, boolean spawnNew) {
            throw new UnsupportedOperationException("retryTask not stubbed");
        }
    }
}
