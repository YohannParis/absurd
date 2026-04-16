package io.absurd.sdk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.absurd.sdk.internal.DbClient;
import io.absurd.sdk.internal.WorkerLoop;

import javax.sql.DataSource;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main client for interacting with the Absurd workflow system.
 *
 * <p>Supports two connection modes:
 * <ul>
 *   <li>{@code .url("jdbc:postgresql://...")} — Absurd creates and owns the connection pool.</li>
 *   <li>{@code .dataSource(ds)} — caller provides and owns the DataSource.</li>
 * </ul>
 *
 * <p>The connection pool (and therefore any DB connections) is created lazily on first use.
 */
public class Absurd implements Closeable {

    private static final long AWAIT_POLL_INITIAL_MS = 100;
    private static final long AWAIT_POLL_MAX_MS = 5000;

    private final String connectionString;
    private final DataSource externalDataSource;
    private final String defaultQueueName;
    private final Map<String, TaskDefinition<?, ?>> taskDefinitions;
    private final AbsurdHooks hooks;
    private final boolean ownedDataSource;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private volatile DataSource dataSource;
    volatile DbClient dbClient;
    private volatile Worker activeWorker;

    private Absurd(Builder builder) {
        this.connectionString = builder.connectionString;
        this.externalDataSource = builder.dataSource;
        this.defaultQueueName = builder.defaultQueueName;
        this.taskDefinitions = new ConcurrentHashMap<>(builder.taskDefinitions);
        this.hooks = builder.hooks != null ? builder.hooks : new DefaultAbsurdHooks();
        this.ownedDataSource = builder.dataSource == null;
    }

    public static Builder builder() {
        return new Builder();
    }

    // ---- Getters ----

    public String getDefaultQueueName() {
        return defaultQueueName;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public Map<String, TaskDefinition<?, ?>> getTaskDefinitions() {
        return taskDefinitions;
    }

    public AbsurdHooks getHooks() {
        return hooks;
    }

    public RetryStrategy getDefaultRetryStrategy() {
        return new RetryStrategy(3, 1000, 2.0);
    }

    public CancellationPolicy getDefaultCancellationPolicy() {
        return new CancellationPolicy(30000, true);
    }

    // ---- Registration ----

    public void register(TaskDefinition<?, ?> definition) {
        taskDefinitions.put(definition.getName(), definition);
    }

    public void registerTask(TaskDefinition<?, ?> definition) {
        register(definition);
    }

    // ---- Queue management ----

    public void createQueue(CreateQueueOptions options) throws AbsurdException {
        getDbClient().createQueue(options.getQueueName());
    }

    public void createQueue(String queueName) throws AbsurdException {
        getDbClient().createQueue(queueName);
    }

    public void dropQueue(String queueName) throws AbsurdException {
        getDbClient().dropQueue(queueName);
    }

    public void dropQueue() throws AbsurdException {
        getDbClient().dropQueue(defaultQueueName);
    }

    public List<String> listQueues() throws AbsurdException {
        return getDbClient().listQueues();
    }

    // ---- Task spawning ----

    public SpawnResult spawn(SpawnOptions options) throws AbsurdException {
        String queue = options.getQueueName() != null ? options.getQueueName() : defaultQueueName;
        String paramsJson = options.getInput() != null ? options.getInput() : "null";
        String optionsJson = buildSpawnOptions(options.getTaskName());
        DbClient.SpawnRecord rec = getDbClient().spawnTask(queue, options.getTaskName(), paramsJson, optionsJson);
        return new SpawnResult(rec.taskId(), rec.runId(), queue, options.getTaskName());
    }

    public SpawnResult spawn(String taskName, Object params) throws AbsurdException {
        String paramsJson = toJson(params);
        String optionsJson = buildSpawnOptions(taskName);
        DbClient.SpawnRecord rec = getDbClient().spawnTask(defaultQueueName, taskName, paramsJson, optionsJson);
        return new SpawnResult(rec.taskId(), rec.runId(), defaultQueueName, taskName);
    }

    public SpawnResult spawn(String taskName, Object params, SpawnOptions opts) throws AbsurdException {
        String queue = opts != null && opts.getQueueName() != null ? opts.getQueueName() : defaultQueueName;
        String paramsJson = toJson(params);
        String optionsJson = buildSpawnOptions(taskName);
        DbClient.SpawnRecord rec = getDbClient().spawnTask(queue, taskName, paramsJson, optionsJson);
        return new SpawnResult(rec.taskId(), rec.runId(), queue, taskName);
    }

    // ---- Events ----

    public void emitEvent(String eventName, Object payload) throws AbsurdException {
        String payloadJson = payload != null ? toJson(payload) : null;
        getDbClient().emitEvent(defaultQueueName, eventName, payloadJson);
    }

    public void emitEvent(String queueName, String eventName, Object payload) throws AbsurdException {
        String payloadJson = payload != null ? toJson(payload) : null;
        getDbClient().emitEvent(queueName, eventName, payloadJson);
    }

    // ---- Results ----

    public TaskResultSnapshot fetchTaskResult(String taskId) throws AbsurdException {
        return fetchTaskResult(defaultQueueName, taskId);
    }

    public TaskResultSnapshot fetchTaskResult(String queueName, String taskId) throws AbsurdException {
        DbClient.TaskResultRecord record = getDbClient().getTaskResultRecord(queueName, taskId);
        if (record == null) return null;
        return new TaskResultSnapshot(
            record.taskId(), queueName, null,
            record.state(), record.result(), record.failureReason(),
            0, null, null, null
        );
    }

    public TaskResultSnapshot awaitTaskResult(String taskId, long timeoutMs)
            throws AbsurdException, InterruptedException {
        return awaitTaskResult(defaultQueueName, taskId, timeoutMs);
    }

    public TaskResultSnapshot awaitTaskResult(String queueName, String taskId, long timeoutMs)
            throws AbsurdException, InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long backoff = AWAIT_POLL_INITIAL_MS;
        while (true) {
            TaskResultSnapshot snapshot = fetchTaskResult(queueName, taskId);
            if (snapshot != null) {
                TaskState state = snapshot.getState();
                if (state == TaskState.COMPLETED || state == TaskState.FAILED || state == TaskState.CANCELLED) {
                    return snapshot;
                }
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) break;
            Thread.sleep(Math.min(backoff, remaining));
            backoff = Math.min(backoff * 2, AWAIT_POLL_MAX_MS);
        }
        throw new AbsurdException("Timed out waiting for task " + taskId);
    }

    // ---- Admin ----

    public void retryTask(String taskId, boolean spawnNew) throws AbsurdException {
        retryTask(defaultQueueName, taskId, spawnNew);
    }

    public void retryTask(String queueName, String taskId, boolean spawnNew) throws AbsurdException {
        getDbClient().retryTask(queueName, taskId, spawnNew);
    }

    public void cancelTask(String taskId) throws AbsurdException {
        cancelTask(defaultQueueName, taskId);
    }

    public void cancelTask(String queueName, String taskId) throws AbsurdException {
        getDbClient().cancelTask(queueName, taskId);
    }

    // ---- Worker ----

    public Worker createWorker(String queueName) {
        return createWorker(WorkerOptions.builder().queueName(queueName).build());
    }

    public Worker createWorker(WorkerOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("options cannot be null");
        }
        return new WorkerLoop(this, options, getDbClient());
    }

    public Worker startWorker() {
        return startWorker(WorkerOptions.builder().queueName(defaultQueueName).build());
    }

    public Worker startWorker(WorkerOptions options) {
        Worker w = createWorker(options);
        w.start();
        activeWorker = w;
        return w;
    }

    // ---- Lifecycle ----

    /**
     * Stops any active worker and closes the connection pool if Absurd created it via .url().
     * When a DataSource is provided via .dataSource(), the caller retains ownership and must
     * close it independently.
     */
    @Override
    public void close() {
        if (activeWorker != null && activeWorker.isRunning()) {
            activeWorker.stop();
        }
        // ownedDataSource is true only when we created the HikariDataSource from a URL.
        // null-safe: dataSource may still be null if no DB call was ever made.
        if (ownedDataSource && dataSource instanceof HikariDataSource hds) {
            hds.close();
        }
    }

    // ---- Internal helpers ----

    private DataSource getDataSource() {
        if (dataSource == null) {
            synchronized (this) {
                if (dataSource == null) {
                    if (externalDataSource != null) {
                        dataSource = externalDataSource;
                    } else {
                        HikariConfig config = new HikariConfig();
                        config.setJdbcUrl(connectionString);
                        dataSource = new HikariDataSource(config);
                    }
                }
            }
        }
        return dataSource;
    }

    DbClient getDbClient() {
        if (dbClient == null) {
            synchronized (this) {
                if (dbClient == null) {
                    dbClient = new DbClient(getDataSource());
                }
            }
        }
        return dbClient;
    }

    private String buildSpawnOptions(String taskName) throws AbsurdException {
        TaskDefinition<?, ?> def = taskDefinitions.get(taskName);
        if (def == null) return null;
        RetryStrategy strategy = def.getRetryStrategy();
        if (strategy == null) return null;

        Map<String, Object> retryStrategy;
        if (strategy.getInitialDelayMs() == 0) {
            retryStrategy = Map.of("kind", "none");
        } else if (strategy.getBackoffFactor() == 1.0) {
            retryStrategy = Map.of("kind", "fixed",
                "base_seconds", strategy.getInitialDelayMs() / 1000.0);
        } else {
            retryStrategy = Map.of("kind", "exponential",
                "base_seconds", strategy.getInitialDelayMs() / 1000.0,
                "factor", strategy.getBackoffFactor());
        }

        return toJson(Map.of("max_attempts", strategy.getMaxAttempts(), "retry_strategy", retryStrategy));
    }

    private String toJson(Object obj) throws AbsurdException {
        if (obj == null) return "null";
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new AbsurdException("Failed to serialize to JSON: " + e.getMessage(), e);
        }
    }

    // ---- Builder ----

    public static class Builder {
        private String connectionString;
        private DataSource dataSource;
        private String defaultQueueName = "default";
        private final Map<String, TaskDefinition<?, ?>> taskDefinitions = new ConcurrentHashMap<>();
        private AbsurdHooks hooks;

        public Builder connectionString(String connectionString) {
            this.connectionString = connectionString;
            return this;
        }

        public Builder url(String url) {
            return connectionString(url);
        }

        public Builder dataSource(DataSource dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        public Builder queueName(String queueName) {
            this.defaultQueueName = queueName;
            return this;
        }

        public Builder registerTask(TaskDefinition<?, ?> definition) {
            this.taskDefinitions.put(definition.getName(), definition);
            return this;
        }

        public Builder register(TaskDefinition<?, ?> definition) {
            return registerTask(definition);
        }

        public Builder hooks(AbsurdHooks hooks) {
            this.hooks = hooks;
            return this;
        }

        public Absurd build() {
            if (dataSource == null && (connectionString == null || connectionString.trim().isEmpty())) {
                throw new IllegalStateException("Either dataSource or connectionString must be specified");
            }
            return new Absurd(this);
        }
    }

    private static class DefaultAbsurdHooks implements AbsurdHooks {
        // All methods are already implemented as defaults in the interface
    }
}
