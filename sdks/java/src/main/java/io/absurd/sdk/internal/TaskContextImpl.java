package io.absurd.sdk.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.absurd.sdk.*;
import io.absurd.sdk.internal.DbClient.ClaimedTask;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of TaskContext that provides durable step/sleep/event functionality.
 */
public class TaskContextImpl implements TaskContext {
    private final DbClient dbClient;
    private final ClaimedTask claimedTask;
    private final String queueName;
    private final Map<String, String> metadata;
    private final Map<String, Object> checkpointCache = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final int claimTimeoutSecs;
    private int stepCounter = 0;

    public TaskContextImpl(DbClient dbClient, ClaimedTask claimedTask, String queueName,
                           Map<String, String> metadata) {
        this(dbClient, claimedTask, queueName, metadata, 30);
    }

    public TaskContextImpl(DbClient dbClient, ClaimedTask claimedTask, String queueName,
                           Map<String, String> metadata, int claimTimeoutSecs) {
        this.dbClient = dbClient;
        this.claimedTask = claimedTask;
        this.queueName = queueName;
        this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
        this.claimTimeoutSecs = claimTimeoutSecs;
    }

    @Override public String getRunId()    { return claimedTask.runId(); }
    @Override public String getQueueName() { return queueName; }
    @Override public String getTaskName() { return claimedTask.taskName(); }
    @Override public int getAttempt()     { return claimedTask.attempt(); }
    @Override public int getMaxAttempts() {
        return claimedTask.maxAttempts() != null ? claimedTask.maxAttempts() : 3;
    }

    @Override
    public Map<String, String> getMetadata() {
        return new HashMap<>(metadata);
    }

    @Override
    public SpawnResult spawn(SpawnOptions options) {
        try {
            String paramsJson = options.getInput() != null ? options.getInput() : "null";
            DbClient.SpawnRecord rec = dbClient.spawnTask(
                options.getQueueName(), options.getTaskName(), paramsJson, null);
            return new SpawnResult(rec.taskId(), rec.runId(), options.getQueueName(), options.getTaskName());
        } catch (AbsurdException e) {
            throw new RuntimeException("Failed to spawn task: " + e.getMessage(), e);
        }
    }

    @Override
    public SpawnResult spawn(String taskName, String input) {
        SpawnOptions options = SpawnOptions.builder()
                .queueName(queueName)
                .taskName(taskName)
                .input(input)
                .build();
        return spawn(options);
    }

    @Override
    public boolean isCancelled() {
        try {
            DbClient.TaskResultRecord record = dbClient.getTaskResultRecord(queueName, claimedTask.taskId());
            return record != null && record.state() == TaskState.CANCELLED;
        } catch (AbsurdException e) {
            return false;
        }
    }

    @Override public void logInfo(String message)  { System.out.println("[INFO] " + message); }
    @Override public void logError(String message) { System.err.println("[ERROR] " + message); }
    @Override public void logError(String message, Throwable t) {
        System.err.println("[ERROR] " + message);
        t.printStackTrace();
    }

    // ---- Step checkpoint ----

    /**
     * Executes a named step with DB-backed idempotency.
     *
     * <p>On first execution the step body runs and the result is persisted as a checkpoint.
     * On subsequent executions (e.g., after a retry) the persisted result is returned
     * directly without re-running the step body.
     *
     * @param name the step name — must be stable across retries
     * @param type an arbitrary type tag (informational)
     * @param fn   the function to execute
     */
    public <T> T step(String name, String type, CheckpointFunction<T> fn) throws SuspendTask {
        String checkpointKey = "step#" + (++stepCounter) + "-" + name;

        // Fast path: in-memory cache hit (same run, already executed)
        if (checkpointCache.containsKey(checkpointKey)) {
            @SuppressWarnings("unchecked")
            T cached = (T) checkpointCache.get(checkpointKey);
            return cached;
        }

        // Slow path: check committed DB checkpoint (survives across retries)
        try {
            String cachedJson = dbClient.getCheckpointState(queueName, claimedTask.taskId(), checkpointKey);
            if (cachedJson != null) {
                @SuppressWarnings("unchecked")
                T cached = (T) objectMapper.readValue(cachedJson, Object.class);
                checkpointCache.put(checkpointKey, cached);
                return cached;
            }
        } catch (AbsurdException | JsonProcessingException e) {
            // If the DB check fails, fall through and re-execute the step.
        }

        // Execute the step body
        try {
            T result = fn.execute();
            String resultJson = objectMapper.writeValueAsString(result);
            dbClient.setCheckpointState(queueName, claimedTask.taskId(), checkpointKey, resultJson,
                                        claimedTask.runId());
            checkpointCache.put(checkpointKey, result);
            return result;
        } catch (SuspendTask e) {
            throw e;
        } catch (AbsurdException e) {
            throw e;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize step result: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new RuntimeException("Step execution failed: " + e.getMessage(), e);
        }
    }

    // ---- Sleep ----

    /**
     * Suspends the task for a specified duration.
     *
     * <p>A checkpoint is saved before scheduling so that on re-execution (post-wake)
     * the sleep is skipped and execution continues past this call.
     */
    public void sleepFor(Duration duration) throws SuspendTask {
        try {
            Instant dbNow = dbClient.getCurrentTime();
            sleepUntil(dbNow.plus(duration));
        } catch (SuspendTask e) {
            throw e;
        } catch (AbsurdException e) {
            // getCurrentTime failed — fall back to wall clock
            sleepUntil(Instant.now().plus(duration));
        }
    }

    /**
     * Suspends the task until a specific instant.
     *
     * <p>A "slept" checkpoint is persisted before transitioning the run to sleeping state,
     * so on resume the call returns immediately without re-sleeping.
     */
    public void sleepUntil(Instant wakeTime) throws SuspendTask {
        String checkpointKey = "sleep#" + (++stepCounter);

        try {
            // On resume, checkpoint exists — skip the sleep.
            String cached = dbClient.getCheckpointState(queueName, claimedTask.taskId(), checkpointKey);
            if (cached != null) {
                return;
            }

            // Persist checkpoint BEFORE scheduling so it's visible if the JVM crashes.
            dbClient.setCheckpointState(queueName, claimedTask.taskId(), checkpointKey,
                                        "true", claimedTask.runId());

            // Transition run to sleeping state.
            dbClient.scheduleRun(queueName, claimedTask.runId(), wakeTime);
        } catch (AbsurdException e) {
            throw new RuntimeException("Failed to schedule sleep: " + e.getMessage(), e);
        }

        throw new SuspendTask("Task sleeping until " + wakeTime);
    }

    // ---- Events ----

    /**
     * Suspends the task until the named event is emitted, or until {@code timeout} elapses.
     *
     * <p>If the event has already been emitted the payload is returned immediately.
     * If the timeout expires the method returns {@code null}.
     *
     * @param eventName the event to wait for
     * @param timeout   the maximum wait time; {@code null} means wait indefinitely
     * @return the event payload JSON string, or {@code null} on timeout
     * @throws SuspendTask if the event has not yet arrived and the task is now sleeping
     */
    public String awaitEvent(String eventName, Duration timeout) throws SuspendTask {
        String checkpointKey = "await#" + (++stepCounter) + "-" + eventName;
        Integer timeoutSeconds = timeout != null ? (int) timeout.toSeconds() : null;

        try {
            DbClient.AwaitEventResult result = dbClient.awaitEvent(
                queueName, claimedTask.taskId(), claimedTask.runId(),
                checkpointKey, eventName, timeoutSeconds);

            if (result.shouldSuspend()) {
                // Run has been transitioned to sleeping; propagate suspension.
                throw new SuspendTask("Awaiting event: " + eventName);
            }
            // Event resolved (or timed out with null payload).
            return result.payload();
        } catch (SuspendTask e) {
            throw e;
        } catch (AbsurdException e) {
            throw new RuntimeException("Failed to await event: " + e.getMessage(), e);
        }
    }

    /**
     * Emits an event visible to all tasks in the same queue.
     */
    public void emitEvent(String eventName, String eventData) {
        try {
            dbClient.emitEvent(queueName, eventName, eventData);
        } catch (AbsurdException e) {
            throw new RuntimeException("Failed to emit event: " + e.getMessage(), e);
        }
    }

    /**
     * Extends the worker's claim on this task run by the configured claim timeout.
     */
    public void heartbeat() {
        try {
            dbClient.heartbeatTask(queueName, claimedTask.runId(), claimTimeoutSecs);
        } catch (AbsurdException e) {
            throw new RuntimeException("Failed to heartbeat task: " + e.getMessage(), e);
        }
    }

    /**
     * Not yet implemented.
     *
     * @throws UnsupportedOperationException always
     */
    public String awaitTaskResult(String runId, Duration timeout) throws SuspendTask {
        throw new UnsupportedOperationException("awaitTaskResult is not yet implemented");
    }

    // ---- Nested interface ----

    @FunctionalInterface
    public interface CheckpointFunction<T> {
        T execute() throws Exception;
    }
}
