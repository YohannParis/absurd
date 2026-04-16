package io.absurd.sdk;

/**
 * Result of spawning a new task.
 *
 * <p>{@link #getTaskId()} is the stable identifier used to track the task across retries
 * and to query its result via {@link Absurd#fetchTaskResult} / {@link Absurd#awaitTaskResult}.
 * {@link #getRunId()} identifies the specific first run attempt.
 */
public class SpawnResult {
    private final String taskId;
    private final String runId;
    private final String queueName;
    private final String taskName;

    public SpawnResult(String taskId, String runId, String queueName, String taskName) {
        this.taskId = taskId;
        this.runId = runId;
        this.queueName = queueName;
        this.taskName = taskName;
    }

    /** The stable task identifier — use this to query or cancel the task. */
    public String getTaskId() {
        return taskId;
    }

    /** The run ID of the first attempt. */
    public String getRunId() {
        return runId;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getTaskName() {
        return taskName;
    }
}
