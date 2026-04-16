package io.absurd.sdk.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.absurd.sdk.AbsurdException;
import io.absurd.sdk.TaskState;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Internal JDBC wrapper for calling Absurd stored procedures.
 */
public class DbClient {
    private final DataSource dataSource;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DbClient(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource cannot be null");
        }
        this.dataSource = dataSource;
    }

    private void validateJson(String json, String parameterName) {
        if (json == null) {
            throw new IllegalArgumentException(parameterName + " cannot be null");
        }
        if (json.trim().isEmpty()) {
            throw new IllegalArgumentException(parameterName + " cannot be empty");
        }
        try {
            objectMapper.readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException(parameterName + " must be valid JSON: " + e.getMessage());
        }
    }

    /** A claimed task returned by {@link #claimTasks}. */
    public record ClaimedTask(String runId, String taskId, String taskName, String input, int attempt, Integer maxAttempts) {}

    /** The task_id and run_id returned by {@link #spawnTask}. */
    public record SpawnRecord(String taskId, String runId) {}

    /** Snapshot of a task result from the database. */
    public record TaskResultRecord(String taskId, TaskState state, String result, String failureReason) {}

    /** Result of calling {@link #awaitEvent}. */
    public record AwaitEventResult(boolean shouldSuspend, String payload) {}

    // ---- Claim ----

    /**
     * Claims tasks from the queue for processing.
     *
     * @param queue          the queue name
     * @param workerId       the worker identifier
     * @param claimTimeoutMs the claim timeout in <em>milliseconds</em> (converted to seconds internally)
     * @param batchSize      maximum number of tasks to claim
     */
    public List<ClaimedTask> claimTasks(String queue, String workerId, int claimTimeoutMs, int batchSize)
            throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (workerId == null || workerId.trim().isEmpty()) {
            throw new IllegalArgumentException("workerId cannot be null or empty");
        }
        if (claimTimeoutMs <= 0) {
            throw new IllegalArgumentException("claimTimeout must be positive");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }

        // SQL function expects seconds; WorkerOptions stores milliseconds.
        int claimTimeoutSecs = Math.max(1, claimTimeoutMs / 1000);

        // claim_task is a set-returning function, not a procedure.
        // Column "params" is aliased to "input" to match ClaimedTask.input().
        String sql = "SELECT run_id::text, task_id::text, attempt, task_name, params::text AS input, max_attempts " +
                     "FROM absurd.claim_task(?, ?, ?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, queue);
            stmt.setString(2, workerId);
            stmt.setInt(3, claimTimeoutSecs);
            stmt.setInt(4, batchSize);

            try (ResultSet rs = stmt.executeQuery()) {
                List<ClaimedTask> tasks = new ArrayList<>();
                while (rs.next()) {
                    int maxAttempts = rs.getInt("max_attempts");
                    tasks.add(new ClaimedTask(
                        rs.getString("run_id"),
                        rs.getString("task_id"),
                        rs.getString("task_name"),
                        rs.getString("input"),
                        rs.getInt("attempt"),
                        rs.wasNull() ? null : maxAttempts
                    ));
                }
                return tasks;
            }
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to claim tasks: " + e.getMessage(), e);
        }
    }

    // ---- Spawn ----

    /**
     * Spawns a new task and returns both its task_id and run_id.
     *
     * <p>The SQL function is {@code absurd.spawn_task(queue, task_name, params jsonb, options jsonb)}.
     */
    public SpawnRecord spawnTask(String queue, String taskName, String paramsJson, String optionsJson)
            throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (taskName == null || taskName.trim().isEmpty()) {
            throw new IllegalArgumentException("taskName cannot be null or empty");
        }
        validateJson(paramsJson, "paramsJson");

        String sql = "SELECT task_id::text, run_id::text " +
                     "FROM absurd.spawn_task(?, ?, CAST(? AS jsonb), CAST(? AS jsonb))";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, queue);
            stmt.setString(2, taskName);
            stmt.setString(3, paramsJson);
            stmt.setString(4, optionsJson != null ? optionsJson : "{}");

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return new SpawnRecord(rs.getString(1), rs.getString(2));
                }
                throw new AbsurdException("No result returned from spawn_task");
            }
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to spawn task: " + e.getMessage(), e);
        }
    }

    // ---- Complete / Fail / Schedule ----

    /**
     * Completes a task run.
     *
     * <p>Calls {@code absurd.complete_run(queue, run_id, state jsonb)}.
     */
    public void completeTask(String queue, String runId, String output) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (runId == null || runId.trim().isEmpty()) {
            throw new IllegalArgumentException("runId cannot be null or empty");
        }
        validateJson(output, "output");

        String sql = "SELECT absurd.complete_run(?, ?::uuid, CAST(? AS jsonb))";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, queue);
            stmt.setString(2, runId);
            stmt.setString(3, output);
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to complete task: " + e.getMessage(), e);
        }
    }

    /**
     * Fails a task run.
     *
     * <p>Calls {@code absurd.fail_run(queue, run_id, reason jsonb)}.
     * The error string is JSON-encoded as a JSON string value.
     */
    public void failTask(String queue, String runId, String error) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (runId == null || runId.trim().isEmpty()) {
            throw new IllegalArgumentException("runId cannot be null or empty");
        }
        if (error == null || error.trim().isEmpty()) {
            throw new IllegalArgumentException("error cannot be null or empty");
        }

        String reasonJson;
        try {
            reasonJson = objectMapper.writeValueAsString(error);
        } catch (JsonProcessingException e) {
            reasonJson = "\"error\"";
        }

        String sql = "SELECT absurd.fail_run(?, ?::uuid, CAST(? AS jsonb))";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, queue);
            stmt.setString(2, runId);
            stmt.setString(3, reasonJson);
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to fail task: " + e.getMessage(), e);
        }
    }

    /**
     * Schedules a sleeping run to wake at the specified time.
     *
     * <p>Calls {@code absurd.schedule_run(queue, run_id, wake_at)}.
     */
    public void scheduleRun(String queue, String runId, Instant wakeAt) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (runId == null || runId.trim().isEmpty()) {
            throw new IllegalArgumentException("runId cannot be null or empty");
        }

        String sql = "SELECT absurd.schedule_run(?, ?::uuid, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, queue);
            stmt.setString(2, runId);
            stmt.setObject(3, java.sql.Timestamp.from(wakeAt));
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to schedule run: " + e.getMessage(), e);
        }
    }

    // ---- Cancel / Retry ----

    /**
     * Cancels a task by task ID.
     *
     * <p>Calls {@code absurd.cancel_task(queue, task_id)}.
     * The {@code ?::uuid} cast is applied server-side to the bound parameter.
     */
    public void cancelTask(String queue, String taskId) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (taskId == null || taskId.trim().isEmpty()) {
            throw new IllegalArgumentException("taskId cannot be null or empty");
        }

        String sql = "SELECT absurd.cancel_task(?, ?::uuid)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, queue);
            stmt.setString(2, taskId);
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to cancel task: " + e.getMessage(), e);
        }
    }

    /**
     * Retries a task, optionally spawning a fresh task instead of incrementing attempt.
     */
    public void retryTask(String queue, String taskId, boolean spawnNew) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (taskId == null || taskId.trim().isEmpty()) {
            throw new IllegalArgumentException("taskId cannot be null or empty");
        }

        String options = spawnNew ? "{\"spawn_new\":true}" : "{}";
        String sql = "SELECT task_id FROM absurd.retry_task(?, ?::uuid, CAST(? AS jsonb))";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, queue);
            stmt.setString(2, taskId);
            stmt.setString(3, options);
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to retry task: " + e.getMessage(), e);
        }
    }

    // ---- State / Result ----

    /**
     * Returns the state of a task by task_id, or {@code null} if the task is not found.
     */
    public TaskState getTaskState(String queue, String taskId) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (taskId == null || taskId.trim().isEmpty()) {
            throw new IllegalArgumentException("taskId cannot be null or empty");
        }
        TaskResultRecord record = getTaskResultRecord(queue, taskId);
        return record != null ? record.state() : null;
    }

    /**
     * @deprecated Prefer {@link #getTaskResultRecord(String, String)}.
     */
    public String getTaskResult(String queue, String runId) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (runId == null || runId.trim().isEmpty()) {
            throw new IllegalArgumentException("runId cannot be null or empty");
        }

        String sql = "SELECT absurd.get_task_result(?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, queue);
            stmt.setString(2, runId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
                throw new AbsurdException("No result returned from get_task_result");
            }
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to get task result: " + e.getMessage(), e);
        }
    }

    /**
     * Gets the full result record for a task by task_id.
     *
     * @return the record, or {@code null} if the task is not found
     */
    public TaskResultRecord getTaskResultRecord(String queue, String taskId) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (taskId == null || taskId.trim().isEmpty()) {
            throw new IllegalArgumentException("taskId cannot be null or empty");
        }

        String sql = "SELECT task_id::text, state, result::text, failure_reason::text " +
                     "FROM absurd.get_task_result(?, ?::uuid)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, queue);
            stmt.setString(2, taskId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String state = rs.getString("state");
                    return new TaskResultRecord(
                        rs.getString("task_id"),
                        state != null ? TaskState.valueOf(state.toUpperCase()) : null,
                        rs.getString("result"),
                        rs.getString("failure_reason")
                    );
                }
                return null;
            }
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to get task result: " + e.getMessage(), e);
        }
    }

    // ---- Queue management ----

    /**
     * Creates a new unpartitioned queue.
     *
     * <p>Calls {@code absurd.create_queue(queue_name)}.
     */
    public void createQueue(String queueName) throws AbsurdException {
        if (queueName == null || queueName.trim().isEmpty()) {
            throw new IllegalArgumentException("queueName cannot be null or empty");
        }

        String sql = "SELECT absurd.create_queue(?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, queueName);
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to create queue: " + e.getMessage(), e);
        }
    }

    /**
     * Drops a queue and all its associated tables.
     */
    public void dropQueue(String queueName) throws AbsurdException {
        if (queueName == null || queueName.trim().isEmpty()) {
            throw new IllegalArgumentException("queueName cannot be null or empty");
        }

        String sql = "SELECT absurd.drop_queue(?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, queueName);
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to drop queue: " + e.getMessage(), e);
        }
    }

    /**
     * Lists all existing queues.
     */
    public List<String> listQueues() throws AbsurdException {
        String sql = "SELECT queue_name FROM absurd.list_queues()";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            List<String> queues = new ArrayList<>();
            while (rs.next()) {
                queues.add(rs.getString("queue_name"));
            }
            return queues;
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to list queues: " + e.getMessage(), e);
        }
    }

    // ---- Heartbeat ----

    /**
     * Extends the claim on a running task by {@code extensionSecs} seconds.
     *
     * <p>Calls {@code absurd.extend_claim(queue, run_id, extension_seconds)}.
     */
    public void heartbeatTask(String queue, String runId, int extensionSecs) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (runId == null || runId.trim().isEmpty()) {
            throw new IllegalArgumentException("runId cannot be null or empty");
        }

        String sql = "SELECT absurd.extend_claim(?, ?::uuid, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, queue);
            stmt.setString(2, runId);
            stmt.setInt(3, extensionSecs);
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to heartbeat task: " + e.getMessage(), e);
        }
    }

    // ---- Events ----

    /**
     * Emits an event to the queue.
     */
    public void emitEvent(String queue, String eventName, String payloadJson) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (eventName == null || eventName.trim().isEmpty()) {
            throw new IllegalArgumentException("eventName cannot be null or empty");
        }

        String sql = "SELECT absurd.emit_event(?, ?, CAST(? AS jsonb))";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, queue);
            stmt.setString(2, eventName);
            stmt.setString(3, payloadJson);
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to emit event: " + e.getMessage(), e);
        }
    }

    /**
     * Awaits an event within a running task.
     *
     * <p>Returns {@code shouldSuspend=true} when the event has not arrived yet and the run
     * has been transitioned to sleeping state.  Returns {@code shouldSuspend=false} with the
     * event payload when the event is already available (or was resolved from checkpoint).
     *
     * @param timeoutSeconds optional timeout in seconds; {@code null} means wait forever
     */
    public AwaitEventResult awaitEvent(String queue, String taskId, String runId,
                                       String stepName, String eventName,
                                       Integer timeoutSeconds) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }

        String sql = "SELECT should_suspend, payload::text " +
                     "FROM absurd.await_event(?, ?::uuid, ?::uuid, ?, ?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, queue);
            stmt.setString(2, taskId);
            stmt.setString(3, runId);
            stmt.setString(4, stepName);
            stmt.setString(5, eventName);
            if (timeoutSeconds != null) {
                stmt.setInt(6, timeoutSeconds);
            } else {
                stmt.setNull(6, Types.INTEGER);
            }

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return new AwaitEventResult(rs.getBoolean(1), rs.getString(2));
                }
                throw new AbsurdException("No result from await_event");
            }
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to await event: " + e.getMessage(), e);
        }
    }

    // ---- Checkpoints ----

    /**
     * Saves a step checkpoint so that re-executions can skip the step body.
     */
    public void setCheckpointState(String queue, String taskId, String stepName,
                                   String stateJson, String ownerRunId) throws AbsurdException {
        String sql = "SELECT absurd.set_task_checkpoint_state(?, ?::uuid, ?, CAST(? AS jsonb), ?::uuid)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, queue);
            stmt.setString(2, taskId);
            stmt.setString(3, stepName);
            stmt.setString(4, stateJson);
            stmt.setString(5, ownerRunId);
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to set checkpoint: " + e.getMessage(), e);
        }
    }

    /**
     * Retrieves a committed checkpoint, or {@code null} if it does not exist.
     *
     * @return the checkpoint state as a JSON string, or {@code null}
     */
    public String getCheckpointState(String queue, String taskId, String stepName) throws AbsurdException {
        String sql = "SELECT state::text FROM absurd.get_task_checkpoint_state(?, ?::uuid, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, queue);
            stmt.setString(2, taskId);
            stmt.setString(3, stepName);

            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to get checkpoint: " + e.getMessage(), e);
        }
    }

    /**
     * Returns the current database time (respects {@code absurd.fake_now}).
     */
    public Instant getCurrentTime() throws AbsurdException {
        String sql = "SELECT absurd.current_time()";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                Timestamp ts = rs.getTimestamp(1);
                return ts != null ? ts.toInstant() : Instant.now();
            }
            return Instant.now();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to get current time: " + e.getMessage(), e);
        }
    }

    // ---- Internal ----

    private void handleSQLError(SQLException e) throws AbsurdException {
        String sqlState = e.getSQLState();
        if ("AB001".equals(sqlState)) {
            throw new AbsurdException("Task was cancelled: " + e.getMessage(), e);
        } else if ("AB002".equals(sqlState)) {
            throw new AbsurdException("Task failed: " + e.getMessage(), e);
        }
    }
}
