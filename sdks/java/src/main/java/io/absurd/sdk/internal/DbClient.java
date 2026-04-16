package io.absurd.sdk.internal;

import io.absurd.sdk.AbsurdException;
import io.absurd.sdk.TaskState;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Internal JDBC wrapper for calling Absurd stored procedures.
 * 
 * <p>This class handles the low-level JDBC operations and provides
 * type-safe methods for interacting with Absurd's PostgreSQL stored procedures.</p>
 */
public class DbClient {
    private final DataSource dataSource;
    
    /**
     * Creates a new DbClient that wraps the given DataSource.
     * 
     * @param dataSource the data source to use for database connections
     * @throws IllegalArgumentException if dataSource is null
     */
    public DbClient(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource cannot be null");
        }
        this.dataSource = dataSource;
    }
    
    /**
     * Validates that a string is valid JSON.
     * 
     * @param json the JSON string to validate
     * @param parameterName the name of the parameter for error messages
     * @throws IllegalArgumentException if the JSON is invalid
     */
    private void validateJson(String json, String parameterName) {
        if (json == null) {
            throw new IllegalArgumentException(parameterName + " cannot be null");
        }
        if (json.trim().isEmpty()) {
            throw new IllegalArgumentException(parameterName + " cannot be empty");
        }
        // Basic JSON validation - check if it starts and ends with valid JSON delimiters
        String trimmed = json.trim();
        if (!trimmed.startsWith("{") && !trimmed.startsWith("[") && !trimmed.equals("null")) {
            throw new IllegalArgumentException(parameterName + " must be valid JSON");
        }
        if (trimmed.startsWith("{") && !trimmed.endsWith("}")) {
            throw new IllegalArgumentException(parameterName + " must be valid JSON object");
        }
        if (trimmed.startsWith("[") && !trimmed.endsWith("]")) {
            throw new IllegalArgumentException(parameterName + " must be valid JSON array");
        }
    }
    
    /**
     * Helper record representing a claimed task.
     * 
     * @param runId the run ID of the claimed task
     * @param taskName the name of the task
     * @param input the input data for the task
     * @param attempt the current attempt number
     */
    public record ClaimedTask(String runId, String taskName, String input, int attempt) {}
    
    /**
     * Claims tasks from the queue for processing.
     * 
     * @param queue the queue name
     * @param workerId the worker identifier
     * @param claimTimeout the timeout in milliseconds for the claim
     * @param batchSize the maximum number of tasks to claim
     * @return list of claimed tasks
     * @throws AbsurdException if there's an error claiming tasks
     */
    public List<ClaimedTask> claimTasks(String queue, String workerId, int claimTimeout, int batchSize) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (workerId == null || workerId.trim().isEmpty()) {
            throw new IllegalArgumentException("workerId cannot be null or empty");
        }
        if (claimTimeout <= 0) {
            throw new IllegalArgumentException("claimTimeout must be positive");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        
        String sql = "CALL absurd.claim_tasks(?, ?, ?, ?)";
        
        try (Connection conn = dataSource.getConnection();
             CallableStatement stmt = conn.prepareCall(sql)) {
            
            stmt.setString(1, queue);
            stmt.setString(2, workerId);
            stmt.setInt(3, claimTimeout);
            stmt.setInt(4, batchSize);
            
            try (ResultSet rs = stmt.executeQuery()) {
                List<ClaimedTask> tasks = new ArrayList<>();
                while (rs.next()) {
                    tasks.add(new ClaimedTask(
                        rs.getString("run_id"),
                        rs.getString("task_name"),
                        rs.getString("input"),
                        rs.getInt("attempt")
                    ));
                }
                return tasks;
            }
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to claim tasks: " + e.getMessage(), e);
        }
    }
    
    /**
     * Spawns a new task in the queue.
     * 
     * @param queue the queue name
     * @param taskName the task name
     * @param input the input data as JSON
     * @param metadata optional metadata as JSON
     * @param parentRunId optional parent run ID
     * @param cronSchedule optional cron schedule
     * @return the run ID of the spawned task
     * @throws AbsurdException if there's an error spawning the task
     */
    /**
     * Spawns a new task.
     *
     * <p>The SQL function signature is {@code absurd.spawn_task(queue, task_name, params jsonb,
     * options jsonb)}, returning {@code (task_id, run_id, attempt, created)}.
     *
     * @param queue the queue name
     * @param taskName the task name
     * @param paramsJson task parameters as JSON (must be valid JSON)
     * @param optionsJson optional options JSON ({@code max_attempts}, {@code retry_strategy},
     *                    {@code headers}, {@code cancellation}, {@code idempotency_key});
     *                    pass {@code null} to use database defaults
     * @return the run_id of the first run for the spawned task
     * @throws AbsurdException if there's an error spawning the task
     */
    public String spawnTask(String queue, String taskName, String paramsJson, String optionsJson) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (taskName == null || taskName.trim().isEmpty()) {
            throw new IllegalArgumentException("taskName cannot be null or empty");
        }
        validateJson(paramsJson, "paramsJson");

        String sql = "SELECT run_id::text FROM absurd.spawn_task(?, ?, CAST(? AS jsonb), CAST(? AS jsonb))";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, queue);
            stmt.setString(2, taskName);
            stmt.setString(3, paramsJson);
            stmt.setString(4, optionsJson != null ? optionsJson : "{}");

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
                throw new AbsurdException("No result returned from spawn_task");
            }
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to spawn task: " + e.getMessage(), e);
        }
    }
    
    /**
     * Completes a task run.
     * 
     * @param queue the queue name
     * @param runId the run ID to complete
     * @param output the output data as JSON
     * @throws AbsurdException if there's an error completing the task
     */
    public void completeTask(String queue, String runId, String output) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (runId == null || runId.trim().isEmpty()) {
            throw new IllegalArgumentException("runId cannot be null or empty");
        }
        validateJson(output, "output");
        
        String sql = "CALL absurd.complete_task(?, ?, CAST(? AS jsonb))";
        
        try (Connection conn = dataSource.getConnection();
             CallableStatement stmt = conn.prepareCall(sql)) {
            
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
     * @param queue the queue name
     * @param runId the run ID to fail
     * @param error the error message
     * @throws AbsurdException if there's an error failing the task
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
        
        String sql = "CALL absurd.fail_task(?, ?, ?)";
        
        try (Connection conn = dataSource.getConnection();
             CallableStatement stmt = conn.prepareCall(sql)) {
            
            stmt.setString(1, queue);
            stmt.setString(2, runId);
            stmt.setString(3, error);
            
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to fail task: " + e.getMessage(), e);
        }
    }
    
    /**
     * Cancels a task by its task ID.
     *
     * @param queue the queue name
     * @param taskId the task ID to cancel
     * @throws AbsurdException if there's an error cancelling the task
     */
    public void cancelTask(String queue, String taskId) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (taskId == null || taskId.trim().isEmpty()) {
            throw new IllegalArgumentException("taskId cannot be null or empty");
        }

        // ?::uuid is a PostgreSQL cast applied to the bound parameter value, not
        // string concatenation — the JDBC driver sends the literal ? to the server.
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
     * Gets the state of a task run.
     * 
     * @param queue the queue name
     * @param runId the run ID to check
     * @return the task state
     * @throws AbsurdException if there's an error getting the task state
     */
    public TaskState getTaskState(String queue, String runId) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (runId == null || runId.trim().isEmpty()) {
            throw new IllegalArgumentException("runId cannot be null or empty");
        }
        
        String sql = "SELECT absurd.get_task_state(?, ?)";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, queue);
            stmt.setString(2, runId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String state = rs.getString(1);
                    return TaskState.valueOf(state);
                }
                throw new AbsurdException("No result returned from get_task_state");
            }
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to get task state: " + e.getMessage(), e);
        }
    }
    
    /**
     * Gets task result snapshot.
     * 
     * @param queue the queue name
     * @param runId the run ID to get results for
     * @return the task result as JSON string
     * @throws AbsurdException if there's an error getting the task result
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
     * Creates a new queue.
     * 
     * @param queueName the name of the queue to create
     * @param retryStrategyJson retry strategy as JSON
     * @param cancellationPolicyJson cancellation policy as JSON
     * @throws AbsurdException if there's an error creating the queue
     */
    public void createQueue(String queueName, String retryStrategyJson, String cancellationPolicyJson) throws AbsurdException {
        if (queueName == null || queueName.trim().isEmpty()) {
            throw new IllegalArgumentException("queueName cannot be null or empty");
        }
        validateJson(retryStrategyJson, "retryStrategyJson");
        validateJson(cancellationPolicyJson, "cancellationPolicyJson");
        
        String sql = "CALL absurd.create_queue(?, CAST(? AS jsonb), CAST(? AS jsonb))";
        
        try (Connection conn = dataSource.getConnection();
             CallableStatement stmt = conn.prepareCall(sql)) {
            
            stmt.setString(1, queueName);
            stmt.setString(2, retryStrategyJson);
            stmt.setString(3, cancellationPolicyJson);
            
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to create queue: " + e.getMessage(), e);
        }
    }
    
    /**
     * Heartbeats a task run to keep it alive.
     * 
     * @param queue the queue name
     * @param runId the run ID to heartbeat
     * @throws AbsurdException if there's an error heartbeat the task
     */
    public void heartbeatTask(String queue, String runId) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (runId == null || runId.trim().isEmpty()) {
            throw new IllegalArgumentException("runId cannot be null or empty");
        }
        
        String sql = "CALL absurd.heartbeat_task(?, ?)";
        
        try (Connection conn = dataSource.getConnection();
             CallableStatement stmt = conn.prepareCall(sql)) {
            
            stmt.setString(1, queue);
            stmt.setString(2, runId);
            
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to heartbeat task: " + e.getMessage(), e);
        }
    }
    
    /**
     * Drops a queue and all its associated tables.
     *
     * @param queueName the name of the queue to drop
     * @throws AbsurdException if there's an error dropping the queue
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
     *
     * @return list of queue names
     * @throws AbsurdException if there's an error listing queues
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

    /**
     * Emits an event to the queue.
     *
     * @param queue the queue name
     * @param eventName the event name
     * @param payloadJson the event payload as JSON (may be null)
     * @throws AbsurdException if there's an error emitting the event
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
     * Snapshot of a task result from the database.
     *
     * @param taskId the task ID
     * @param state the task state
     * @param result the result JSON (if completed)
     * @param failureReason the failure reason JSON (if failed)
     */
    public record TaskResultRecord(String taskId, TaskState state, String result, String failureReason) {}

    /**
     * Gets the result record for a task.
     *
     * @param queue the queue name
     * @param taskId the task ID
     * @return the task result record, or null if not found
     * @throws AbsurdException if there's an error getting the task result
     */
    public TaskResultRecord getTaskResultRecord(String queue, String taskId) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (taskId == null || taskId.trim().isEmpty()) {
            throw new IllegalArgumentException("taskId cannot be null or empty");
        }

        String sql = "SELECT task_id::text, state, result::text, failure_reason::text FROM absurd.get_task_result(?, ?::uuid)";

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

    /**
     * Retries a failed task, optionally spawning a new task.
     *
     * @param queue the queue name
     * @param taskId the task ID to retry
     * @param spawnNew if true, spawn a new task instead of retrying in place
     * @throws AbsurdException if there's an error retrying the task
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

    /**
     * Handles SQL errors and converts them to appropriate exceptions.
     *
     * @param e the SQL exception to handle
     * @throws AbsurdException with appropriate error details
     */
    private void handleSQLError(SQLException e) throws AbsurdException {
        String sqlState = e.getSQLState();
        
        // Handle specific Absurd error codes
        if ("AB001".equals(sqlState)) {
            throw new AbsurdException("Task was cancelled: " + e.getMessage(), e);
        } else if ("AB002".equals(sqlState)) {
            throw new AbsurdException("Task failed: " + e.getMessage(), e);
        }
        
        // For other errors, just let them propagate with context
    }
}