package io.absurd.sdk.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.absurd.sdk.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class WorkerLoop implements Worker {
    private static final Logger log = Logger.getLogger(WorkerLoop.class.getName());

    private final Absurd absurd;
    private final WorkerOptions options;
    final DbClient dbClient;
    private final Closeable dataSourceCloseable;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String workerId = UUID.randomUUID().toString();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AtomicInteger inFlightCount = new AtomicInteger(0);
    private final Object inFlightLock = new Object();
    private volatile Thread loopThread;

    public WorkerLoop(Absurd absurd, WorkerOptions options, DbClient dbClient) {
        this(absurd, options, dbClient, null);
    }

    public WorkerLoop(Absurd absurd, WorkerOptions options, DbClient dbClient, Closeable dataSourceCloseable) {
        this.absurd = absurd;
        this.options = options;
        this.dbClient = dbClient;
        this.dataSourceCloseable = dataSourceCloseable;
    }

    @Override
    public void start() {
        loopThread = Thread.ofVirtual()
            .name("absurd-worker-" + options.getQueueName())
            .start(this::pollLoop);
    }

    @Override
    public void stop() {
        shutdown.set(true);
        if (loopThread != null) {
            loopThread.interrupt();
        }
        synchronized (inFlightLock) {
            while (inFlightCount.get() > 0) {
                try {
                    inFlightLock.wait(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (loopThread != null) {
            try {
                loopThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (dataSourceCloseable != null) {
            try {
                dataSourceCloseable.close();
            } catch (IOException e) {
                log.warning("Error closing data source: " + e.getMessage());
            }
        }
    }

    @Override
    public boolean isRunning() {
        return loopThread != null && loopThread.isAlive();
    }

    @Override
    public WorkerOptions getOptions() {
        return options;
    }

    private void pollLoop() {
        while (!shutdown.get()) {
            try {
                List<DbClient.ClaimedTask> tasks = dbClient.claimTasks(
                    options.getQueueName(),
                    workerId,
                    options.getPollTimeoutMs(),
                    options.getConcurrency()
                );
                if (tasks.isEmpty()) {
                    Thread.sleep(options.getPollIntervalMs());
                    continue;
                }
                for (DbClient.ClaimedTask task : tasks) {
                    if (shutdown.get()) break;
                    inFlightCount.incrementAndGet();
                    Thread.ofVirtual()
                        .name("absurd-task-" + task.runId())
                        .start(() -> {
                            try {
                                executeTask(task);
                            } finally {
                                synchronized (inFlightLock) {
                                    if (inFlightCount.decrementAndGet() == 0) {
                                        inFlightLock.notifyAll();
                                    }
                                }
                            }
                        });
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (AbsurdException e) {
                log.warning("Error claiming tasks: " + e.getMessage());
                try {
                    Thread.sleep(options.getPollIntervalMs());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    void executeTask(DbClient.ClaimedTask claimedTask) {
        // 1. Look up TaskDefinition by name
        TaskDefinition<?, ?> definition = absurd.getTaskDefinitions().get(claimedTask.taskName());
        if (definition == null) {
            failTask(claimedTask.runId(), "Unknown task: " + claimedTask.taskName());
            return;
        }

        // 2. Deserialize params via Jackson
        Object params;
        try {
            params = objectMapper.readValue(claimedTask.input(), Object.class);
        } catch (Exception e) {
            failTask(claimedTask.runId(), "Failed to deserialize input: " + e.getMessage());
            return;
        }

        // 3. Create TaskContextImpl
        TaskContextImpl ctx = new TaskContextImpl(dbClient, claimedTask, options.getQueueName(), Map.of());

        // 4. Schedule watchdog: warn at claimTimeout, exit JVM at 2x
        int claimTimeout = options.getPollTimeoutMs();
        Thread watchdog = Thread.ofVirtual()
            .name("absurd-watchdog-" + claimedTask.runId())
            .start(() -> {
                try {
                    Thread.sleep(claimTimeout);
                    log.warning("Task " + claimedTask.runId() + " exceeded claim timeout of " + claimTimeout + "ms");
                    Thread.sleep(claimTimeout);
                    log.severe("Task " + claimedTask.runId() + " exceeded 2x claim timeout — exiting JVM");
                    System.exit(1);
                } catch (InterruptedException ignored) {
                    // Task completed normally, watchdog cancelled
                }
            });

        // 5. Call handler.handle(params, ctx) and dispatch result
        try {
            @SuppressWarnings("unchecked")
            TaskHandler<Object, Object> handler = (TaskHandler<Object, Object>) definition.getHandler();
            Object result = handler.handle(params, ctx);

            // Success -> complete_run
            String resultJson = objectMapper.writeValueAsString(result);
            dbClient.completeTask(options.getQueueName(), claimedTask.runId(), resultJson);

        } catch (SuspendTask e) {
            // SuspendTask -> return (task already suspended via stored procedure)
        } catch (JsonProcessingException e) {
            failTask(claimedTask.runId(), "Failed to serialize result: " + e.getMessage());
        } catch (AbsurdException e) {
            failTask(claimedTask.runId(), e.getMessage());
        } catch (Exception e) {
            failTask(claimedTask.runId(), e.getMessage() != null ? e.getMessage() : e.getClass().getName());
        } finally {
            watchdog.interrupt();
        }
    }

    private void failTask(String runId, String error) {
        try {
            dbClient.failTask(options.getQueueName(), runId, error != null ? error : "Unknown error");
        } catch (AbsurdException e) {
            log.log(java.util.logging.Level.SEVERE,
                "Failed to mark task " + runId + " as failed", e);
        }
    }
}
