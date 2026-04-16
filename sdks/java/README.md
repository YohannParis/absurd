# Absurd SDK for Java

Java SDK for [Absurd](https://github.com/earendil-works/absurd): a PostgreSQL-based durable task execution system.

Absurd is the simplest durable execution workflow system you can think of. It's entirely based on Postgres and nothing else. It's almost as easy to use as a queue, but it handles scheduling and retries, and it does all of that without needing any other services to run in addition to Postgres.

**Warning:** _this is an early experiment and should not be used in production._

## What is Durable Execution?

Durable execution (or durable workflows) is a way to run long-lived, reliable
functions that can survive crashes, restarts, and network failures without losing
state or duplicating work. Instead of running your logic in memory, a durable
execution system decomposes a task into smaller pieces (step functions) and
records every step and decision.

## Prerequisites

- **Java 21+** (virtual threads are used)
- A PostgreSQL database (16+) with the Absurd schema loaded

### Initializing the Schema

Before using the SDK, you need to initialize Absurd in your PostgreSQL database:

```bash
# One-off usage
uvx absurdctl init -d your-database-name
uvx absurdctl create-queue -d your-database-name default

# Or install it once
uv tool install absurdctl
absurdctl init -d your-database-name
absurdctl create-queue -d your-database-name default
```

See the [absurdctl docs](https://earendil-works.github.io/absurd/tools/absurdctl/) for installation details and
the full CLI reference, including
[`uvx`](https://docs.astral.sh/uv/guides/tools/) usage.

## Installation

Add the SDK to your Gradle project. Release artifacts are published to Maven Central.

```groovy
dependencies {
    implementation 'dev.absurd:absurd-sdk-java:0.1.0'
}
```

> **Note:** The group `dev.absurd` and version `0.1.0-SNAPSHOT` are the current
> development values.  Check the latest release tag for the production version.

## Synchronous API

If you omit the connection argument, the client uses `ABSURD_DATABASE_URL`,
then `PGDATABASE`, then `postgresql://localhost/absurd`.

```java
import io.absurd.sdk.*;

var app = Absurd.builder()
    .url("jdbc:postgresql://localhost/mydb")
    .build();

// Register a task
app.registerTask(TaskDefinition.of("order-fulfillment", (params, ctx) -> {
    // Each step is checkpointed, so if the process crashes, we resume
    // from the last completed step
    var payment = ctx.step("process-payment", () ->
        Map.of("paymentId", "pay-" + params.get("orderId"), "amount", params.get("amount")));

    var inventory = ctx.step("reserve-inventory", () ->
        Map.of("reservedItems", params.get("items")));

    // Wait for an event — the task suspends until the event arrives
    var shipment = ctx.awaitEvent("shipment.packed:" + params.get("orderId"));

    ctx.step("send-notification", () ->
        Map.of("sentTo", params.get("email"), "trackingNumber", shipment.get("trackingNumber")));

    return Map.of(
        "orderId", params.get("orderId"),
        "payment", payment,
        "inventory", inventory,
        "trackingNumber", shipment.get("trackingNumber")
    );
}));

// Start a worker that pulls tasks from Postgres
app.startWorker();
```

## Spawning Tasks

```java
// Spawn a task — it will be executed durably with automatic retries
app.spawn("order-fulfillment", Map.of(
    "orderId", "42",
    "amount", 9999,
    "items", List.of("widget-1", "gadget-2"),
    "email", "customer@example.com"
));
```

If the task is not registered in this process, pass `queueName` via `SpawnOptions` explicitly.
For unregistered tasks, defaults from `registerTask(...)` are unavailable;
spawn options (or client defaults) are used.

## Task Result Snapshots

You can inspect or wait for a task's terminal result:

```java
TaskResultSnapshot snap = app.fetchTaskResult(taskId);
if (snap != null) {
    System.out.println(snap.getState() + " — " + snap.getResult());
}

// Wait up to 30 seconds for the task to complete
TaskResultSnapshot finalSnap = app.awaitTaskResult(taskId, 30_000);
if (finalSnap.getState() == TaskState.COMPLETED) {
    System.out.println("Done! Result: " + finalSnap.getResult());
}
```

Inside a task handler, you can also wait for child tasks durably via
`ctx.awaitTaskResult(childTaskId, timeout)`.

## Emitting Events

```java
// Emit an event that a suspended task might be waiting for
app.emitEvent("shipment.packed:42", Map.of("trackingNumber", "TRACK123"));

// With explicit queue
app.emitEvent("order-queue", "order.shipped:42", Map.of("carrier", "UPS"));
```

## Step Checkpoints

When you need to split step handling into two phases (e.g. around an external
loop), use `beginStep()` / `completeStep()`:

```java
var handle = ctx.beginStep("persist-turn");
if (handle.isDone()) {
    // Step was already checkpointed — reuse the cached state
    var messages = handle.getState();
} else {
    // First execution — compute the state and checkpoint it
    var messages = computeTurn(params);
    ctx.completeStep(handle, messages);
}

return messages;
```

This is useful when integrating with event-driven loops (for example agent
runtimes) where the checkpoint boundary is not a single inline callback.

## Configuration Options

### Connection Pooling

You can provide your own `DataSource` or let Absurd create one:

```java
// Auto-managed pool
var app = Absurd.builder()
    .url("jdbc:postgresql://localhost/mydb")
    .build();

// Own pool: Absurd uses it but does not close it
var dataSource = createYourPool();
var app = Absurd.builder()
    .dataSource(dataSource)
    .build();
```

### Worker Options

Fine-tune the worker that polls for tasks:

```java
app.startWorker(WorkerOptions.builder()
    .queueName("default")
    .pollIntervalMs(200)
    .maxPollTimeoutMs(5_000)
    .concurrency(4)
    .backoffMultiplier(1.5)
    .build());
```

| Option | Type | Description |
|--------|------|-------------|
| `queueName` | `String` | Queue to pull tasks from (required) |
| `concurrency` | `int` | Number of concurrent task threads |
| `pollIntervalMs` | `long` | Sleep interval when no tasks are found |
| `maxPollTimeoutMs` | `long` | Maximum database poll wait time |
| `backoffMultiplier` | `double` | Factor for exponential backoff between polls |

### Spawn Options

Control where and how a task is spawned:

```java
app.spawn("my-task", input, SpawnOptions.builder()
    .queueName("high-priority")
    .parentRunId(app.getRunId())
    .metadata(Map.of("env", "prod"))
    .build());
```

| Option | Type | Description |
|--------|------|-------------|
| `queueName` | `String` | Target queue |
| `parentRunId` | `String` / `UUID` | Parent run for lineage |
| `metadata` | `Map<String, String>` | Key-value metadata attached to the run |
| `cronExpression` | `String` | Cron expression for scheduled execution |

## Task Definition Defaults

Each registered `TaskDefinition` provides defaults for **retry** and **cancellation**
behavior. Override them in your definition:

```java
app.registerTask(TaskDefinition.<String, String>of("flaky-task", handler)
    .withRetryStrategy(RetryStrategy.builder()
        .maxAttempts(5)
        .initialDelayMs(1_000)
        .backoffFactor(2.0)
        .maxDelayMs(30_0_00)
        .build())
    .withCancellationPolicy(CancellationPolicy.builder()
        .timeoutMs(60_000)
        .interruptible(true)
        .build())
    .withHooks(new MyHooks()));
```

### Retry Strategy

| Option | Type | Description |
|--------|------|-------------|
| `maxAttempts` | `int` | Maximum number of retry attempts |
| `initialDelayMs` | `long` | Initial delay in milliseconds |
| `backoffFactor` | `double` | Exponential backoff multiplier |
| `maxDelayMs` | `long` | Maximum delay between retries |

### Cancellation Policy

| Option | Type | Description |
|--------|------|-------------|
| `timeoutMs` | `long` | Timeout in milliseconds before force termination |
| `interruptible` | `boolean` | Whether to interrupt the running thread |

## Lifecycle

- `app.startWorker()` — starts the worker (returns immediately; worker runs asynchronously)
- `app.stopWorker()` — gracefully stops the worker after draining running tasks
- `app.spawn(...)` — schedules a task for execution (non-blocking)
- `app.stop()` — stops the worker and closes the underlying connection pool

## Example: Multi-Step Workflow

```java
import io.absurd.sdk.*;

var app = Absurd.builder()
    .url("jdbc:postgresql://localhost/absurd")
    .build();

app.registerTask(TaskDefinition.of("checkout", (params, ctx) -> {
    String orderId = (String) params.get("orderId");

    // Step 1: reserve inventory (checkpointed)
    var reservation = ctx.step("reserve", () -> reserveInventory(orderId));

    // Step 2: process payment (checkpointed)
    var payment = ctx.step("pay", () -> chargeCard(reservation));

    // Step 3: wait for packing event
    var packingEvent = ctx.awaitEvent("packing.completed:" + orderId);

    // Step 4: ship order (checkpointed)
    ctx.step("ship", () -> shipOrder(reservation, packingEvent));

    return Map.of("status", "shipped", "orderId", orderId);
}));

app.startWorker(WorkerOptions.builder()
    .queueName("default")
    .concurrency(10)
    .pollIntervalMs(500)
    .build());

// Somewhere else in your code:
app.spawn("checkout", Map.of("orderId", "12345"));
```

## Hooks (Lifecycle Callbacks)

Register hooks to observe task lifecycle events:

```java
app.registerTask(TaskDefinition.of("my-task", handler)
    .withHooks(new AbsurdHooks() {
        @Override
        public void afterTaskSuccess(TaskContext ctx, Object result) {
            System.out.println("Task completed: " + ctx.getTaskId());
        }

        @Override
        public void afterTaskFailure(TaskContext ctx, Throwable error) {
            System.err.println("Task failed: " + ctx.getTaskId());
        }
    }));
```

Hooks provided: `beforeTaskExecution`, `afterTaskSuccess`, `afterTaskFailure`,
`onTaskRetry`.

## Idempotency Keys

Use the task ID to derive idempotency keys for external APIs:

```java
var payment = ctx.step("process-payment", () -> {
    String idempotencyKey = ctx.getTaskId() + ":payment";
    return chargeExternalApi(idempotencyKey);
});
```

## License and Links

- [Examples](https://github.com/earendil-works/absurd/tree/main/sdks/java/examples)
- [Issue Tracker](https://github.com/earendil-works/absurd/issues)
- License: [Apache-2.0](https://github.com/earendil-works/absurd/blob/main/LICENSE)
