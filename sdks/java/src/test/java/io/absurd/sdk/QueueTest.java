package io.absurd.sdk;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests covering createQueue, dropQueue, and listQueues.
 */
class QueueTest {

    private static final String EXTRA = "extra_queue";

    @AfterEach
    void cleanup() throws Exception {
        TestSetup.clearFakeNow();
        TestSetup.cleanupQueue();
        // Drop the extra queue if it exists
        try (var conn = TestSetup.getConnection();
             var stmt = conn.createStatement()) {
            stmt.execute("SELECT absurd.drop_queue('" + EXTRA + "')");
        } catch (Exception ignored) {}
    }

    @Test
    void defaultQueueIsListed() throws AbsurdException {
        Absurd absurd = TestSetup.createAbsurd();
        List<String> queues = absurd.listQueues();
        assertTrue(queues.contains(TestSetup.DEFAULT_QUEUE));
    }

    @Test
    void createQueueAppearsInList() throws AbsurdException {
        Absurd absurd = TestSetup.createAbsurd();
        absurd.createQueue(EXTRA);

        List<String> queues = absurd.listQueues();
        assertTrue(queues.contains(EXTRA), "Newly created queue must appear in listQueues()");
    }

    @Test
    void dropQueueRemovesFromList() throws AbsurdException {
        Absurd absurd = TestSetup.createAbsurd();
        absurd.createQueue(EXTRA);
        absurd.dropQueue(EXTRA);

        List<String> queues = absurd.listQueues();
        assertFalse(queues.contains(EXTRA), "Dropped queue must not appear in listQueues()");
    }

    @Test
    void createQueueIsIdempotent() throws AbsurdException {
        Absurd absurd = TestSetup.createAbsurd();
        absurd.createQueue(EXTRA);
        // Calling createQueue again must not throw
        assertDoesNotThrow(() -> absurd.createQueue(EXTRA));

        assertTrue(absurd.listQueues().contains(EXTRA));
    }

    @Test
    void dropDefaultQueueRemovesIt() throws AbsurdException {
        // Create a separate queue to drop so the default stays intact.
        Absurd absurd = TestSetup.createAbsurd(EXTRA);
        absurd.createQueue(EXTRA);
        absurd.dropQueue();  // drops the default queue of this client, which is EXTRA

        assertFalse(absurd.listQueues().contains(EXTRA));
    }

    @Test
    void createQueueWithOptionsSucceeds() throws AbsurdException {
        Absurd absurd = TestSetup.createAbsurd();
        CreateQueueOptions opts = CreateQueueOptions.builder()
            .queueName(EXTRA)
            .build();
        absurd.createQueue(opts);

        assertTrue(absurd.listQueues().contains(EXTRA));
    }
}
