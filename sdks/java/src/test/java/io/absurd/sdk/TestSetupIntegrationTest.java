package io.absurd.sdk;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests that verify the TestSetup infrastructure works end-to-end
 * against a live PostgreSQL container.
 */
class TestSetupIntegrationTest {

    @AfterEach
    void cleanup() throws Exception {
        TestSetup.clearFakeNow();
        TestSetup.cleanupQueue();
    }

    @Test
    void schemaIsLoaded() throws Exception {
        // absurd schema and absurd.queues table must exist
        String result = TestSetup.queryScalar(
            "SELECT COUNT(*)::text FROM information_schema.schemata WHERE schema_name = 'absurd'",
            String.class
        );
        assertEquals("1", result);
    }

    @Test
    void defaultQueueExists() throws Exception {
        String result = TestSetup.queryScalar(
            "SELECT queue_name FROM absurd.queues WHERE queue_name = '" + TestSetup.DEFAULT_QUEUE + "'",
            String.class
        );
        assertEquals(TestSetup.DEFAULT_QUEUE, result);
    }

    @Test
    void createAbsurdReturnsConfiguredClient() {
        Absurd absurd = TestSetup.createAbsurd();
        assertNotNull(absurd);
        assertEquals(TestSetup.DEFAULT_QUEUE, absurd.getDefaultQueueName());
    }

    @Test
    void createAbsurdWithQueueName() {
        Absurd absurd = TestSetup.createAbsurd("my_queue");
        assertEquals("my_queue", absurd.getDefaultQueueName());
    }

    @Test
    void setFakeNowAffectsCurrentTime() throws Exception {
        Instant fakeTime = Instant.parse("2025-06-15T12:00:00Z");
        TestSetup.setFakeNow(fakeTime);

        String result = TestSetup.queryScalar(
            "SELECT absurd.current_time()::text",
            String.class
        );

        assertNotNull(result);
        assertTrue(result.startsWith("2025-06-15"), "Expected fake time prefix, got: " + result);
    }

    @Test
    void clearFakeNowRestoresRealClock() throws Exception {
        Instant fakeTime = Instant.parse("2020-01-01T00:00:00Z");
        TestSetup.setFakeNow(fakeTime);
        TestSetup.clearFakeNow();

        String result = TestSetup.queryScalar(
            "SELECT absurd.current_time()::text",
            String.class
        );

        assertNotNull(result);
        // Real clock should be after 2020
        assertFalse(result.startsWith("2020-01-01"), "Expected real clock, got: " + result);
    }

    @Test
    void getConnectionAllowsDirectAssertions() throws Exception {
        try (Connection conn = TestSetup.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1 + 1")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void listQueuesViaSdkSeesDefaultQueue() throws Exception {
        Absurd absurd = TestSetup.createAbsurd();
        List<String> queues = absurd.listQueues();
        assertTrue(queues.contains(TestSetup.DEFAULT_QUEUE));
    }

    @Test
    void cleanupQueueTruncatesTables() throws Exception {
        Absurd absurd = TestSetup.createAbsurd();

        // Spawn a task to populate the queue tables (pass a Map, not a pre-serialized string)
        absurd.spawn("any-task", java.util.Map.of());

        // Verify something is in the task table
        String beforeCount = TestSetup.queryScalar(
            "SELECT COUNT(*)::text FROM absurd.t_" + TestSetup.DEFAULT_QUEUE,
            String.class
        );
        assertEquals("1", beforeCount);

        // Cleanup
        TestSetup.cleanupQueue();

        String afterCount = TestSetup.queryScalar(
            "SELECT COUNT(*)::text FROM absurd.t_" + TestSetup.DEFAULT_QUEUE,
            String.class
        );
        assertEquals("0", afterCount);
    }
}
