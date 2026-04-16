package io.absurd.sdk;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.Instant;

/**
 * Shared integration-test infrastructure.
 *
 * <p>Starts one PostgreSQL container per JVM, loads {@code sql/absurd.sql},
 * creates a default test queue, and exposes helpers for:
 * <ul>
 *   <li>Creating {@link Absurd} clients wired to the container</li>
 *   <li>Controlling {@code absurd.fake_now} for deterministic time</li>
 *   <li>Direct JDBC access for assertions</li>
 *   <li>Cleaning up queue tables between tests</li>
 * </ul>
 *
 * <p>The pool is fixed at one physical connection ({@code maximumPoolSize=1})
 * so that {@code setFakeNow} affects the same connection used by the SDK,
 * making time-sensitive assertions deterministic.
 *
 * <p>Typical test pattern:
 * <pre>
 *   {@literal @}AfterEach
 *   void cleanup() throws Exception {
 *       TestSetup.clearFakeNow();
 *       TestSetup.cleanupQueue();
 *   }
 *
 *   {@literal @}Test
 *   void myTest() throws Exception {
 *       TestSetup.setFakeNow(Instant.parse("2025-01-01T00:00:00Z"));
 *       Absurd absurd = TestSetup.createAbsurd();
 *       // ...
 *   }
 * </pre>
 */
public final class TestSetup {

    /** Name of the queue created automatically during setup. */
    public static final String DEFAULT_QUEUE = "test_queue";

    // Relative to sdks/java/ — the Gradle working directory during tests.
    private static final Path ABSURD_SQL = Path.of("../../sql/absurd.sql");

    // PostgreSQL identifier: starts with letter/underscore, followed by letters/digits/underscores.
    // Used to validate queue names before embedding them in TRUNCATE statements.
    private static final java.util.regex.Pattern SAFE_IDENTIFIER =
        java.util.regex.Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

    private static final PostgreSQLContainer<?> CONTAINER;
    private static final HikariDataSource DATA_SOURCE;

    static {
        try {
            CONTAINER = new PostgreSQLContainer<>("postgres:16-alpine");
            CONTAINER.start();
            DATA_SOURCE = createPool(CONTAINER, 1);
            loadSchema();
            createQueue(DEFAULT_QUEUE);
            // Close the pool when the JVM exits so connections are released cleanly.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (!DATA_SOURCE.isClosed()) {
                    DATA_SOURCE.close();
                }
            }, "TestSetup-shutdown"));
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private TestSetup() {}

    // ---- Absurd factory ----

    /** Creates an {@link Absurd} client pointed at the test database using the default queue. */
    public static Absurd createAbsurd() {
        return createAbsurd(DEFAULT_QUEUE);
    }

    /**
     * Creates an {@link Absurd} client pointed at the test database using the given queue.
     *
     * @param queueName the queue name (must be a valid PostgreSQL identifier)
     * @throws IllegalArgumentException if queueName is null or empty
     */
    public static Absurd createAbsurd(String queueName) {
        if (queueName == null || queueName.isBlank()) {
            throw new IllegalArgumentException("queueName cannot be null or empty");
        }
        return Absurd.builder()
            .dataSource(DATA_SOURCE)
            .queueName(queueName)
            .build();
    }

    // ---- Time control ----

    /**
     * Sets the fake time returned by {@code absurd.current_time()}.
     *
     * <p>Because the pool has {@code maximumPoolSize=1}, the session variable
     * is visible to all subsequent SDK calls on the same physical connection.
     */
    public static void setFakeNow(Instant time) throws SQLException {
        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT set_config('absurd.fake_now', ?, false)")) {
            // is_local=false: session scope, survives transaction boundaries
            stmt.setString(1, time.toString());
            stmt.execute();
        }
    }

    /** Clears the fake time so {@code absurd.current_time()} returns the real clock. */
    public static void clearFakeNow() throws SQLException {
        try (Connection conn = DATA_SOURCE.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SET absurd.fake_now = DEFAULT");
        }
    }

    // ---- Direct JDBC access ----

    /**
     * Returns a connection from the shared pool for direct assertions.
     * The caller is responsible for closing it (use try-with-resources).
     */
    public static Connection getConnection() throws SQLException {
        return DATA_SOURCE.getConnection();
    }

    /** Returns the shared {@link DataSource} for advanced use. */
    public static DataSource getDataSource() {
        return DATA_SOURCE;
    }

    /**
     * Executes a query and returns the first column of the first row, cast to {@code type}.
     * Returns {@code null} if no rows are returned.
     */
    public static <T> T queryScalar(String sql, Class<T> type) throws SQLException {
        try (Connection conn = DATA_SOURCE.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? type.cast(rs.getObject(1)) : null;
        }
    }

    // ---- Cleanup ----

    /**
     * Truncates all five queue tables ({@code t_}, {@code r_}, {@code c_},
     * {@code e_}, {@code w_}), resetting the queue to an empty state between tests.
     *
     * @param queueName must match {@code [a-zA-Z_][a-zA-Z0-9_]*} — validated before use
     *                  to prevent SQL injection since table names cannot be parameterized
     */
    public static void cleanupQueue(String queueName) throws SQLException {
        requireSafeIdentifier(queueName, "queueName");
        String sql = String.format(
            "TRUNCATE absurd.t_%1$s, absurd.r_%1$s, absurd.c_%1$s, absurd.e_%1$s, absurd.w_%1$s",
            queueName
        );
        try (Connection conn = DATA_SOURCE.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    /** Truncates all queue tables for {@link #DEFAULT_QUEUE}. */
    public static void cleanupQueue() throws SQLException {
        cleanupQueue(DEFAULT_QUEUE);
    }

    // ---- Internal ----

    private static HikariDataSource createPool(PostgreSQLContainer<?> container, int maxPoolSize) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(container.getJdbcUrl());
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());
        config.setMaximumPoolSize(maxPoolSize);
        return new HikariDataSource(config);
    }

    private static void loadSchema() throws IOException, InterruptedException {
        if (!Files.exists(ABSURD_SQL)) {
            throw new IllegalStateException(
                "absurd.sql not found at " + ABSURD_SQL.toAbsolutePath() +
                ". Ensure tests are run from the sdks/java/ directory."
            );
        }
        CONTAINER.copyFileToContainer(
            MountableFile.forHostPath(ABSURD_SQL.toAbsolutePath()),
            "/tmp/absurd.sql"
        );
        var result = CONTAINER.execInContainer(
            "psql",
            "-U", CONTAINER.getUsername(),
            "-d", CONTAINER.getDatabaseName(),
            "-f", "/tmp/absurd.sql"
        );
        if (result.getExitCode() != 0) {
            throw new RuntimeException("Failed to load absurd.sql:\n" + result.getStderr());
        }
    }

    private static void createQueue(String queueName) throws SQLException {
        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT absurd.create_queue(?)")) {
            stmt.setString(1, queueName);
            stmt.execute();
        }
    }

    /**
     * Validates that {@code name} is a safe PostgreSQL identifier before embedding
     * it in a DDL statement where parameterization is not possible.
     */
    private static void requireSafeIdentifier(String name, String paramName) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException(paramName + " cannot be null or empty");
        }
        if (!SAFE_IDENTIFIER.matcher(name).matches()) {
            throw new IllegalArgumentException(
                paramName + " must be a valid PostgreSQL identifier ([a-zA-Z_][a-zA-Z0-9_]*), got: " + name
            );
        }
    }
}
