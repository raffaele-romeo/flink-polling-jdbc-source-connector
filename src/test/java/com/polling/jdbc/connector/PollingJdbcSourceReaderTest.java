package com.polling.jdbc.connector;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class PollingJdbcSourceReaderTest {

    private static final String JDBC_URL = "jdbc:h2:mem:readertest;DB_CLOSE_DELAY=-1";
    private Connection setupConnection;

    @BeforeEach
    void setUp() throws Exception {
        setupConnection = DriverManager.getConnection(JDBC_URL);
        try (Statement stmt = setupConnection.createStatement()) {
            stmt.execute("CREATE TABLE events (" +
                    "id INT PRIMARY KEY, " +
                    "name VARCHAR(100), " +
                    "price FLOAT, " +
                    "created_at TIMESTAMP)");
            stmt.execute("INSERT INTO events VALUES (1, 'alpha', 1.5, '2024-01-01 00:00:00')");
            stmt.execute("INSERT INTO events VALUES (2, 'beta', 2.5, '2024-01-02 00:00:00')");
            stmt.execute("INSERT INTO events VALUES (3, 'gamma', 3.5, '2024-01-03 00:00:00')");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        try (Statement stmt = setupConnection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS events");
        }
        setupConnection.close();
    }

    @Test
    void firstPollReadsAllRows() throws Exception {
        var config = PollingJdbcSourceConfig.builder(
                JDBC_URL, DatabaseDialect.POSTGRESQL, "EVENTS", "ID", 100).build();
        var reader = new PollingJdbcSourceReader(config);

        reader.addSplits(Collections.singletonList(new PollingJdbcSplit(null)));

        var collected = collectRows(reader, 10);
        assertEquals(3, collected.size());
        assertEquals(1, collected.getFirst().getField(0));
        assertEquals("alpha", collected.getFirst().getField(1));
        assertEquals(1.5f, collected.getFirst().getField(2));

        reader.close();
    }

    @Test
    void subsequentPollReadsOnlyNewRows() throws Exception {
        var config = PollingJdbcSourceConfig.builder(
                JDBC_URL, DatabaseDialect.POSTGRESQL, "EVENTS", "ID", 50).build();
        var reader = new PollingJdbcSourceReader(config);

        reader.addSplits(Collections.singletonList(new PollingJdbcSplit(null)));

        var firstBatch = collectRows(reader, 10);
        assertEquals(3, firstBatch.size());

        try (Statement stmt = setupConnection.createStatement()) {
            stmt.execute("INSERT INTO events VALUES (4, 'delta', 4.5, '2024-01-04 00:00:00')");
            stmt.execute("INSERT INTO events VALUES (5, 'epsilon', 5.5, '2024-01-05 00:00:00')");
        }

        // Poll until new rows are picked up
        var secondBatch = new ArrayList<Row>();
        var output = new TestReaderOutput(secondBatch);
        await().atMost(Duration.ofSeconds(2))
                .pollInterval(Duration.ofMillis(10))
                .untilAsserted(() -> {
                    reader.pollNext(output);
                    assertEquals(2, secondBatch.size());
                });
        assertEquals(4, secondBatch.get(0).getField(0));
        assertEquals(5, secondBatch.get(1).getField(0));

        reader.close();
    }

    @Test
    void noNewRowsEmitsNothing() throws Exception {
        var config = PollingJdbcSourceConfig.builder(
                JDBC_URL, DatabaseDialect.POSTGRESQL, "EVENTS", "ID", 50).build();
        var reader = new PollingJdbcSourceReader(config);

        reader.addSplits(Collections.singletonList(new PollingJdbcSplit(null)));

        collectRows(reader, 10);

        var secondBatch = new ArrayList<Row>();
        var output = new TestReaderOutput(secondBatch);
        await().during(Duration.ofMillis(200))
                .atMost(Duration.ofSeconds(2))
                .pollInterval(Duration.ofMillis(10))
                .untilAsserted(() -> {
                    reader.pollNext(output);
                    assertTrue(secondBatch.isEmpty());
                });

        reader.close();
    }

    @Test
    void snapshotStatePreservesOffset() throws Exception {
        var config = PollingJdbcSourceConfig.builder(
                JDBC_URL, DatabaseDialect.POSTGRESQL, "EVENTS", "ID", 100).build();
        var reader = new PollingJdbcSourceReader(config);

        reader.addSplits(Collections.singletonList(new PollingJdbcSplit(null)));
        collectRows(reader, 10);

        var state = reader.snapshotState(1L);
        assertEquals(1, state.size());
        assertEquals(new OffsetType.IntegerOffset(3), state.getFirst().currentOffset());

        reader.close();
    }

    @Test
    void staticStartingOffsetSkipsRows() throws Exception {
        var config = PollingJdbcSourceConfig.builder(
                JDBC_URL, DatabaseDialect.POSTGRESQL, "EVENTS", "ID", 100)
                .staticStartingOffset(new OffsetType.IntegerOffset(2))
                .build();

        var reader = new PollingJdbcSourceReader(config);
        reader.addSplits(Collections.singletonList(new PollingJdbcSplit(null)));

        var collected = collectRows(reader, 10);
        assertEquals(1, collected.size());
        assertEquals(3, collected.getFirst().getField(0));

        reader.close();
    }

    @Test
    void checkpointOffsetTakesPriorityOverStaticOffset() throws Exception {
        var config = PollingJdbcSourceConfig.builder(
                JDBC_URL, DatabaseDialect.POSTGRESQL, "EVENTS", "ID", 100)
                .staticStartingOffset(new OffsetType.IntegerOffset(1))
                .build();

        var reader = new PollingJdbcSourceReader(config);

        reader.addSplits(Collections.singletonList(new PollingJdbcSplit(new OffsetType.IntegerOffset(2))));

        var collected = collectRows(reader, 10);
        assertEquals(1, collected.size());
        assertEquals(3, collected.getFirst().getField(0));

        reader.close();
    }

    /**
     * Polls the reader up to maxAttempts times, collecting all emitted rows.
     */
    private List<Row> collectRows(PollingJdbcSourceReader reader, int maxAttempts) throws Exception {
        List<Row> collected = new ArrayList<>();
        var output = new TestReaderOutput(collected);

        for (int i = 0; i < maxAttempts; i++) {
            InputStatus status = reader.pollNext(output);
            if (status == InputStatus.NOTHING_AVAILABLE && i > 0) {
                break;
            }
        }
        return collected;
    }

    /**
     * Minimal ReaderOutput implementation for testing.
     */
    private static class TestReaderOutput implements ReaderOutput<Row> {
        private final List<Row> collected;

        TestReaderOutput(List<Row> collected) {
            this.collected = collected;
        }

        @Override
        public void collect(Row record) {
            collected.add(record);
        }

        @Override
        public void collect(Row record, long timestamp) {
            collected.add(record);
        }

        @Override
        public SourceOutput<Row> createOutputForSplit(String splitId) {
            return new SourceOutput<>() {
                @Override public void collect(Row record) { collected.add(record); }
                @Override public void collect(Row record, long timestamp) { collected.add(record); }
                @Override public void markIdle() {}
                @Override public void markActive() {}
                @Override public void emitWatermark(Watermark watermark) {}
            };
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}

        @Override
        public void emitWatermark(Watermark watermark) {}
    }
}
