package com.polling.jdbc.connector;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public class PollingJdbcSourceReader implements SourceReader<Row, PollingJdbcSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(PollingJdbcSourceReader.class);

    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 1000;
    private static final int CONNECTION_VALID_TIMEOUT_SECONDS = 5;

    private final PollingJdbcSourceConfig config;

    private Connection connection;
    private boolean splitAssigned = false;

    private OffsetType currentOffset;
    private long lastPollTimeMs = 0;
    private final Queue<Row> pendingRows = new ArrayDeque<>();
    private int columnCount = -1;
    private int[] columnTypes;

    private CompletableFuture<Void> availabilityFuture = new CompletableFuture<>();

    public PollingJdbcSourceReader(PollingJdbcSourceConfig config) {
        this.config = config;
    }

    @Override
    public void start() {
        // Connection is established when the split is assigned
    }

    @Override
    public InputStatus pollNext(ReaderOutput<Row> output) throws Exception {
        if (!splitAssigned) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        if (!pendingRows.isEmpty()) {
            output.collect(pendingRows.poll());
            return pendingRows.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
        }

        // Check if it's time to poll
        long now = System.currentTimeMillis();
        long elapsed = now - lastPollTimeMs;
        if (elapsed < config.getPollingIntervalMs()) {
            long remainingMs = config.getPollingIntervalMs() - elapsed;
            resetAvailabilityFuture(remainingMs);
            return InputStatus.NOTHING_AVAILABLE;
        }

        executePoll();
        lastPollTimeMs = System.currentTimeMillis();

        if (!pendingRows.isEmpty()) {
            output.collect(pendingRows.poll());
            return pendingRows.isEmpty() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
        }

        resetAvailabilityFuture(config.getPollingIntervalMs());
        return InputStatus.NOTHING_AVAILABLE;
    }

    private void resetAvailabilityFuture(long delayMs) {
        availabilityFuture = new CompletableFuture<>();
        Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            availabilityFuture.complete(null);
        });
    }

    private void executePoll() throws Exception {
        ensureConnection();

        String sql = buildPollQuery();
        LOG.debug("Executing poll query: {}", sql);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            if (currentOffset != null) {
                setOffsetParameter(stmt, 1, currentOffset);
            }

            try (ResultSet rs = stmt.executeQuery()) {
                if (columnCount < 0) {
                    ResultSetMetaData meta = rs.getMetaData();
                    columnCount = meta.getColumnCount();
                    columnTypes = new int[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        columnTypes[i] = meta.getColumnType(i + 1);
                    }
                }

                int rowCount = 0;
                while (rs.next()) {
                    Row row = mapRow(rs);
                    pendingRows.add(row);
                    rowCount++;

                    currentOffset = readOffset(rs, config.getOffsetColumnName());
                }
                LOG.debug("Poll fetched {} rows, current offset: {}", rowCount, currentOffset);
            }
        }
    }

    private String buildPollQuery() {
        DatabaseDialect dialect = config.getDialect();
        String table = dialect.quoteIdentifier(config.getTableName());
        String offsetCol = dialect.quoteIdentifier(config.getOffsetColumnName());

        if (currentOffset == null) {
            return "SELECT * FROM " + table + " ORDER BY " + offsetCol;
        } else {
            return "SELECT * FROM " + table + " WHERE " + offsetCol + " > ? ORDER BY " + offsetCol;
        }
    }

    private void setOffsetParameter(PreparedStatement stmt, int index, OffsetType offset) throws SQLException {
        switch (offset) {
            case OffsetType.IntegerOffset i -> stmt.setInt(index, i.value());
            case OffsetType.TimestampOffset ts -> stmt.setTimestamp(index, ts.value());
        }
    }

    private OffsetType readOffset(ResultSet rs, String columnName) throws SQLException {
        Object value = rs.getObject(columnName);
        return switch (value) {
            case Integer i -> new OffsetType.IntegerOffset(i);
            case Timestamp ts -> new OffsetType.TimestampOffset(ts);
            case null -> throw new SQLException("Offset column '" + columnName + "' returned null");
            default -> throw new SQLException("Unsupported offset type: " + value.getClass().getName());
        };
    }

    private Row mapRow(ResultSet rs) throws SQLException {
        Row row = new Row(columnCount);
        for (int i = 0; i < columnCount; i++) {
            row.setField(i, mapColumn(rs, i + 1, columnTypes[i]));
        }
        return row;
    }

    private Object mapColumn(ResultSet rs, int columnIndex, int sqlType) throws SQLException {
        return switch (sqlType) {
            case Types.INTEGER -> {
                int v = rs.getInt(columnIndex);
                yield rs.wasNull() ? null : v;
            }
            case Types.FLOAT -> {
                float v = rs.getFloat(columnIndex);
                yield rs.wasNull() ? null : v;
            }
            case Types.VARCHAR -> rs.getString(columnIndex);
            case Types.TIMESTAMP -> rs.getTimestamp(columnIndex);
            default -> throw new SQLException("Unsupported column type: " + sqlType);
        };
    }

    private void ensureConnection() throws SQLException {
        if (connection != null && connection.isValid(CONNECTION_VALID_TIMEOUT_SECONDS)) {
            return;
        }

        if (connection != null) {
            LOG.warn("Existing connection is no longer valid, reconnecting");
            try { connection.close(); } catch (SQLException ignored) {}
        }

        SQLException lastException = null;
        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                connection = DriverManager.getConnection(
                        config.getJdbcUrl(), config.getUsername(), config.getPassword());
                LOG.info("Database connection established");
                return;
            } catch (SQLException e) {
                lastException = e;
                LOG.warn("Connection attempt {}/{} failed: {}", attempt, MAX_RETRY_ATTEMPTS, e.getMessage());
                if (attempt < MAX_RETRY_ATTEMPTS) {
                    try { Thread.sleep(RETRY_DELAY_MS); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                }
            }
        }
        throw new SQLException("Failed to connect after " + MAX_RETRY_ATTEMPTS + " attempts", lastException);
    }

    @Override
    public void addSplits(List<PollingJdbcSplit> splits) {
        if (!splits.isEmpty()) {
            PollingJdbcSplit split = splits.getFirst();
            this.splitAssigned = true;

            OffsetType checkpointOffset = split.currentOffset();
            if (checkpointOffset != null) {
                this.currentOffset = checkpointOffset;
            } else if (config.getStaticStartingOffset() != null) {
                this.currentOffset = config.getStaticStartingOffset();
            }

            try {
                ensureConnection();
                StartupValidator.validate(connection, config);
                LOG.info("Startup validation passed for table '{}', offset column '{}'",
                        config.getTableName(), config.getOffsetColumnName());
            } catch (SQLException e) {
                closeConnectionQuietly();
                throw new RuntimeException("Startup validation failed", e);
            }

            availabilityFuture.complete(null);
        }
    }

    private void closeConnectionQuietly() {
        if (connection != null) {
            try { connection.close(); } catch (SQLException ignored) {}
            connection = null;
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        // Single-split design — no action needed
    }

    @Override
    public List<PollingJdbcSplit> snapshotState(long checkpointId) {
        return Collections.singletonList(new PollingJdbcSplit(currentOffset));
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return availabilityFuture;
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            LOG.info("Database connection closed");
        }
    }
}
