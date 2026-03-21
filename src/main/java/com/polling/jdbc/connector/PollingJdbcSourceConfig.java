package com.polling.jdbc.connector;

import java.io.Serializable;

public final class PollingJdbcSourceConfig implements Serializable {

    private final String jdbcUrl;
    private final DatabaseDialect dialect;
    private final String tableName;
    private final String offsetColumnName;
    private final long pollingIntervalMs;
    private final String username;
    private final String password;
    private final OffsetType staticStartingOffset;

    private PollingJdbcSourceConfig(Builder builder) {
        this.jdbcUrl = builder.jdbcUrl;
        this.dialect = builder.dialect;
        this.tableName = builder.tableName;
        this.offsetColumnName = builder.offsetColumnName;
        this.pollingIntervalMs = builder.pollingIntervalMs;
        this.username = builder.username;
        this.password = builder.password;
        this.staticStartingOffset = builder.staticStartingOffset;
    }

    public String getJdbcUrl() { return jdbcUrl; }
    public DatabaseDialect getDialect() { return dialect; }
    public String getTableName() { return tableName; }
    public String getOffsetColumnName() { return offsetColumnName; }
    public long getPollingIntervalMs() { return pollingIntervalMs; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public OffsetType getStaticStartingOffset() { return staticStartingOffset; }

    public static Builder builder(String jdbcUrl, DatabaseDialect dialect,
                                  String tableName, String offsetColumnName, long pollingIntervalMs) {
        return new Builder(jdbcUrl, dialect, tableName, offsetColumnName, pollingIntervalMs);
    }

    public static final class Builder {
        private final String jdbcUrl;
        private final DatabaseDialect dialect;
        private final String tableName;
        private final String offsetColumnName;
        private final long pollingIntervalMs;

        private String username;
        private String password;
        private OffsetType staticStartingOffset;

        private Builder(String jdbcUrl, DatabaseDialect dialect,
                        String tableName, String offsetColumnName, long pollingIntervalMs) {
            if (jdbcUrl == null || jdbcUrl.isBlank()) {
                throw new IllegalArgumentException("JDBC URL must not be null or empty");
            }
            if (dialect == null) {
                throw new IllegalArgumentException("Database dialect must not be null");
            }
            if (tableName == null || tableName.isBlank()) {
                throw new IllegalArgumentException("Table name must not be null or empty");
            }
            if (offsetColumnName == null || offsetColumnName.isBlank()) {
                throw new IllegalArgumentException("Offset column name must not be null or empty");
            }
            if (pollingIntervalMs <= 0) {
                throw new IllegalArgumentException("Polling interval must be positive");
            }

            this.jdbcUrl = jdbcUrl;
            this.dialect = dialect;
            this.tableName = tableName;
            this.offsetColumnName = offsetColumnName;
            this.pollingIntervalMs = pollingIntervalMs;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder staticStartingOffset(OffsetType staticStartingOffset) {
            this.staticStartingOffset = staticStartingOffset;
            return this;
        }

        public PollingJdbcSourceConfig build() {
            return new PollingJdbcSourceConfig(this);
        }
    }
}
