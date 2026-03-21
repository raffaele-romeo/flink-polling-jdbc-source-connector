package com.polling.jdbc.connector;

public enum DatabaseDialect {
    MYSQL("com.mysql.cj.jdbc.Driver") {
        @Override
        public String quoteIdentifier(String identifier) {
            return "`" + identifier + "`";
        }
    },
    POSTGRESQL("org.postgresql.Driver") {
        @Override
        public String quoteIdentifier(String identifier) {
            return "\"" + identifier + "\"";
        }
    };

    private final String driverClassName;

    DatabaseDialect(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public abstract String quoteIdentifier(String identifier);
}
