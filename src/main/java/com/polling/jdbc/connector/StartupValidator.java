package com.polling.jdbc.connector;

import java.sql.*;
import java.util.Set;

public class StartupValidator {

    private static final Set<Integer> SUPPORTED_SQL_TYPES = Set.of(
            Types.INTEGER,
            Types.VARCHAR,
            Types.FLOAT,
            Types.TIMESTAMP
    );

    public static void validate(Connection connection, PollingJdbcSourceConfig config) throws SQLException {
        String tableName = config.getTableName();
        String offsetColumnName = config.getOffsetColumnName();

        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet columns = metaData.getColumns(null, null, tableName, offsetColumnName)) {
            if (!columns.next()) {
                try (ResultSet upperColumns = metaData.getColumns(null, null,
                        tableName.toUpperCase(), offsetColumnName.toUpperCase())) {
                    if (!upperColumns.next()) {
                        throw new IllegalStateException(
                                "Offset column '" + offsetColumnName + "' not found in table '" + tableName + "'");
                    }
                    checkColumnType(upperColumns, offsetColumnName, tableName);
                    return;
                }
            }
            checkColumnType(columns, offsetColumnName, tableName);
        }
    }

    private static void checkColumnType(ResultSet columns, String offsetColumnName, String tableName) throws SQLException {
        int sqlType = columns.getInt("DATA_TYPE");
        if (!SUPPORTED_SQL_TYPES.contains(sqlType)) {
            String typeName = columns.getString("TYPE_NAME");
            throw new IllegalStateException(
                    "Offset column '" + offsetColumnName + "' in table '" + tableName +
                    "' has unsupported type '" + typeName + "'. " +
                    "Supported types: INT, VARCHAR, FLOAT, TIMESTAMP");
        }
    }
}
