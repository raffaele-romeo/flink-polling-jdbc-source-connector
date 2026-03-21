package com.polling.jdbc.connector;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

class StartupValidatorTest {

    private Connection connection;

    @BeforeEach
    void setUp() throws Exception {
        connection = DriverManager.getConnection("jdbc:h2:mem:validatortest;DB_CLOSE_DELAY=-1");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE orders (id INT PRIMARY KEY, name VARCHAR(100), price FLOAT, created_at TIMESTAMP)");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS orders");
        }
        connection.close();
    }

    @Test
    void validOffsetColumnPasses() {
        var config = PollingJdbcSourceConfig.builder(
                "jdbc:h2:mem:validatortest", DatabaseDialect.POSTGRESQL, "ORDERS", "ID", 1000).build();
        assertDoesNotThrow(() -> StartupValidator.validate(connection, config));
    }

    @Test
    void missingOffsetColumnThrows() {
        var config = PollingJdbcSourceConfig.builder(
                "jdbc:h2:mem:validatortest", DatabaseDialect.POSTGRESQL, "ORDERS", "nonexistent", 1000).build();
        var ex = assertThrows(IllegalStateException.class,
                () -> StartupValidator.validate(connection, config));
        assertTrue(ex.getMessage().contains("not found"));
    }

    @Test
    void unsupportedColumnTypeThrows() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE blobtable (id INT, data BLOB)");
        }
        var config = PollingJdbcSourceConfig.builder(
                "jdbc:h2:mem:validatortest", DatabaseDialect.POSTGRESQL, "BLOBTABLE", "DATA", 1000).build();
        var ex = assertThrows(IllegalStateException.class,
                () -> StartupValidator.validate(connection, config));
        assertTrue(ex.getMessage().contains("unsupported type"));

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE blobtable");
        }
    }
}
