package com.polling.jdbc.connector;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PollingJdbcSourceConfigTest {

    @Test
    void validConfigCreatesSuccessfully() {
        var config = PollingJdbcSourceConfig.builder(
                "jdbc:h2:mem:test", DatabaseDialect.POSTGRESQL, "orders", "id", 5000).build();
        assertEquals("orders", config.getTableName());
        assertEquals("id", config.getOffsetColumnName());
        assertEquals(5000, config.getPollingIntervalMs());
    }

    @Test
    void nullJdbcUrlThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                PollingJdbcSourceConfig.builder(null, DatabaseDialect.MYSQL, "t", "id", 1000));
    }

    @Test
    void nullDialectThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                PollingJdbcSourceConfig.builder("jdbc:h2:mem:test", null, "t", "id", 1000));
    }

    @Test
    void emptyTableNameThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                PollingJdbcSourceConfig.builder("jdbc:h2:mem:test", DatabaseDialect.MYSQL, "", "id", 1000));
    }

    @Test
    void nullOffsetColumnThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                PollingJdbcSourceConfig.builder("jdbc:h2:mem:test", DatabaseDialect.MYSQL, "t", null, 1000));
    }

    @Test
    void zeroPollingIntervalThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                PollingJdbcSourceConfig.builder("jdbc:h2:mem:test", DatabaseDialect.MYSQL, "t", "id", 0));
    }
}
