package com.polling.jdbc.connector;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.*;

class SerializerTest {

    @Test
    void splitSerializerRoundTripWithIntOffset() throws IOException {
        var serializer = new PollingJdbcSplitSerializer();
        var split = new PollingJdbcSplit(new OffsetType.IntegerOffset(42));

        byte[] bytes = serializer.serialize(split);
        var deserialized = serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(new OffsetType.IntegerOffset(42), deserialized.currentOffset());
    }

    @Test
    void splitSerializerRoundTripWithTimestampOffset() throws IOException {
        var serializer = new PollingJdbcSplitSerializer();
        var ts = new Timestamp(System.currentTimeMillis());
        var split = new PollingJdbcSplit(new OffsetType.TimestampOffset(ts));

        byte[] bytes = serializer.serialize(split);
        var deserialized = serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(new OffsetType.TimestampOffset(ts), deserialized.currentOffset());
    }

    @Test
    void splitSerializerRoundTripWithNullOffset() throws IOException {
        var serializer = new PollingJdbcSplitSerializer();
        var split = new PollingJdbcSplit(null);

        byte[] bytes = serializer.serialize(split);
        var deserialized = serializer.deserialize(serializer.getVersion(), bytes);

        assertNull(deserialized.currentOffset());
    }

    @Test
    void enumeratorStateSerializerRoundTrip() throws IOException {
        var serializer = new PollingJdbcEnumeratorStateSerializer();

        var stateTrue = new PollingJdbcEnumeratorState(true);
        byte[] bytes = serializer.serialize(stateTrue);
        var deserialized = serializer.deserialize(serializer.getVersion(), bytes);
        assertTrue(deserialized.splitAssigned());

        var stateFalse = new PollingJdbcEnumeratorState(false);
        bytes = serializer.serialize(stateFalse);
        deserialized = serializer.deserialize(serializer.getVersion(), bytes);
        assertFalse(deserialized.splitAssigned());
    }
}
