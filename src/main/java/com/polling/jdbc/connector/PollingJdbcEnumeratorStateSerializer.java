package com.polling.jdbc.connector;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class PollingJdbcEnumeratorStateSerializer implements SimpleVersionedSerializer<PollingJdbcEnumeratorState> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(PollingJdbcEnumeratorState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeBoolean(state.splitAssigned());
            dos.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public PollingJdbcEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream dis = new DataInputStream(bais)) {
            boolean splitAssigned = dis.readBoolean();
            return new PollingJdbcEnumeratorState(splitAssigned);
        }
    }
}
