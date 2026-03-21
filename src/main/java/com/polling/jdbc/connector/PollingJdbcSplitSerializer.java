package com.polling.jdbc.connector;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.sql.Timestamp;

public class PollingJdbcSplitSerializer implements SimpleVersionedSerializer<PollingJdbcSplit> {

    private static final int VERSION = 1;

    private static final byte TYPE_NULL = 0;
    private static final byte TYPE_INT = 1;
    private static final byte TYPE_TIMESTAMP = 2;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(PollingJdbcSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            OffsetType offset = split.currentOffset();
            switch (offset) {
                case null -> dos.writeByte(TYPE_NULL);
                case OffsetType.IntegerOffset i -> { dos.writeByte(TYPE_INT); dos.writeInt(i.value()); }
                case OffsetType.TimestampOffset ts -> { dos.writeByte(TYPE_TIMESTAMP); dos.writeLong(ts.value().getTime()); }
            }
            dos.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public PollingJdbcSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream dis = new DataInputStream(bais)) {
            byte type = dis.readByte();
            OffsetType offset = switch (type) {
                case TYPE_NULL -> null;
                case TYPE_INT -> new OffsetType.IntegerOffset(dis.readInt());
                case TYPE_TIMESTAMP -> new OffsetType.TimestampOffset(new Timestamp(dis.readLong()));
                default -> throw new IOException("Unknown offset type marker: " + type);
            };
            return new PollingJdbcSplit(offset);
        }
    }
}
