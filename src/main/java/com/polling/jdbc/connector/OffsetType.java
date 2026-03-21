package com.polling.jdbc.connector;

import java.io.Serializable;
import java.sql.Timestamp;

public sealed interface OffsetType extends Serializable {

    record IntegerOffset(int value) implements OffsetType {}

    record TimestampOffset(Timestamp value) implements OffsetType {}
}
