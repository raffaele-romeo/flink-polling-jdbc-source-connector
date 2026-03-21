package com.polling.jdbc.connector;

import org.apache.flink.api.connector.source.SourceSplit;

public record PollingJdbcSplit(OffsetType currentOffset) implements SourceSplit {

    private static final String SPLIT_ID = "polling-jdbc-split-0";

    @Override
    public String splitId() {
        return SPLIT_ID;
    }
}
