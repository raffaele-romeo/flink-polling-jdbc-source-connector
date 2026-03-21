package com.polling.jdbc.connector;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

public class PollingJdbcSource implements Source<Row, PollingJdbcSplit, PollingJdbcEnumeratorState> {

    private final PollingJdbcSourceConfig config;

    public PollingJdbcSource(PollingJdbcSourceConfig config) {
        this.config = config;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<PollingJdbcSplit, PollingJdbcEnumeratorState> createEnumerator(
            SplitEnumeratorContext<PollingJdbcSplit> enumContext) {
        return new PollingJdbcSplitEnumerator(enumContext, null);
    }

    @Override
    public SplitEnumerator<PollingJdbcSplit, PollingJdbcEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<PollingJdbcSplit> enumContext,
            PollingJdbcEnumeratorState checkpoint) {
        return new PollingJdbcSplitEnumerator(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<PollingJdbcSplit> getSplitSerializer() {
        return new PollingJdbcSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<PollingJdbcEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new PollingJdbcEnumeratorStateSerializer();
    }

    @Override
    public SourceReader<Row, PollingJdbcSplit> createReader(SourceReaderContext readerContext) {
        return new PollingJdbcSourceReader(config);
    }
}
