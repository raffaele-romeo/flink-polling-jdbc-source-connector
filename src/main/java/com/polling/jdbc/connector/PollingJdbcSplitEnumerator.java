package com.polling.jdbc.connector;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class PollingJdbcSplitEnumerator implements SplitEnumerator<PollingJdbcSplit, PollingJdbcEnumeratorState> {

    private final SplitEnumeratorContext<PollingJdbcSplit> context;
    private boolean splitAssigned;

    public PollingJdbcSplitEnumerator(SplitEnumeratorContext<PollingJdbcSplit> context,
                                      PollingJdbcEnumeratorState restoredState) {
        this.context = context;
        this.splitAssigned = restoredState != null && restoredState.splitAssigned();
    }

    @Override
    public void start() {
        // Nothing to do — splits are assigned when readers register
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // Not used — we proactively assign in addReader
    }

    @Override
    public void addSplitsBack(List<PollingJdbcSplit> splits, int subtaskId) {
        // Split returned from a failed reader — mark as unassigned so it can be reassigned
        if (!splits.isEmpty()) {
            splitAssigned = false;
        }
    }

    @Override
    public void addReader(int subtaskId) {
        if (!splitAssigned) {
            context.assignSplit(new PollingJdbcSplit(null), subtaskId);
            context.signalNoMoreSplits(subtaskId);
            splitAssigned = true;
        }
    }

    @Override
    public PollingJdbcEnumeratorState snapshotState(long checkpointId) throws Exception {
        return new PollingJdbcEnumeratorState(splitAssigned);
    }

    @Override
    public void close() throws IOException {
        // Nothing to close
    }
}
