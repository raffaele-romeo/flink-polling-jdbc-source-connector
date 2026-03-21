package com.polling.jdbc.connector;

import java.io.Serializable;

public record PollingJdbcEnumeratorState(boolean splitAssigned) implements Serializable {
}
