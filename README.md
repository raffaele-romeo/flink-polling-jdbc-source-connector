# Systems design - Hybrid Edge/Cloud Data Pipelines

The system design solution can be found in the `cloud-data-pipelines.excalidraw` file.

# Flink Polling JDBC Source Connector

A custom Apache Flink source connector that polls a relational database table for new rows and emits them into an unbounded stream.

## What it does

- Periodically queries a database table at a configurable polling interval
- Detects new rows using a monotonically increasing column (auto-increment ID or timestamp)
- Emits rows as Flink `Row` objects into a streaming pipeline
- Supports **MySQL** and **PostgreSQL**
- Supports column types: `Int`, `String`, `Float`, `Timestamp`
- Validates the offset column exists and has a supported type at startup
- Integrates with Flink's checkpoint mechanism for fault-tolerant offset recovery
- Supports a static starting offset for manual recovery

## Usage

```java
var config = new PollingJdbcSourceConfig(
    "jdbc:postgresql://localhost:5432/mydb",
    DatabaseDialect.POSTGRESQL,
    "orders",       // table name
    "id",           // offset tracking column
    5000            // polling interval in ms
);
config.setUsername("user");
config.setPassword("pass");
config.setStaticStartingOffset(1000); // optional: start from id > 1000

var source = new PollingJdbcSource(config);

var env = StreamExecutionEnvironment.getExecutionEnvironment();
env.fromSource(source, WatermarkStrategy.noWatermarks(), "polling-jdbc-source")
   .print();
env.execute();
```

## How to run the tests

```bash
./gradlew test
```

With console output:

```bash
./gradlew test --info
```

Run a specific test class:

```bash
./gradlew test --tests "com.polling.jdbc.connector.PollingJdbcSourceReaderTest"
```

## Offset recovery strategy

The connector resolves its starting offset in this order:

1. **Checkpoint offset** — If Flink is restoring from a checkpoint/savepoint
2. **Static offset** — If configured via `setStaticStartingOffset()`
3. **No offset** — Reads all existing rows on first poll

## TODO

- Add integration tests for MySQL and PostgreSQL databases
