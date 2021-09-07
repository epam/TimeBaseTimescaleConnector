# TimescaleDB Connector

## Overview

Use our [open source TimescaleDB connector](https://github.com/epam/TimeBaseTimescaleConnector) to replicate [TimeBase](https://kb.timebase.info/) streams to [Timescale](https://www.timescale.com/) database.

[TimeBase Community Edition Repository](https://github.com/finos/TimeBase-CE)

TimeBase being a time-series database stores time-series events as [Messages](https://kb.timebase.info/messages.html) and each type of event has a personal message class assigned to it in TimeBase. Message class has a set of fields (attributes) that characterize, describe, identify each specific type of event. In object-oriented programing languages messages can be seen as classes, each with a specific set of fields. Messages are stored in [Streams](https://kb.timebase.info/streams.html) chronologically by their timestamps for each symbol. Refer to a [Basic Concepts](https://kb.timebase.info/basic_concepts.html) page to learn more about TimeBase main principles and data structure.

To replicate TimeBase stream data to Timescale, we take fields, objects and classes from a particular TimeBase stream and *unfold* them so each field corresponds to a particular Timescale table column. In case of an `ARRAY` of objects, data is inserted in a Timescale table as a JSON object, that contains all array elements and their fields. `EventTime`, `Id` and `Symbol` are auto generated and common for all Timescale tables where `EventTime` + `Id` = `PrimaryKey`. `EventTime` is mapped on a TimeBase message timestamp, `Symbol` on a TimeBase message symbol, `id` is an auto generated sequence by PostrgeSQL. Timescale tables are named after TimeBase stream names. Tables rows are created for each TimeBase message in a chronological order.

**The column naming convention:**

* column names for fixed-type objects' fields are named after the particular fields as is: for example `Symbol`.
* column names for nested objects' fields follow this pattern: `nested-object_field-name`, for example `trade_size`.

## Data Type Mappings

* Refer to [PostgreSQL Documentation](https://www.postgresql.org/docs/9.5/datatype.html) to learn more about data types supported by Timescale.
* Refer to [Data Types](https://kb.timebase.info/data_types.html) to learn more about data types supported by TimeBase.

|TimeBase Type/Encoding|Timescale Type|
|-------------|---------------|
|INTEGER/SIGNED (8-32)|INTEGER|
|INTEGER/SIGNED (64)|BIGINT|
|ENUM|VARCHAR|
|VARCHAR|VARCHAR|
|CHAR|CHAR|
|BINARY|BYTEA|
|BOOLEAN|BOOLEAN|
|FLOAT/DECIMAL64|DECIMAL(36, 18)|
|FLOAT|DECIMAL|
|ARRAY|JSON|
|TIMESTAMP|TIMESTAMP|
|TIMEOFDAY|TIME|


## Examples 

Let's take a look at a **simplified** example. In this example we will show how a message with a polymorphic array is transformed into a Timescale table. 

Refer to [Example](https://github.com/epam/TimeBaseTimescaleConnector/tree/main/example) to view a step-by-step instruction on how to run this demo example and try the replication in action. 

```sql
/*TimeBase stream schema*/

DURABLE STREAM "timescale_stream" (
    CLASS "trade" (
        "price" '' FLOAT DECIMAL64,
        "size" '' FLOAT DECIMAL64
    );
    CLASS "bbo" (
        "askPrice" '' FLOAT DECIMAL64,
        "askSize" '' FLOAT DECIMAL64,
        "bidPrice" '' FLOAT DECIMAL64,
        "bidSize" '' FLOAT DECIMAL64
    );
    CLASS "Message" (
        "entries" ARRAY(OBJECT("trade", "bbo") NOT NULL)
    );
)
OPTIONS (POLYMORPHIC; PERIODICITY = 'IRREGULAR'; HIGHAVAILABILITY = FALSE)
```

The Timescale table will have the following structure:

```bash
                                           Table "public.timescale_stream"
     Column      |            Type             | Collation | Nullable |              Default
-----------------+-----------------------------+-----------+----------+------------------------------------
 entries         | json                        |           |          |
 descriptor_name | character varying           |           |          |
 id              | integer                     |           | not null | nextval('stream_id_seq'::regclass)
 eventtime       | timestamp without time zone |           | not null |
 symbol          | character varying           |           |          |
Indexes:
    "stream_pkey" PRIMARY KEY, btree (id, eventtime)
    "stream_eventtime_idx" btree (eventtime DESC)
Triggers:
    ts_insert_blocker BEFORE INSERT ON stream FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker()
```

```sql
CREATE TABLE public.timescale_stream (
    entries json,
    descriptor_name character varying,
    id integer NOT NULL,
    eventtime timestamp without time zone NOT NULL,
    symbol character varying
);
```

Here we can see, that each field has been parsed in a separate table colum with an appropriate data type mapping. Entries array has been inserted as a JSON string. 

## Failover Support 

```yaml
...

replicator:
  retryAttempts: ${RETRY_ATTEMPTS:5}
...

```

`RETRY_ATTEMPT` is an important Timescale configuration parameter. Use it to set a number of replication retry attempts in case there has been any interruptions in the replication process or connection failures. With each retry the last ms data is deleted and the replication resumes from this timestamp.

The flow is as follows: 

1. Locate the `MAX` timestamp - a suspected crush point.<br>
```sql
SELECT MAX(EventTime) AS EventTime FROM table_name
```
2. Delete data with this timestamp.<br>
```sql
DELETE FROM table_name WHERE EventTime = max_time
```
3. Continue replication from the `MAX` timestamp.

## Schema Consistency

Timescale replicator can monitor changes made to the source TimeBase **stream schema** and propagate those to the target database, thus ensuring the target database schema is always consistent with the data source: 

1. In case the source stream schema has been changed
2. we take the list of changes, prepare queries,
3. and execute them to update the target database. 

## Deployment

Refer to [Example](https://github.com/epam/TimeBaseTimescaleConnector/tree/main/example) to learn more about starting the replication.

## Configuration

Supply and configure this [configuration file](https://github.com/epam/TimeBaseTimescaleConnector/blob/main/timescaledb-connector/src/main/resources/application.yaml) with your replicator. 

```yaml
# Configuration file example with default parameters' values. 

spring:
  datasource:
    url: jdbc:postgresql://${POSTGRES_HOST:localhost}:${POSTGRES_PORT:5432}/${POSTGRES_DATABASE:postgres}?reWriteBatchedInserts=true
    username: ${POSTGRES_USERNAME:postgres}
    password: ${POSTGRES_PASSWORD:password}
    hikari:
      minimum-idle: ${POSTGRES_MIN_IDLE:8}
      maximum-pool-size: ${POSTGRES_MAX_POOL_SIZE:16}
      driver-class-name: org.postgresql.Driver

timebase:
  url: dxtick://${TIMEBASE_HOST:localhost}:${TIMEBASE_PORT:8011}
  batchSize: ${TIMEBASE_BATCH_SIZE:5000}
  streams: ${TIMEBASE_STREAMS_FOR_REPLICATION:orders, four_hours}
  autoDiscovery: ${TIMEBASE_AUTO_DISCOVERY:false}

replicator:
  retryAttempts: ${RETRY_ATTEMPTS:5}

logging:
  level:
    root: ${ROOT_LOG_LEVEL:INFO}
    deltix:
      timebase:
        connector: ${APP_LOG_LEVEL:INFO}
```

**Timescale Configurations:**

* `POSTGRES_HOST` - Timescale host name.
* `POSTGRES_DATABASE` - Timescale database.
* `POSTGRES_MIN_IDLE` - min number of connections that is not going to be terminated.
* `POSTGRES_MAX_POOL_SIZE` - max number of connections in a pool.

**TimeBase Configurations:**

* `TIMEBASE_HOST` - TimeBase host.
* `TIMEBASE_PORT` - TimeBase port.
* `TIMEBASE_BATCH_SIZE` - number of messages in one batch.
* `TIMEBASE_STREAMS_FOR_REPLICATION` - comma-separated list of stream names that will be replicated. 
* `TIMEBASE_AUTO_DISCOVERY` - flag that enables/disables the automated discovery of streams to be replicated. 

**Replicator Configurations:**

* `RETRY_ATTEMPT` - number of replication retry attempts in case there has been any interruptions in the replication process or connection failures. With each retry the last ms data is deleted and the replication resumes from this timestamp.

**Logging Configurations:**

* `ROOT_LOG_LEVEL` - logs root level (TRACE, DEBUG, INFO, WARN, ERROR, FATAL).
* `APP_LOG_LEVEL` - logging level.

## Known Limitations

* Timescale replicator does not currently support the replication of primitives' arrays. 
* Timescale replicator does not currently support stream TRUNCATE, PURGE, DELETE commands. 

## Replication Tracker

Once you start the replication, we automatically create a system table called `migrations_tracker` with metadata about all the replicated streams, replication timestamps and the replication statuses. You can use this table to track your replication statistics. 

```bash
                                            Table "public.migrations_tracker"
      Column       |            Type             | Collation | Nullable |                    Default                     
-------------------+-----------------------------+-----------+----------+------------------------------------------------
 id                | integer                     |           | not null | nextval('migrations_tracker_id_seq'::regclass)
 stream            | character varying           |           |          | 
 version           | bigint                      |           |          | 
 issuccess         | boolean                     |           |          | 
 migrationdatetime | timestamp without time zone |           |          | 
Indexes:
    "migrations_tracker_pkey" PRIMARY KEY, btree (id)
```