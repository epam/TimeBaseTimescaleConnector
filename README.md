# TimescaleDB Connector

Use our [proprietary open source connector](https://github.com/epam/TimeBaseTimescaleConnector) to replicate TimeBase streams to [Timescale](https://www.timescale.com/) database.

[Timescale](https://www.timescale.com/) is a relational database for time-series data, built as a [PostgreSQL](https://www.postgresql.org/) extension with full support of SQL features. Refer to [Timescale Documentation](https://docs.timescale.com/) to learn more.

TimeBase being a time-series database stores time-series events as [Messages](messages.html) and each type of event has a personal message class assigned to it in TimeBase. Message class has a set of fields (attributes) that characterize, describe, identify each specific type of event. In object-oriented programing languages messages can be seen as classes, each with a specific set of fields. Messages are stored in [Streams](streams.html) chronologically by their timestamps for each symbol. Refer to a [Basic Concepts](basic_concepts.html) page to learn more about TimeBase main principles and data structure.

To replicate TimeBase stream data to Timescale, we take fields, objects and classes from a particular TimeBase stream and *unfold* them so each field corresponds to a particular Timescale table column. In case of an `ARRAY` of objects, data is inserted in a Timescale table as a JSON object, that contains all array elements and their fields. `EventTime`, `Id` and `Symbol` are auto generated and common for all Timescale tables where `EventTime` + `Id` = `PrimaryKey`. `EventTime` is mapped on a TimeBase message timestamp, `Symbol` on a TimeBase message symbol, `id` is an auto generated sequence by PostrgeSQL. Timescale tables are named after TimeBase stream names. Tables rows are created for each TimeBase message in a chronological order. Data is replicated in batches (`TIMEBASE_BATCH_SIZE` parameter if the [application config]() to set the number of messages in one batch). 

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

Use `migrations_tracker` table to track replication process. This tables contains metadata about all the replicated streams, replication timestamps and the replication statuses.

## Examples 

### Message with a polymorphic array

Let's take a look at a **simplified** example. In this example we will show how a message with fixed-type fields and a polymorphic array is transformed into a Timescale table. 

![](/src/img/message_example2.png/)

For the example, we take a message with two fixed-type fields `Symbol` and `Timestamp`, and a polymorphic array with two types of entries (classes) `Trade` and `BBO`, each having a specific set of fields - shown on the illustration above. 

Such data structure can be transformed to a Timescale table the following way: 

<table>
  <tbody>
    <tr>
      <td>EventTime</td>
      <td>Id</td>
      <td>Symbol</td>
      <td>Array</td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>SERIAL</td>
      <td>VARCHAR</td>
      <td>JSON</td>
    </tr>
  </tbody>
</table>

Here is how it may look with real message data: 

<table>
  <tbody>
    <tr>
      <td>EventTime</td>
      <td>Id</td>
      <td>Symbol</td>
      <td>Array</td>
    </tr>
    <tr>
      <td>2021-08-25
      T07:00:00.025Z</td>
      <td>123</td>
      <td>btcusd</td>
      <td>
      "array": [
            {
                "$type": "trade",
                "Price": "645.23",
                "Size": "0.81551393",
                "Exchange": "SKRAKEN"
            },
            {
                "$type": "bbo",
                "AskPrice": "645.23",
                "AskSize": "0.81551393",
                "Exchange": "SKRAKEN"
            }
        ]
      </td>
    </tr>
  </tbody>
</table>

### Message with a nested object

Let's now take an abstract message with a nested object.

![](/src/img/message_example3.png/)

<table>
  <tbody>
    <tr>
      <td>EventTime</td>
      <td>Id</td>
      <td>Symbol</td>
      <td>trade_price</td>
      <td>trade_size</td>
      <td>trade_exchange</td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>SERIAL</td>
      <td>VARCHAR</td>
      <td>DECIMAL64</td>
      <td>DECIMAL64</td>
      <td>VARCHAR</td>
    </tr>
  </tbody>
</table>

Here is how it may look with real message data: 

<table>
  <tbody>
    <tr>
      <td>EventTime</td>
      <td>Id</td>
      <td>Symbol</td>
      <td>trade_price</td>
      <td>trade_size</td>
      <td>trade_exchange</td>
    </tr>
    <tr>
      <td>2021-08-25
      T07:00:00.025Z</td>
      <td>123</td>
      <td>btcusd</td>
      <td>645.23</td>
      <td>0.81551393</td>
      <td>SKRAKEN</td>
    </tr>
  </tbody>
</table>


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

1. Run TimeBase<br>
```bash
# start TimeBase Community Edition
docker run --rm -d \
  -p 8011:8011 \
  --name=timebase-server \
  --ulimit nofile=65536:65536 \
  finos/timebase-ce-server:latest
```
2. Run replicator in [Docker](https://github.com/epam/TimeBaseTimescaleConnector/blob/main/timescaledb-connector/Dockerfile) or directly via `java -jar`

* Refer to [TimeBase Quick Start](quick-start.html) to learn more about starting TimeBase.
* Refer to [Replicator GitHub Repository](https://github.com/epam/TimeBaseTimescaleConnector/blob/main/timescaledb-connector/Dockerfile) to learn more about it's deployment. 

## Configuration

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

[Connector configuration example](https://github.com/epam/TimeBaseTimescaleConnector/blob/main/timescaledb-connector/src/main/resources/application.yaml).

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

## Data Type Mappings

* Refer to [PostgreSQL Documentation](https://www.postgresql.org/docs/9.5/datatype.html) to learn more about data types supported by Timescale.
* Refer to [Data Types](data_types.html) to learn more about data types supported by TimeBase.

|TimeBase Type/Encoding|Timescale Type|
|-------------|---------------|
|INTEGER/SIGNED (8)|INTEGER|
|INTEGER/SIGNED (16)|INTEGER|
|INTEGER/SIGNED (32)|INTEGER|
|INTEGER/SIGNED (64)|BIGINT|
|ENUM|VARCHAR|
|BINARY|BYTEA|
|BOOLEAN|BOOLEAN|
|CHAR|CHAR|
|VARCHAR|VARCHAR|
|FLOAT/DECIMAL64|DECIMAL(36, 18)|
|FLOAT|DECIMAL|
|ARRAY|JSON|
|TIMESTAMP|TIMESTAMP|
|TIMEOFDAY|TIME|