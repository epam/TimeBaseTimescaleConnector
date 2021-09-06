# TimescaleDB Connector

## Overview

Use our [proprietary open source connector](https://github.com/epam/TimeBaseTimescaleConnector) to replicate TimeBase streams to [Timescale](https://www.timescale.com/) database.

[Timescale](https://www.timescale.com/) is a relational database for time-series data, built as a [PostgreSQL](https://www.postgresql.org/) extension with full support of SQL features. Refer to [Timescale Documentation](https://docs.timescale.com/) to learn more.

TimeBase being a time-series database stores time-series events as [Messages](https://kb.timebase.info/messages.html) and each type of event has a personal message class assigned to it in TimeBase. Message class has a set of fields (attributes) that characterize, describe, identify each specific type of event. In object-oriented programing languages messages can be seen as classes, each with a specific set of fields. Messages are stored in [Streams](https://kb.timebase.info/streams.html) chronologically by their timestamps for each symbol. Refer to a [Basic Concepts](https://kb.timebase.info/basic_concepts.html) page to learn more about TimeBase main principles and data structure.

To replicate TimeBase stream data to Timescale, we take fields, objects and classes from a particular TimeBase stream and *unfold* them so each field corresponds to a particular Timescale table column. In case of an `ARRAY` of objects, data is inserted in a Timescale table as a JSON object, that contains all array elements and their fields. `EventTime`, `Id` and `Symbol` are auto generated and common for all Timescale tables where `EventTime` + `Id` = `PrimaryKey`. `EventTime` is mapped on a TimeBase message timestamp, `Symbol` on a TimeBase message symbol, `id` is an auto generated sequence by PostrgeSQL. Timescale tables are named after TimeBase stream names. Tables rows are created for each TimeBase message in a chronological order. Data is replicated in batches (`TIMEBASE_BATCH_SIZE` parameter if the application config to set the number of messages in one batch). 

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

![](/timescaledb-connector/src/img/message_example2.png/)

For the example, we take a message with two fixed-type fields `Symbol` and `Timestamp`, and a polymorphic array with two types of entries (classes) `Trade` and `BBO`, each having a specific set of fields - shown on the illustration above. 

Such data structure can be transformed to a Timescale table the following way: 

|EventTime|Id|Symbol|Array|
|---------|--|------|-----|
|TIMESTAMP|SERIAL|VARCHAR|JSON|

Here is how it may look with real message data: 

|EventTime|Id|Symbol|Array|
|---------|--|------|-----|
|2021-08-25T07:00:00.025Z|123|btcusd|"array": [{"$type": "trade","Price": "645.23","Size": "0.81551393","Exchange": "SKRAKEN"},{"$type": "bbo","AskPrice": "645.23","AskSize": "0.81551393","Exchange": "SKRAKEN"}]|

### Message with a nested object

Let's now take an abstract message with a nested object.

![](/timescaledb-connector/src/img/message_example3.png/)

|EventTime|Id|Symbol|trade_price|trade_size|trade_exchange|
|---------|--|------|-----------|----------|--------------|
|TIMESTAMP|SERIAL|VARCHAR|DECIMAL64|DECIMAL64|VARCHAR|

Here is how it may look with real message data: 

|EventTime|Id|Symbol|trade_price|trade_size|trade_exchange|
|---------|--|------|-----------|----------|--------------|
|2021-08-25T07:00:00.025Z|123|btcusd|645.23|0.81551393|SKRAKEN|

We can now take a look at a **more realistic** example. Let's take a Kraken TimeBase stream. 

```java
// Kraken stream schema. Here you can find all the included classes, enums and objects, their fields and data types.

DURABLE STREAM "kraken" (
    CLASS "deltix.timebase.api.messages.MarketMessage" 'Market Message' (
        "currencyCode" 'Currency Code' INTEGER SIGNED (16),
        "originalTimestamp" 'Original Timestamp' TIMESTAMP,
        "sequenceNumber" 'Sequence Number' INTEGER,
        "sourceId" 'Source Id' VARCHAR ALPHANUMERIC (10)
    );
    ENUM "deltix.timebase.api.messages.universal.PackageType" 'Package Type' (
        "VENDOR_SNAPSHOT" = 0,
        "PERIODICAL_SNAPSHOT" = 1,
        "INCREMENTAL_UPDATE" = 2
    );
    CLASS "deltix.timebase.api.messages.universal.PackageHeader" 'Package Header' 
    UNDER "deltix.timebase.api.messages.MarketMessage" (
        "packageType" 'Package Type' "deltix.timebase.api.messages.universal.PackageType" NOT NULL
    );
    CLASS "deltix.timebase.api.messages.universal.BaseEntry" 'Base Entry' (
        "contractId" 'Contract ID' VARCHAR ALPHANUMERIC (10),
        "exchangeId" 'Exchange Code' VARCHAR ALPHANUMERIC (10),
        "isImplied" 'Is Implied' BOOLEAN
    );
    CLASS "deltix.timebase.api.messages.universal.BasePriceEntry" 'Base Price Entry' 
    UNDER "deltix.timebase.api.messages.universal.BaseEntry" (
        "numberOfOrders" 'Number Of Orders' INTEGER,
        "participantId" 'Participant' VARCHAR,
        "price" 'Price' FLOAT DECIMAL64,
        "quoteId" 'Quote ID' VARCHAR,
        "size" 'Size' FLOAT DECIMAL64
    );
    ENUM "deltix.timebase.api.messages.QuoteSide" (
        "BID" = 0,
        "ASK" = 1
    );
    CLASS "deltix.timebase.api.messages.universal.L1Entry" 'L1Entry' 
    UNDER "deltix.timebase.api.messages.universal.BasePriceEntry" (
        "isNational" 'Is National' BOOLEAN,
        "side" 'Side' "deltix.timebase.api.messages.QuoteSide" NOT NULL
    );
    CLASS "deltix.timebase.api.messages.universal.L2EntryNew" 'L2EntryNew' 
    UNDER "deltix.timebase.api.messages.universal.BasePriceEntry" (
        "level" 'Level Index' INTEGER NOT NULL SIGNED (16),
        "side" 'Side' "deltix.timebase.api.messages.QuoteSide" NOT NULL
    );
    ENUM "deltix.timebase.api.messages.BookUpdateAction" 'Book Update Action' (
        "INSERT" = 0,
        "UPDATE" = 1,
        "DELETE" = 2
    );
    CLASS "deltix.timebase.api.messages.universal.L2EntryUpdate" 'L2EntryUpdate' 
    UNDER "deltix.timebase.api.messages.universal.BasePriceEntry" (
        "action" 'Action' "deltix.timebase.api.messages.BookUpdateAction" NOT NULL,
        "level" 'Level Index' INTEGER NOT NULL SIGNED (16),
        "side" 'Side' "deltix.timebase.api.messages.QuoteSide"
    );
    ENUM "deltix.timebase.api.messages.AggressorSide" 'Aggressor Side' (
        "BUY" = 0,
        "SELL" = 1
    );
    ENUM "deltix.timebase.api.messages.TradeType" (
        "REGULAR_TRADE" = 0,
        "AUCTION_CLEARING_PRICE" = 1,
        "CORRECTION" = 2,
        "CANCELLATION" = 3,
        "UNKNOWN" = 4
    );
    CLASS "deltix.timebase.api.messages.universal.TradeEntry" 'Trade Entry' 
    UNDER "deltix.timebase.api.messages.universal.BaseEntry" (
        "buyerNumberOfOrders" 'Buyer Number Of Orders' INTEGER,
        "buyerOrderId" 'Buyer Order ID' VARCHAR,
        "buyerParticipantId" 'Buyer Participant ID' VARCHAR,
        "condition" 'Condition' VARCHAR,
        "matchId" 'Match ID' VARCHAR,
        "price" 'Price' FLOAT DECIMAL64,
        "sellerNumberOfOrders" 'Seller Number Of Orders' INTEGER,
        "sellerOrderId" 'Seller Order ID' VARCHAR,
        "sellerParticipantId" 'Seller Participant ID' VARCHAR,
        "side" 'Side' "deltix.timebase.api.messages.AggressorSide",
        "size" 'Size' FLOAT DECIMAL64,
        "tradeType" 'Trade Type' "deltix.timebase.api.messages.TradeType"
    );
    CLASS "deltix.qsrv.hf.plugins.data.kraken.types.KrakenTradeEntry" 'Kraken Trade Entry' 
    UNDER "deltix.timebase.api.messages.universal.TradeEntry" (
        "orderType" 'Order Type' CHAR
    );
    ENUM "deltix.timebase.api.messages.DataModelType" (
        "LEVEL_ONE" = 0,
        "LEVEL_TWO" = 1,
        "LEVEL_THREE" = 2,
        "MAX" = 3
    );
    CLASS "deltix.timebase.api.messages.universal.BookResetEntry" 'Book Reset Entry' 
    UNDER "deltix.timebase.api.messages.universal.BaseEntry" (
        "modelType" 'Model Type' "deltix.timebase.api.messages.DataModelType" NOT NULL,
        "side" 'Side' "deltix.timebase.api.messages.QuoteSide"
    );
    CLASS "deltix.qsrv.hf.plugins.data.kraken.types.KrakenPackageHeader" 'Kraken Package Header' 
    UNDER "deltix.timebase.api.messages.universal.PackageHeader" (
        "entries" 'Entries' 
        ARRAY(OBJECT("deltix.timebase.api.messages.universal.L1Entry", "deltix.timebase.api.messages.universal.L2EntryNew", 
        "deltix.timebase.api.messages.universal.L2EntryUpdate", "deltix.qsrv.hf.plugins.data.kraken.types.KrakenTradeEntry", 
        "deltix.timebase.api.messages.universal.BookResetEntry") NOT NULL) NOT NULL
    );
    ENUM "deltix.timebase.api.messages.service.DataConnectorStatus" 'Data Connector Status' (
        "INITIAL" = 0,
        "CONNECTED_BY_USER" = 1,
        "AUTOMATICALLY_RESTORED" = 2,
        "DISCONNECTED_BY_USER" = 3,
        "DISCONNECTED_BY_COMPLETED_BATCH" = 4,
        "DISCONNECTED_BY_VENDOR_AND_RECONNECTING" = 5,
        "DISCONNECTED_BY_VENDOR_AND_HALTED" = 6,
        "DISCONNECTED_BY_ERROR_AND_RECONNECTING" = 7,
        "DISCONNECTED_BY_ERROR_AND_HALTED" = 8,
        "RECOVERING_BEGIN" = 9,
        "LIVE_BEGIN" = 10
    );
    CLASS "deltix.timebase.api.messages.service.ConnectionStatusChangeMessage" 'Connection Status Change Message' (
        "cause" 'Cause' VARCHAR,
        "status" 'Status' "deltix.timebase.api.messages.service.DataConnectorStatus"
    );
    ENUM "deltix.timebase.api.messages.status.SecurityStatus" (
        "FEED_CONNECTED" = 0,
        "FEED_DISCONNECTED" = 1,
        "TRADING_STARTED" = 2,
        "TRADING_STOPPED" = 3
    );
    CLASS "deltix.timebase.api.messages.status.SecurityStatusMessage" 'Security Status Change Message' 
    UNDER "deltix.timebase.api.messages.MarketMessage" (
        "cause" 'Cause' VARCHAR,
        "exchangeId" 'Exchange Code' VARCHAR ALPHANUMERIC (10)
        "originalStatus" 'Original Status' VARCHAR,
        "status" 'Status' "deltix.timebase.api.messages.status.SecurityStatus"
    );
)
OPTIONS (POLYMORPHIC; PERIODICITY = 'IRREGULAR'; HIGHAVAILABILITY = FALSE)
```

The Timescale table will have the following structure. 

```bash
    
                                            Table "public.kraken"
      Column       |            Type             | Collation | Nullable |              Default               
-------------------+-----------------------------+-----------+----------+------------------------------------
 exchangeid        | character varying           |           |          | 
 sourceid          | character varying           |           |          | 
 sequencenumber    | bigint                      |           |          | 
 entries           | json                        |           |          | 
 originalstatus    | character varying           |           |          | 
 descriptor_name   | character varying           |           |          | 
 cause             | character varying           |           |          | 
 originaltimestamp | timestamp without time zone |           |          | 
 currencycode      | integer                     |           |          | 
 packagetype       | character varying           |           |          | 
 status            | character varying           |           |          | 
 id                | integer                     |           | not null | nextval('kraken_id_seq'::regclass)
 eventtime         | timestamp without time zone |           | not null | 
 symbol            | character varying           |           |          | 
Indexes:
    "kraken_pkey" PRIMARY KEY, btree (id, eventtime)
    "kraken_eventtime_idx" btree (eventtime DESC)
Triggers:
    ts_insert_blocker BEFORE INSERT ON kraken FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker()
Number of child tables: 1 (Use \d+ to list them.)
```

Here we can see, that each field has been parsed in a separate table colum with an appropriate data type mapping. Entries array has been inserted as a JSON string. 

This table can be displayed in the TimeBase integration with [pgAdmin](https://www.pgadmin.org/) PostgreSQL administration and management platform as follows: 

![](/timescaledb-connector/src/img/pgAdmin.png)

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

* Refer to [TimeBase Quick Start](https://kb.timebase.info/quick-start.html) to learn more about starting TimeBase.
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
* Refer to [Data Types](https://kb.timebase.info/data_types.html) to learn more about data types supported by TimeBase.

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