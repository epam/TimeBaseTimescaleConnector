# Run This Example to Test Replicator

This example serves **only** for demonstration purposes to show the replication process in action.

**Prerequisites**

* Docker
* Java 11+

**1. Launch Timescale**
  * Get timescale Docker image<br>
  ```bash
  docker pull timescale/timescaledb:1.7.4-pg12
  ```
  * Enter your password and run Timescale<br>
  ```bash
  docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb:1.7.4-pg12
  ```
**2. Launch TimeBase**
  * Get and run the TimeBase Community Edition<br>
  ```bash
  docker pull finos/timebase-ce-server
 
  docker run -d --name tbserver -p 8011:8011 finos/timebase-ce-server
  ```
**3. Start TimeBase Shell CLI**<br>
  ```bash
  docker exec -it tbserver /timebase-server/bin/tickdb.sh
  
  ==> set db dxtick://localhost:8011

  ==> open
  ```
**4. Create a timescale_stream Stream in TimeBase**<br>
```bash

==> ??
create DURABLE STREAM "timescale_stream" (
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
        "entries" ARRAY(OBJECT("bbo", "trade") NOT NULL)
    );
)
OPTIONS (POLYMORPHIC; PERIODICITY = 'IRREGULAR'; HIGHAVAILABILITY = FALSE)
/
```
**5. Write Into a timescale_stream Stream**
```bash
==> set stream timescale_stream
==> send [{"type":"Message","symbol":"btcusd","timestamp":"2021-09-06T23:08:45.790Z","entries":[{"type":"trade","price":"333.1","size":"444.2"}]}]
```
**6. Run Replicator**
  * **In a new console window**, go to the Timescale replicator directory<br>
  ```bash
  cd TimeBaseTimescaleConnector
  ```
  * Build the Timescale replicator<br>
  ```bash
  ./gradlew clean build
  ```
  * Start the Timescale replicator. Refer to README to learn more about the available [configuration parameters](https://github.com/epam/TimeBaseTimescaleConnector/blob/main/README.md#configuration).<br>
  ```bash
  set TIMEBASE_STREAMS_FOR_REPLICATION=timescale_stream
  java -jar timescale-conntecor/build/libs/timescaledb-connector-1.0.1-SNAPSHOT.jar
  ```
**7. View Stream in Timescale**
  * Go to Timescale Docker container and run a `select` command. You can also run the `/d table_name` command to get the table description.<br>
  ```bash
  docker exec -it timescaledb /bin/bash
  
  bash-5.0# psql -h localhost -U postgres
  
  postgres=# select * from timescale_stream;
  
  postgres=# \d timescale_stream;
  ```

---------------------------------------------------

**To Run Replicator in Docker**

1. Build the Replicator Docker image<br>
```bash
./gradlew buildDockerImage
```
2. Create the container from the Docker image<br>
```bash
docker create --name replicator -e POSTGRES_HOST=timescaledb -e TIMEBASE_HOST=tbserver -e TIMEBASE_STREAMS_FOR_REPLICATION=timescale_stream null/deltix.docker/connectors/timescale-connector:1.0
```
3. Create a network between Replicator, TimeBase and Timescale<br>
```bash
docker network create tsrep

docker network connect tsrep tbserver

docker network connect tsrep timescaledb

docker network connect tsrep replicator
```
4. Start the Replicator container<br>
```bash
docker start replicator
```
5. Refer to **Step 7** form the above example to view the table. 

