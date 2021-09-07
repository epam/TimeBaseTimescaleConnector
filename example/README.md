# Run This Example to Test Replicator


1. Launch Timescale
  * get timescale docker image<br>
  ```bash
  docker pull timescale/timescaledb:1.7.4-pg12
  ```
  * run timescale<br>
  ```bash
  docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb:1.7.4-pg12
  ```
2. Lauch TimeBase
  * get and run tb ce server<br>
  ```bash
  docker pull finos/timebase-ce-server
  ```<br>
  ```bash
  docker run -d --name tbserver -p 8011:8011 finos/timebase-ce-server
  ```
3. Start TimeBase Shell CLI<br>
  ```bash
  docker exec -it tbserver /timebase-server/bin/tickdb.sh
  ==> set db dxtick://localhost:8011
  ```
4. Create strema using QQL<br>
```bash
==> open
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
5. Write into a new stream
```bash
==> set stream timescale_stream
==> send [{"type":"Message","symbol":"btcusd","timestamp":"2021-09-06T23:08:45.790Z","entries":[{"type":"trade","price":"333.1","size":"444.2"}]}]
```
6. Run Replicator
  * go to Timescale replicator directory<br>
  ```bash
  cd TimeBaseTimescaleConnector
  ```
  * build timescale replicator<br>
  ```bash
  ./gradlew clean build
  ```
  * open build artifacts directory<br>
  ```bash
  cd timescale-connector/build/libs
  ```
  * start the Timescale replicator<br>
  ```bash
  set TIMEBASE_STREAMS_FOR_REPLICATION=timescale_stream
  java -jar timescaledb-connector-1.0.1-SNAPSHOT.jar
  ```
7. View stream in Timescale
  * go to timescale docker container and make select<br>
  ```bash
  docker exec -it timescaledb /bin/bash
  
  bash-5.0# psql -h localhost -U postgres
  
  postgres=# select * from timescale_stream;
  
  postgres=# \d timescale_stream;
  ```


