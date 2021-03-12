FROM openjdk:11

COPY build/libs/timescaledb-connector-*.jar /Epam/timescaledb-connector.jar
COPY timescale-connector.sh "/"

WORKDIR /Epam

RUN chmod u+x /timescale-connector.sh

ENTRYPOINT ["/timescale-connector.sh"]
