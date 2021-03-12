#!/bin/sh

echo "JAVA_ARGS=" $JAVA_ARGS

exec java $JAVA_ARGS -jar timescaledb-connector.jar org.springframework.boot.loader.JarLauncher "$@"
