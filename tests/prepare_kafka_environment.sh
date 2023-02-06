#!/bin/bash
docker compose up -d
sleep 10
docker compose run ksql-datagen ksql-datagen quickstart=users topic=users1 bootstrap-server=broker:29092 iterations=500
docker compose run ksql-datagen ksql-datagen quickstart=users topic=users2 bootstrap-server=broker:29092 iterations=500
docker compose run ksqldb-cli ksql -f /tmp/ksql_setup.txt http://ksqldb-server:8088
