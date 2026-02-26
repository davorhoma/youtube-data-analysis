#!/bin/bash

echo "> Bringing down cluster services"

echo ">> Shutting down Airflow"
docker compose -f Airflow/docker-compose.yml down

cho ">> Shutting down Metabase"
docker compose -f Metabase/docker-compose.yml down

echo ">> Shutting down MongoDB"
docker compose -f MongoDB/docker-compose.yml down

echo ">> Shutting down Apache Spark"
docker compose -f Spark/docker-compose.yml down

echo ">> Shutting down Hadoop"
docker compose -f Hadoop/docker-compose.yml down

echo ">> Shutting down Kafka"
docker compose -f Kafka/docker-compose.yml down

echo "> Deleting 'youtube-trending-data-analysis' network"
docker network rm youtube-trending-data-analysis