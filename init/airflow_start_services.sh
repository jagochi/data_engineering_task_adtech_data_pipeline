#!/usr/bin/env bash
set -e

# wait for Postgres
until pg_isready -h postgres -p 5432; do
  sleep 1
done

if [ ! -f /opt/airflow/airflow_initialized ]; then
  airflow db init
  airflow users create \
    --username airflow \
    --password airflow \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
  touch /opt/airflow/airflow_initialized
fi

airflow scheduler &
sleep 10

airflow dags unpause initial_load
airflow dags trigger initial_load

exec airflow webserver