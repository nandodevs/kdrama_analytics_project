#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
    $(command -v pip) install --user -r requirements.txt
fi

# Initialize the database if it hasn't been initialized yet
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username Admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@nandodevs.com \
    --password admin
fi

$(command -v airflow) db upgrade

exec airflow webserver
