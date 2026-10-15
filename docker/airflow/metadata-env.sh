#!/usr/bin/env bash
# Resolve once, without logging credentials or spawning a process per config read.
if [[ "${RETAILPULSE_AIRFLOW_METADATA:-0}" == "1" ]]; then
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="$(python /opt/airflow/src/retailpulse/common/airflow_dsn.py)"
    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
fi
