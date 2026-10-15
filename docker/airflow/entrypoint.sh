#!/usr/bin/env bash
set -euo pipefail
source /opt/airflow/docker/airflow/metadata-env.sh
exec /entrypoint "$@"
