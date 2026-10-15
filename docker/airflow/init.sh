#!/usr/bin/env bash
set -euo pipefail
source /opt/airflow/docker/airflow/metadata-env.sh
python -m retailpulse.common.initialize
airflow db migrate
if ! airflow users list --output json | python -c 'import json,sys; sys.exit(0 if any(u["username"] == "admin" for u in json.load(sys.stdin)) else 1)'; then
    airflow users create --username admin --password "${AIRFLOW_ADMIN_PASSWORD}" \
        --firstname RetailPulse --lastname Admin --role Admin --email admin@example.com
fi
