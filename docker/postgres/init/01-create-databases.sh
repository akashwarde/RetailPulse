#!/usr/bin/env bash
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=warehouse_db="${WAREHOUSE_DB:-warehouse_db}" <<'SQL'
SELECT format('CREATE DATABASE %I', :'warehouse_db')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = :'warehouse_db')\gexec
SELECT 'CREATE DATABASE airflow_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec
SQL
