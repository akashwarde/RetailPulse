"""Emit the escaped metadata URI for the container startup wrapper."""
import os
import sys
from urllib.parse import quote


def main() -> None:
    user = quote(os.getenv("POSTGRES_USER", "retail"), safe="")
    password = quote(os.environ["POSTGRES_PASSWORD"], safe="")
    sys.stdout.write(f"postgresql+psycopg2://{user}:{password}@postgres:5432/airflow_db")


if __name__ == "__main__":
    main()
