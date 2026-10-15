"""Create the RetailPulse Metabase dashboard through the Metabase REST API (idempotent).

Usage (inside the Airflow container, where the service names resolve):

    python /opt/airflow/dashboards/provision_metabase.py            # via `make dashboard`
    python /opt/airflow/dashboards/provision_metabase.py --check-sql # only run every card's SQL on the warehouse

What it does: completes Metabase's first-run setup (or logs in), registers the warehouse database,
creates one native-SQL question per entry in ``cards.json`` and assembles them into a dashboard.
Re-running replaces the previous dashboard and cards of the same name. Only the standard library
is used, so it needs no extra packages.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional

CARDS_FILE = Path(__file__).with_name("cards.json")
DB_NAME = "RetailPulse Warehouse"


class MetabaseClient:
    def __init__(self, base_url: str) -> None:
        self.base = base_url.rstrip("/")
        self.session: Optional[str] = None

    def request(self, method: str, path: str, body: Optional[Dict[str, Any]] = None) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"}
        if self.session:
            headers["X-Metabase-Session"] = self.session
        req = urllib.request.Request(f"{self.base}{path}", data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")[:500]
            raise RuntimeError(f"{method} {path} -> HTTP {exc.code}: {detail}") from exc

    def wait_until_ready(self, timeout: int = 420) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if self.request("GET", "/api/health").get("status") == "ok":
                    return
            except Exception:  # noqa: BLE001 - still starting
                pass
            time.sleep(5)
        raise RuntimeError(f"Metabase not healthy after {timeout}s at {self.base}")

    def authenticate(self, email: str, password: str) -> None:
        props = self.request("GET", "/api/session/properties")
        token = props.get("setup-token")
        if token:  # first run: create the admin user
            print("Metabase first-run setup ...")
            resp = self.request("POST", "/api/setup", {
                "token": token,
                "user": {"first_name": "RetailPulse", "last_name": "Admin", "email": email, "password": password},
                "prefs": {"site_name": "RetailPulse", "allow_tracking": False},
            })
            self.session = resp["id"]
        else:
            self.session = self.request("POST", "/api/session", {"username": email, "password": password})["id"]


def ensure_database(mb: MetabaseClient) -> int:
    existing = mb.request("GET", "/api/database")
    items = existing["data"] if isinstance(existing, dict) else existing
    for db in items:
        if db.get("name") == DB_NAME:
            return db["id"]
    created = mb.request("POST", "/api/database", {
        "name": DB_NAME, "engine": "postgres", "is_full_sync": True,
        "details": {
            "host": os.getenv("POSTGRES_HOST", "postgres"), "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "dbname": os.getenv("WAREHOUSE_DB", "warehouse_db"), "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"], "ssl": False,
        },
    })
    return created["id"]


def remove_previous(mb: MetabaseClient, dashboard_name: str, card_names: set) -> None:
    for d in mb.request("GET", "/api/dashboard"):
        if d.get("name") == dashboard_name and not d.get("archived"):
            mb.request("DELETE", f"/api/dashboard/{d['id']}")
    for c in mb.request("GET", "/api/card"):
        if c.get("name") in card_names and not c.get("archived"):
            mb.request("PUT", f"/api/card/{c['id']}", {"archived": True})


def provision(base_url: str) -> str:
    spec = json.loads(CARDS_FILE.read_text(encoding="utf-8"))
    mb = MetabaseClient(base_url)
    mb.wait_until_ready()
    mb.authenticate(os.getenv("METABASE_ADMIN_EMAIL", "admin@retailpulse.local"),
                    os.getenv("METABASE_ADMIN_PASSWORD", "RetailPulse-Admin-2026"))
    db_id = ensure_database(mb)
    remove_previous(mb, spec["dashboard"]["name"], {c["name"] for c in spec["cards"]})

    placed = []
    for i, card in enumerate(spec["cards"]):
        created = mb.request("POST", "/api/card", {
            "name": card["name"], "display": card["display"], "collection_id": None,
            "dataset_query": {"type": "native", "database": db_id, "native": {"query": card["sql"]}},
            "visualization_settings": {},
        })
        placed.append({"id": -(i + 1), "card_id": created["id"], "row": card["row"], "col": card["col"],
                       "size_x": card["size_x"], "size_y": card["size_y"],
                       "visualization_settings": {}, "parameter_mappings": []})
        print(f"  card: {card['name']}")
    dash = mb.request("POST", "/api/dashboard", spec["dashboard"])
    mb.request("PUT", f"/api/dashboard/{dash['id']}", {"dashcards": placed})
    return f"{base_url.rstrip('/')}/dashboard/{dash['id']}"


def check_sql() -> int:
    """Run every card's SQL on the warehouse -- verifies the queries without needing Metabase."""
    import psycopg2

    spec = json.loads(CARDS_FILE.read_text(encoding="utf-8"))
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"), port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("WAREHOUSE_DB", "warehouse_db"), user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"])
    failures = 0
    for card in spec["cards"]:
        try:
            with conn.cursor() as cur:
                cur.execute(card["sql"])
                rows = cur.fetchall()
            print(f"OK    {card['name']:<40} {len(rows):>4} rows")
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            failures += 1
            print(f"FAIL  {card['name']}: {exc}")
    conn.close()
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-sql", action="store_true")
    parser.add_argument("--url", default=os.getenv("METABASE_URL", "http://metabase:3000"))
    args = parser.parse_args()
    if args.check_sql:
        return check_sql()
    url = provision(args.url)
    print(f"Dashboard ready: {url.replace('metabase', 'localhost')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
