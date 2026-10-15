"""Static checks on SQL layout, the dashboard spec, compose files and the Makefile."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

from retailpulse.common.initialize import DDL_PLAN

ROOT = Path(__file__).resolve().parents[2]


def test_every_ddl_file_in_the_plan_exists():
    for files in DDL_PLAN.values():
        for name in files:
            assert (ROOT / "sql" / "ddl" / name).is_file(), name


def test_every_ddl_file_is_in_the_plan():
    planned = {n for files in DDL_PLAN.values() for n in files}
    assert {p.name for p in (ROOT / "sql" / "ddl").glob("*.sql")} == planned


def test_ddl_is_idempotent_by_construction():
    for p in (ROOT / "sql" / "ddl").glob("*.sql"):
        text = p.read_text().upper()
        for stmt in re.findall(r"CREATE (?:TABLE|SCHEMA|INDEX)\s+(?!IF NOT EXISTS|UNIQUE)", text):
            pytest.fail(f"{p.name} has a non-idempotent CREATE: {stmt!r}")


def test_dashboard_spec_is_valid():
    spec = json.loads((ROOT / "dashboards" / "cards.json").read_text())
    names = [c["name"] for c in spec["cards"]]
    assert len(names) == len(set(names)) >= 10
    required = {"Revenue trend by day", "Top 10 products by revenue", "Store performance",
                "Online vs in-store revenue", "Low-stock alerts (latest snapshot)", "Data-quality scorecard"}
    assert required <= set(names)
    for c in spec["cards"]:
        assert c["sql"].lstrip().upper().startswith("SELECT"), c["name"]
        assert c["col"] >= 0 and c["col"] + c["size_x"] <= 24, c["name"]
        assert c["display"] in {"scalar", "line", "bar", "pie", "table"}


def test_compose_files_define_expected_services_and_profiles():
    core = yaml.safe_load((ROOT / "compose.yaml").read_text())["services"]
    assert {"postgres", "minio", "airflow-init", "airflow-webserver", "airflow-scheduler"} <= set(core)
    assert all(core[s]["profiles"] == ["core"] for s in core)
    bi = yaml.safe_load((ROOT / "compose.bi.yml").read_text())["services"]
    assert {"metabase", "metabase-db"} == set(bi)
    assert "latest" not in bi["metabase"]["image"]                       # pinned
    for svc in list(core.values()) + list(bi.values()):
        image = svc.get("image", "")
        assert not image.endswith(":latest"), image


def test_airflow_waits_for_init_and_mounts_project_dirs():
    core = yaml.safe_load((ROOT / "compose.yaml").read_text())["services"]
    for name in ("airflow-webserver", "airflow-scheduler"):
        assert "airflow-init" in core[name]["depends_on"]
    vols = " ".join(core["airflow-scheduler"]["volumes"])
    for part in ("/dags", "/src", "/sql", "/dashboards", "/data"):
        assert part in vols


def test_dockerfile_does_not_hardcode_cpu_architecture():
    text = (ROOT / "docker" / "airflow" / "Dockerfile").read_text()
    code = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("#"))
    assert "amd64" not in code


def test_makefile_has_the_documented_targets():
    text = (ROOT / "Makefile").read_text()
    for target in ("up", "down", "demo", "smoke", "test", "dashboard", "gate-demo", "recover", "backfill"):
        assert re.search(rf"^{target}:", text, re.M), target
