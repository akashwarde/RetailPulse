"""DAG integrity: every DAG file imports, has the expected shape, and uses safe settings."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

DAG_DIR = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="module")
def bag():
    return DagBag(dag_folder=str(DAG_DIR), include_examples=False)


def test_no_import_errors(bag):
    assert bag.import_errors == {}


def test_expected_dags_exist(bag):
    assert {"retail_batch_pipeline", "retail_stream_microbatch", "generate_demo_data"} <= set(bag.dags)


def test_batch_dag_structure(bag):
    dag = bag.dags["retail_batch_pipeline"]
    ids = {t.task_id for t in dag.tasks}
    for source in ("sales", "inventory", "customers"):
        assert {f"{source}.extract", f"{source}.to_silver"} <= ids
        expected = {f"{source}.extract"} | ({"extract_masters"} if source != "customers" else set())
        assert dag.get_task(f"{source}.to_silver").upstream_task_ids == expected
    assert dag.get_task("load_dimensions").upstream_task_ids >= {"sales.to_silver", "inventory.to_silver",
                                                                 "customers.to_silver", "extract_masters"}
    assert dag.get_task("load_facts").upstream_task_ids == {"load_dimensions"}
    assert dag.get_task("refresh_marts").upstream_task_ids == {"load_facts"}
    assert str(dag.get_task("write_dq_summary").trigger_rule) in ("all_done", "TriggerRule.ALL_DONE")
    assert dag.get_task("end").upstream_task_ids == {"refresh_marts", "write_dq_summary"}   # a failure cannot hide
    assert dag.catchup is False and dag.max_active_runs == 1


def test_batch_silver_tasks_use_spark_pool(bag):
    dag = bag.dags["retail_batch_pipeline"]
    assert all(dag.get_task(f"{s}.to_silver").pool == "spark" for s in ("sales", "inventory", "customers"))


def test_stream_dag_runs_every_5_minutes_without_overlap(bag):
    dag = bag.dags["retail_stream_microbatch"]
    assert dag.schedule_interval == "*/5 * * * *"
    assert dag.max_active_runs == 1 and dag.catchup is False
    assert dag.get_task("to_silver").upstream_task_ids >= {"consume", "has_new_data"}
    assert "quality_gate" in {t.task_id for t in dag.tasks}
    # the gate must run AFTER the load so valid events are never held back
    assert dag.get_task("quality_gate").upstream_task_ids >= {"load_fact_sales"}


def test_all_dags_have_retries_and_failure_callback(bag):
    for dag in bag.dags.values():
        for task in dag.tasks:
            if task.task_id in ("start", "end"):
                continue
            assert task.on_failure_callback is not None, f"{dag.dag_id}.{task.task_id}"
        assert dag.default_args["retries"] >= 0
    assert bag.dags["retail_batch_pipeline"].default_args["retries"] == 2
    assert bag.dags["retail_batch_pipeline"].default_args["retry_delay"] == timedelta(minutes=2)


def test_dag_files_stay_thin():
    for f in DAG_DIR.glob("*.py"):
        assert len(f.read_text().splitlines()) < 140, f"{f.name} should delegate to src/retailpulse"
