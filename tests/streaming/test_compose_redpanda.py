"""Unit tests for Phase 4 Task 4.1 — Compose Redpanda infrastructure."""

import os
from pathlib import Path
import pytest
import yaml

from retailpulse.common.config import AppConfig, RedpandaConfig, get_config


class TestComposeStreamingStructure:
    """Tests verifying the structure, profiles, images, and ports in compose.streaming.yml."""

    @pytest.fixture
    def compose_data(self) -> dict:
        compose_path = Path("compose.streaming.yml")
        assert compose_path.exists(), "compose.streaming.yml must exist at project root"
        content = compose_path.read_text(encoding="utf-8")
        return yaml.safe_load(content)

    def test_services_present(self, compose_data: dict) -> None:
        services = compose_data.get("services", {})
        assert "redpanda" in services, "redpanda service must be defined"
        assert "redpanda-init" in services, "redpanda-init service must be defined"
        assert "redpanda-console" in services, "redpanda-console service must be defined"
        assert "order-producer" in services, "order-producer service must be defined"

    def test_redpanda_service_spec(self, compose_data: dict) -> None:
        redpanda = compose_data["services"]["redpanda"]
        assert redpanda["profiles"] == ["streaming"], "redpanda must be under profile 'streaming'"
        assert redpanda["image"] == "docker.redpanda.com/redpandadata/redpanda:v24.2.18", "redpanda image must be pinned"
        
        # Verify resource efficiency for 8GB RAM laptops
        command_str = " ".join(redpanda.get("command", []))
        assert "--smp=1" in command_str, "smp must be set to 1 for lightweight execution"
        assert "--memory=512M" in command_str, "memory must be set to 512M"
        assert "--mode=dev-container" in command_str, "mode must be dev-container"

        # Verify dual connectivity: internal container + host external
        assert "internal://0.0.0.0:9092" in command_str
        assert "external://0.0.0.0:19092" in command_str
        assert "internal://redpanda:9092" in command_str
        assert "external://localhost:19092" in command_str

        # Ports: 19092 (host Kafka API) and 9644 (Admin API)
        ports = redpanda.get("ports", [])
        assert "19092:19092" in ports
        assert "9644:9644" in ports

        # Healthcheck
        healthcheck = redpanda.get("healthcheck", {})
        assert "test" in healthcheck
        test_cmd = " ".join(healthcheck["test"]) if isinstance(healthcheck["test"], list) else healthcheck["test"]
        assert "rpk cluster info" in test_cmd

    def test_redpanda_init_service_spec(self, compose_data: dict) -> None:
        init_service = compose_data["services"]["redpanda-init"]
        assert init_service["profiles"] == ["streaming"]
        assert init_service["image"] == "docker.redpanda.com/redpandadata/redpanda:v24.2.18"
        assert init_service["restart"] == "no"
        
        # Must wait for redpanda healthcheck
        depends_on = init_service.get("depends_on", {})
        assert "redpanda" in depends_on
        assert depends_on["redpanda"]["condition"] == "service_healthy"

        # Mounts init_topics.sh
        volumes = init_service.get("volumes", [])
        assert any("init_topics.sh" in v for v in volumes)

    def test_redpanda_console_spec(self, compose_data: dict) -> None:
        console = compose_data["services"]["redpanda-console"]
        assert console["profiles"] == ["streaming"]
        assert console["image"] == "docker.redpanda.com/redpandadata/console:v2.8.2"
        assert "8085:8080" in console.get("ports", [])
        assert console.get("depends_on", {}).get("redpanda", {}).get("condition") == "service_healthy"

    def test_order_producer_service_spec(self, compose_data: dict) -> None:
        producer = compose_data["services"]["order-producer"]
        assert producer["profiles"] == ["streaming"]
        assert producer["image"] == "retailpulse-airflow:2.10.5"
        assert producer["command"] == "python -m retailpulse.ingestion.stream.order_producer"
        assert producer["depends_on"]["redpanda-init"]["condition"] == "service_completed_successfully"
        assert "./src:/opt/retailpulse/src" in producer.get("volumes", [])
        env = producer.get("environment", {})
        assert env.get("REDPANDA_BROKER") == "redpanda:9092"
        assert env.get("PYTHONPATH") == "/opt/retailpulse/src"

    def test_compose_yaml_includes_streaming(self) -> None:
        compose_yaml = Path("compose.yaml")
        assert compose_yaml.exists()
        content = compose_yaml.read_text(encoding="utf-8")
        data = yaml.safe_load(content)
        includes = data.get("include", [])
        assert "compose.streaming.yml" in includes or any(
            isinstance(i, dict) and i.get("path") == "compose.streaming.yml" for i in includes
        ), "compose.yaml must include compose.streaming.yml"


class TestInitTopicsScript:
    """Verifies that docker/redpanda/init_topics.sh meets idempotent provisioning requirements."""

    def test_script_exists_and_contains_required_topics(self) -> None:
        script_path = Path("docker/redpanda/init_topics.sh")
        assert script_path.exists(), "docker/redpanda/init_topics.sh must exist"
        content = script_path.read_text(encoding="utf-8")
        
        # Must create orders_stream and orders_dlq
        assert "orders_stream" in content
        assert "orders_dlq" in content

        # Must handle existing topics idempotently
        assert "already exists" in content.lower() or "grep" in content

        # Must verify both topics exist and fail loudly if missing
        assert "verification failed" in content.lower() or "exit 1" in content


class TestRedpandaConfig:
    """Tests for RedpandaConfig in retailpulse.common.config."""

    def test_default_values(self) -> None:
        cfg = RedpandaConfig()
        assert cfg.broker == "localhost:19092"
        assert cfg.container_broker == "redpanda:9092"
        assert cfg.orders_topic == "orders_stream"
        assert cfg.dlq_topic == "orders_dlq"
        assert cfg.get_broker(in_container=False) == "localhost:19092"
        assert cfg.get_broker(in_container=True) == "redpanda:9092"

    def test_env_overrides(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
        monkeypatch.setenv("MINIO_ROOT_PASSWORD", "secret")
        monkeypatch.setenv("REDPANDA_BROKER", "my-kafka:19092")
        monkeypatch.setenv("REDPANDA_CONTAINER_BROKER", "cluster-kafka:9092")
        monkeypatch.setenv("REDPANDA_ORDERS_TOPIC", "test_orders")
        monkeypatch.setenv("REDPANDA_DLQ_TOPIC", "test_dlq")

        app_cfg = get_config()
        assert app_cfg.redpanda.broker == "my-kafka:19092"
        assert app_cfg.redpanda.container_broker == "cluster-kafka:9092"
        assert app_cfg.redpanda.orders_topic == "test_orders"
        assert app_cfg.redpanda.dlq_topic == "test_dlq"
