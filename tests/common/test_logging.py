"""Tests for retailpulse.common.logging."""

import logging

import retailpulse.common.logging as rp_logging
from retailpulse.common.logging import get_logger, setup_logging


class TestSetupLogging:
    """Tests for setup_logging()."""

    def setup_method(self) -> None:
        """Reset logging state between tests."""
        rp_logging._CONFIGURED = False
        root = logging.getLogger()
        root.handlers.clear()

    def test_adds_handler_to_root(self) -> None:
        setup_logging("DEBUG")
        root = logging.getLogger()
        assert len(root.handlers) >= 1
        assert root.level == logging.DEBUG

    def test_sets_level_info(self) -> None:
        setup_logging("INFO")
        assert logging.getLogger().level == logging.INFO

    def test_idempotent(self) -> None:
        setup_logging("INFO")
        handler_count = len(logging.getLogger().handlers)
        setup_logging("DEBUG")  # second call should be ignored
        assert len(logging.getLogger().handlers) == handler_count
        # Level should NOT have changed
        assert logging.getLogger().level == logging.INFO

    def test_formatter_includes_asctime(self) -> None:
        setup_logging("INFO")
        handler = logging.getLogger().handlers[-1]
        fmt = handler.formatter
        assert fmt is not None
        assert "asctime" in fmt._fmt


class TestGetLogger:
    """Tests for get_logger()."""

    def test_returns_namespaced_logger(self) -> None:
        log = get_logger("mymod")
        assert log.name == "retailpulse.mymod"
        assert isinstance(log, logging.Logger)

    def test_different_names_return_different_loggers(self) -> None:
        a = get_logger("alpha")
        b = get_logger("beta")
        assert a is not b
        assert a.name != b.name