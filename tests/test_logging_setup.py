import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.logging_setup import setup_logger


class TestLoggingSetup:
    def test_setup_logger_writes_to_file(self, tmp_path, monkeypatch):
        import config.settings as settings

        monkeypatch.setattr(settings, "LOG_DIR", tmp_path / "logs")
        logging.getLogger("test_component").handlers.clear()

        logger = setup_logger("test_component", "test.log")
        logger.info("hello from test")

        log_file = tmp_path / "logs" / "test.log"
        assert log_file.exists()
        assert "hello from test" in log_file.read_text()

    def test_setup_logger_is_idempotent(self, tmp_path, monkeypatch):
        import config.settings as settings

        monkeypatch.setattr(settings, "LOG_DIR", tmp_path / "logs")
        logging.getLogger("idempotent_test").handlers.clear()

        first = setup_logger("idempotent_test", "idempotent.log")
        second = setup_logger("idempotent_test", "idempotent.log")

        assert first is second
        assert len(first.handlers) == 2
