import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from streaming.progress import format_query_progress


def _query(name: str, progress=None, message: str = "waiting"):
    return SimpleNamespace(
        name=name,
        lastProgress=progress,
        status={"message": message},
    )


class TestProgressLogging:
    def test_format_progress_with_batch(self):
        query = _query(
            "iot_good_records",
            progress={
                "batchId": 3,
                "numInputRows": 12,
                "inputRowsPerSecond": 0.4,
                "durationMs": {"triggerExecution": 1500},
            },
        )
        formatted = format_query_progress(query)
        assert "[iot_good_records]" in formatted
        assert "batch=3" in formatted
        assert "rows=12" in formatted
        assert "duration_ms=1500" in formatted

    def test_format_progress_without_batch(self):
        query = _query("iot_dead_letter", progress=None, message="waiting for data")
        formatted = format_query_progress(query)
        assert "[iot_dead_letter] waiting for data" in formatted
