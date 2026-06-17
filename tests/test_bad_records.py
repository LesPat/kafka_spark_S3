import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.schema import (
    DEAD_LETTER_FIELD_NAMES,
    DEAD_LETTER_PARTITION_COLUMNS,
    DEAD_LETTER_REASON,
    DEVICE_TYPES,
    STATUS_VALUES,
    is_valid_parsed_record,
    parsed_record_errors,
    validate_event,
)
from producer.synthetic_data_generator import generate_iot_event


class TestParsedRecordValidation:
    def test_valid_producer_event_passes(self):
        event = generate_iot_event({"device_id": "sensor_001", "type": "temperature"})
        assert is_valid_parsed_record(event)
        assert parsed_record_errors(event) == []

    def test_validate_event_implies_valid_parsed_record(self):
        for device_type in DEVICE_TYPES:
            event = generate_iot_event({"device_id": "sensor_x", "type": device_type})
            assert validate_event(event) == []
            assert is_valid_parsed_record(event)

    def test_rejects_null_value(self):
        record = {"device_id": "s1", "device_type": "temperature", "timestamp": "2025-01-01T00:00:00+00:00", "value": None, "unit": "°C", "status": "OK"}
        assert not is_valid_parsed_record(record)
        assert any("value is required" in error for error in parsed_record_errors(record))

    def test_rejects_invalid_status(self):
        record = {"device_id": "s1", "device_type": "temperature", "timestamp": "2025-01-01T00:00:00+00:00", "value": 1.0, "unit": "°C", "status": "CRITICAL"}
        assert not is_valid_parsed_record(record)

    def test_rejects_invalid_timestamp(self):
        record = {"device_id": "s1", "device_type": "temperature", "timestamp": "not-a-date", "value": 1.0, "unit": "°C", "status": "OK"}
        assert not is_valid_parsed_record(record)
        assert any("timestamp must be ISO 8601" in error for error in parsed_record_errors(record))

    def test_rejects_unknown_device_type(self):
        record = {"device_id": "s1", "device_type": "pressure", "timestamp": "2025-01-01T00:00:00+00:00", "value": 1.0, "unit": "bar", "status": "OK"}
        assert not is_valid_parsed_record(record)

    def test_rejects_empty_device_id(self):
        record = {"device_id": "", "device_type": "temperature", "timestamp": "2025-01-01T00:00:00+00:00", "value": 1.0, "unit": "°C", "status": "OK"}
        assert not is_valid_parsed_record(record)

    def test_rejects_malformed_json_parse_result(self):
        record = {field: None for field in ("device_id", "device_type", "timestamp", "value", "unit", "status")}
        assert not is_valid_parsed_record(record)


class TestDeadLetterContract:
    def test_dead_letter_fields(self):
        assert DEAD_LETTER_FIELD_NAMES == ("raw_json", "rejected_at", "reason")

    def test_dead_letter_partitions(self):
        assert DEAD_LETTER_PARTITION_COLUMNS == ("rejected_date",)

    def test_dead_letter_reason_constant(self):
        assert DEAD_LETTER_REASON == "schema_validation_failed"

    def test_status_values_used_by_filter(self):
        assert set(STATUS_VALUES) == {"OK", "WARN", "ALERT"}
