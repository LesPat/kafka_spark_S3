import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.schema import (
    DEVICE_TYPES,
    IOT_EVENT_FIELD_NAMES,
    PARTITION_COLUMNS,
    SPARK_OUTPUT_FIELD_NAMES,
    SPARK_TYPE_NAMES,
    STATUS_VALUES,
    SYNTHETIC_DEVICES,
    UNITS_BY_DEVICE_TYPE,
    build_spark_schema,
    validate_event,
)
from producer.synthetic_data_generator import generate_iot_event


def _spark_type_name(field) -> str:
    type_name = type(field.dataType).__name__
    mapping = {
        "StringType": "string",
        "DoubleType": "double",
        "IntegerType": "int",
        "LongType": "long",
        "BooleanType": "boolean",
        "TimestampType": "timestamp",
    }
    return mapping.get(type_name, type_name.lower().replace("type", ""))


class TestProducerContract:
    def test_event_has_exact_field_set(self):
        for device in SYNTHETIC_DEVICES:
            event = generate_iot_event(device)
            assert set(event.keys()) == set(IOT_EVENT_FIELD_NAMES)

    def test_event_passes_validation(self):
        for device in SYNTHETIC_DEVICES:
            event = generate_iot_event(device)
            errors = validate_event(event)
            assert errors == [], f"validation failed: {errors}"

    def test_each_device_type_is_produced(self):
        produced_types = {device["type"] for device in SYNTHETIC_DEVICES}
        assert produced_types == set(DEVICE_TYPES)

    def test_unit_matches_device_type(self):
        for device in SYNTHETIC_DEVICES:
            event = generate_iot_event(device)
            assert event["unit"] == UNITS_BY_DEVICE_TYPE[event["device_type"]]

    def test_status_is_allowed_value(self):
        for device in SYNTHETIC_DEVICES:
            event = generate_iot_event(device)
            assert event["status"] in STATUS_VALUES


class TestSparkSchemaContract:
    @pytest.fixture(autouse=True)
    def _require_pyspark(self):
        pytest.importorskip("pyspark")

    def test_spark_schema_field_names(self):
        schema = build_spark_schema()
        assert [field.name for field in schema.fields] == list(IOT_EVENT_FIELD_NAMES)

    def test_spark_schema_field_types(self):
        schema = build_spark_schema()
        actual = {field.name: _spark_type_name(field) for field in schema.fields}
        assert actual == SPARK_TYPE_NAMES


class TestSparkOutputContract:
    def test_output_field_names_exclude_raw_timestamp(self):
        assert "timestamp" not in SPARK_OUTPUT_FIELD_NAMES
        assert "event_time" in SPARK_OUTPUT_FIELD_NAMES

    def test_partition_columns(self):
        assert PARTITION_COLUMNS == ("event_date", "device_type")


class TestValidateEvent:
    def test_rejects_missing_field(self):
        event = generate_iot_event(SYNTHETIC_DEVICES[0])
        del event["status"]
        errors = validate_event(event)
        assert any("missing fields" in error for error in errors)

    def test_rejects_invalid_status(self):
        event = generate_iot_event(SYNTHETIC_DEVICES[0])
        event["status"] = "CRITICAL"
        errors = validate_event(event)
        assert any("status must be one of" in error for error in errors)

    def test_rejects_string_value(self):
        event = generate_iot_event(SYNTHETIC_DEVICES[0])
        event["value"] = "24.5"
        errors = validate_event(event)
        assert any("value must be numeric" in error for error in errors)
