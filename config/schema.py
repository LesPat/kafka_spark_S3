from datetime import datetime

# Canonical Kafka message contract (producer → Spark consumer)
IOT_EVENT_FIELD_NAMES = (
    "device_id",
    "device_type",
    "timestamp",
    "value",
    "unit",
    "status",
)

DEVICE_TYPES = ("temperature", "humidity", "vibration", "co2", "sound")
STATUS_VALUES = ("OK", "WARN", "ALERT")

UNITS_BY_DEVICE_TYPE = {
    "temperature": "°C",
    "humidity": "%",
    "vibration": "g",
    "co2": "ppm",
    "sound": "dB",
}

SYNTHETIC_DEVICES = [
    {"device_id": "sensor_001", "type": "temperature"},
    {"device_id": "sensor_002", "type": "humidity"},
    {"device_id": "sensor_003", "type": "vibration"},
    {"device_id": "sensor_004", "type": "co2"},
    {"device_id": "sensor_005", "type": "sound"},
]

SPARK_TYPE_NAMES = {
    "device_id": "string",
    "device_type": "string",
    "timestamp": "string",
    "value": "double",
    "unit": "string",
    "status": "string",
}

# Spark output schema (after parsing Kafka JSON)
SPARK_OUTPUT_FIELD_NAMES = (
    "device_id",
    "device_type",
    "event_time",
    "value",
    "unit",
    "status",
)

PARTITION_COLUMNS = ("event_date", "device_type")

DEAD_LETTER_FIELD_NAMES = (
    "raw_json",
    "rejected_at",
    "reason",
)

DEAD_LETTER_PARTITION_COLUMNS = ("rejected_date",)

DEAD_LETTER_REASON = "schema_validation_failed"


def build_spark_schema():
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        DoubleType,
    )

    return StructType([
        StructField("device_id", StringType()),
        StructField("device_type", StringType()),
        StructField("timestamp", StringType()),
        StructField("value", DoubleType()),
        StructField("unit", StringType()),
        StructField("status", StringType()),
    ])


def validate_event(event: dict) -> list[str]:
    """Return validation errors for an IoT event (empty list means valid)."""
    errors: list[str] = []

    expected = set(IOT_EVENT_FIELD_NAMES)
    actual = set(event.keys())
    if actual != expected:
        missing = expected - actual
        extra = actual - expected
        if missing:
            errors.append(f"missing fields: {sorted(missing)}")
        if extra:
            errors.append(f"extra fields: {sorted(extra)}")

    device_id = event.get("device_id")
    if not isinstance(device_id, str) or not device_id:
        errors.append("device_id must be a non-empty string")

    device_type = event.get("device_type")
    if device_type not in DEVICE_TYPES:
        errors.append(f"device_type must be one of {DEVICE_TYPES}, got {device_type!r}")

    timestamp = event.get("timestamp")
    if not isinstance(timestamp, str):
        errors.append("timestamp must be a string")
    else:
        try:
            datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            errors.append(f"timestamp must be ISO 8601, got {timestamp!r}")

    value = event.get("value")
    if not isinstance(value, (int, float)):
        errors.append("value must be numeric (int or float)")

    unit = event.get("unit")
    if device_type in UNITS_BY_DEVICE_TYPE and unit != UNITS_BY_DEVICE_TYPE[device_type]:
        errors.append(
            f"unit for {device_type!r} must be {UNITS_BY_DEVICE_TYPE[device_type]!r}, got {unit!r}"
        )

    status = event.get("status")
    if status not in STATUS_VALUES:
        errors.append(f"status must be one of {STATUS_VALUES}, got {status!r}")

    return errors


def parsed_record_errors(record: dict) -> list[str]:
    """Validate a record after JSON parsing (used by Spark filter + tests)."""
    errors: list[str] = []

    device_id = record.get("device_id")
    if not isinstance(device_id, str) or not device_id:
        errors.append("device_id is required")

    device_type = record.get("device_type")
    if device_type not in DEVICE_TYPES:
        errors.append(f"device_type must be one of {DEVICE_TYPES}, got {device_type!r}")

    timestamp = record.get("timestamp")
    if not isinstance(timestamp, str) or not timestamp:
        errors.append("timestamp is required")
    else:
        try:
            datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            errors.append(f"timestamp must be ISO 8601, got {timestamp!r}")

    value = record.get("value")
    if value is None or not isinstance(value, (int, float)):
        errors.append("value is required and must be numeric")

    unit = record.get("unit")
    if not isinstance(unit, str) or not unit:
        errors.append("unit is required")
    elif device_type in UNITS_BY_DEVICE_TYPE and unit != UNITS_BY_DEVICE_TYPE[device_type]:
        errors.append(
            f"unit for {device_type!r} must be {UNITS_BY_DEVICE_TYPE[device_type]!r}, got {unit!r}"
        )

    status = record.get("status")
    if status not in STATUS_VALUES:
        errors.append(f"status must be one of {STATUS_VALUES}, got {status!r}")

    return errors


def is_valid_parsed_record(record: dict) -> bool:
    return not parsed_record_errors(record)
