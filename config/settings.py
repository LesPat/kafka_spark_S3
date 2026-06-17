import os
import re
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

# Local runs: credentials live in .env on your machine (gitignored).
# override=True makes .env win over any stale AWS_* exports in your shell.
load_dotenv(ENV_FILE, override=True)

ALLOWED_KAFKA_STARTING_OFFSETS = ("earliest", "latest")

PLACEHOLDER_CREDENTIALS = frozenset({
    "your_access_key_id",
    "your_secret_access_key",
    "changeme",
    "replace_me",
    "xxx",
})

_S3_BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_UNSET = object()


def _clean_env(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


# AWS
AWS_ACCESS_KEY_ID = _clean_env(os.getenv("AWS_ACCESS_KEY_ID"))
AWS_SECRET_ACCESS_KEY = _clean_env(os.getenv("AWS_SECRET_ACCESS_KEY"))
AWS_SESSION_TOKEN = _clean_env(os.getenv("AWS_SESSION_TOKEN"))
AWS_REGION = _clean_env(os.getenv("AWS_REGION")) or "us-east-1"

# Kafka
KAFKA_BOOTSTRAP_SERVERS = _clean_env(os.getenv("KAFKA_BOOTSTRAP_SERVERS")) or "localhost:9092"
KAFKA_TOPIC = _clean_env(os.getenv("KAFKA_TOPIC")) or "iot_sensors"
KAFKA_STARTING_OFFSETS = _clean_env(os.getenv("KAFKA_STARTING_OFFSETS")) or "latest"

# S3
S3_BUCKET = _clean_env(os.getenv("S3_BUCKET")) or "real-time-processing-kafka-sink"
S3_OUTPUT_PREFIX = _clean_env(os.getenv("S3_OUTPUT_PREFIX")) or "new_iot_streaming_data/"
S3_CHECKPOINT_PREFIX = _clean_env(os.getenv("S3_CHECKPOINT_PREFIX")) or "new_checkpoints/iot_streaming_data/"
S3_DEAD_LETTER_PREFIX = _clean_env(os.getenv("S3_DEAD_LETTER_PREFIX")) or "new_dead_letter/iot_streaming_data/"
S3_DEAD_LETTER_CHECKPOINT_PREFIX = (
    _clean_env(os.getenv("S3_DEAD_LETTER_CHECKPOINT_PREFIX"))
    or "new_checkpoints/dead_letter/iot_streaming_data/"
)

# Spark streaming
SPARK_TRIGGER_INTERVAL = _clean_env(os.getenv("SPARK_TRIGGER_INTERVAL")) or "30 seconds"

# Logging
LOG_DIR = Path(_clean_env(os.getenv("LOG_DIR")) or str(PROJECT_ROOT / "logs"))


def s3_path(prefix: str) -> str:
    return f"s3a://{S3_BUCKET}/{prefix}"


S3_OUTPUT_PATH = s3_path(S3_OUTPUT_PREFIX)
S3_CHECKPOINT_PATH = s3_path(S3_CHECKPOINT_PREFIX)
S3_DEAD_LETTER_PATH = s3_path(S3_DEAD_LETTER_PREFIX)
S3_DEAD_LETTER_CHECKPOINT_PATH = s3_path(S3_DEAD_LETTER_CHECKPOINT_PREFIX)


def _resolve_env(value: str | None | object, default: str | None) -> str | None:
    if value is _UNSET:
        return _clean_env(default)
    return _clean_env(value)  # type: ignore[arg-type]


def aws_credential_errors(
    access_key_id: str | None | object = _UNSET,
    secret_access_key: str | None | object = _UNSET,
    session_token: str | None | object = _UNSET,
) -> list[str]:
    access_key_id = _resolve_env(access_key_id, AWS_ACCESS_KEY_ID)
    secret_access_key = _resolve_env(secret_access_key, AWS_SECRET_ACCESS_KEY)
    session_token = _resolve_env(session_token, AWS_SESSION_TOKEN)

    errors: list[str] = []

    if not access_key_id:
        errors.append(
            "AWS_ACCESS_KEY_ID is required. Copy .env.example to .env and set your access key."
        )
    elif access_key_id.lower() in PLACEHOLDER_CREDENTIALS:
        errors.append(
            "AWS_ACCESS_KEY_ID still has the placeholder value from .env.example — replace it with a real key."
        )

    if not secret_access_key:
        errors.append(
            "AWS_SECRET_ACCESS_KEY is required. Copy .env.example to .env and set your secret key."
        )
    elif secret_access_key.lower() in PLACEHOLDER_CREDENTIALS:
        errors.append(
            "AWS_SECRET_ACCESS_KEY still has the placeholder value from .env.example — replace it with a real key."
        )

    if access_key_id and access_key_id.startswith("ASIA") and not session_token:
        errors.append(
            "AWS_SESSION_TOKEN is required when using temporary credentials (access key starts with ASIA)."
        )

    if not ENV_FILE.exists() and (not access_key_id or not secret_access_key):
        errors.append(f"No .env file found at {ENV_FILE}. Run: cp .env.example .env")

    return errors


def kafka_config_errors(
    bootstrap_servers: str | None | object = _UNSET,
    topic: str | None | object = _UNSET,
    starting_offsets: str | None | object = _UNSET,
) -> list[str]:
    bootstrap_servers = _resolve_env(bootstrap_servers, KAFKA_BOOTSTRAP_SERVERS)
    topic = _resolve_env(topic, KAFKA_TOPIC)
    starting_offsets = _resolve_env(starting_offsets, KAFKA_STARTING_OFFSETS)

    errors: list[str] = []

    if not bootstrap_servers:
        errors.append("KAFKA_BOOTSTRAP_SERVERS must not be empty.")

    if not topic:
        errors.append("KAFKA_TOPIC must not be empty.")

    if starting_offsets not in ALLOWED_KAFKA_STARTING_OFFSETS:
        errors.append(
            f"KAFKA_STARTING_OFFSETS must be one of {ALLOWED_KAFKA_STARTING_OFFSETS}, "
            f"got {starting_offsets!r}."
        )

    return errors


def s3_config_errors(
    bucket: str | None | object = _UNSET,
    output_prefix: str | None | object = _UNSET,
    checkpoint_prefix: str | None | object = _UNSET,
    dead_letter_prefix: str | None | object = _UNSET,
    dead_letter_checkpoint_prefix: str | None | object = _UNSET,
) -> list[str]:
    bucket = _resolve_env(bucket, S3_BUCKET)
    output_prefix = _resolve_env(output_prefix, S3_OUTPUT_PREFIX)
    checkpoint_prefix = _resolve_env(checkpoint_prefix, S3_CHECKPOINT_PREFIX)
    dead_letter_prefix = _resolve_env(dead_letter_prefix, S3_DEAD_LETTER_PREFIX)
    dead_letter_checkpoint_prefix = _resolve_env(
        dead_letter_checkpoint_prefix, S3_DEAD_LETTER_CHECKPOINT_PREFIX
    )

    errors: list[str] = []

    if not bucket:
        errors.append("S3_BUCKET must not be empty.")
    elif bucket.startswith(("s3://", "s3a://")):
        errors.append("S3_BUCKET must be a bucket name only, without s3:// or s3a:// prefix.")
    elif not _S3_BUCKET_PATTERN.match(bucket):
        errors.append(
            f"S3_BUCKET {bucket!r} does not look like a valid S3 bucket name."
        )

    for name, prefix in (
        ("S3_OUTPUT_PREFIX", output_prefix),
        ("S3_CHECKPOINT_PREFIX", checkpoint_prefix),
        ("S3_DEAD_LETTER_PREFIX", dead_letter_prefix),
        ("S3_DEAD_LETTER_CHECKPOINT_PREFIX", dead_letter_checkpoint_prefix),
    ):
        if not prefix:
            errors.append(f"{name} must not be empty.")
        elif prefix.startswith("/"):
            errors.append(f"{name} must not start with '/'.")

    unique_paths = {
        "output": output_prefix,
        "checkpoint": checkpoint_prefix,
        "dead_letter": dead_letter_prefix,
        "dead_letter_checkpoint": dead_letter_checkpoint_prefix,
    }
    seen = {}
    for label, prefix in unique_paths.items():
        if prefix in seen:
            errors.append(
                f"S3 paths must be unique: {label} and {seen[prefix]} both use {prefix!r}."
            )
        else:
            seen[prefix] = label

    return errors


def config_errors() -> list[str]:
    return [
        *aws_credential_errors(),
        *kafka_config_errors(),
        *s3_config_errors(),
    ]


def validate_aws_credentials() -> None:
    _raise_config_errors(aws_credential_errors())


def validate_config() -> None:
    _raise_config_errors(config_errors())


def _raise_config_errors(errors: list[str]) -> None:
    if errors:
        raise ValueError("\n".join(errors))
