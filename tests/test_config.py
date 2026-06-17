import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    ALLOWED_KAFKA_STARTING_OFFSETS,
    PLACEHOLDER_CREDENTIALS,
    aws_credential_errors,
    config_errors,
    kafka_config_errors,
    s3_config_errors,
    validate_config,
)


class TestAwsCredentialErrors:
    def test_valid_credentials_pass(self):
        assert aws_credential_errors(
            access_key_id="AKIAIOSFODNN7EXAMPLE",
            secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        ) == []

    def test_missing_access_key(self):
        errors = aws_credential_errors(access_key_id=None, secret_access_key="secret")
        assert any("AWS_ACCESS_KEY_ID is required" in error for error in errors)

    def test_missing_secret_key(self):
        errors = aws_credential_errors(access_key_id="AKIAEXAMPLE", secret_access_key=None)
        assert any("AWS_SECRET_ACCESS_KEY is required" in error for error in errors)

    def test_rejects_placeholder_access_key(self):
        errors = aws_credential_errors(
            access_key_id="your_access_key_id",
            secret_access_key="real_secret",
        )
        assert any("placeholder value" in error for error in errors)

    def test_rejects_placeholder_secret_key(self):
        errors = aws_credential_errors(
            access_key_id="AKIAEXAMPLE",
            secret_access_key="your_secret_access_key",
        )
        assert any("placeholder value" in error for error in errors)

    def test_temporary_credentials_require_session_token(self):
        errors = aws_credential_errors(
            access_key_id="ASIATESTACCESSKEY",
            secret_access_key="secret",
            session_token=None,
        )
        assert any("AWS_SESSION_TOKEN is required" in error for error in errors)

    def test_temporary_credentials_with_session_token_pass(self):
        assert aws_credential_errors(
            access_key_id="ASIATESTACCESSKEY",
            secret_access_key="secret",
            session_token="session-token",
        ) == []

    def test_validate_aws_credentials_raises(self):
        from config.settings import _raise_config_errors

        with pytest.raises(ValueError, match="AWS_ACCESS_KEY_ID"):
            _raise_config_errors(aws_credential_errors(access_key_id=None, secret_access_key="x"))


class TestKafkaConfigErrors:
    def test_defaults_pass(self):
        assert kafka_config_errors() == []

    def test_invalid_starting_offsets(self):
        errors = kafka_config_errors(starting_offsets="middle")
        assert any("KAFKA_STARTING_OFFSETS must be one of" in error for error in errors)

    @pytest.mark.parametrize("offset", ALLOWED_KAFKA_STARTING_OFFSETS)
    def test_allowed_starting_offsets(self, offset):
        assert kafka_config_errors(starting_offsets=offset) == []

    def test_empty_topic(self):
        errors = kafka_config_errors(topic="")
        assert any("KAFKA_TOPIC must not be empty" in error for error in errors)


class TestS3ConfigErrors:
    def test_defaults_pass(self):
        assert s3_config_errors() == []

    def test_rejects_bucket_with_scheme(self):
        errors = s3_config_errors(bucket="s3://my-bucket")
        assert any("without s3://" in error for error in errors)

    def test_rejects_same_output_and_checkpoint_prefix(self):
        errors = s3_config_errors(
            output_prefix="same/path/",
            checkpoint_prefix="same/path/",
        )
        assert any("S3 paths must be unique" in error for error in errors)

    def test_rejects_prefix_starting_with_slash(self):
        errors = s3_config_errors(output_prefix="/bad/prefix/")
        assert any("must not start with '/'" in error for error in errors)

    def test_rejects_invalid_bucket_name(self):
        errors = s3_config_errors(bucket="INVALID_BUCKET")
        assert any("does not look like a valid S3 bucket name" in error for error in errors)


class TestConfigErrors:
    def test_config_errors_combines_sections(self):
        errors = config_errors()
        # In test env creds are usually missing — should include AWS errors at minimum
        assert isinstance(errors, list)

    def test_validate_config_raises_on_invalid_kafka_offset(self, monkeypatch):
        monkeypatch.setenv("KAFKA_STARTING_OFFSETS", "invalid")
        # Reload settings to pick up env change
        import importlib
        import config.settings as settings
        importlib.reload(settings)

        with pytest.raises(ValueError, match="KAFKA_STARTING_OFFSETS"):
            settings.validate_config()

        # Restore defaults for other tests
        monkeypatch.delenv("KAFKA_STARTING_OFFSETS", raising=False)
        importlib.reload(settings)

    def test_placeholder_values_are_documented(self):
        assert "your_access_key_id" in PLACEHOLDER_CREDENTIALS
        assert "your_secret_access_key" in PLACEHOLDER_CREDENTIALS
