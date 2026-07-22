"""
Tests for configuration module.

Author: tunnell (https://github.com/tunnell)
"""

import os
import tempfile
from pathlib import Path

import pytest

from pipeline.config import Config, _parse_ignore_dirs, _parse_bool


class TestConfig:
    """Tests for Config class."""

    def test_from_env_with_all_values(self, tmp_path):
        """Test loading config from environment."""
        # Create a temp SSH key file
        ssh_key = tmp_path / "test_key"
        ssh_key.write_text("fake key")

        # Create a temp source directory
        source = tmp_path / "source"
        source.mkdir()

        # Set environment variables
        os.environ["SSH_HOST"] = "test.example.com"
        os.environ["SSH_USERNAME"] = "testuser"
        os.environ["SSH_KEY_PATH"] = str(ssh_key)
        os.environ["LOCAL_SOURCE_PATH"] = str(source)
        os.environ["REMOTE_DEST_PATH"] = "/remote/path"
        os.environ["DATABASE_PATH"] = str(tmp_path / "db.json")
        os.environ["MAX_PARALLEL_TRANSFERS"] = "5"
        os.environ["SKIP_RECENT_MINUTES"] = "30"

        try:
            config = Config.from_env()

            assert config.ssh_host == "test.example.com"
            assert config.ssh_username == "testuser"
            assert config.ssh_key_path == ssh_key
            assert config.local_source_path == source
            assert config.remote_dest_path == "/remote/path"
            assert config.max_parallel_transfers == 5
            assert config.skip_recent_minutes == 30

        finally:
            # Clean up environment
            for key in ["SSH_HOST", "SSH_USERNAME", "SSH_KEY_PATH",
                        "LOCAL_SOURCE_PATH", "REMOTE_DEST_PATH", "DATABASE_PATH",
                        "MAX_PARALLEL_TRANSFERS", "SKIP_RECENT_MINUTES"]:
                os.environ.pop(key, None)

    def test_from_env_missing_required(self, tmp_path):
        """Test that missing required values raises error."""
        # Clear all relevant env vars
        for key in ["SSH_HOST", "SSH_USERNAME", "SSH_KEY_PATH",
                    "LOCAL_SOURCE_PATH", "REMOTE_DEST_PATH", "DATABASE_PATH"]:
            os.environ.pop(key, None)

        # Use a non-existent .env path so load_dotenv doesn't pick up the real one
        with pytest.raises(ValueError) as exc_info:
            Config.from_env(env_path=tmp_path / "nonexistent.env")

        assert "Missing required config" in str(exc_info.value)

    def test_validate_missing_ssh_key(self, tmp_path):
        """Test validation catches missing SSH key."""
        config = Config(
            ssh_host="test.com",
            ssh_username="user",
            ssh_key_path=Path("/nonexistent/key"),
            local_source_path=tmp_path,
            remote_dest_path="/remote",
            database_path=tmp_path / "db.json",
            max_parallel_transfers=3,
            skip_recent_minutes=15,
            slack_webhook_url=None,
            full_verify_hour=3
        )

        errors = config.validate()
        assert any("SSH key not found" in e for e in errors)

    def test_validate_missing_source(self, tmp_path):
        """Test validation catches missing source path."""
        ssh_key = tmp_path / "key"
        ssh_key.write_text("fake")

        config = Config(
            ssh_host="test.com",
            ssh_username="user",
            ssh_key_path=ssh_key,
            local_source_path=Path("/nonexistent/source"),
            remote_dest_path="/remote",
            database_path=tmp_path / "db.json",
            max_parallel_transfers=3,
            skip_recent_minutes=15,
            slack_webhook_url=None,
            full_verify_hour=3
        )

        errors = config.validate()
        assert any("Source path not found" in e for e in errors)

    def test_validate_valid_config(self, tmp_path):
        """Test validation passes for valid config."""
        ssh_key = tmp_path / "key"
        ssh_key.write_text("fake")

        source = tmp_path / "source"
        source.mkdir()

        config = Config(
            ssh_host="test.com",
            ssh_username="user",
            ssh_key_path=ssh_key,
            local_source_path=source,
            remote_dest_path="/remote",
            database_path=tmp_path / "db.json",
            max_parallel_transfers=3,
            skip_recent_minutes=15,
            slack_webhook_url=None,
            full_verify_hour=3
        )

        errors = config.validate()
        assert len(errors) == 0

    def test_parse_ignore_dirs_comma_separated(self):
        """Test parsing a comma-separated ignore list."""
        assert _parse_ignore_dirs("a, b ,c", None) == ["a", "b", "c"]

    def test_parse_ignore_dirs_preserves_internal_spaces(self):
        """Test that spaces inside folder names are preserved."""
        raw = "00 - TDMS_2020_Jan_and_2021_Sep_unsorted"
        assert _parse_ignore_dirs(raw, None) == ["00 - TDMS_2020_Jan_and_2021_Sep_unsorted"]

    def test_parse_ignore_dirs_empty(self):
        """Test that missing or empty input yields an empty list."""
        assert _parse_ignore_dirs(None, None) == []
        assert _parse_ignore_dirs("", None) == []

    def test_parse_ignore_dirs_from_file(self, tmp_path):
        """Test reading the ignore list from a file, skipping comments and blanks."""
        list_file = tmp_path / "ignore.txt"
        list_file.write_text(
            "# skip these old runs\n"
            "Run12_2020_August\n"
            "\n"
            "00 - TDMS_2020_Jan_and_2021_Sep_unsorted\n"
        )

        result = _parse_ignore_dirs(None, list_file)

        assert result == [
            "Run12_2020_August",
            "00 - TDMS_2020_Jan_and_2021_Sep_unsorted"
        ]

    def test_ignore_file_takes_precedence_over_env(self, tmp_path):
        """Test that the ignore list file wins over the comma-separated string."""
        list_file = tmp_path / "ignore.txt"
        list_file.write_text("FromFile\n")

        result = _parse_ignore_dirs("FromEnv1,FromEnv2", list_file)

        assert result == ["FromFile"]

    def test_parse_bool(self):
        """Test boolean parsing from env var strings."""
        assert _parse_bool(None, True) is True
        assert _parse_bool("false", True) is False
        assert _parse_bool("yes", False) is True
        assert _parse_bool("1", False) is True

    def test_config_defaults_ignore_empty(self, tmp_path):
        """Test that ignore settings have safe defaults."""
        ssh_key = tmp_path / "key"
        ssh_key.write_text("fake")

        source = tmp_path / "source"
        source.mkdir()

        config = Config(
            ssh_host="test.com",
            ssh_username="user",
            ssh_key_path=ssh_key,
            local_source_path=source,
            remote_dest_path="/remote",
            database_path=tmp_path / "db.json",
            max_parallel_transfers=3,
            skip_recent_minutes=15,
            slack_webhook_url=None,
            full_verify_hour=3
        )

        assert config.ignore_dirs == []
        assert config.skip_root_files is True
