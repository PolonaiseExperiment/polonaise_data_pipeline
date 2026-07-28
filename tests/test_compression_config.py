"""
Tests for compression/priority config and orchestrator routing.

Author: tunnell (https://github.com/tunnell)
"""

import os
from pathlib import Path

import pytest

from pipeline.config import Config


def _base_config(tmp_path, **overrides) -> Config:
    key = tmp_path / "key"
    key.write_text("fake")
    defaults = dict(
        ssh_host="example.com",
        ssh_username="user",
        ssh_key_path=key,
        local_source_path=tmp_path,
        remote_dest_path="/data/uncompressed",
        database_path=tmp_path / "db.json",
        max_parallel_transfers=2,
        skip_recent_minutes=0,
        slack_webhook_url=None,
        full_verify_hour=3,
    )
    defaults.update(overrides)
    return Config(**defaults)


class TestCompressionConfig:
    def test_from_env_parses_compression_fields(self, tmp_path, monkeypatch):
        key = tmp_path / "key"
        key.write_text("fake")
        for k, v in {
            "SSH_HOST": "h", "SSH_USERNAME": "u", "SSH_KEY_PATH": str(key),
            "LOCAL_SOURCE_PATH": str(tmp_path),
            "REMOTE_DEST_PATH": "/data/uncompressed",
            "DATABASE_PATH": str(tmp_path / "db.json"),
            "COMPRESSION_ENABLED": "true",
            "REMOTE_COMPRESSED_PATH": "/data/compressed",
            "COMPRESSION_MIN_BYTES": "1234",
            "REMOTE_PYTHON": "/venv/bin/python3",
            "REMOTE_REPO_PATH": "/repo",
            "REMOTE_DB_PATH": "/db/pipeline.json",
            "PRIORITY_DIRS": "Run48_2026_May, Run49_2026_Juli",
        }.items():
            monkeypatch.setenv(k, v)

        config = Config.from_env(env_path=tmp_path / "nonexistent.env")
        assert config.compression_enabled is True
        assert config.remote_compressed_path == "/data/compressed"
        assert config.compression_min_bytes == 1234
        assert config.remote_python == "/venv/bin/python3"
        assert config.remote_repo_path == "/repo"
        assert config.remote_db_path == "/db/pipeline.json"
        assert config.priority_dirs == ["Run48_2026_May", "Run49_2026_Juli"]

    def test_compression_defaults_off(self, tmp_path):
        config = _base_config(tmp_path)
        assert config.compression_enabled is False
        assert config.priority_dirs == []
        assert config.validate() == []

    def test_compression_requires_remote_settings(self, tmp_path):
        config = _base_config(tmp_path, compression_enabled=True)
        errors = config.validate()
        assert any("REMOTE_COMPRESSED_PATH" in e for e in errors)
        assert any("REMOTE_PYTHON" in e for e in errors)
        assert any("REMOTE_REPO_PATH" in e for e in errors)

    def test_compression_valid_when_complete(self, tmp_path):
        config = _base_config(
            tmp_path,
            compression_enabled=True,
            remote_compressed_path="/data/compressed",
            remote_python="/venv/bin/python3",
            remote_repo_path="/repo",
        )
        assert config.validate() == []

    def test_priority_dirs_must_be_top_level_names(self, tmp_path):
        config = _base_config(tmp_path, priority_dirs=["a/b"])
        assert any("PRIORITY_DIRS" in e for e in config.validate())


class TestOrchestratorRouting:
    @pytest.fixture
    def orchestrator(self, tmp_path):
        from pipeline.orchestrator import SyncOrchestrator

        config = _base_config(
            tmp_path,
            compression_enabled=True,
            remote_compressed_path="/data/compressed",
            remote_python="/venv/bin/python3",
            remote_repo_path="/repo",
            priority_dirs=["Run48_2026_May"],
        )
        orch = SyncOrchestrator(config)
        yield orch
        orch.close()

    def test_tdms_index_routes_to_compressed_tree(self, orchestrator):
        assert orchestrator._desired_tree(".tdms_index") == "compressed"
        assert orchestrator._desired_tree(".TDMS_INDEX") == "compressed"
        assert orchestrator._desired_tree(".tdms") == "uncompressed"
        assert orchestrator._desired_tree(".csv") == "uncompressed"

    def test_remote_path_follows_tree(self, orchestrator):
        from pipeline.database import FileRecord

        record = FileRecord(
            file_path="Run48_2026_May\\TDMS\\x.tdms_index",
            file_name="x.tdms_index", file_extension=".tdms_index",
            file_size_bytes=10, remote_tree="compressed")
        assert orchestrator._remote_path_for(record) == \
            "/data/compressed/Run48_2026_May/TDMS/x.tdms_index"
        record.remote_tree = "uncompressed"
        assert orchestrator._remote_path_for(record) == \
            "/data/uncompressed/Run48_2026_May/TDMS/x.tdms_index"

    def test_priority_rank(self, orchestrator):
        assert orchestrator._priority_rank("Run48_2026_May\\TDMS\\x.tdms") == 0
        assert orchestrator._priority_rank("Run45_2025_December_UHDM/x.tdms") == 1

    def test_compression_eligibility(self, orchestrator, tmp_path):
        from pipeline.database import FileRecord

        big = FileRecord(file_path="a.tdms", file_name="a.tdms",
                         file_extension=".tdms",
                         file_size_bytes=20 * 1024 * 1024)
        small = FileRecord(file_path="b.tdms", file_name="b.tdms",
                           file_extension=".tdms", file_size_bytes=100)
        csv = FileRecord(file_path="c.csv", file_name="c.csv",
                         file_extension=".csv",
                         file_size_bytes=20 * 1024 * 1024)
        assert orchestrator._compression_eligible(Path("a.tdms"), big)
        assert not orchestrator._compression_eligible(Path("b.tdms"), small)
        assert not orchestrator._compression_eligible(Path("c.csv"), csv)
