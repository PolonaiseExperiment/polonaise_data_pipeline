"""
Tests for database module.

Author: tunnell (https://github.com/tunnell)
"""

import tempfile
from pathlib import Path
from datetime import datetime

import pytest

from pipeline.database import (
    FileDatabase, FileRecord, SyncState,
    TransferStatus, DataQuality
)


@pytest.fixture
def db(tmp_path):
    """Create a temporary database."""
    db_path = tmp_path / "test_pipeline.json"
    database = FileDatabase(db_path)
    yield database
    database.close()


class TestFileRecord:
    """Tests for FileRecord dataclass."""

    def test_to_dict(self):
        """Test converting record to dictionary."""
        record = FileRecord(
            file_path="test/file.tdms",
            file_name="file.tdms",
            file_extension=".tdms",
            file_size_bytes=1024
        )
        d = record.to_dict()

        assert d["file_path"] == "test/file.tdms"
        assert d["file_size_bytes"] == 1024
        assert d["transfer_status"] == TransferStatus.PENDING.value

    def test_from_dict(self):
        """Test creating record from dictionary."""
        data = {
            "file_path": "test/file.tdms",
            "file_name": "file.tdms",
            "file_extension": ".tdms",
            "file_size_bytes": 2048,
            "transfer_status": TransferStatus.VERIFIED.value
        }
        record = FileRecord.from_dict(data)

        assert record.file_path == "test/file.tdms"
        assert record.file_size_bytes == 2048
        assert record.transfer_status == TransferStatus.VERIFIED.value


class TestFileDatabase:
    """Tests for FileDatabase class."""

    def test_upsert_and_get(self, db):
        """Test inserting and retrieving a record."""
        record = FileRecord(
            file_path="data/test.tdms",
            file_name="test.tdms",
            file_extension=".tdms",
            file_size_bytes=1000,
            local_checksum="abc123"
        )

        db.upsert_file(record)
        retrieved = db.get_file("data/test.tdms")

        assert retrieved is not None
        assert retrieved.file_path == "data/test.tdms"
        assert retrieved.local_checksum == "abc123"

    def test_upsert_updates_existing(self, db):
        """Test that upsert updates existing records."""
        record = FileRecord(
            file_path="data/test.tdms",
            file_name="test.tdms",
            file_extension=".tdms",
            file_size_bytes=1000
        )
        db.upsert_file(record)

        # Update the record
        record.file_size_bytes = 2000
        record.local_checksum = "updated"
        db.upsert_file(record)

        retrieved = db.get_file("data/test.tdms")
        assert retrieved.file_size_bytes == 2000
        assert retrieved.local_checksum == "updated"

    def test_get_nonexistent(self, db):
        """Test getting a record that doesn't exist."""
        result = db.get_file("nonexistent/file.tdms")
        assert result is None

    def test_delete_file(self, db):
        """Test deleting a record."""
        record = FileRecord(
            file_path="to/delete.tdms",
            file_name="delete.tdms",
            file_extension=".tdms",
            file_size_bytes=100
        )
        db.upsert_file(record)

        assert db.delete_file("to/delete.tdms") is True
        assert db.get_file("to/delete.tdms") is None

    def test_delete_nonexistent(self, db):
        """Test deleting a nonexistent record."""
        assert db.delete_file("nonexistent.tdms") is False

    def test_get_files_by_status(self, db):
        """Test filtering files by status."""
        for i, status in enumerate([
            TransferStatus.PENDING,
            TransferStatus.PENDING,
            TransferStatus.VERIFIED,
            TransferStatus.FAILED
        ]):
            record = FileRecord(
                file_path=f"file{i}.tdms",
                file_name=f"file{i}.tdms",
                file_extension=".tdms",
                file_size_bytes=100,
                transfer_status=status.value
            )
            db.upsert_file(record)

        pending = db.get_files_by_status(TransferStatus.PENDING)
        assert len(pending) == 2

        verified = db.get_files_by_status(TransferStatus.VERIFIED)
        assert len(verified) == 1

    def test_get_pending_transfers(self, db):
        """Test getting files that need transfer."""
        statuses = [
            TransferStatus.PENDING,
            TransferStatus.FAILED,
            TransferStatus.CHECKSUM_MISMATCH,
            TransferStatus.VERIFIED,
            TransferStatus.TRANSFERRED
        ]

        for i, status in enumerate(statuses):
            record = FileRecord(
                file_path=f"file{i}.tdms",
                file_name=f"file{i}.tdms",
                file_extension=".tdms",
                file_size_bytes=100,
                transfer_status=status.value
            )
            db.upsert_file(record)

        pending = db.get_pending_transfers()
        # Should get PENDING, FAILED, and CHECKSUM_MISMATCH
        assert len(pending) == 3

    def test_count_by_status(self, db):
        """Test counting files by status."""
        for i in range(5):
            status = TransferStatus.PENDING if i < 3 else TransferStatus.VERIFIED
            record = FileRecord(
                file_path=f"file{i}.tdms",
                file_name=f"file{i}.tdms",
                file_extension=".tdms",
                file_size_bytes=100,
                transfer_status=status.value
            )
            db.upsert_file(record)

        counts = db.count_by_status()
        assert counts[TransferStatus.PENDING.value] == 3
        assert counts[TransferStatus.VERIFIED.value] == 2

    def test_sync_state(self, db):
        """Test sync state operations."""
        # Initial state should be empty
        state = db.get_sync_state()
        assert state.last_scan_time is None

        # Update state
        now = datetime.now()
        db.set_last_scan_time(now)

        state = db.get_sync_state()
        assert state.last_scan_time is not None

        retrieved_time = db.get_last_scan_time()
        assert retrieved_time is not None
        # Allow small time difference
        assert abs((retrieved_time - now).total_seconds()) < 1

    def test_get_all_files_iterator(self, db):
        """Test iterating over all files."""
        for i in range(3):
            record = FileRecord(
                file_path=f"file{i}.tdms",
                file_name=f"file{i}.tdms",
                file_extension=".tdms",
                file_size_bytes=100 * (i + 1)
            )
            db.upsert_file(record)

        all_files = list(db.get_all_files())
        assert len(all_files) == 3
        assert sum(f.file_size_bytes for f in all_files) == 600

    def test_context_manager(self, tmp_path):
        """Test database as context manager."""
        db_path = tmp_path / "context_test.json"

        with FileDatabase(db_path) as db:
            record = FileRecord(
                file_path="test.tdms",
                file_name="test.tdms",
                file_extension=".tdms",
                file_size_bytes=100
            )
            db.upsert_file(record)

        # Should be able to reopen
        with FileDatabase(db_path) as db:
            retrieved = db.get_file("test.tdms")
            assert retrieved is not None
