"""
Database module for tracking file transfer state.

Uses TinyDB (JSON-based) for easy migration to MongoDB later.

Author: tunnell (https://github.com/tunnell)
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Iterator
from dataclasses import dataclass, field, asdict
from enum import Enum

from tinydb import TinyDB, Query
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage
from tinydb.table import Document


class TransferStatus(str, Enum):
    """Status of a file transfer."""
    PENDING = "pending"
    TRANSFERRING = "transferring"
    TRANSFERRED = "transferred"
    VERIFIED = "verified"
    FAILED = "failed"
    CHECKSUM_MISMATCH = "checksum_mismatch"


class DataQuality(str, Enum):
    """Data quality assessment."""
    UNKNOWN = "unknown"
    GOOD = "good"
    BAD = "bad"
    QUARANTINE = "quarantine"


@dataclass
class FileRecord:
    """Record for a tracked file."""

    # Identification
    file_path: str  # Primary key - relative path from source root
    file_name: str
    file_extension: str

    # Size & timestamps
    file_size_bytes: int
    file_mtime: Optional[str] = None  # ISO format
    file_ctime: Optional[str] = None  # ISO format

    # Parsed timestamps
    filename_timestamp: Optional[str] = None
    tdms_start_time: Optional[str] = None
    tdms_end_time: Optional[str] = None

    # Checksums
    local_checksum: Optional[str] = None
    remote_checksum: Optional[str] = None
    checksum_algorithm: str = "xxh64"

    # Compressed transfer (TDMS -> FLAC codec)
    # remote_tree: which remote root this file lives under -
    #   "uncompressed" (REMOTE_DEST_PATH) or "compressed" (REMOTE_COMPRESSED_PATH).
    # For compressed TDMS records, remote_checksum holds the hash of the
    # REBUILT remote .tdms (not the original bytes); local_checksum stays the
    # original file's hash; data_checksum is the xxh64 of the float64 samples
    # and is the end-to-end integrity reference.
    remote_tree: str = "uncompressed"
    compressed: bool = False
    data_checksum: Optional[str] = None
    compressed_checksum: Optional[str] = None  # .flac bytes as sent
    sidecar_checksum: Optional[str] = None  # .json bytes as sent
    compressed_bytes: Optional[int] = None

    # Transfer status
    transfer_status: str = TransferStatus.PENDING.value
    transfer_attempts: int = 0
    last_transfer_time: Optional[str] = None
    failed_checksum_count: int = 0

    # Verification
    first_seen: Optional[str] = None
    last_verified_local: Optional[str] = None
    last_verified_remote: Optional[str] = None

    # Data quality
    data_quality: str = DataQuality.UNKNOWN.value
    data_quality_notes: Optional[str] = None

    # Error tracking
    last_error: Optional[str] = None
    last_error_time: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for database storage."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "FileRecord":
        """Create from dictionary."""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class SyncState:
    """Global sync state."""
    last_scan_time: Optional[str] = None
    last_full_verify_time: Optional[str] = None
    last_db_sync_time: Optional[str] = None
    total_files_tracked: int = 0
    total_bytes_tracked: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "SyncState":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


class FileDatabase:
    """Database for tracking file transfers.

    Uses TinyDB for storage. Interface designed for easy MongoDB migration.
    """

    def __init__(self, db_path: Path):
        """Initialize database.

        Args:
            db_path: Path to the TinyDB JSON file.
        """
        self.db_path = db_path
        self.db = TinyDB(db_path, storage=CachingMiddleware(JSONStorage))
        self.files = self.db.table("files")
        self.state = self.db.table("state")
        self.daemon_control = self.db.table("daemon_control")
        self._query = Query()

    def flush(self):
        """Write the in-memory cache to disk.

        CachingMiddleware batches writes (flushing only every ~1000 writes or
        on close); call this at checkpoints so a killed process loses at most
        the current in-flight records, and before uploading the on-disk file.
        """
        self.db.storage.flush()

    def close(self):
        """Close database connection."""
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # File operations

    def get_file(self, file_path: str) -> Optional[FileRecord]:
        """Get a file record by path."""
        result = self.files.get(self._query.file_path == file_path)
        if result:
            return FileRecord.from_dict(result)
        return None

    def upsert_file(self, record: FileRecord) -> None:
        """Insert or update a file record."""
        self.files.upsert(record.to_dict(), self._query.file_path == record.file_path)

    def delete_file(self, file_path: str) -> bool:
        """Delete a file record. Returns True if deleted."""
        removed = self.files.remove(self._query.file_path == file_path)
        return len(removed) > 0

    def get_files_by_status(self, status: TransferStatus) -> List[FileRecord]:
        """Get all files with a specific transfer status."""
        results = self.files.search(self._query.transfer_status == status.value)
        return [FileRecord.from_dict(r) for r in results]

    def get_pending_transfers(self) -> List[FileRecord]:
        """Get files that need to be transferred."""
        results = self.files.search(
            (self._query.transfer_status == TransferStatus.PENDING.value) |
            (self._query.transfer_status == TransferStatus.FAILED.value) |
            (self._query.transfer_status == TransferStatus.CHECKSUM_MISMATCH.value)
        )
        return [FileRecord.from_dict(r) for r in results]

    def get_unverified_files(self, older_than: Optional[datetime] = None) -> List[FileRecord]:
        """Get files that haven't been verified recently."""
        if older_than:
            threshold = older_than.isoformat()
            results = self.files.search(
                (self._query.last_verified_remote == None) |
                (self._query.last_verified_remote < threshold)
            )
        else:
            results = self.files.search(self._query.last_verified_remote == None)
        return [FileRecord.from_dict(r) for r in results]

    def get_all_files(self) -> Iterator[FileRecord]:
        """Iterate over all file records."""
        for doc in self.files.all():
            yield FileRecord.from_dict(doc)

    def count_files(self) -> int:
        """Get total number of tracked files."""
        return len(self.files)

    def count_by_status(self) -> dict[str, int]:
        """Get count of files by status."""
        counts = {}
        for status in TransferStatus:
            counts[status.value] = len(self.files.search(
                self._query.transfer_status == status.value
            ))
        return counts

    def get_recent_stats(self, hours: int = 6) -> dict:
        """Get stats for files modified in the last N hours.

        Returns dict with:
            total: total files modified recently
            by_status: {status: count} for recent files
            failed_names: list of filenames that failed/mismatched recently
        """
        cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
        recent = self.files.search(self._query.file_mtime > cutoff)

        by_status = {}
        failed_names = []
        for rec in recent:
            status = rec.get("transfer_status", "unknown")
            by_status[status] = by_status.get(status, 0) + 1
            if status in (TransferStatus.FAILED.value,
                          TransferStatus.CHECKSUM_MISMATCH.value,
                          TransferStatus.PENDING.value):
                failed_names.append(rec.get("file_name", rec.get("file_path", "?")))

        return {
            "total": len(recent),
            "by_status": by_status,
            "failed_names": failed_names,
        }

    # Sync state operations

    def get_sync_state(self) -> SyncState:
        """Get the global sync state."""
        results = self.state.all()
        if results:
            return SyncState.from_dict(results[0])
        return SyncState()

    def update_sync_state(self, **kwargs) -> None:
        """Update sync state fields."""
        state = self.get_sync_state()
        for key, value in kwargs.items():
            if hasattr(state, key):
                setattr(state, key, value)

        # Update totals
        state.total_files_tracked = self.count_files()
        state.total_bytes_tracked = sum(
            f.file_size_bytes for f in self.get_all_files()
        )

        self.state.truncate()
        self.state.insert(state.to_dict())

    def set_last_scan_time(self, dt: datetime) -> None:
        """Update last scan time."""
        self.update_sync_state(last_scan_time=dt.isoformat())

    def get_last_scan_time(self) -> Optional[datetime]:
        """Get last scan time."""
        state = self.get_sync_state()
        if state.last_scan_time:
            return datetime.fromisoformat(state.last_scan_time)
        return None

    # Daemon control operations

    def set_daemon_command(self, command: str) -> None:
        """Set a command for the daemon to pick up.

        Valid commands: pause, resume, quit, verbose_on, verbose_off
        """
        self.daemon_control.truncate()
        self.daemon_control.insert({
            "command": command,
            "timestamp": datetime.now().isoformat()
        })

    def get_daemon_command(self) -> Optional[str]:
        """Get and clear pending daemon command. Returns None if no command."""
        results = self.daemon_control.all()
        if results:
            command = results[0].get("command")
            self.daemon_control.truncate()
            return command
        return None
