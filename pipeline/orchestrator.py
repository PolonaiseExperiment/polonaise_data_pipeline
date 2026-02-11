"""
Main orchestration logic for the data pipeline.

Author: tunnell (https://github.com/tunnell)
"""

from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm

from .config import Config, get_config
from .database import FileDatabase, FileRecord, TransferStatus, SyncState
from .scanner import FileScanner
from .checksum import ChecksumManager
from .transfer import SSHConnection, TransferManager, TransferResult
from .slack import SlackNotifier


@dataclass
class SyncStats:
    """Statistics from a sync run."""
    files_scanned: int = 0
    files_skipped_recent: int = 0
    files_skipped_not_modified: int = 0
    files_new: int = 0
    files_transferred: int = 0
    files_failed: int = 0
    files_verified: int = 0
    files_checksum_mismatch: int = 0
    checksums_computed: int = 0
    checksums_compared: int = 0
    bytes_transferred: int = 0
    duration_seconds: float = 0.0
    errors: List[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class SyncOrchestrator:
    """Orchestrates the sync process."""

    def __init__(self, config: Optional[Config] = None):
        """Initialize orchestrator.

        Args:
            config: Pipeline configuration. If None, loads from environment.
        """
        self.config = config or get_config()
        self.db = FileDatabase(self.config.database_path)
        self.scanner = FileScanner(
            self.config.local_source_path,
            skip_recent_minutes=self.config.skip_recent_minutes
        )
        self.checksum = ChecksumManager()
        self.transfer = TransferManager(self.config)
        self.slack = SlackNotifier(self.config.slack_webhook_url)

    def close(self):
        """Close database connection."""
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def check_connections(self) -> Tuple[bool, str]:
        """Check that source and destination are accessible.

        Returns:
            Tuple of (success, message).
        """
        # Check source
        if not self.config.local_source_path.exists():
            return False, f"Source not found: {self.config.local_source_path}"

        # Check SSH
        try:
            with SSHConnection(self.config) as conn:
                conn.get_ssh().exec_command("echo OK", timeout=10)
            return True, "All connections OK"
        except Exception as e:
            return False, f"SSH connection failed: {e}"

    def _process_file(
        self,
        file_path: Path,
        conn: SSHConnection,
        stats: SyncStats
    ) -> Optional[FileRecord]:
        """Process a single file: create record, check remote, transfer if needed.

        Returns the updated FileRecord or None on error.
        """
        relative_path = self.scanner.get_relative_path(file_path)
        remote_path = f"{self.config.remote_dest_path}/{relative_path}".replace("\\", "/")

        # Get or create database record
        record = self.db.get_file(relative_path)
        now = datetime.now().isoformat()

        if record is None:
            # New file
            stats.files_new += 1
            file_info = self.scanner.get_file_info(file_path)

            # Compute local checksum
            local_checksum = self.checksum.compute_local(file_path)

            record = FileRecord(
                file_path=relative_path,
                file_name=file_info["file_name"],
                file_extension=file_info["file_extension"],
                file_size_bytes=file_info["file_size_bytes"],
                file_mtime=file_info["file_mtime"],
                file_ctime=file_info["file_ctime"],
                filename_timestamp=file_info["filename_timestamp"],
                local_checksum=local_checksum,
                checksum_algorithm="xxh64",
                first_seen=now,
                transfer_status=TransferStatus.PENDING.value
            )
        else:
            # Existing file - check if changed
            current_size = file_path.stat().st_size
            if current_size != record.file_size_bytes:
                # File changed, recompute checksum
                record.file_size_bytes = current_size
                record.local_checksum = self.checksum.compute_local(file_path)
                record.transfer_status = TransferStatus.PENDING.value

        # Check if file needs transfer
        needs_transfer = record.transfer_status in [
            TransferStatus.PENDING.value,
            TransferStatus.FAILED.value,
            TransferStatus.CHECKSUM_MISMATCH.value
        ]

        if not needs_transfer:
            return record

        # Check if remote file exists
        remote_exists = conn.file_exists(remote_path)

        if remote_exists:
            # Verify remote checksum
            remote_checksum, error = self.checksum.compute_remote(conn.get_ssh(), remote_path)

            if error:
                record.last_error = error
                record.last_error_time = now
                stats.errors.append(f"{relative_path}: {error}")
                self.db.upsert_file(record)
                return record

            # Compare checksums (both using xxhash)
            if remote_checksum == record.local_checksum:
                # Already transferred correctly
                record.remote_checksum = remote_checksum
                record.transfer_status = TransferStatus.VERIFIED.value
                record.last_verified_remote = now
                stats.files_verified += 1
                self.db.upsert_file(record)
                return record
            else:
                # Checksum mismatch - need to retransfer
                record.failed_checksum_count += 1
                stats.files_checksum_mismatch += 1

        # Transfer the file
        record.transfer_status = TransferStatus.TRANSFERRING.value
        record.transfer_attempts += 1
        record.last_transfer_time = now
        self.db.upsert_file(record)

        result = self.transfer.transfer_file(
            conn, file_path, relative_path, show_progress=True
        )

        if result.success:
            stats.files_transferred += 1
            stats.bytes_transferred += result.bytes_transferred

            # Verify transfer
            remote_checksum, error = self.checksum.compute_remote(conn.get_ssh(), remote_path)

            if error:
                record.transfer_status = TransferStatus.FAILED.value
                record.last_error = f"Verification failed: {error}"
                record.last_error_time = now
                stats.files_failed += 1
            else:
                record.remote_checksum = remote_checksum
                record.transfer_status = TransferStatus.VERIFIED.value
                record.last_verified_remote = now
        else:
            stats.files_failed += 1
            record.transfer_status = TransferStatus.FAILED.value
            record.last_error = result.error
            record.last_error_time = now
            stats.errors.append(f"{relative_path}: {result.error}")

        self.db.upsert_file(record)
        return record

    def _prepare_file(self, file_path: Path, conn: SSHConnection, stats: SyncStats) -> Tuple[Path, FileRecord, bool]:
        """Prepare a file for transfer: compute checksum, check remote status.

        Returns:
            Tuple of (file_path, record, needs_transfer)
        """
        relative_path = self.scanner.get_relative_path(file_path)
        remote_path = f"{self.config.remote_dest_path}/{relative_path}".replace("\\", "/")
        now = datetime.now().isoformat()

        # Get or create database record
        record = self.db.get_file(relative_path)

        if record is None:
            # New file
            file_info = self.scanner.get_file_info(file_path)
            local_checksum = self.checksum.compute_local(file_path)
            stats.checksums_computed += 1

            record = FileRecord(
                file_path=relative_path,
                file_name=file_info["file_name"],
                file_extension=file_info["file_extension"],
                file_size_bytes=file_info["file_size_bytes"],
                file_mtime=file_info["file_mtime"],
                file_ctime=file_info["file_ctime"],
                filename_timestamp=file_info["filename_timestamp"],
                local_checksum=local_checksum,
                checksum_algorithm="xxh64",
                first_seen=now,
                transfer_status=TransferStatus.PENDING.value
            )
            self.db.upsert_file(record)
        else:
            # Check if file changed
            current_size = file_path.stat().st_size
            if current_size != record.file_size_bytes:
                record.file_size_bytes = current_size
                record.local_checksum = self.checksum.compute_local(file_path)
                stats.checksums_computed += 1
                record.transfer_status = TransferStatus.PENDING.value
                self.db.upsert_file(record)

        # Check if needs transfer
        if record.transfer_status not in [
            TransferStatus.PENDING.value,
            TransferStatus.FAILED.value,
            TransferStatus.CHECKSUM_MISMATCH.value
        ]:
            return file_path, record, False

        # Check remote
        remote_exists = conn.file_exists(remote_path)
        if remote_exists:
            remote_checksum, error = self.checksum.compute_remote(conn.get_ssh(), remote_path)
            stats.checksums_computed += 1
            stats.checksums_compared += 1
            if not error and remote_checksum == record.local_checksum:
                # Already on remote with matching checksum
                record.remote_checksum = remote_checksum
                record.transfer_status = TransferStatus.VERIFIED.value
                record.last_verified_remote = now
                self.db.upsert_file(record)
                return file_path, record, False

        return file_path, record, True

    def _transfer_and_verify(
        self,
        file_path: Path,
        record: FileRecord
    ) -> Tuple[FileRecord, TransferResult]:
        """Transfer a single file and verify. Each call gets its own SSH connection."""
        relative_path = record.file_path
        remote_path = f"{self.config.remote_dest_path}/{relative_path}".replace("\\", "/")
        now = datetime.now().isoformat()

        with SSHConnection(self.config) as conn:
            result = self.transfer.transfer_file(conn, file_path, relative_path, show_progress=True)

            if result.success:
                # Verify
                remote_checksum, error = self.checksum.compute_remote(conn.get_ssh(), remote_path)
                if error:
                    record.transfer_status = TransferStatus.FAILED.value
                    record.last_error = f"Verification failed: {error}"
                    record.last_error_time = now
                    result = TransferResult(
                        file_path=relative_path,
                        success=False,
                        bytes_transferred=result.bytes_transferred,
                        duration_seconds=result.duration_seconds,
                        error=f"Verification failed: {error}"
                    )
                else:
                    record.remote_checksum = remote_checksum
                    record.transfer_status = TransferStatus.VERIFIED.value
                    record.last_verified_remote = now
            else:
                record.transfer_status = TransferStatus.FAILED.value
                record.last_error = result.error
                record.last_error_time = now

            record.transfer_attempts += 1
            record.last_transfer_time = now

        return record, result

    def run_incremental_sync(self, dry_run: bool = False, verbose: bool = False, full_sync: bool = False) -> SyncStats:
        """Run incremental sync - only process new/modified files.

        Args:
            dry_run: If True, don't actually transfer files.

        Returns:
            SyncStats with results.
        """
        stats = SyncStats()
        start_time = datetime.now()

        # Check connections first
        ok, msg = self.check_connections()
        if not ok:
            self.slack.notify_connection_error(msg)
            stats.errors.append(msg)
            return stats

        # Get last scan time (ignore if full_sync)
        last_scan = None if full_sync else self.db.get_last_scan_time()

        # Scan for files
        if last_scan:
            print(f"Scanning for files modified since {last_scan}")
            if verbose:
                print(f"Skip threshold: files modified < {self.config.skip_recent_minutes} min ago")
            scan_result = self.scanner.scan_since_with_stats(last_scan, verbose=verbose)
        else:
            if full_sync:
                print("Full sync - scanning all files")
            else:
                print("First run - scanning all files")
            if verbose:
                print(f"Skip threshold: files modified < {self.config.skip_recent_minutes} min ago")
            scan_result = self.scanner.scan_all_with_stats(verbose=verbose)

        files = scan_result.files
        stats.files_scanned = len(files)
        stats.files_skipped_recent = scan_result.skipped_recent
        stats.files_skipped_not_modified = scan_result.skipped_not_modified

        print(f"Found {len(files)} files to process")
        if scan_result.skipped_recent > 0:
            print(f"  Skipped (too recent): {scan_result.skipped_recent}")
        if scan_result.skipped_not_modified > 0:
            print(f"  Skipped (not modified): {scan_result.skipped_not_modified}")

        if len(files) == 0:
            print("No files to process")
            self.db.set_last_scan_time(start_time)
            return stats

        if dry_run:
            print("DRY RUN - not transferring files")
            for f in files:
                print(f"  Would process: {f}")
            return stats

        # Notify start
        self.slack.notify_sync_started(len(files))

        # Phase 1: Prepare files (compute checksums, check what needs transfer)
        print(f"\nPhase 1: Checking {len(files)} files...")
        files_to_transfer = []

        with SSHConnection(self.config) as conn:
            for file_path in tqdm(files, desc="Checking files", unit="file"):
                try:
                    _, record, needs_transfer = self._prepare_file(file_path, conn, stats)
                    if needs_transfer:
                        files_to_transfer.append((file_path, record))
                        stats.files_new += 1
                    else:
                        if record.transfer_status == TransferStatus.VERIFIED.value:
                            stats.files_verified += 1
                except Exception as e:
                    stats.errors.append(f"{file_path}: {e}")

        print(f"Files needing transfer: {len(files_to_transfer)}")
        print(f"Files already verified: {stats.files_verified}")

        if len(files_to_transfer) == 0:
            print("No files need transfer")
            self.db.set_last_scan_time(start_time)
            stats.duration_seconds = (datetime.now() - start_time).total_seconds()
            return stats

        # Phase 2: Transfer files in parallel
        max_workers = self.config.max_parallel_transfers
        print(f"\nPhase 2: Transferring {len(files_to_transfer)} files ({max_workers} parallel)...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._transfer_and_verify, fp, rec): rec.file_path
                for fp, rec in files_to_transfer
            }

            for future in as_completed(futures):
                file_path = futures[future]
                try:
                    record, result = future.result()
                    self.db.upsert_file(record)

                    if result.success:
                        stats.files_transferred += 1
                        stats.bytes_transferred += result.bytes_transferred
                        speed_mb = (result.bytes_transferred / 1024 / 1024) / result.duration_seconds if result.duration_seconds > 0 else 0
                        print(f"[OK] {file_path} ({result.bytes_transferred / 1024 / 1024:.1f} MB in {result.duration_seconds:.1f}s, {speed_mb:.1f} MB/s)")
                    else:
                        stats.files_failed += 1
                        stats.errors.append(f"{file_path}: {result.error}")
                        print(f"[FAIL] {file_path}: {result.error}")
                except Exception as e:
                    stats.files_failed += 1
                    stats.errors.append(f"{file_path}: {e}")
                    print(f"[FAIL] {file_path}: {e}")

        # Sync database to remote
        with SSHConnection(self.config) as conn:
            if self.transfer.sync_database_to_remote(conn, self.config.database_path):
                print("Database synced to remote")
            else:
                print("Warning: Failed to sync database to remote")

        # Update state
        self.db.set_last_scan_time(start_time)
        stats.duration_seconds = (datetime.now() - start_time).total_seconds()

        # Notify completion
        self.slack.notify_sync_complete(
            transferred=stats.files_transferred,
            failed=stats.files_failed,
            bytes_transferred=stats.bytes_transferred,
            duration_seconds=stats.duration_seconds
        )

        return stats

    def run_full_verification(self) -> SyncStats:
        """Run full verification of all tracked files.

        Returns:
            SyncStats with verification results.
        """
        stats = SyncStats()
        start_time = datetime.now()

        # Check connections
        ok, msg = self.check_connections()
        if not ok:
            self.slack.notify_connection_error(msg)
            stats.errors.append(msg)
            return stats

        print("Running full verification of all tracked files...")

        with SSHConnection(self.config) as conn:
            for record in self.db.get_all_files():
                stats.files_scanned += 1
                local_path = self.config.local_source_path / record.file_path
                remote_path = f"{self.config.remote_dest_path}/{record.file_path}".replace("\\", "/")

                # Verify local
                if not local_path.exists():
                    stats.errors.append(f"Local file missing: {record.file_path}")
                    continue

                local_checksum = self.checksum.compute_local(local_path)
                if local_checksum != record.local_checksum:
                    stats.files_checksum_mismatch += 1
                    stats.errors.append(f"Local checksum changed: {record.file_path}")
                    record.local_checksum = local_checksum
                    record.transfer_status = TransferStatus.PENDING.value
                    self.db.upsert_file(record)
                    continue

                # Verify remote
                remote_checksum, error = self.checksum.compute_remote(conn.get_ssh(), remote_path)

                if error:
                    stats.files_failed += 1
                    stats.errors.append(f"Remote verification failed: {record.file_path}: {error}")
                    continue

                # Compare checksums (both using xxhash)
                if remote_checksum == local_checksum:
                    stats.files_verified += 1
                    record.last_verified_local = datetime.now().isoformat()
                    record.last_verified_remote = datetime.now().isoformat()
                    self.db.upsert_file(record)
                else:
                    stats.files_checksum_mismatch += 1
                    record.failed_checksum_count += 1
                    record.transfer_status = TransferStatus.CHECKSUM_MISMATCH.value
                    self.db.upsert_file(record)
                    stats.errors.append(f"Checksum mismatch: {record.file_path}")

        # Update state
        self.db.update_sync_state(last_full_verify_time=datetime.now().isoformat())
        stats.duration_seconds = (datetime.now() - start_time).total_seconds()

        # Notify
        self.slack.notify_verification_complete(
            total_files=stats.files_scanned,
            verified_ok=stats.files_verified,
            mismatches=stats.files_checksum_mismatch,
            errors=stats.files_failed
        )

        return stats

    def get_status(self) -> dict:
        """Get current pipeline status."""
        state = self.db.get_sync_state()
        counts = self.db.count_by_status()

        return {
            "total_files": self.db.count_files(),
            "status_counts": counts,
            "last_scan": state.last_scan_time,
            "last_full_verify": state.last_full_verify_time,
            "total_bytes": state.total_bytes_tracked
        }
