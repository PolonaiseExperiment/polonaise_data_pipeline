"""
Main orchestration logic for the data pipeline.

Author: tunnell (https://github.com/tunnell)
"""

import sys
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
from .output import PipelineLogger


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

    def __init__(self, config: Optional[Config] = None, logger: Optional[PipelineLogger] = None):
        """Initialize orchestrator.

        Args:
            config: Pipeline configuration. If None, loads from environment.
            logger: Pipeline logger. If None, creates a default one.
        """
        self.config = config or get_config()
        self.logger = logger or PipelineLogger()
        self.db = FileDatabase(self.config.database_path)
        self.scanner = FileScanner(
            self.config.local_source_path,
            skip_recent_minutes=self.config.skip_recent_minutes
        )
        self.checksum = ChecksumManager(self.config)
        self.transfer = TransferManager(self.config)
        self.slack = SlackNotifier(
            self.config.slack_webhook_url,
            timeout=self.config.slack_timeout,
            run_label=self.logger.run_label
        )

    def close(self):
        """Close database connection."""
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _format_recent_stats(self, hours: int = 6) -> str:
        """Format a summary of file activity in the last N hours.

        Returns empty string if nothing noteworthy.
        """
        recent = self.db.get_recent_stats(hours=hours)
        total = recent["total"]
        if total == 0:
            return ""

        by_status = recent["by_status"]
        verified = by_status.get("verified", 0)
        pending = by_status.get("pending", 0)
        failed = by_status.get("failed", 0)
        mismatch = by_status.get("checksum_mismatch", 0)
        problems = pending + failed + mismatch

        parts = [f"Last {hours}h: {total} files modified"]
        if problems > 0:
            parts.append(f"{problems} with issues")
            for name in recent["failed_names"][:5]:
                parts.append(f"  - {name}")
            if len(recent["failed_names"]) > 5:
                parts.append(f"  ... and {len(recent['failed_names']) - 5} more")
        else:
            parts.append(f"all {verified} verified OK")

        return ", ".join(parts[:2]) + (
            "\n" + "\n".join(parts[2:]) if len(parts) > 2 else ""
        )

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
            self.logger.debug(f"Checksum {local_checksum} for {file_info['file_name']}")

            record = FileRecord(
                file_path=relative_path,
                file_name=file_info["file_name"],
                file_extension=file_info["file_extension"],
                file_size_bytes=file_info["file_size_bytes"],
                file_mtime=file_info["file_mtime"],
                file_ctime=file_info["file_ctime"],
                filename_timestamp=file_info["filename_timestamp"],
                local_checksum=local_checksum,
                checksum_algorithm=self.config.checksum_algorithm,
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
                self.logger.debug(f"File changed, new checksum {record.local_checksum} for {record.file_name}")

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
                self.logger.debug(f"Already on remote: {record.file_name} (checksum {remote_checksum})")
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
                elif remote_checksum == record.local_checksum:
                    record.remote_checksum = remote_checksum
                    record.transfer_status = TransferStatus.VERIFIED.value
                    record.last_verified_remote = now
                    self.logger.debug(
                        f"Verified {record.file_name}: "
                        f"local={record.local_checksum} remote={remote_checksum}"
                    )
                else:
                    # Transfer succeeded but checksums don't match
                    record.transfer_status = TransferStatus.CHECKSUM_MISMATCH.value
                    record.failed_checksum_count += 1
                    record.last_error = (
                        f"Post-transfer mismatch: "
                        f"local={record.local_checksum} remote={remote_checksum}"
                    )
                    record.last_error_time = now
                    result = TransferResult(
                        file_path=relative_path,
                        success=False,
                        bytes_transferred=result.bytes_transferred,
                        duration_seconds=result.duration_seconds,
                        error=record.last_error
                    )
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
            verbose: If True, show debug details (overrides logger.verbose for this run).
            full_sync: If True, scan all files ignoring last scan time.

        Returns:
            SyncStats with results.
        """
        # Temporarily set verbose if requested
        old_verbose = self.logger.verbose
        if verbose:
            self.logger.verbose = True

        stats = SyncStats()
        start_time = datetime.now()

        try:
            stats = self._do_incremental_sync(stats, start_time, dry_run, full_sync)
        finally:
            self.logger.verbose = old_verbose

        return stats

    def _do_incremental_sync(self, stats: SyncStats, start_time: datetime,
                              dry_run: bool, full_sync: bool) -> SyncStats:
        """Internal sync logic."""
        log = self.logger

        # Check connections first
        ok, msg = self.check_connections()
        if not ok:
            self.slack.notify_connection_error(msg)
            stats.errors.append(msg)
            log.info(f"Connection error: {msg}")
            return stats

        # Get last scan time (ignore if full_sync)
        last_scan = None if full_sync else self.db.get_last_scan_time()

        # Scan for files
        log.info(f"Scanning {self.config.local_source_path} (may take a while on network drives)...")
        if last_scan:
            scan_result = self.scanner.scan_since_with_stats(
                last_scan, progress_stream=log.stream)
        else:
            scan_result = self.scanner.scan_all_with_stats(
                progress_stream=log.stream)

        files = scan_result.files
        stats.files_scanned = len(files)
        stats.files_skipped_recent = scan_result.skipped_recent
        stats.files_skipped_not_modified = scan_result.skipped_not_modified

        # Report skip details in debug
        skip_parts = []
        if scan_result.skipped_extension > 0:
            skip_parts.append(f"{scan_result.skipped_extension} wrong extension")
        if scan_result.skipped_not_modified > 0:
            skip_parts.append(f"{scan_result.skipped_not_modified} unchanged")
        if scan_result.skipped_recent > 0:
            skip_parts.append(f"{scan_result.skipped_recent} still being written")
        if skip_parts:
            log.debug(f"Skipped: {', '.join(skip_parts)}")

        n = len(files)
        if n == 0:
            total_skipped = (scan_result.skipped_extension +
                             scan_result.skipped_not_modified +
                             scan_result.skipped_recent)
            if total_skipped > 0:
                log.info(f"Nothing to do ({total_skipped} files checked)")
            else:
                log.info("Nothing to do (no files found)")
            self.db.set_last_scan_time(start_time)
            return stats

        log.info(f"Found {n} {'file' if n == 1 else 'files'} to process")

        if dry_run:
            log.info("DRY RUN — not transferring files")
            for f in files:
                log.info(f"  Would process: {f}")
            return stats

        # Notify start
        self.slack.notify_sync_started(len(files))

        # Check which files actually need transfer
        log.info("Checking which files need transfer...")
        files_to_transfer = []

        with SSHConnection(self.config) as conn:
            for file_path in tqdm(files, desc="Checking", unit=" file",
                                  file=log.stream, leave=False):
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

        log.info(f"  {len(files_to_transfer)} {'file needs' if len(files_to_transfer) == 1 else 'files need'} transfer")
        if stats.files_verified > 0:
            log.info(f"  {stats.files_verified} already on remote")

        if len(files_to_transfer) == 0:
            log.info("All files already on remote.")
            self.db.set_last_scan_time(start_time)
            stats.duration_seconds = (datetime.now() - start_time).total_seconds()
            return stats

        # Transfer files in parallel
        max_workers = self.config.max_parallel_transfers
        log.info(f"Transferring {len(files_to_transfer)} {'file' if len(files_to_transfer) == 1 else 'files'}...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._transfer_and_verify, fp, rec): rec
                for fp, rec in files_to_transfer
            }

            for future in as_completed(futures):
                rec = futures[future]
                try:
                    record, result = future.result()
                    self.db.upsert_file(record)

                    if result.success:
                        stats.files_transferred += 1
                        stats.bytes_transferred += result.bytes_transferred
                        mb = result.bytes_transferred / 1024 / 1024
                        log.info(f"  {rec.file_name}  {mb:.1f} MB  {result.duration_seconds:.0f}s  OK")
                        log.debug(
                            f"  Checksum: local={record.local_checksum} "
                            f"remote={record.remote_checksum}"
                        )
                    else:
                        stats.files_failed += 1
                        stats.errors.append(f"{rec.file_path}: {result.error}")
                        log.info(f"  {rec.file_name}  FAILED: {result.error}")
                except Exception as e:
                    stats.files_failed += 1
                    stats.errors.append(f"{rec.file_path}: {e}")
                    log.info(f"  {rec.file_name}  FAILED: {e}")

        # Sync database to remote
        with SSHConnection(self.config) as conn:
            db_synced = self.transfer.sync_database_to_remote(conn, self.config.database_path)
            if not db_synced:
                log.info("Warning: failed to sync database to remote")

        # Update state
        self.db.set_last_scan_time(start_time)
        stats.duration_seconds = (datetime.now() - start_time).total_seconds()

        # Summary line
        mb = stats.bytes_transferred / (1024 * 1024)
        log.info(
            f"Sync complete: {stats.files_transferred} transferred, "
            f"{stats.files_failed} failed, "
            f"{mb:.1f} MB in {stats.duration_seconds:.0f}s"
        )

        # Debug stats
        log.debug(
            f"Checksums computed: {stats.checksums_computed}, "
            f"compared: {stats.checksums_compared}"
        )

        # Recent activity breakdown
        recent_summary = self._format_recent_stats()
        if recent_summary:
            log.info(recent_summary)

        # Notify completion
        self.slack.notify_sync_complete(
            transferred=stats.files_transferred,
            failed=stats.files_failed,
            bytes_transferred=stats.bytes_transferred,
            duration_seconds=stats.duration_seconds,
            recent_summary=recent_summary
        )

        return stats

    def run_full_verification(self) -> SyncStats:
        """Run full verification of all tracked files.

        Returns:
            SyncStats with verification results.
        """
        stats = SyncStats()
        start_time = datetime.now()
        log = self.logger

        # Check connections
        ok, msg = self.check_connections()
        if not ok:
            self.slack.notify_connection_error(msg)
            stats.errors.append(msg)
            log.info(f"Connection error: {msg}")
            return stats

        log.info("Verifying all tracked files...")

        with SSHConnection(self.config) as conn:
            all_files = list(self.db.get_all_files())
            for record in tqdm(all_files, desc="Verifying", unit=" file",
                               file=log.stream, leave=False):
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
                    log.debug(
                        f"Local changed: {record.file_path} "
                        f"was={record.local_checksum} now={local_checksum}"
                    )
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

                if remote_checksum == local_checksum:
                    stats.files_verified += 1
                    record.last_verified_local = datetime.now().isoformat()
                    record.last_verified_remote = datetime.now().isoformat()
                    self.db.upsert_file(record)
                    log.debug(f"OK: {record.file_path} ({remote_checksum})")
                else:
                    stats.files_checksum_mismatch += 1
                    record.failed_checksum_count += 1
                    log.info(
                        f"MISMATCH: {record.file_path} "
                        f"local={local_checksum} remote={remote_checksum}"
                    )

                    # Quarantine the bad remote file
                    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
                    filename = remote_path.rsplit("/", 1)[-1]
                    quarantine_path = (
                        f"{self.config.remote_dest_path}/_mismatched/"
                        f"{filename}.{ts}"
                    )
                    try:
                        conn.move_remote_file(remote_path, quarantine_path)
                        log.info(f"  Moved bad copy to _mismatched/{filename}.{ts}")
                    except Exception as move_err:
                        log.info(f"  Warning: could not quarantine: {move_err}")

                    # Mark as PENDING so next sync retransfers
                    record.transfer_status = TransferStatus.PENDING.value
                    self.db.upsert_file(record)
                    stats.errors.append(f"Checksum mismatch (quarantined): {record.file_path}")

        # Update state
        self.db.update_sync_state(last_full_verify_time=datetime.now().isoformat())
        stats.duration_seconds = (datetime.now() - start_time).total_seconds()

        log.info(
            f"Verification complete: {stats.files_verified} OK, "
            f"{stats.files_checksum_mismatch} mismatches, "
            f"{stats.files_failed} errors "
            f"({stats.duration_seconds:.0f}s)"
        )

        # Recent activity breakdown
        recent_summary = self._format_recent_stats()
        if recent_summary:
            log.info(recent_summary)

        # Notify
        self.slack.notify_verification_complete(
            total_files=stats.files_scanned,
            verified_ok=stats.files_verified,
            mismatches=stats.files_checksum_mismatch,
            errors=stats.files_failed,
            recent_summary=recent_summary
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
