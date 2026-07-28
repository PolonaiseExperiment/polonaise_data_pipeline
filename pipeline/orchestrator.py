"""
Main orchestration logic for the data pipeline.

Author: tunnell (https://github.com/tunnell)
"""

import json
import shlex
import shutil
import sys
import tempfile
import threading
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
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


# After this many consecutive remote-decode failures, stop attempting
# compressed transfers for the rest of the pass (the remote env is broken;
# failing fast beats silently falling back to 2 TB of plain transfers).
DECODE_FAIL_HALT_STREAK = 5

# Abort the whole pass after this many checksum mismatches: a systemic
# transport bug should halt loudly, not burn days quarantining terabytes.
MISMATCH_ABORT_COUNT = 5

# Refuse to start transferring if the remote filesystem has less free space
# than this (the archive once filled the disk, which showed up as thousands
# of cryptic SSH banner failures).
REMOTE_MIN_FREE_GB = 100

# At most this many SSH handshakes in flight at once: sshd's MaxStartups
# (default ~10) drops connection BURSTS, not established connections, so
# many workers may hold connections as long as they authenticate a few at
# a time.
CONNECT_CONCURRENCY = 4

# At most this many local FLAC encodes at once: each 229 MB TDMS encode
# peaks ~1 GB of RAM (intermediates are freed eagerly in flaccodec), so
# this — not worker count — bounds local memory AND system throughput:
# files/s = ENCODE_CONCURRENCY / encode seconds.
ENCODE_CONCURRENCY = 4


def _looks_like_conn_error(error: Optional[str]) -> bool:
    """Heuristic: is this failure worth one retry on a fresh SSH connection?"""
    if not error:
        return False
    needles = ("ssh", "banner", "socket", "connection", "eof", "closed",
               "timed out", "timeout", "reset", "broken pipe")
    lower = error.lower()
    return any(n in lower for n in needles)


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
            skip_recent_minutes=self.config.skip_recent_minutes,
            ignore_dirs=self.config.ignore_dirs,
            skip_root_files=self.config.skip_root_files
        )
        self.checksum = ChecksumManager(self.config)
        self.transfer = TransferManager(self.config)
        self.slack = SlackNotifier(
            self.config.slack_webhook_url,
            timeout=self.config.slack_timeout,
            run_label=self.logger.run_label
        )

        # One long-lived SSH connection per worker thread (rebuilt on error)
        self._tls = threading.local()
        self._all_conns: List[SSHConnection] = []
        self._conns_lock = threading.Lock()
        self._connect_sem = threading.Semaphore(CONNECT_CONCURRENCY)
        self._encode_sem = threading.Semaphore(ENCODE_CONCURRENCY)

        # TinyDB tables are not thread-safe; serialize all writes
        self._db_lock = threading.Lock()

        # Remote-decode failure tracking (see DECODE_FAIL_HALT_STREAK)
        self._decode_fail_streak = 0
        self._compression_halted = False

    def close(self):
        """Close database and any per-thread SSH connections."""
        self._close_all_conns()
        self.db.close()

    # -- per-thread SSH connections ------------------------------------------

    def _thread_conn(self) -> SSHConnection:
        """Get this thread's SSH connection, opening it on first use."""
        conn = getattr(self._tls, "conn", None)
        if conn is None:
            conn = SSHConnection(self.config)
            with self._connect_sem:
                conn.connect()
            self._tls.conn = conn
            with self._conns_lock:
                self._all_conns.append(conn)
        return conn

    def _reset_thread_conn(self) -> None:
        """Drop this thread's connection so the next use opens a fresh one."""
        conn = getattr(self._tls, "conn", None)
        if conn is not None:
            self._tls.conn = None
            with self._conns_lock:
                if conn in self._all_conns:
                    self._all_conns.remove(conn)
            try:
                conn.close()
            except Exception:
                pass

    def _close_all_conns(self) -> None:
        with self._conns_lock:
            conns, self._all_conns = self._all_conns, []
        for conn in conns:
            try:
                conn.close()
            except Exception:
                pass
        self._tls = threading.local()

    def _db_upsert(self, record: FileRecord) -> None:
        with self._db_lock:
            self.db.upsert_file(record)

    # -- remote path mapping --------------------------------------------------

    def _desired_tree(self, extension: str) -> str:
        """Which remote root a file belongs under.

        With compression on, .tdms_index files ship to the compressed tree:
        they describe the ORIGINAL file's segment layout, which does not match
        a rebuilt .tdms — placing them next to rebuilt files would corrupt
        reads that trust the index.
        """
        if self.config.compression_enabled and extension.lower() == ".tdms_index":
            return "compressed"
        return "uncompressed"

    def _desired_tree_for_record(self, record: FileRecord) -> str:
        """Like _desired_tree, but an index whose sibling .tdms is REBUILT on
        the remote must stay in the compressed tree even if compression is
        later turned off — never plant an original-layout index next to a
        rebuilt tdms."""
        ext = (record.file_extension or "").lower()
        if ext == ".tdms_index" and record.file_path.lower().endswith(".tdms_index"):
            sibling_rel = record.file_path[:-len("_index")]
            with self._db_lock:
                sibling = self.db.get_file(sibling_rel)
            if sibling is not None and sibling.compressed:
                return "compressed"
        return self._desired_tree(ext)

    def _remote_root(self, record: FileRecord) -> str:
        if record.remote_tree == "compressed" and self.config.remote_compressed_path:
            return self.config.remote_compressed_path
        return self.config.remote_dest_path

    def _remote_path_for(self, record: FileRecord) -> str:
        return f"{self._remote_root(record)}/{record.file_path}".replace("\\", "/")

    def _priority_rank(self, relative_path: str) -> int:
        if not self.config.priority_dirs:
            return 0
        top = str(relative_path).replace("\\", "/").split("/")[0]
        return 0 if top in self.config.priority_dirs else 1

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
        """Check that source and destination are accessible and have space.

        Returns:
            Tuple of (success, message).
        """
        # Check source
        if not self.config.local_source_path.exists():
            return False, f"Source not found: {self.config.local_source_path}"

        # Check SSH + remote free space
        try:
            with SSHConnection(self.config) as conn:
                dest = self.config.remote_dest_path
                stdin, stdout, stderr = conn.get_ssh().exec_command(
                    f"df -Pk {shlex.quote(dest)} 2>/dev/null | tail -1", timeout=30)
                out = stdout.read().decode().split()
                if len(out) >= 4 and out[3].isdigit():
                    free_gb = int(out[3]) / 1024 / 1024
                    if free_gb < REMOTE_MIN_FREE_GB:
                        return False, (
                            f"Remote {dest} has only {free_gb:.0f} GB free "
                            f"(< {REMOTE_MIN_FREE_GB} GB) — refusing to fill the disk")
            return True, "All connections OK"
        except Exception as e:
            return False, f"SSH connection failed: {e}"

    def _prepare_file(self, file_path: Path, conn: SSHConnection, stats: SyncStats) -> Tuple[Path, FileRecord, bool]:
        """Prepare a file for transfer: compute checksum, check remote status.

        Returns:
            Tuple of (file_path, record, needs_transfer)
        """
        relative_path = self.scanner.get_relative_path(file_path)
        now = datetime.now().isoformat()

        # Get or create database record
        with self._db_lock:
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
                transfer_status=TransferStatus.PENDING.value,
                remote_tree=self._desired_tree(file_info["file_extension"] or "")
            )
            self._db_upsert(record)
        else:
            # Check if file changed (or is a stub from a failed check pass
            # that never got a checksum)
            current_size = file_path.stat().st_size
            if current_size != record.file_size_bytes or not record.local_checksum:
                record.file_size_bytes = current_size
                record.local_checksum = self.checksum.compute_local(file_path)
                stats.checksums_computed += 1
                record.transfer_status = TransferStatus.PENDING.value
                self._db_upsert(record)
                self.logger.debug(f"File changed, new checksum {record.local_checksum} for {record.file_name}")

        # Not-yet-verified files follow the current tree routing; verified
        # ones stay wherever they were verified (e.g. pre-compression layout)
        if record.transfer_status != TransferStatus.VERIFIED.value:
            new_tree = self._desired_tree_for_record(record)
            if new_tree != record.remote_tree and record.remote_checksum:
                # The file was uploaded under the old tree; remove the
                # superseded copy so a stale artifact can't be trusted later
                old_path = self._remote_path_for(record)
                try:
                    conn.get_sftp().remove(old_path)
                    self.logger.debug(f"Removed superseded {old_path}")
                except FileNotFoundError:
                    pass
            record.remote_tree = new_tree
        remote_path = self._remote_path_for(record)

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
                self._db_upsert(record)
                return file_path, record, False

        return file_path, record, True

    def _check_one(self, file_path: Path, stats: SyncStats) -> Tuple[Path, FileRecord, bool]:
        """_prepare_file on this thread's connection, one retry on a fresh one."""
        for attempt in (1, 2):
            try:
                conn = self._thread_conn()
                return self._prepare_file(file_path, conn, stats)
            except Exception:
                self._reset_thread_conn()
                if attempt == 2:
                    raise

    def _transfer_and_verify(
        self,
        file_path: Path,
        record: FileRecord
    ) -> Tuple[FileRecord, TransferResult]:
        """Transfer a single file and verify.

        Runs on a worker thread, reusing that thread's SSH connection.
        Connection-looking failures get exactly one retry on a fresh
        connection; other failures are reported as-is.
        """
        for attempt in (1, 2):
            try:
                conn = self._thread_conn()

                result = None
                if self._compression_eligible(file_path, record):
                    result = self._transfer_compressed(conn, file_path, record)
                if result is None:
                    # Not eligible, or the codec declined -> plain byte copy
                    result = self._transfer_plain(conn, file_path, record)

                if result.success or attempt == 2 or not _looks_like_conn_error(result.error):
                    return record, result
                self._reset_thread_conn()

            except Exception as e:
                self._reset_thread_conn()
                if attempt == 2:
                    now = datetime.now().isoformat()
                    record.transfer_status = TransferStatus.FAILED.value
                    record.last_error = str(e)
                    record.last_error_time = now
                    record.transfer_attempts += 1
                    record.last_transfer_time = now
                    return record, TransferResult(
                        file_path=record.file_path,
                        success=False,
                        bytes_transferred=0,
                        duration_seconds=0.0,
                        error=str(e)
                    )

    def _transfer_plain(
        self,
        conn: SSHConnection,
        file_path: Path,
        record: FileRecord
    ) -> TransferResult:
        """Byte-copy transfer with post-transfer remote checksum verification."""
        relative_path = record.file_path
        remote_path = self._remote_path_for(record)
        now = datetime.now().isoformat()

        result = self.transfer.transfer_file(
            conn, file_path, relative_path,
            show_progress=self.config.max_parallel_transfers == 1,
            remote_path=remote_path)

        if result.success:
            # Update record with the hash of exactly what was sent
            sent_checksum = result.local_checksum
            if sent_checksum:
                record.local_checksum = sent_checksum

            # Verify remote matches what was sent
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
                record.compressed = False
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
                    f"sent={record.local_checksum} remote={remote_checksum}"
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

        return result

    # -- compressed transfers --------------------------------------------------

    def _compression_eligible(self, file_path: Path, record: FileRecord) -> bool:
        return (self.config.compression_enabled
                and file_path.suffix.lower() == ".tdms"
                and record.file_size_bytes >= self.config.compression_min_bytes)

    def _transfer_compressed(
        self,
        conn: SSHConnection,
        file_path: Path,
        record: FileRecord
    ) -> Optional[TransferResult]:
        """Compress locally, ship .flac+.json, rebuild + verify on the remote.

        Hash chain: original file hash (local_checksum, computed at prepare) ->
        data_xxh64 of the float64 samples (local encode, re-checked by the
        remote after decode) -> file hash of the rebuilt remote .tdms
        (remote_checksum). The .flac/.json wire bytes are verified with
        xxh64sum like any plain transfer.

        Returns None when the codec can't take this file (multi-channel, not
        bit-exact, too many levels) — the caller then falls back to a plain
        byte copy, which is always safe.
        """
        from . import flaccodec

        record.remote_tree = "uncompressed"  # the rebuilt .tdms is the final artifact
        relative_path = record.file_path
        rel_posix = relative_path.replace("\\", "/")
        final_remote = f"{self.config.remote_dest_path}/{rel_posix}"
        flac_rel = str(PurePosixPath(rel_posix).with_suffix(".flac"))
        flac_remote = f"{self.config.remote_compressed_path}/{flac_rel}"
        sidecar_remote = flac_remote[:-len(".flac")] + ".json"
        start_time = datetime.now()
        now = start_time.isoformat()

        def fail(error: str, bytes_transferred: int = 0, decode_infra: bool = False) -> TransferResult:
            if decode_infra:
                with self._conns_lock:
                    self._decode_fail_streak += 1
                    if (self._decode_fail_streak >= DECODE_FAIL_HALT_STREAK
                            and not self._compression_halted):
                        self._compression_halted = True
                        self.logger.info(
                            f"HALTING compressed transfers: {self._decode_fail_streak} "
                            f"consecutive remote decode failures — check the remote "
                            f"venv/repo ({self.config.remote_repo_path})")
            record.transfer_status = TransferStatus.FAILED.value
            record.last_error = error
            record.last_error_time = now
            record.transfer_attempts += 1
            record.last_transfer_time = now
            return TransferResult(
                file_path=relative_path,
                success=False,
                bytes_transferred=bytes_transferred,
                duration_seconds=(datetime.now() - start_time).total_seconds(),
                error=error
            )

        if self._compression_halted:
            return fail("compressed transfers halted this pass (repeated remote decode failures)")

        tmpdir = Path(tempfile.mkdtemp(prefix="polonaise_flac_"))
        try:
            # 1. Encode locally; the encoder decodes its own output and
            #    bit-compares before we ship anything
            try:
                with self._encode_sem:
                    rep = flaccodec.encode_tdms_to_flac(
                        file_path, tmpdir, exact=True,
                        original_file_xxh64=record.local_checksum)
            except flaccodec.CodecUnsupported as e:
                self.logger.debug(f"Codec declined {record.file_name}: {e}")
                return None
            except Exception as e:
                # Encode never touched the remote; a plain byte copy is
                # always a safe fallback for a file the codec chokes on
                self.logger.info(
                    f"  {record.file_name}: encode failed "
                    f"({type(e).__name__}: {e}), sending original instead")
                return None
            if not rep["exact"]:
                self.logger.info(
                    f"  {record.file_name}: encode not bit-exact "
                    f"({rep['encoding']}), sending original instead")
                return None

            # 2. Ship .flac + .json into the compressed tree, wire-verified
            r_flac = self.transfer.transfer_file(
                conn, Path(rep["flac"]), relative_path,
                show_progress=self.config.max_parallel_transfers == 1,
                remote_path=flac_remote)
            if not r_flac.success:
                return fail(f"flac upload failed: {r_flac.error}")
            remote_flac_hash, err = self.checksum.compute_remote(conn.get_ssh(), flac_remote)
            if err or remote_flac_hash != r_flac.local_checksum:
                return fail(f"flac wire mismatch: {err or remote_flac_hash} != {r_flac.local_checksum}",
                            r_flac.bytes_transferred)

            r_json = self.transfer.transfer_file(
                conn, Path(rep["sidecar"]), relative_path,
                show_progress=False, remote_path=sidecar_remote)
            if not r_json.success:
                return fail(f"sidecar upload failed: {r_json.error}", r_flac.bytes_transferred)
            remote_json_hash, err = self.checksum.compute_remote(conn.get_ssh(), sidecar_remote)
            if err or remote_json_hash != r_json.local_checksum:
                return fail(f"sidecar wire mismatch: {err or remote_json_hash} != {r_json.local_checksum}",
                            r_flac.bytes_transferred + r_json.bytes_transferred)

            wire_bytes = r_flac.bytes_transferred + r_json.bytes_transferred

            # 3. Rebuild the .tdms on the remote and verify the data hash there
            cmd = (
                f"{shlex.quote(self.config.remote_python)} "
                f"{shlex.quote(self.config.remote_repo_path + '/scripts/remote_decode.py')} "
                f"--flac {shlex.quote(flac_remote)} "
                f"--out {shlex.quote(final_remote)} "
                f"--expect-data-hash {rep['data_xxh64']}"
            )
            try:
                stdin, stdout, stderr = conn.get_ssh().exec_command(
                    cmd, timeout=self.config.remote_command_timeout)
                # Read output BEFORE waiting on exit status: recv_exit_status
                # first can deadlock if output fills the channel window, and
                # it ignores the timeout entirely
                out_text = stdout.read().decode().strip()
                err_text = stderr.read().decode().strip()
                chan = stdout.channel
                if not chan.status_event.wait(self.config.remote_command_timeout):
                    chan.close()
                    return fail("remote decode timed out", wire_bytes, decode_infra=True)
                exit_status = chan.recv_exit_status()
            except Exception as e:
                return fail(f"remote decode exec failed: {e}", wire_bytes, decode_infra=True)

            res = None
            if out_text:
                try:
                    res = json.loads(out_text.splitlines()[-1])
                except (ValueError, IndexError):
                    res = None
            if res is None or not isinstance(res, dict):
                return fail(
                    f"remote decode produced no result (exit {exit_status}): "
                    f"{err_text or out_text or 'no output'}",
                    wire_bytes, decode_infra=True)
            if not res.get("ok"):
                return fail(f"remote decode failed: {res.get('error')}",
                            wire_bytes, decode_infra=True)
            if res.get("data_xxh64") != rep["data_xxh64"]:
                return fail(
                    f"end-to-end data hash mismatch: local={rep['data_xxh64']} "
                    f"remote={res.get('data_xxh64')}", wire_bytes)

            with self._conns_lock:
                self._decode_fail_streak = 0

            # The rebuild deleted any stale sibling .tdms_index next to the
            # rebuilt file; re-route the index record to the compressed tree
            # so the original index is preserved there on a future pass
            index_rel = record.file_path + "_index"
            with self._db_lock:
                index_rec = self.db.get_file(index_rel)
            if index_rec is not None and index_rec.remote_tree != "compressed":
                index_rec.remote_tree = "compressed"
                if index_rec.transfer_status == TransferStatus.VERIFIED.value:
                    index_rec.transfer_status = TransferStatus.PENDING.value
                self._db_upsert(index_rec)

            # 4. Success — record the whole hash chain
            record.compressed = True
            record.data_checksum = rep["data_xxh64"]
            record.compressed_checksum = r_flac.local_checksum
            record.sidecar_checksum = r_json.local_checksum
            record.compressed_bytes = rep["flac_bytes"] + rep["sidecar_bytes"]
            record.remote_checksum = res.get("file_xxh64")
            record.transfer_status = TransferStatus.VERIFIED.value
            record.last_verified_remote = now
            record.transfer_attempts += 1
            record.last_transfer_time = now
            self.logger.debug(
                f"Compressed {record.file_name}: "
                f"{record.file_size_bytes} -> {record.compressed_bytes} bytes, "
                f"data={record.data_checksum} rebuilt={record.remote_checksum}")

            return TransferResult(
                file_path=relative_path,
                success=True,
                bytes_transferred=wire_bytes,
                duration_seconds=(datetime.now() - start_time).total_seconds(),
                local_checksum=record.local_checksum
            )
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

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
            # Early returns and escaping exceptions must not strand the
            # per-thread SSH connections opened during the pass
            self._close_all_conns()
            self.db.flush()
            self.logger.verbose = old_verbose

        return stats

    def _do_incremental_sync(self, stats: SyncStats, start_time: datetime,
                              dry_run: bool, full_sync: bool) -> SyncStats:
        """Internal sync logic."""
        log = self.logger

        # A broken remote decode env halts compression only for one pass
        self._decode_fail_streak = 0
        self._compression_halted = False

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

        # Failed/pending files from earlier passes usually aren't "recently
        # modified", so an incremental scan alone would never retry them.
        # Fold them back in from the database — but honor the same guards the
        # scanner applies (ignore list, still-being-written files).
        known = {str(f) for f in files}
        recent_cutoff = datetime.now() - timedelta(minutes=self.config.skip_recent_minutes)
        requeued = 0
        for rec in self.db.get_pending_transfers():
            top = rec.file_path.replace("\\", "/").split("/")[0]
            if top in self.scanner.ignore_dirs:
                continue
            p = self.config.local_source_path / rec.file_path
            if str(p) in known or not p.exists():
                continue
            if (self.config.skip_recent_minutes > 0
                    and datetime.fromtimestamp(p.stat().st_mtime) > recent_cutoff):
                stats.files_skipped_recent += 1
                continue
            files.append(p)
            requeued += 1
        if requeued:
            log.debug(f"Re-queued {requeued} pending/failed files from database")

        # Priority folders (e.g. the current run) first; stable otherwise
        if self.config.priority_dirs:
            files.sort(key=lambda p: self._priority_rank(
                p.relative_to(self.config.local_source_path)))

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

        # Check which files actually need transfer (parallel: each check is a
        # remote stat + maybe a remote checksum, all round-trip bound)
        log.info("Checking which files need transfer...")
        files_to_transfer = []
        check_workers = min(16, self.config.max_parallel_transfers)

        with ThreadPoolExecutor(max_workers=check_workers) as executor:
            check_futures = {
                executor.submit(self._check_one, file_path, stats): file_path
                for file_path in files
            }
            for future in tqdm(as_completed(check_futures), total=len(files),
                               desc="Checking", unit=" file",
                               file=log.stream, leave=False):
                file_path = check_futures[future]
                try:
                    _, record, needs_transfer = future.result()
                    if needs_transfer:
                        files_to_transfer.append((file_path, record))
                        stats.files_new += 1
                    else:
                        if record.transfer_status == TransferStatus.VERIFIED.value:
                            stats.files_verified += 1
                except Exception as e:
                    stats.errors.append(f"{file_path}: {e}")
                    # A NEW file whose check failed has no DB record yet; the
                    # pass still advances the scan cursor, so without one the
                    # file would never be seen again. Leave a PENDING stub for
                    # the re-queue logic to retry next pass.
                    try:
                        rel = self.scanner.get_relative_path(file_path)
                        with self._db_lock:
                            existing = self.db.get_file(rel)
                        if existing is None:
                            info = self.scanner.get_file_info(file_path)
                            self._db_upsert(FileRecord(
                                file_path=rel,
                                file_name=info["file_name"],
                                file_extension=info["file_extension"],
                                file_size_bytes=info["file_size_bytes"],
                                file_mtime=info["file_mtime"],
                                file_ctime=info["file_ctime"],
                                first_seen=datetime.now().isoformat(),
                                transfer_status=TransferStatus.PENDING.value,
                                remote_tree=self._desired_tree(info["file_extension"] or "")
                            ))
                    except Exception:
                        pass  # local FS failing too; a --full pass will find it

        # as_completed scrambled the priority order; restore it
        if self.config.priority_dirs:
            files_to_transfer.sort(key=lambda t: self._priority_rank(t[1].file_path))

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

            mismatches_this_pass = 0
            completions = 0
            for future in as_completed(futures):
                rec = futures[future]
                try:
                    record, result = future.result()
                    self._db_upsert(record)
                    # Passes run for hours; checkpoint the write cache so a
                    # kill loses at most ~50 records of progress
                    completions += 1
                    if completions % 50 == 0:
                        self.db.flush()

                    if record.transfer_status == TransferStatus.CHECKSUM_MISMATCH.value:
                        mismatches_this_pass += 1
                        if mismatches_this_pass >= MISMATCH_ABORT_COUNT:
                            msg = (f"ABORTING pass: {mismatches_this_pass} checksum "
                                   f"mismatches — this smells systemic, not random")
                            log.info(msg)
                            stats.errors.append(msg)
                            self.slack.notify_connection_error(msg)
                            executor.shutdown(wait=False, cancel_futures=True)
                            break

                    if result.success:
                        stats.files_transferred += 1
                        stats.bytes_transferred += result.bytes_transferred
                        mb = result.bytes_transferred / 1024 / 1024
                        if record.compressed and result.bytes_transferred:
                            ratio = record.file_size_bytes / result.bytes_transferred
                            log.info(
                                f"  {rec.file_name}  {mb:.1f} MB on wire "
                                f"({ratio:.1f}x compressed)  {result.duration_seconds:.0f}s  OK")
                        else:
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

        # Workers are done; drop their per-thread connections
        self._close_all_conns()

        # Sync database to remote (flush first — the on-disk file lags the
        # write cache, and the remote copy should reflect this pass)
        self.db.flush()
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
        mismatched_files = []

        with SSHConnection(self.config) as conn:
            all_files = list(self.db.get_all_files())
            for record in tqdm(all_files, desc="Verifying", unit=" file",
                               file=log.stream, leave=False):
                stats.files_scanned += 1
                local_path = self.config.local_source_path / record.file_path
                remote_path = self._remote_path_for(record)

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

                # Verify remote. For compressed records, the remote .tdms was
                # REBUILT from FLAC — its bytes legitimately differ from the
                # original, so it verifies against its own recorded hash; the
                # archived .flac/.json verify against their as-sent hashes.
                rel_posix = record.file_path.replace("\\", "/")
                if record.compressed:
                    # (path, expected hash, label, quarantine root, quarantine rel)
                    checks = [(remote_path, record.remote_checksum, "rebuilt tdms",
                               self.config.remote_dest_path, rel_posix)]
                    if self.config.remote_compressed_path:
                        flac_rel = str(PurePosixPath(rel_posix).with_suffix(".flac"))
                        flac_remote = f"{self.config.remote_compressed_path}/{flac_rel}"
                        if record.compressed_checksum:
                            checks.append((flac_remote, record.compressed_checksum, "flac",
                                           self.config.remote_compressed_path, flac_rel))
                        if record.sidecar_checksum:
                            sidecar_rel = flac_rel[:-len(".flac")] + ".json"
                            checks.append((flac_remote[:-len('.flac')] + ".json",
                                           record.sidecar_checksum, "sidecar",
                                           self.config.remote_compressed_path, sidecar_rel))
                else:
                    checks = [(remote_path, local_checksum, "file",
                               self._remote_root(record), rel_posix)]

                error = None
                mismatch = None
                for check_path, expected, label, q_root, q_rel in checks:
                    remote_checksum, error = self.checksum.compute_remote(conn.get_ssh(), check_path)
                    if error:
                        break
                    if remote_checksum != expected:
                        mismatch = (check_path, expected, remote_checksum, label, q_root, q_rel)
                        break

                if error:
                    stats.files_failed += 1
                    stats.errors.append(f"Remote verification failed: {record.file_path}: {error}")
                    continue

                if mismatch is None:
                    stats.files_verified += 1
                    record.last_verified_local = datetime.now().isoformat()
                    record.last_verified_remote = datetime.now().isoformat()
                    self.db.upsert_file(record)
                    log.debug(f"OK: {record.file_path}")
                else:
                    bad_path, expected, remote_checksum, label, q_root, q_rel = mismatch
                    stats.files_checksum_mismatch += 1
                    record.failed_checksum_count += 1
                    log.info(
                        f"MISMATCH ({label}): {record.file_path} "
                        f"expected={expected} remote={remote_checksum}"
                    )

                    # Quarantine the bad artifact within its own tree, under
                    # its own name (a bad .flac must not masquerade as a .tdms)
                    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
                    filename = bad_path.rsplit("/", 1)[-1]
                    quarantine_path = f"{q_root}/_mismatched/{q_rel}.{ts}"
                    try:
                        conn.move_remote_file(bad_path, quarantine_path)
                        log.info(f"  Moved bad copy to _mismatched/{filename}.{ts}")
                    except Exception as move_err:
                        log.info(f"  Warning: could not quarantine: {move_err}")

                    # Mark as PENDING so next sync retransfers
                    record.transfer_status = TransferStatus.PENDING.value
                    self.db.upsert_file(record)
                    mismatched_files.append(record.file_name)
                    stats.errors.append(f"Checksum mismatch (quarantined): {record.file_path}")

        # Update state
        self.db.update_sync_state(last_full_verify_time=datetime.now().isoformat())
        self.db.flush()
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

        # Notify — include mismatched filenames in Slack
        mismatch_text = ""
        if mismatched_files:
            mismatch_text = "*Mismatched files:*\n" + "\n".join(
                f"• {name}" for name in mismatched_files[:10]
            )
            if len(mismatched_files) > 10:
                mismatch_text += f"\n… and {len(mismatched_files) - 10} more"
            if recent_summary:
                mismatch_text += f"\n\n{recent_summary}"
        else:
            mismatch_text = recent_summary

        self.slack.notify_verification_complete(
            total_files=stats.files_scanned,
            verified_ok=stats.files_verified,
            mismatches=stats.files_checksum_mismatch,
            errors=stats.files_failed,
            recent_summary=mismatch_text
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
