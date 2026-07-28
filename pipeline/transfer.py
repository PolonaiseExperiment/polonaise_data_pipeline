"""
File transfer module using paramiko SFTP.

Author: tunnell (https://github.com/tunnell)
"""

import os
import socket
from pathlib import Path
from typing import Optional, Callable, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime

import paramiko
import xxhash
from tqdm import tqdm

from .config import Config


@dataclass
class TransferResult:
    """Result of a file transfer."""
    file_path: str
    success: bool
    bytes_transferred: int
    duration_seconds: float
    error: Optional[str] = None
    local_checksum: Optional[str] = None  # Hash of bytes actually sent


class SSHConnection:
    """Manages SSH/SFTP connection to remote server."""

    def __init__(self, config: Config):
        """Initialize SSH connection.

        Args:
            config: Pipeline configuration.
        """
        self.config = config
        self.ssh_client: Optional[paramiko.SSHClient] = None
        self.sftp_client: Optional[paramiko.SFTPClient] = None
        self._known_dirs: set = set()  # remote dirs already ensured on this connection

    # Windows' default socket send buffer caps paramiko at ~64 KB in flight
    # (~0.5 MB/s at 130 ms RTT). A 4 MiB SO_SNDBUF + TCP_NODELAY measured
    # 9.3 MB/s per stream on the production route, checksum-verified.
    SOCKET_SNDBUF = 4 * 1024 * 1024

    def connect(self) -> None:
        """Establish SSH connection.

        Compression is off: TDMS payloads are raw ADC data that barely
        compress, and zlib caps per-stream throughput once writes are
        pipelined.
        """
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        sock = socket.create_connection(
            (self.config.ssh_host, 22), timeout=self.config.ssh_timeout)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.SOCKET_SNDBUF)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        self.ssh_client.connect(
            hostname=self.config.ssh_host,
            username=self.config.ssh_username,
            key_filename=str(self.config.ssh_key_path),
            timeout=self.config.ssh_timeout,
            compress=False,
            sock=sock
        )
        transport = self.ssh_client.get_transport()
        transport.set_keepalive(self.config.ssh_keepalive)

    def get_sftp(self) -> paramiko.SFTPClient:
        """Get SFTP client, connecting if needed."""
        if self.ssh_client is None:
            self.connect()

        if self.sftp_client is None:
            self.sftp_client = self.ssh_client.open_sftp()

        return self.sftp_client

    def get_ssh(self) -> paramiko.SSHClient:
        """Get SSH client, connecting if needed."""
        if self.ssh_client is None:
            self.connect()
        return self.ssh_client

    def close(self) -> None:
        """Close all connections."""
        if self.sftp_client:
            self.sftp_client.close()
            self.sftp_client = None

        if self.ssh_client:
            self.ssh_client.close()
            self.ssh_client = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def ensure_remote_dir(self, remote_path: str) -> None:
        """Ensure remote directory exists, creating if needed."""
        if remote_path in self._known_dirs:
            return
        sftp = self.get_sftp()

        # Split path and create each component
        parts = remote_path.split("/")
        current = ""

        for part in parts:
            if not part:
                continue
            current = f"{current}/{part}"
            if current in self._known_dirs:
                continue
            try:
                sftp.stat(current)
            except FileNotFoundError:
                try:
                    sftp.mkdir(current)
                except IOError:
                    # Another worker created it between our stat and mkdir
                    sftp.stat(current)
            self._known_dirs.add(current)

    def file_exists(self, remote_path: str) -> bool:
        """Check if remote file exists."""
        try:
            self.get_sftp().stat(remote_path)
            return True
        except FileNotFoundError:
            return False

    def get_remote_size(self, remote_path: str) -> Optional[int]:
        """Get size of remote file, or None if doesn't exist."""
        try:
            return self.get_sftp().stat(remote_path).st_size
        except FileNotFoundError:
            return None

    def move_remote_file(self, src_path: str, dest_path: str) -> None:
        """Move/rename a file on the remote server.

        Creates destination directory if needed.
        """
        dest_dir = "/".join(dest_path.split("/")[:-1])
        self.ensure_remote_dir(dest_dir)
        self.get_sftp().rename(src_path, dest_path)


class TransferManager:
    """Manages file transfers to remote server."""

    def __init__(
        self,
        config: Config,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ):
        """Initialize transfer manager.

        Args:
            config: Pipeline configuration.
            progress_callback: Optional callback(filename, bytes_sent, total_bytes).
        """
        self.config = config
        self.progress_callback = progress_callback

    def _get_remote_path(self, relative_path: str) -> str:
        """Convert relative path to full remote path."""
        # Normalize path separators for remote (Unix)
        relative_path = relative_path.replace("\\", "/")
        return f"{self.config.remote_dest_path}/{relative_path}"

    def transfer_file(
        self,
        conn: SSHConnection,
        local_path: Path,
        relative_path: str,
        show_progress: bool = True,
        remote_path: Optional[str] = None
    ) -> TransferResult:
        """Transfer a single file.

        Args:
            conn: SSH connection.
            local_path: Full local path.
            relative_path: Path relative to source root (used for reporting,
                and to derive the remote path when remote_path is None).
            show_progress: Show tqdm progress bar.
            remote_path: Explicit remote destination; overrides the default
                REMOTE_DEST_PATH/relative_path mapping.

        Returns:
            TransferResult with success/failure info.
        """
        if remote_path is None:
            remote_path = self._get_remote_path(relative_path)
        start_time = datetime.now()

        try:
            # Ensure remote directory exists
            remote_dir = "/".join(remote_path.split("/")[:-1])
            conn.ensure_remote_dir(remote_dir)

            file_size = local_path.stat().st_size
            sftp = conn.get_sftp()

            BUFFER_SIZE = self.config.transfer_buffer_size

            # Hash during transfer so the checksum reflects exactly what was sent
            hasher = xxhash.xxh64()

            if show_progress:
                with tqdm(
                    total=file_size,
                    unit="B",
                    unit_scale=True,
                    desc=local_path.name,
                    leave=False
                ) as pbar:
                    with open(local_path, "rb") as f_in:
                        with sftp.file(remote_path, "wb") as f_out:
                            # Don't wait for a server ack per 32KB packet;
                            # without this each stream is capped at ~32KB/RTT.
                            # Same mode paramiko's own put() uses. Write errors
                            # may surface at close(), still inside this try.
                            f_out.set_pipelined(True)
                            while chunk := f_in.read(BUFFER_SIZE):
                                f_out.write(chunk)
                                hasher.update(chunk)
                                pbar.update(len(chunk))
                                if self.progress_callback:
                                    self.progress_callback(relative_path, pbar.n, file_size)
            else:
                with open(local_path, "rb") as f_in:
                    with sftp.file(remote_path, "wb") as f_out:
                        f_out.set_pipelined(True)
                        while chunk := f_in.read(BUFFER_SIZE):
                            f_out.write(chunk)
                            hasher.update(chunk)

            duration = (datetime.now() - start_time).total_seconds()

            return TransferResult(
                file_path=relative_path,
                success=True,
                bytes_transferred=file_size,
                duration_seconds=duration,
                local_checksum=hasher.hexdigest()
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            return TransferResult(
                file_path=relative_path,
                success=False,
                bytes_transferred=0,
                duration_seconds=duration,
                error=str(e)
            )

    def transfer_files_parallel(
        self,
        files: list[Tuple[Path, str]],
        max_workers: Optional[int] = None,
        show_progress: bool = True
    ) -> list[TransferResult]:
        """Transfer multiple files in parallel.

        Args:
            files: List of (local_path, relative_path) tuples.
            max_workers: Number of parallel transfers (default from config).
            show_progress: Show progress bars.

        Returns:
            List of TransferResult objects.
        """
        if max_workers is None:
            max_workers = self.config.max_parallel_transfers

        results = []

        # Each thread needs its own connection
        def transfer_with_new_connection(local_path: Path, relative_path: str):
            with SSHConnection(self.config) as conn:
                return self.transfer_file(conn, local_path, relative_path, show_progress)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(transfer_with_new_connection, local, rel): rel
                for local, rel in files
            }

            for future in as_completed(futures):
                result = future.result()
                results.append(result)

        return results

    def sync_database_to_remote(self, conn: SSHConnection, db_path: Path) -> bool:
        """Sync the database file to remote.

        Returns True on success.
        """
        remote_path = self.config.remote_db_path or f"{self.config.remote_dest_path}/pipeline.json"

        try:
            conn.get_sftp().put(str(db_path), remote_path)
            return True
        except Exception:
            return False
