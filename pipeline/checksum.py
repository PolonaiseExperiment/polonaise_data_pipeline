"""
Checksum computation for local and remote files using xxHash.

Author: tunnell (https://github.com/tunnell)
"""

from pathlib import Path
from typing import Optional, Tuple

import xxhash
import paramiko


def compute_xxhash(file_path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    """Compute xxHash64 of a local file.

    Args:
        file_path: Path to the file.
        chunk_size: Size of chunks to read (default 8MB for performance).

    Returns:
        Hex string of the xxHash64 checksum.
    """
    hasher = xxhash.xxh64()
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()


def compute_remote_xxhash(
    ssh_client: paramiko.SSHClient,
    remote_path: str
) -> Tuple[Optional[str], Optional[str]]:
    """Compute xxHash64 of a remote file via SSH command.

    Args:
        ssh_client: Connected paramiko SSH client.
        remote_path: Path to file on remote server.

    Returns:
        Tuple of (checksum, error_message). One will be None.
    """
    cmd = f'xxh64sum "{remote_path}"'

    try:
        stdin, stdout, stderr = ssh_client.exec_command(cmd, timeout=300)
        exit_status = stdout.channel.recv_exit_status()

        if exit_status != 0:
            error = stderr.read().decode().strip()
            return None, f"Command failed (exit {exit_status}): {error}"

        output = stdout.read().decode().strip()
        # Output format: "checksum  filename"
        checksum = output.split()[0]
        return checksum, None

    except Exception as e:
        return None, str(e)


def check_remote_file_exists(
    ssh_client: paramiko.SSHClient,
    remote_path: str
) -> Tuple[bool, Optional[int]]:
    """Check if a remote file exists and get its size.

    Args:
        ssh_client: Connected paramiko SSH client.
        remote_path: Path to file on remote server.

    Returns:
        Tuple of (exists, size_in_bytes). Size is None if file doesn't exist.
    """
    try:
        sftp = ssh_client.open_sftp()
        try:
            stat = sftp.stat(remote_path)
            return True, stat.st_size
        except FileNotFoundError:
            return False, None
        finally:
            sftp.close()
    except Exception:
        return False, None


class ChecksumManager:
    """Manages checksum computation for local and remote files using xxHash."""

    def __init__(self):
        """Initialize checksum manager."""
        self.algorithm = "xxh64"

    def compute_local(self, file_path: Path) -> str:
        """Compute xxHash64 checksum of a local file."""
        return compute_xxhash(file_path)

    def compute_remote(
        self,
        ssh_client: paramiko.SSHClient,
        remote_path: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """Compute xxHash64 checksum of a remote file.

        Returns:
            Tuple of (checksum, error_message).
        """
        return compute_remote_xxhash(ssh_client, remote_path)

    def verify_transfer(
        self,
        local_path: Path,
        ssh_client: paramiko.SSHClient,
        remote_path: str
    ) -> Tuple[bool, str]:
        """Verify a file was transferred correctly by comparing xxHash checksums.

        Args:
            local_path: Local file path.
            ssh_client: SSH client for remote access.
            remote_path: Remote file path.

        Returns:
            Tuple of (success, message).
        """
        local_checksum = self.compute_local(local_path)
        remote_checksum, error = self.compute_remote(ssh_client, remote_path)

        if error:
            return False, f"Failed to get remote checksum: {error}"

        if local_checksum == remote_checksum:
            return True, "Checksums match"
        else:
            return False, f"Checksum mismatch: local={local_checksum}, remote={remote_checksum}"
