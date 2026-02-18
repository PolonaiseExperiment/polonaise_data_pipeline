"""
Configuration management for the data pipeline.

Loads settings from environment variables and .env file.

Author: tunnell (https://github.com/tunnell)
"""

import os
import argparse
from pathlib import Path
from dataclasses import dataclass, fields
from typing import Optional

from dotenv import load_dotenv


@dataclass
class Config:
    """Pipeline configuration."""

    # SSH settings
    ssh_host: str
    ssh_username: str
    ssh_key_path: Path

    # Paths
    local_source_path: Path
    remote_dest_path: str
    database_path: Path

    # Transfer settings
    max_parallel_transfers: int
    skip_recent_minutes: int

    # Slack
    slack_webhook_url: Optional[str]

    # Verification
    full_verify_hour: int

    # Checksum settings
    checksum_algorithm: str = "xxh64"
    checksum_chunk_size: int = 8 * 1024 * 1024  # 8 MB
    remote_command_timeout: int = 300  # seconds

    # SSH tuning
    ssh_timeout: int = 30  # seconds
    ssh_keepalive: int = 30  # seconds
    ssh_window_size: int = 2147483647
    ssh_max_packet_size: int = 32768 * 4  # 128 KB

    # Transfer tuning
    transfer_buffer_size: int = 1024 * 1024  # 1 MB

    # Slack tuning
    slack_timeout: int = 30  # seconds

    # Logging
    log_file_path: Optional[str] = "transfer.log"

    @classmethod
    def from_env(cls, env_path: Optional[Path] = None) -> "Config":
        """Load configuration from environment variables.

        Args:
            env_path: Path to .env file. If None, searches in standard locations.
        """
        # Load .env file
        if env_path:
            load_dotenv(env_path)
        else:
            # Try to find .env in common locations
            for path in [
                Path(__file__).parent.parent / ".env",
                Path.cwd() / ".env",
            ]:
                if path.exists():
                    load_dotenv(path)
                    break

        # Required settings
        ssh_host = os.getenv("SSH_HOST")
        ssh_username = os.getenv("SSH_USERNAME")
        ssh_key_path = os.getenv("SSH_KEY_PATH")
        local_source_path = os.getenv("LOCAL_SOURCE_PATH")
        remote_dest_path = os.getenv("REMOTE_DEST_PATH")
        database_path = os.getenv("DATABASE_PATH")

        # Validate required settings
        missing = []
        if not ssh_host:
            missing.append("SSH_HOST")
        if not ssh_username:
            missing.append("SSH_USERNAME")
        if not ssh_key_path:
            missing.append("SSH_KEY_PATH")
        if not local_source_path:
            missing.append("LOCAL_SOURCE_PATH")
        if not remote_dest_path:
            missing.append("REMOTE_DEST_PATH")
        if not database_path:
            missing.append("DATABASE_PATH")

        if missing:
            raise ValueError(f"Missing required config: {', '.join(missing)}")

        return cls(
            ssh_host=ssh_host,
            ssh_username=ssh_username,
            ssh_key_path=Path(ssh_key_path),
            local_source_path=Path(local_source_path),
            remote_dest_path=remote_dest_path,
            database_path=Path(database_path),
            max_parallel_transfers=int(os.getenv("MAX_PARALLEL_TRANSFERS", "3")),
            skip_recent_minutes=int(os.getenv("SKIP_RECENT_MINUTES", "15")),
            slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
            full_verify_hour=int(os.getenv("FULL_VERIFY_HOUR", "3")),
            checksum_chunk_size=int(os.getenv("CHECKSUM_CHUNK_SIZE", str(8 * 1024 * 1024))),
            remote_command_timeout=int(os.getenv("REMOTE_COMMAND_TIMEOUT", "300")),
            ssh_timeout=int(os.getenv("SSH_TIMEOUT", "30")),
            ssh_keepalive=int(os.getenv("SSH_KEEPALIVE", "30")),
            ssh_window_size=int(os.getenv("SSH_WINDOW_SIZE", "2147483647")),
            ssh_max_packet_size=int(os.getenv("SSH_MAX_PACKET_SIZE", str(32768 * 4))),
            transfer_buffer_size=int(os.getenv("TRANSFER_BUFFER_SIZE", str(1024 * 1024))),
            slack_timeout=int(os.getenv("SLACK_TIMEOUT", "30")),
            log_file_path=os.getenv("LOG_FILE_PATH", "transfer.log"),
        )

    def validate(self) -> list[str]:
        """Validate configuration. Returns list of errors."""
        errors = []

        if not self.ssh_key_path.exists():
            errors.append(f"SSH key not found: {self.ssh_key_path}")

        if not self.local_source_path.exists():
            errors.append(f"Source path not found: {self.local_source_path}")

        if self.max_parallel_transfers < 1:
            errors.append("MAX_PARALLEL_TRANSFERS must be >= 1")

        if self.skip_recent_minutes < 0:
            errors.append("SKIP_RECENT_MINUTES must be >= 0")

        return errors

    def apply_overrides(self, overrides: dict) -> None:
        """Apply CLI overrides to config fields.

        Args:
            overrides: Dict of field_name -> value. None values are skipped.
        """
        for key, value in overrides.items():
            if value is None:
                continue
            if hasattr(self, key):
                field_type = type(getattr(self, key))
                if field_type == Path:
                    setattr(self, key, Path(value))
                elif field_type == int:
                    setattr(self, key, int(value))
                else:
                    setattr(self, key, value)


# Maps CLI arg names to Config field names
_CLI_OVERRIDES = {
    "ssh_host": ("--ssh-host", "SSH host"),
    "ssh_username": ("--ssh-username", "SSH username"),
    "ssh_key_path": ("--ssh-key-path", "Path to SSH private key"),
    "local_source_path": ("--local-source-path", "Local source directory"),
    "remote_dest_path": ("--remote-dest-path", "Remote destination path"),
    "database_path": ("--database-path", "Path to database file"),
    "max_parallel_transfers": ("--max-parallel-transfers", "Number of parallel transfers"),
    "skip_recent_minutes": ("--skip-recent-minutes", "Skip files modified within N minutes"),
    "slack_webhook_url": ("--slack-webhook-url", "Slack webhook URL"),
    "full_verify_hour": ("--full-verify-hour", "Hour (0-23) for daily verification"),
    "ssh_timeout": ("--ssh-timeout", "SSH connection timeout in seconds"),
    "ssh_keepalive": ("--ssh-keepalive", "SSH keepalive interval in seconds"),
    "transfer_buffer_size": ("--transfer-buffer-size", "Transfer buffer size in bytes"),
    "checksum_chunk_size": ("--checksum-chunk-size", "Checksum chunk size in bytes"),
    "remote_command_timeout": ("--remote-command-timeout", "Remote command timeout in seconds"),
    "slack_timeout": ("--slack-timeout", "Slack request timeout in seconds"),
    "log_file_path": ("--log-file-path", "Path to log file"),
}


def add_config_args(parser: argparse.ArgumentParser) -> None:
    """Add config override arguments to an argparse parser."""
    group = parser.add_argument_group("config overrides", "Override .env settings from the command line")
    for field_name, (flag, help_text) in _CLI_OVERRIDES.items():
        group.add_argument(flag, dest=field_name, default=None, help=help_text)


def apply_cli_overrides(config: "Config", args: argparse.Namespace) -> None:
    """Apply parsed CLI args as config overrides."""
    overrides = {
        field_name: getattr(args, field_name, None)
        for field_name in _CLI_OVERRIDES
    }
    config.apply_overrides(overrides)


def resolve_config_path(
    run_name: Optional[str] = None,
    config_path: Optional[Path] = None
) -> tuple:
    """Resolve which .env file to load based on --run or --config.

    Args:
        run_name: Name of a run (e.g. "run45"), loads runs/<name>.env
        config_path: Explicit path to a .env file.

    Returns:
        Tuple of (env_path_or_None, label) where label is for display.

    Raises:
        ValueError: If both run_name and config_path are given,
                    or if the resolved file doesn't exist.
    """
    if run_name and config_path:
        raise ValueError("Cannot specify both --run and --config")

    if run_name:
        project_root = Path(__file__).parent.parent
        env_path = project_root / "runs" / f"{run_name}.env"
        if not env_path.exists():
            raise ValueError(
                f"Run config not found: {env_path}\n"
                f"Create it with: cp runs/template.env runs/{run_name}.env"
            )
        return env_path, run_name

    if config_path:
        if not config_path.exists():
            raise ValueError(f"Config file not found: {config_path}")
        return config_path, str(config_path)

    # Default: no explicit path, Config.from_env() will search standard locations
    return None, None


# Global config instance (lazy loaded)
_config: Optional[Config] = None


def get_config() -> Config:
    """Get the global configuration instance."""
    global _config
    if _config is None:
        _config = Config.from_env()
    return _config


def set_config(config: Config) -> None:
    """Set the global configuration instance (useful for testing)."""
    global _config
    _config = config
