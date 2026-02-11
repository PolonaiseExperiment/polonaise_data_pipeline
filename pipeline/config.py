"""
Configuration management for the data pipeline.

Loads settings from environment variables and .env file.

Author: tunnell (https://github.com/tunnell)
"""

import os
from pathlib import Path
from dataclasses import dataclass
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
