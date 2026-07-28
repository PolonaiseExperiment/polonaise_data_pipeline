"""
Pytest configuration and fixtures.

Author: tunnell (https://github.com/tunnell)
"""

import os
import sys
from pathlib import Path

import pytest

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture(autouse=True)
def clean_env():
    """Clean environment variables before each test."""
    env_vars = [
        "SSH_HOST", "SSH_USERNAME", "SSH_KEY_PATH",
        "LOCAL_SOURCE_PATH", "REMOTE_DEST_PATH", "DATABASE_PATH",
        "MAX_PARALLEL_TRANSFERS", "SKIP_RECENT_MINUTES",
        "SLACK_WEBHOOK_URL", "FULL_VERIFY_HOUR",
        "IGNORE_DIRS", "IGNORE_LIST_FILE", "SKIP_ROOT_FILES",
        "COMPRESSION_ENABLED", "REMOTE_COMPRESSED_PATH", "COMPRESSION_MIN_BYTES",
        "REMOTE_PYTHON", "REMOTE_REPO_PATH", "REMOTE_DB_PATH", "PRIORITY_DIRS"
    ]

    # Save original values
    original = {k: os.environ.get(k) for k in env_vars}

    # Clear them so the developer's shell and .env can't leak into tests
    for k in env_vars:
        os.environ.pop(k, None)

    yield

    # Restore original values
    for k, v in original.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
