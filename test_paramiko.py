#!/usr/bin/env python3
"""
Test script to verify paramiko SSH/SFTP connection to fried.rice.edu

Author: tunnell (https://github.com/tunnell)
"""

import sys
import os
from pathlib import Path

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Load .env from same directory as script
    env_path = Path(__file__).parent / ".env"
    load_dotenv(env_path)
    print(f"[OK] Loaded config from {env_path}")
except ImportError:
    print("[WARN] python-dotenv not installed, using environment variables only")

# Check if paramiko is installed
try:
    import paramiko
    print(f"[OK] paramiko version {paramiko.__version__} installed")
except ImportError:
    print("[FAIL] paramiko not installed")
    print("Install with: pip install -r requirements.txt")
    sys.exit(1)

# Configuration from environment
HOST = os.getenv("SSH_HOST")
USERNAME = os.getenv("SSH_USERNAME")
SSH_KEY = os.getenv("SSH_KEY_PATH")
REMOTE_TEST_PATH = os.getenv("REMOTE_PATH")

# Validate config
missing = [k for k, v in {"SSH_HOST": HOST, "SSH_USERNAME": USERNAME,
                          "SSH_KEY_PATH": SSH_KEY, "REMOTE_PATH": REMOTE_TEST_PATH}.items() if not v]
if missing:
    print(f"[FAIL] Missing config: {', '.join(missing)}")
    print("Copy .env.example to .env and fill in your values")
    sys.exit(1)

def test_ssh_connection():
    """Test basic SSH connection."""
    print(f"\nTesting SSH connection to {USERNAME}@{HOST}...")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(
            HOST,
            username=USERNAME,
            key_filename=SSH_KEY,
            timeout=30
        )
        print("[OK] SSH connection successful")

        # Test command execution
        stdin, stdout, stderr = ssh.exec_command("echo 'Hello from fried'")
        output = stdout.read().decode().strip()
        print(f"[OK] Remote command executed: {output}")

        return ssh
    except paramiko.AuthenticationException:
        print("[FAIL] Authentication failed - check SSH key")
        return None
    except paramiko.SSHException as e:
        print(f"[FAIL] SSH error: {e}")
        return None
    except Exception as e:
        print(f"[FAIL] Connection error: {e}")
        return None


def test_sftp(ssh):
    """Test SFTP file operations."""
    print(f"\nTesting SFTP to {REMOTE_TEST_PATH}...")

    try:
        sftp = ssh.open_sftp()
        print("[OK] SFTP session opened")

        # List remote directory
        files = sftp.listdir(REMOTE_TEST_PATH)
        print(f"[OK] Listed {len(files)} items in {REMOTE_TEST_PATH}")

        # Show first 5 items
        for f in files[:5]:
            print(f"     - {f}")
        if len(files) > 5:
            print(f"     ... and {len(files) - 5} more")

        sftp.close()
        print("[OK] SFTP session closed")
        return True
    except Exception as e:
        print(f"[FAIL] SFTP error: {e}")
        return False


def main():
    print("=" * 50)
    print("Paramiko SSH/SFTP Test")
    print("=" * 50)

    # Test SSH
    ssh = test_ssh_connection()
    if not ssh:
        print("\n[RESULT] SSH test FAILED")
        return 1

    # Test SFTP
    sftp_ok = test_sftp(ssh)

    # Cleanup
    ssh.close()
    print("\n[OK] SSH connection closed")

    if sftp_ok:
        print("\n[RESULT] All tests PASSED")
        return 0
    else:
        print("\n[RESULT] SFTP test FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
