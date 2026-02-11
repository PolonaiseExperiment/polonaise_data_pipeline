#!/usr/bin/env python3
"""
Full verification script - run daily to verify all checksums.

Usage:
    python verify.py           # Full verification of all files
    python verify.py --quick   # Verify only files not verified in last 24h

Author: tunnell (https://github.com/tunnell)
"""

import sys
import argparse
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.config import Config, get_config
from pipeline.orchestrator import SyncOrchestrator


def main():
    parser = argparse.ArgumentParser(description="Run full data verification")
    parser.add_argument("--quick", action="store_true", help="Only verify files not verified in last 24h")
    parser.add_argument("--config", type=Path, help="Path to .env file")
    args = parser.parse_args()

    # Load config
    try:
        if args.config:
            config = Config.from_env(args.config)
        else:
            config = get_config()

        errors = config.validate()
        if errors:
            print("Configuration errors:")
            for e in errors:
                print(f"  - {e}")
            sys.exit(1)

    except ValueError as e:
        print(f"Configuration error: {e}")
        sys.exit(1)

    # Run orchestrator
    with SyncOrchestrator(config) as orchestrator:
        if args.quick:
            print("Quick verification mode - checking files not verified in last 24h")
            # TODO: Implement quick verification
            print("Quick mode not yet implemented, running full verification")

        print("Starting full verification...")
        print(f"Source: {config.local_source_path}")
        print(f"Destination: {config.ssh_username}@{config.ssh_host}:{config.remote_dest_path}")
        print()

        stats = orchestrator.run_full_verification()

        # Print summary
        print()
        print("=" * 40)
        print("Verification Summary")
        print("=" * 40)
        print(f"Files checked: {stats.files_scanned}")
        print(f"Files verified OK: {stats.files_verified}")
        print(f"Checksum mismatches: {stats.files_checksum_mismatch}")
        print(f"Errors: {stats.files_failed}")
        print(f"Duration: {stats.duration_seconds:.1f} seconds")

        if stats.errors:
            print(f"\nIssues found ({len(stats.errors)}):")
            for error in stats.errors[:20]:
                print(f"  - {error}")
            if len(stats.errors) > 20:
                print(f"  ... and {len(stats.errors) - 20} more")

        sys.exit(0 if (stats.files_failed == 0 and stats.files_checksum_mismatch == 0) else 1)


if __name__ == "__main__":
    main()
