#!/usr/bin/env python3
"""
Incremental sync script - run every 10 minutes via Task Scheduler.

Usage:
    python sync.py              # Normal incremental sync
    python sync.py --dry-run    # Show what would be transferred
    python sync.py --status     # Show pipeline status

Author: tunnell (https://github.com/tunnell)
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.config import Config, get_config
from pipeline.orchestrator import SyncOrchestrator


class TeeOutput:
    """Write to both stdout and a log file."""
    def __init__(self, log_path: Path):
        self.terminal = sys.stdout
        self.log_file = open(log_path, "a", encoding="utf-8")
        # Write session header
        self.log_file.write(f"\n{'='*60}\n")
        self.log_file.write(f"Session started: {datetime.now().isoformat()}\n")
        self.log_file.write(f"{'='*60}\n")
        self.log_file.flush()

    def write(self, message):
        self.terminal.write(message)
        self.log_file.write(message)
        self.log_file.flush()

    def flush(self):
        self.terminal.flush()
        self.log_file.flush()

    def close(self):
        self.log_file.write(f"\nSession ended: {datetime.now().isoformat()}\n")
        self.log_file.close()


def main():
    parser = argparse.ArgumentParser(description="Run incremental data sync")
    parser.add_argument("--dry-run", action="store_true", help="Don't transfer, just show what would happen")
    parser.add_argument("--full", action="store_true", help="Full sync - ignore last scan time, check all files")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show debug info about skipped files")
    parser.add_argument("--status", action="store_true", help="Show pipeline status and exit")
    parser.add_argument("--config", type=Path, help="Path to .env file")
    parser.add_argument("--no-log", action="store_true", help="Don't write to log file")
    args = parser.parse_args()

    # Setup logging to file (unless --no-log)
    log_output = None
    if not args.no_log:
        log_path = Path(__file__).parent.parent / "transfer.log"
        log_output = TeeOutput(log_path)
        sys.stdout = log_output
        sys.stderr = log_output

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
        if args.status:
            status = orchestrator.get_status()
            print("Pipeline Status")
            print("=" * 40)
            print(f"Total files tracked: {status['total_files']}")
            print(f"Total data: {status['total_bytes'] / (1024**3):.2f} GB")
            print(f"Last scan: {status['last_scan'] or 'Never'}")
            print(f"Last verification: {status['last_full_verify'] or 'Never'}")
            print("\nFiles by status:")
            for status_name, count in status['status_counts'].items():
                print(f"  {status_name}: {count}")
            sys.exit(0)

        # Run sync
        print("Starting incremental sync...")
        print(f"Source: {config.local_source_path}")
        print(f"Destination: {config.ssh_username}@{config.ssh_host}:{config.remote_dest_path}")
        print()

        stats = orchestrator.run_incremental_sync(
            dry_run=args.dry_run,
            verbose=args.verbose,
            full_sync=args.full
        )

        # Print summary
        print()
        print("=" * 40)
        print("Sync Summary")
        print("=" * 40)
        print(f"Files scanned: {stats.files_scanned}")
        if stats.files_skipped_recent > 0:
            print(f"  Skipped (too recent): {stats.files_skipped_recent}")
        if stats.files_skipped_not_modified > 0:
            print(f"  Skipped (not modified): {stats.files_skipped_not_modified}")
        print(f"New files: {stats.files_new}")
        print(f"Files transferred: {stats.files_transferred}")
        print(f"Files verified (already on remote): {stats.files_verified}")
        print(f"Files failed: {stats.files_failed}")
        print(f"Checksum mismatches: {stats.files_checksum_mismatch}")
        print(f"Checksums computed: {stats.checksums_computed}")
        print(f"Checksums compared: {stats.checksums_compared}")
        print(f"Data transferred: {stats.bytes_transferred / (1024**2):.2f} MB")
        print(f"Duration: {stats.duration_seconds:.1f} seconds")

        if stats.errors:
            print(f"\nErrors ({len(stats.errors)}):")
            for error in stats.errors[:10]:
                print(f"  - {error}")
            if len(stats.errors) > 10:
                print(f"  ... and {len(stats.errors) - 10} more")

        exit_code = 0 if stats.files_failed == 0 else 1

        if log_output:
            log_output.close()
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
