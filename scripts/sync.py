#!/usr/bin/env python3
"""
Incremental sync script.

Usage:
    python sync.py --run run45              # Single sync for run45
    python sync.py --run run46 --daemon     # Daemon mode for run46
    python sync.py --dry-run                # Show what would be transferred
    python sync.py --status                 # Show pipeline status

Author: tunnell (https://github.com/tunnell)
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.config import Config, get_config, add_config_args, apply_cli_overrides, resolve_config_path
from pipeline.output import PipelineLogger
from pipeline.orchestrator import SyncOrchestrator
from pipeline.daemon import DaemonRunner


def main():
    parser = argparse.ArgumentParser(description="Run incremental data sync")
    parser.add_argument("--run", dest="run_name", help="Run name (loads runs/<name>.env)")
    parser.add_argument("--config", type=Path, help="Path to .env file")
    parser.add_argument("--dry-run", action="store_true", help="Don't transfer, just show what would happen")
    parser.add_argument("--full", action="store_true", help="Full sync - ignore last scan time, check all files")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show debug details (checksums, skip reasons)")
    parser.add_argument("--status", action="store_true", help="Show pipeline status and exit")
    parser.add_argument("--no-log", action="store_true", help="Don't write to log file")

    # Daemon mode
    daemon_group = parser.add_argument_group("daemon mode", "Run as a long-lived process")
    daemon_group.add_argument("--daemon", action="store_true", help="Run in daemon mode (loop forever)")
    daemon_group.add_argument("--interval", type=float, default=10, help="Minutes between scans (default: 10)")
    daemon_group.add_argument("--continuous", action="store_true", help="Scan back-to-back with 1 min pause")

    add_config_args(parser)
    args = parser.parse_args()

    # Resolve config file
    try:
        env_path, run_label = resolve_config_path(
            run_name=args.run_name,
            config_path=args.config
        )
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Load config
    try:
        if env_path:
            config = Config.from_env(env_path)
        else:
            config = get_config()

        apply_cli_overrides(config, args)

        errors = config.validate()
        if errors:
            print("Configuration errors:")
            for e in errors:
                print(f"  - {e}")
            sys.exit(1)

    except ValueError as e:
        print(f"Configuration error: {e}")
        sys.exit(1)

    # Setup logger
    log_file_path = None
    if not args.no_log and config.log_file_path:
        log_path = Path(config.log_file_path)
        if not log_path.is_absolute():
            log_path = Path(__file__).parent.parent / log_path
        log_file_path = str(log_path)

    logger = PipelineLogger(
        run_label=run_label or "",
        verbose=args.verbose,
        log_file_path=log_file_path,
    )

    # Run orchestrator
    with SyncOrchestrator(config, logger=logger) as orchestrator:
        if args.status:
            status = orchestrator.get_status()
            logger.info("Pipeline Status")
            logger.info(f"  Total files tracked: {status['total_files']}")
            logger.info(f"  Last scan: {status['last_scan'] or 'Never'}")
            logger.info(f"  Last verification: {status['last_full_verify'] or 'Never'}")
            logger.info(f"  Files by status:")
            for status_name, count in status['status_counts'].items():
                logger.info(f"    {status_name}: {count}")
            logger.close()
            sys.exit(0)

        logger.info(f"Source: {config.local_source_path}")
        logger.info(f"Destination: {config.ssh_username}@{config.ssh_host}:{config.remote_dest_path}")

        if args.daemon:
            # Daemon mode
            runner = DaemonRunner(
                orchestrator=orchestrator,
                config=config,
                logger=logger,
                interval_minutes=args.interval,
                continuous=args.continuous,
                dry_run=args.dry_run,
            )
            try:
                runner.run()
            except KeyboardInterrupt:
                logger.info("Interrupted by user")

            logger.close()
            sys.exit(0)

        # Single sync mode
        stats = orchestrator.run_incremental_sync(
            dry_run=args.dry_run,
            verbose=args.verbose,
            full_sync=args.full
        )

        # Errors
        if stats.errors:
            logger.info(f"Errors ({len(stats.errors)}):")
            for error in stats.errors[:10]:
                logger.info(f"  {error}")
            if len(stats.errors) > 10:
                logger.info(f"  ... and {len(stats.errors) - 10} more")

        # Verbose details
        logger.debug(f"Checksums computed: {stats.checksums_computed}")
        logger.debug(f"Checksums compared: {stats.checksums_compared}")
        logger.debug(f"Files scanned: {stats.files_scanned}")
        if stats.files_skipped_recent > 0:
            logger.debug(f"Skipped (too recent): {stats.files_skipped_recent}")
        if stats.files_skipped_not_modified > 0:
            logger.debug(f"Skipped (not modified): {stats.files_skipped_not_modified}")

        exit_code = 0 if stats.files_failed == 0 else 1
        logger.close()
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
