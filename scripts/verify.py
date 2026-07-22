#!/usr/bin/env python3
"""
Full verification script - run daily to verify all checksums.

Usage:
    python verify.py                   # Verify every file tracked in the archive database
    python verify.py --quick           # (not yet implemented - falls back to full verification)

Note: verification iterates the database, not the filesystem, so it still checks
folders that were added to archive_ignore.txt after they were synced.

Author: tunnell (https://github.com/tunnell)
"""

import sys
import argparse
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.config import Config, get_config, add_config_args, apply_cli_overrides, resolve_config_path
from pipeline.output import PipelineLogger
from pipeline.orchestrator import SyncOrchestrator


def main():
    parser = argparse.ArgumentParser(description="Run full data verification")
    parser.add_argument("--run", dest="run_name", help="Run name (loads runs/<name>.env)")
    parser.add_argument("--config", type=Path, help="Path to .env file")
    parser.add_argument("--quick", action="store_true", help="Only verify files not verified in last 24h")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show debug details (checksums)")
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
    if config.log_file_path:
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
        if args.quick:
            logger.info("Quick verification mode — checking files not verified in last 24h")
            logger.info("Quick mode not yet implemented, running full verification")

        logger.info(f"Source: {config.local_source_path}")
        logger.info(f"Destination: {config.ssh_username}@{config.ssh_host}:{config.remote_dest_path}")

        stats = orchestrator.run_full_verification()

        if stats.errors:
            logger.info(f"Issues ({len(stats.errors)}):")
            for error in stats.errors[:20]:
                logger.info(f"  {error}")
            if len(stats.errors) > 20:
                logger.info(f"  ... and {len(stats.errors) - 20} more")

        logger.close()
        sys.exit(0 if (stats.files_failed == 0 and stats.files_checksum_mismatch == 0) else 1)


if __name__ == "__main__":
    main()
