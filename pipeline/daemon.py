"""
Daemon mode for continuous sync operation.

Runs incremental sync on a schedule with interactive keyboard controls
and optional remote control via the database.

Author: tunnell (https://github.com/tunnell)
"""

import sys
import time
from datetime import datetime
from typing import Optional

from .config import Config
from .database import FileDatabase
from .orchestrator import SyncOrchestrator
from .output import PipelineLogger


def _kbhit():
    """Non-blocking check for keyboard input (cross-platform)."""
    try:
        import msvcrt
        return msvcrt.kbhit()
    except ImportError:
        # Unix fallback
        import select
        return select.select([sys.stdin], [], [], 0)[0] != []


def _getch():
    """Read a single character without waiting for enter (cross-platform)."""
    try:
        import msvcrt
        return msvcrt.getch().decode("utf-8", errors="ignore")
    except ImportError:
        # Unix fallback
        import tty
        import termios
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            return sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)


HELP_TEXT = """\
  Keyboard controls:
    q  - Quit (finishes current scan first)
    p  - Pause / resume scanning
    v  - Toggle verbose mode (show checksums and debug info)
    f  - Run full sync with verification
    s  - Trigger immediate scan
    ?  - Show this help"""


class DaemonRunner:
    """Runs incremental sync in a loop with interactive controls."""

    def __init__(
        self,
        orchestrator: SyncOrchestrator,
        config: Config,
        logger: Optional[PipelineLogger] = None,
        interval_minutes: float = 10,
        continuous: bool = False,
        dry_run: bool = False,
    ):
        self.orchestrator = orchestrator
        self.config = config
        self.logger = logger or PipelineLogger()
        self.interval_minutes = interval_minutes
        self.continuous = continuous
        self.dry_run = dry_run

        self.quit_requested = False
        self.paused = False
        self.scan_now = False
        self.full_sync_requested = False

    def run(self) -> None:
        """Main daemon loop."""
        log = self.logger
        pause_minutes = 1 if self.continuous else self.interval_minutes

        interval_desc = "continuous" if self.continuous else f"{self.interval_minutes} min"
        log.info(f"Daemon started (interval: {interval_desc})")
        log.info("Press ? for keyboard controls")

        while not self.quit_requested:
            # Check for remote commands from database
            self._check_db_commands()

            if not self.paused:
                self._run_one_sync()

            if self.quit_requested:
                break

            # Sleep between scans (interruptible)
            self._interruptible_sleep(pause_minutes)

        log.info("Daemon stopped")

    def _run_one_sync(self) -> None:
        """Run a single incremental sync."""
        log = self.logger
        full = self.full_sync_requested
        self.full_sync_requested = False

        try:
            if full:
                log.info("Running full sync with verification...")
                stats = self.orchestrator.run_full_verification()
            else:
                stats = self.orchestrator.run_incremental_sync(
                    dry_run=self.dry_run,
                    verbose=log.verbose,
                    full_sync=False
                )

        except Exception as e:
            log.info(f"Error during sync: {e}")

    def _interruptible_sleep(self, minutes: float) -> None:
        """Sleep for the given minutes, checking for keypresses each second."""
        total_seconds = int(minutes * 60)
        self.scan_now = False

        for remaining in range(total_seconds, 0, -1):
            if self.quit_requested or self.scan_now:
                break

            # Show countdown
            mins, secs = divmod(remaining, 60)
            if self.paused:
                status = "Paused (p=resume, q=quit)"
            else:
                status = f"Next scan in {mins}:{secs:02d} (?=help)"
            self.logger.write_status(status)

            # Check keyboard
            self._check_keyboard()

            # Check DB commands every 10 seconds
            if remaining % 10 == 0:
                self._check_db_commands()

            time.sleep(1)

        # Clear the countdown line
        self.logger.clear_status()

    def _check_keyboard(self) -> None:
        """Check for and handle keyboard input."""
        if not _kbhit():
            return

        key = _getch().lower()
        log = self.logger

        # Clear the in-place status line before printing feedback
        log.clear_status()

        if key == "q":
            log.info("Quit requested")
            self.quit_requested = True

        elif key == "p":
            self.paused = not self.paused
            state = "Paused" if self.paused else "Resumed"
            log.info(state)

        elif key == "v":
            log.verbose = not log.verbose
            state = "on" if log.verbose else "off"
            log.info(f"Verbose {state}")

        elif key == "f":
            log.info("Full sync + verification requested")
            self.full_sync_requested = True
            self.scan_now = True

        elif key == "s":
            log.info("Scan now")
            self.scan_now = True

        elif key == "?":
            # Print help without prefix (it's a UI element)
            log.stream.write("\n" + HELP_TEXT + "\n\n")
            log.stream.flush()

    def _check_db_commands(self) -> None:
        """Check for control commands in the database."""
        try:
            db = self.orchestrator.db
            command = db.get_daemon_command()
            if command is None:
                return

            # Clear the in-place status line before printing feedback
            self.logger.clear_status()
            self.logger.info(f"Remote command: {command}")

            if command == "quit":
                self.quit_requested = True
            elif command == "pause":
                self.paused = True
            elif command == "resume":
                self.paused = False
            elif command == "verbose_on":
                self.logger.verbose = True
            elif command == "verbose_off":
                self.logger.verbose = False
            elif command == "full_sync":
                self.full_sync_requested = True
                self.scan_now = True
        except Exception:
            pass  # Don't crash the daemon if DB check fails
