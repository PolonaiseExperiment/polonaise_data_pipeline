"""
Output management for the pipeline.

Provides PipelineLogger with structured prefixes:
    [run_label - LEVEL - HH:MM:SS] message

Author: tunnell (https://github.com/tunnell)
"""

import sys
from pathlib import Path
from datetime import datetime


class PipelineLogger:
    """Structured logger with [run - LEVEL - timestamp] prefixes.

    INFO lines always print. DEBUG lines only print when verbose=True.
    Writes to terminal and optionally to a log file.
    """

    def __init__(self, run_label: str = "", verbose: bool = False,
                 stream=None, log_file_path=None):
        self.run_label = run_label
        self.verbose = verbose
        self.stream = stream or sys.__stdout__
        self._log_file = None

        if log_file_path:
            log_path = Path(log_file_path)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            self._log_file = open(log_path, "a", encoding="utf-8")
            self._log_file.write(f"\n{'=' * 60}\n")
            self._log_file.write(f"Session started: {datetime.now().isoformat()}\n")
            self._log_file.write(f"{'=' * 60}\n")
            self._log_file.flush()

    def _format(self, level: str, msg: str) -> str:
        ts = datetime.now().strftime("%H:%M:%S")
        if self.run_label:
            return f"[{self.run_label} - {level} - {ts}] {msg}"
        return f"[{level} - {ts}] {msg}"

    def info(self, msg: str = "") -> None:
        """Log an INFO message (always shown)."""
        line = self._format("INFO", msg) if msg else ""
        self.stream.write(line + "\n")
        self.stream.flush()
        if self._log_file:
            self._log_file.write(line + "\n")
            self._log_file.flush()

    def debug(self, msg: str = "") -> None:
        """Log a DEBUG message (only shown when verbose=True)."""
        if not self.verbose:
            return
        line = self._format("DEBUG", msg)
        self.stream.write(line + "\n")
        self.stream.flush()
        if self._log_file:
            self._log_file.write(line + "\n")
            self._log_file.flush()

    def write_status(self, msg: str) -> None:
        """Write an in-place status line (uses \\r). No log file output."""
        ts = datetime.now().strftime("%H:%M:%S")
        if self.run_label:
            prefix = f"[{self.run_label} - INFO - {ts}]"
        else:
            prefix = f"[INFO - {ts}]"
        self.stream.write(f"\r{prefix} {msg}    ")
        self.stream.flush()

    def clear_status(self) -> None:
        """Clear an in-place status line."""
        self.stream.write("\r" + " " * 100 + "\r")
        self.stream.flush()

    def close(self) -> None:
        if self._log_file:
            self._log_file.write(f"\nSession ended: {datetime.now().isoformat()}\n")
            self._log_file.close()
            self._log_file = None
