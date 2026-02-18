"""
File scanner for discovering new and modified files.

Author: tunnell (https://github.com/tunnell)
"""

import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator, Optional, List, Tuple
from dataclasses import dataclass

from tqdm import tqdm

from .config import Config


def parse_timestamp_from_filename(filename: str) -> Optional[datetime]:
    """Try to parse a timestamp from a filename.

    Supports common patterns like:
    - 2025-01-15_data.tdms
    - data_20250115_143000.tdms
    - experiment_2025-01-15T14-30-00.tdms

    Returns None if no timestamp found.
    """
    patterns = [
        # ISO-like: 2025-01-15 or 2025-01-15T14:30:00
        r"(\d{4}-\d{2}-\d{2}(?:T\d{2}[:-]\d{2}[:-]\d{2})?)",
        # Compact: 20250115 or 20250115_143000
        r"(\d{8}(?:_\d{6})?)",
    ]

    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            timestamp_str = match.group(1)
            # Try various formats
            for fmt in [
                "%Y-%m-%d",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H-%M-%S",
                "%Y%m%d",
                "%Y%m%d_%H%M%S",
            ]:
                try:
                    return datetime.strptime(timestamp_str, fmt)
                except ValueError:
                    continue
    return None


def get_file_times(file_path: Path) -> Tuple[datetime, datetime]:
    """Get modification and creation times for a file.

    Returns:
        Tuple of (mtime, ctime).
    """
    stat = file_path.stat()
    mtime = datetime.fromtimestamp(stat.st_mtime)
    ctime = datetime.fromtimestamp(stat.st_ctime)
    return mtime, ctime


@dataclass
class ScanResult:
    """Result of a directory scan."""
    files: List[Path]
    skipped_recent: int = 0
    skipped_not_modified: int = 0
    skipped_extension: int = 0


class FileScanner:
    """Scans directories for files to process."""

    def __init__(
        self,
        source_path: Path,
        skip_recent_minutes: int = 15,
        extensions: Optional[List[str]] = None
    ):
        """Initialize scanner.

        Args:
            source_path: Root directory to scan.
            skip_recent_minutes: Skip files modified within this many minutes.
            extensions: List of extensions to include (e.g., [".tdms"]).
                        If None, include all files.
        """
        self.source_path = source_path
        self.skip_recent_minutes = skip_recent_minutes
        self.extensions = [e.lower() for e in extensions] if extensions else None

    def _should_include(self, file_path: Path) -> bool:
        """Check if a file should be included based on extension."""
        if self.extensions is None:
            return True
        return file_path.suffix.lower() in self.extensions

    def _is_too_recent(self, file_path: Path) -> bool:
        """Check if file was modified too recently (might still be written)."""
        if self.skip_recent_minutes <= 0:
            return False

        mtime, _ = get_file_times(file_path)
        cutoff = datetime.now() - timedelta(minutes=self.skip_recent_minutes)
        return mtime > cutoff

    def scan_all(self, verbose: bool = False) -> Iterator[Path]:
        """Scan all files in the source directory.

        Args:
            verbose: If True, print summary of skipped files.

        Yields:
            Path objects for each file (excluding too-recent files).
        """
        skipped_extension = 0
        skipped_recent = 0

        for root, dirs, files in os.walk(self.source_path):
            for filename in files:
                file_path = Path(root) / filename

                if not self._should_include(file_path):
                    skipped_extension += 1
                    continue

                if self._is_too_recent(file_path):
                    skipped_recent += 1
                    continue

                yield file_path

        if verbose and (skipped_extension or skipped_recent):
            parts = []
            if skipped_extension:
                parts.append(f"{skipped_extension} wrong extension")
            if skipped_recent:
                parts.append(f"{skipped_recent} too recent (<{self.skip_recent_minutes} min)")
            print(f"  Skipped: {', '.join(parts)}")

    def scan_since(self, since: datetime, verbose: bool = False) -> Iterator[Path]:
        """Scan for files modified after a specific time.

        Args:
            since: Only include files modified after this time.
            verbose: If True, print summary of skipped files.

        Yields:
            Path objects for each matching file.
        """
        cutoff_recent = datetime.now() - timedelta(minutes=self.skip_recent_minutes)
        skipped_extension = 0
        skipped_not_modified = 0
        skipped_recent = 0

        for root, dirs, files in os.walk(self.source_path):
            for filename in files:
                file_path = Path(root) / filename

                if not self._should_include(file_path):
                    skipped_extension += 1
                    continue

                mtime, _ = get_file_times(file_path)

                if mtime <= since:
                    skipped_not_modified += 1
                    continue

                if mtime > cutoff_recent:
                    skipped_recent += 1
                    continue

                yield file_path

        if verbose and (skipped_extension or skipped_not_modified or skipped_recent):
            parts = []
            if skipped_extension:
                parts.append(f"{skipped_extension} wrong extension")
            if skipped_not_modified:
                parts.append(f"{skipped_not_modified} not modified")
            if skipped_recent:
                parts.append(f"{skipped_recent} too recent (<{self.skip_recent_minutes} min)")
            print(f"  Skipped: {', '.join(parts)}")

    def get_relative_path(self, file_path: Path) -> str:
        """Get path relative to source root."""
        return str(file_path.relative_to(self.source_path))

    def get_file_info(self, file_path: Path) -> dict:
        """Get metadata for a file.

        Returns dict with:
            - file_path: relative path
            - file_name: filename only
            - file_extension: extension
            - file_size_bytes: size
            - file_mtime: modification time (ISO)
            - file_ctime: creation time (ISO)
            - filename_timestamp: parsed from filename (ISO or None)
        """
        mtime, ctime = get_file_times(file_path)
        filename_ts = parse_timestamp_from_filename(file_path.name)

        return {
            "file_path": self.get_relative_path(file_path),
            "file_name": file_path.name,
            "file_extension": file_path.suffix,
            "file_size_bytes": file_path.stat().st_size,
            "file_mtime": mtime.isoformat(),
            "file_ctime": ctime.isoformat(),
            "filename_timestamp": filename_ts.isoformat() if filename_ts else None,
        }

    def count_files(self) -> int:
        """Count total files (excluding too-recent)."""
        return sum(1 for _ in self.scan_all())

    def count_files_since(self, since: datetime) -> int:
        """Count files modified since a specific time."""
        return sum(1 for _ in self.scan_since(since))

    def scan_all_with_stats(self, progress_stream=None) -> ScanResult:
        """Scan all files and return stats about skipped files.

        Args:
            progress_stream: Stream for tqdm progress bar (default: sys.stdout).
        """
        result = ScanResult(files=[])
        cutoff_recent = datetime.now() - timedelta(minutes=self.skip_recent_minutes)

        pbar = tqdm(desc="Scanning", unit=" dirs",
                    file=progress_stream or sys.stdout, leave=False)
        try:
            for root, dirs, files in os.walk(self.source_path):
                pbar.update(1)
                for filename in files:
                    file_path = Path(root) / filename

                    if not self._should_include(file_path):
                        result.skipped_extension += 1
                        continue

                    mtime, _ = get_file_times(file_path)

                    if mtime > cutoff_recent:
                        result.skipped_recent += 1
                        continue

                    result.files.append(file_path)
        finally:
            pbar.close()

        return result

    def scan_since_with_stats(self, since: datetime, progress_stream=None) -> ScanResult:
        """Scan for files modified after a specific time and return stats.

        Args:
            since: Only include files modified after this time.
            progress_stream: Stream for tqdm progress bar (default: sys.stdout).
        """
        result = ScanResult(files=[])
        cutoff_recent = datetime.now() - timedelta(minutes=self.skip_recent_minutes)

        pbar = tqdm(desc="Scanning", unit=" dirs",
                    file=progress_stream or sys.stdout, leave=False)
        try:
            for root, dirs, files in os.walk(self.source_path):
                pbar.update(1)
                for filename in files:
                    file_path = Path(root) / filename

                    if not self._should_include(file_path):
                        result.skipped_extension += 1
                        continue

                    mtime, _ = get_file_times(file_path)

                    if mtime <= since:
                        result.skipped_not_modified += 1
                        continue

                    if mtime > cutoff_recent:
                        result.skipped_recent += 1
                        continue

                    result.files.append(file_path)
        finally:
            pbar.close()

        return result
