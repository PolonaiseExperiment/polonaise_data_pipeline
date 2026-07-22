"""
Tests for scanner module.

Author: tunnell (https://github.com/tunnell)
"""

import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from pipeline.scanner import (
    FileScanner,
    parse_timestamp_from_filename,
    get_file_times
)


@pytest.fixture
def source_dir(tmp_path):
    """Create a source directory with test files."""
    source = tmp_path / "source"
    source.mkdir()

    # Create some files
    (source / "file1.tdms").write_text("data1")
    (source / "file2.tdms").write_text("data2data2")
    (source / "file3.txt").write_text("text")

    # Create a subdirectory
    subdir = source / "subdir"
    subdir.mkdir()
    (subdir / "file4.tdms").write_text("sub")

    return source


class TestParseTimestamp:
    """Tests for timestamp parsing from filenames."""

    def test_iso_date(self):
        """Test parsing ISO date."""
        result = parse_timestamp_from_filename("2025-01-15_data.tdms")
        assert result == datetime(2025, 1, 15)

    def test_iso_datetime(self):
        """Test parsing ISO datetime."""
        result = parse_timestamp_from_filename("experiment_2025-01-15T14-30-00.tdms")
        assert result == datetime(2025, 1, 15, 14, 30, 0)

    def test_compact_date(self):
        """Test parsing compact date."""
        result = parse_timestamp_from_filename("data_20250115.tdms")
        assert result == datetime(2025, 1, 15)

    def test_compact_datetime(self):
        """Test parsing compact datetime."""
        result = parse_timestamp_from_filename("experiment_20250115_143000.tdms")
        assert result == datetime(2025, 1, 15, 14, 30, 0)

    def test_no_timestamp(self):
        """Test file with no timestamp."""
        result = parse_timestamp_from_filename("data.tdms")
        assert result is None

    def test_invalid_timestamp(self):
        """Test file with invalid timestamp."""
        result = parse_timestamp_from_filename("data_99999999.tdms")
        assert result is None


class TestGetFileTimes:
    """Tests for file time retrieval."""

    def test_get_times(self, tmp_path):
        """Test getting mtime and ctime."""
        file_path = tmp_path / "test.txt"
        file_path.write_text("test")

        mtime, ctime = get_file_times(file_path)

        assert isinstance(mtime, datetime)
        assert isinstance(ctime, datetime)
        # Both times should be recent
        assert mtime > datetime.now() - timedelta(minutes=1)


class TestFileScanner:
    """Tests for FileScanner class."""

    def test_scan_all(self, source_dir):
        """Test scanning all files."""
        scanner = FileScanner(source_dir, skip_recent_minutes=0)
        files = list(scanner.scan_all())

        # Should find all 4 files
        assert len(files) == 4
        assert all(isinstance(f, Path) for f in files)

    def test_scan_with_extension_filter(self, source_dir):
        """Test scanning with extension filter."""
        scanner = FileScanner(
            source_dir,
            skip_recent_minutes=0,
            extensions=[".tdms"]
        )
        files = list(scanner.scan_all())

        # Should find only .tdms files
        assert len(files) == 3
        assert all(f.suffix == ".tdms" for f in files)

    def test_skip_recent_files(self, source_dir):
        """Test skipping recently modified files."""
        # Create a file and scan with skip
        scanner = FileScanner(source_dir, skip_recent_minutes=60)
        files = list(scanner.scan_all())

        # All files are recent, should skip all
        assert len(files) == 0

    def test_scan_since(self, source_dir):
        """Test incremental scanning."""
        # First, create scanner without skip
        scanner = FileScanner(source_dir, skip_recent_minutes=0)

        # Scan since an hour ago - should find all
        since = datetime.now() - timedelta(hours=1)
        files = list(scanner.scan_since(since))
        assert len(files) == 4

        # Scan since the future - should find none
        since = datetime.now() + timedelta(hours=1)
        files = list(scanner.scan_since(since))
        assert len(files) == 0

    def test_get_relative_path(self, source_dir):
        """Test getting relative path."""
        scanner = FileScanner(source_dir, skip_recent_minutes=0)
        full_path = source_dir / "subdir" / "file4.tdms"

        relative = scanner.get_relative_path(full_path)

        # Should be relative to source
        assert relative == "subdir/file4.tdms" or relative == "subdir\\file4.tdms"

    def test_get_file_info(self, source_dir):
        """Test getting file info."""
        scanner = FileScanner(source_dir, skip_recent_minutes=0)
        file_path = source_dir / "file1.tdms"

        info = scanner.get_file_info(file_path)

        assert info["file_name"] == "file1.tdms"
        assert info["file_extension"] == ".tdms"
        assert info["file_size_bytes"] == 5  # "data1"
        assert "file_mtime" in info
        assert "file_ctime" in info

    def test_count_files(self, source_dir):
        """Test counting files."""
        scanner = FileScanner(source_dir, skip_recent_minutes=0)

        count = scanner.count_files()
        assert count == 4

    def test_count_with_filter(self, source_dir):
        """Test counting with extension filter."""
        scanner = FileScanner(
            source_dir,
            skip_recent_minutes=0,
            extensions=[".txt"]
        )

        count = scanner.count_files()
        assert count == 1


@pytest.fixture
def archive_dir(tmp_path):
    """Create an archive-shaped directory: run folders plus a loose root file."""
    archive = tmp_path / "archive"
    archive.mkdir()

    files = [
        "Run45_2025_December_UHDM/TDMS/a.tdms",
        "Run45_2025_December_UHDM/nested/deep/b.tdms",
        "Run46_2026_February/TDMS/c.tdms",
        "Run12_2020_August/TDMS/old1.tdms",
        "Run12_2020_August/TDMS/old2.tdms",
        "00 - TDMS_2020_Jan_and_2021_Sep_unsorted/x.tdms",
        "loose_at_root.xlsx",
    ]

    for relative in files:
        target = archive / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(relative)

    return archive


class TestFileScannerIgnore:
    """Tests for ignore_dirs and skip_root_files scanning options."""

    def test_no_ignore_scans_everything(self, archive_dir):
        """Without any ignore options every file is returned."""
        scanner = FileScanner(archive_dir, skip_recent_minutes=0)

        files = list(scanner.scan_all())

        assert len(files) == 7

    def test_ignore_excludes_directory(self, archive_dir):
        """A top-level directory listed in ignore_dirs is never descended into."""
        scanner = FileScanner(
            archive_dir,
            skip_recent_minutes=0,
            ignore_dirs=["Run12_2020_August"]
        )

        files = list(scanner.scan_all())

        assert len(files) == 5
        assert all("Run12_2020_August" not in str(f) for f in files)

    def test_ignore_multiple_including_spaces(self, archive_dir):
        """Multiple ignore entries work, including folder names containing spaces."""
        scanner = FileScanner(
            archive_dir,
            skip_recent_minutes=0,
            ignore_dirs=[
                "Run12_2020_August",
                "00 - TDMS_2020_Jan_and_2021_Sep_unsorted"
            ]
        )

        files = list(scanner.scan_all())

        assert len(files) == 4

    def test_ignore_only_applies_at_top_level(self, archive_dir):
        """Ignoring a name that only exists deeper than the root prunes nothing."""
        scanner = FileScanner(
            archive_dir,
            skip_recent_minutes=0,
            ignore_dirs=["TDMS"]
        )

        files = list(scanner.scan_all())

        assert len(files) == 7

    def test_skip_root_files(self, archive_dir):
        """skip_root_files drops loose root files but still descends into subdirs."""
        scanner = FileScanner(
            archive_dir,
            skip_recent_minutes=0,
            skip_root_files=True
        )

        files = list(scanner.scan_all())

        assert len(files) == 6
        assert all(not str(f).endswith("loose_at_root.xlsx") for f in files)
        assert any(str(f).endswith("b.tdms") for f in files)
        assert any(str(f).endswith("a.tdms") for f in files)

    def test_ignore_applies_to_scan_all_with_stats(self, archive_dir):
        """scan_all_with_stats honours ignore_dirs (used by the orchestrator)."""
        scanner = FileScanner(
            archive_dir,
            skip_recent_minutes=0,
            ignore_dirs=["Run12_2020_August"]
        )

        result = scanner.scan_all_with_stats()

        assert len(result.files) == 5

    def test_ignore_applies_to_scan_since_with_stats(self, archive_dir):
        """scan_since_with_stats honours ignore_dirs (used by the orchestrator)."""
        scanner = FileScanner(
            archive_dir,
            skip_recent_minutes=0,
            ignore_dirs=["Run12_2020_August"]
        )

        result = scanner.scan_since_with_stats(datetime(2000, 1, 1))

        assert len(result.files) == 5

    def test_ignore_applies_to_count_files(self, archive_dir):
        """count_files honours ignore_dirs."""
        scanner = FileScanner(
            archive_dir,
            skip_recent_minutes=0,
            ignore_dirs=["Run12_2020_August"]
        )

        assert scanner.count_files() == 5
