"""
Tests for checksum module.

Author: tunnell (https://github.com/tunnell)
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipeline.checksum import compute_xxhash, ChecksumManager
from pipeline.config import Config


@pytest.fixture
def sample_file(tmp_path):
    """Create a sample file with known content."""
    file_path = tmp_path / "sample.bin"
    content = b"Hello, World! This is test data for checksum verification."
    file_path.write_bytes(content)
    return file_path


@pytest.fixture
def large_file(tmp_path):
    """Create a larger file for testing chunked reading."""
    file_path = tmp_path / "large.bin"
    chunk = b"X" * 1024 * 1024  # 1MB chunk
    with open(file_path, "wb") as f:
        for _ in range(10):
            f.write(chunk)
    return file_path


class TestXXHash:
    """Tests for xxHash computation."""

    def test_consistency(self, sample_file):
        """Test that xxhash produces consistent results."""
        hash1 = compute_xxhash(sample_file)
        hash2 = compute_xxhash(sample_file)

        assert hash1 == hash2
        assert len(hash1) == 16  # xxh64 produces 16 hex chars

    def test_different_files_different_hashes(self, tmp_path):
        """Test that different files produce different hashes."""
        file1 = tmp_path / "file1.bin"
        file2 = tmp_path / "file2.bin"

        file1.write_bytes(b"Content A")
        file2.write_bytes(b"Content B")

        hash1 = compute_xxhash(file1)
        hash2 = compute_xxhash(file2)

        assert hash1 != hash2

    def test_large_file(self, large_file):
        """Test xxhash on larger file."""
        hash1 = compute_xxhash(large_file)
        assert len(hash1) == 16

    def test_empty_file(self, tmp_path):
        """Test checksum of empty file."""
        empty = tmp_path / "empty.bin"
        empty.write_bytes(b"")

        xxhash_result = compute_xxhash(empty)
        assert len(xxhash_result) == 16


class TestChecksumManager:
    """Tests for ChecksumManager class."""

    @pytest.fixture
    def mock_config(self):
        """Create a minimal config for ChecksumManager."""
        config = MagicMock(spec=Config)
        config.checksum_algorithm = "xxh64"
        config.checksum_chunk_size = 8 * 1024 * 1024
        config.remote_command_timeout = 300
        return config

    def test_compute_local(self, sample_file, mock_config):
        """Test manager computes xxhash."""
        manager = ChecksumManager(mock_config)
        checksum = manager.compute_local(sample_file)

        assert len(checksum) == 16

    def test_algorithm_is_xxh64(self, mock_config):
        """Test manager uses xxh64 algorithm."""
        manager = ChecksumManager(mock_config)
        assert manager.algorithm == "xxh64"

    def test_same_as_direct_call(self, sample_file, mock_config):
        """Test manager returns same result as direct function."""
        manager = ChecksumManager(mock_config)

        manager_result = manager.compute_local(sample_file)
        direct_result = compute_xxhash(sample_file)

        assert manager_result == direct_result
