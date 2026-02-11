# Database Schema Documentation

Author: tunnell (https://github.com/tunnell)

This document describes all fields in the pipeline database.

---

## File Records Table

Each file being tracked has a record with the following fields:

### Identification

| Field | Type | Description |
|-------|------|-------------|
| `file_path` | string | **Primary key.** Relative path from source root (e.g., `2025-01-15/data_001.tdms`) |
| `file_name` | string | Base filename without path (e.g., `data_001.tdms`) |
| `file_extension` | string | File extension (e.g., `.tdms`) |

### Size & Timestamps

| Field | Type | Description |
|-------|------|-------------|
| `file_size_bytes` | int | File size in bytes |
| `file_mtime` | datetime | **Modification time** from filesystem. When file contents were last modified. |
| `file_ctime` | datetime | **Creation time** from filesystem. When file was created (Windows) or inode changed (Linux). |

### Parsed Timestamps

These are extracted from the filename or file metadata. May be null if not parseable.

| Field | Type | Description |
|-------|------|-------------|
| `filename_timestamp` | datetime | Timestamp parsed from filename pattern. Null if filename doesn't contain parseable time. |
| `tdms_start_time` | datetime | Start time from TDMS file metadata (wf_start_time property). Null if not a TDMS file or not readable. |
| `tdms_end_time` | datetime | End time from TDMS file metadata. Null if not available. |

### Checksums

| Field | Type | Description |
|-------|------|-------------|
| `local_checksum` | string | xxHash64 checksum of local file. 16-character hex string. |
| `remote_checksum` | string | xxHash64 checksum of remote file. Null if not yet transferred or verified. |
| `checksum_algorithm` | string | Algorithm used (always `xxh64` for this pipeline). |

### Transfer Status

| Field | Type | Description |
|-------|------|-------------|
| `transfer_status` | enum | Current state of the file. See values below. |
| `transfer_attempts` | int | Number of transfer attempts made. |
| `last_transfer_time` | datetime | When the last transfer was attempted. |
| `failed_checksum_count` | int | Number of times remote checksum mismatched after transfer. Indicates potential corruption or transfer issues. |

**transfer_status values:**
- `pending` - File detected but not yet transferred
- `transferring` - Transfer in progress
- `transferred` - Transfer complete, awaiting verification
- `verified` - Transfer complete AND remote checksum matches
- `failed` - Transfer failed (see `last_error`)
- `checksum_mismatch` - Remote checksum doesn't match local

### Verification

| Field | Type | Description |
|-------|------|-------------|
| `first_seen` | datetime | When this file was first detected by the pipeline. |
| `last_verified_local` | datetime | When local checksum was last verified (re-computed and matched). |
| `last_verified_remote` | datetime | When remote checksum was last verified. |

### Data Quality

| Field | Type | Description |
|-------|------|-------------|
| `data_quality` | enum | Quality assessment of the data. Set by external script. |
| `data_quality_notes` | string | Optional notes about quality issues. |

**data_quality values:**
- `unknown` - Not yet assessed (default)
- `good` - Data passes quality checks
- `bad` - Data has quality issues
- `quarantine` - Data flagged for review

### Error Tracking

| Field | Type | Description |
|-------|------|-------------|
| `last_error` | string | Most recent error message, if any. Null if no errors. |
| `last_error_time` | datetime | When the last error occurred. |

---

## Sync State Table

Global state for the pipeline (single record).

| Field | Type | Description |
|-------|------|-------------|
| `last_scan_time` | datetime | When we last scanned for new/modified files. Used for incremental scans. |
| `last_full_verify_time` | datetime | When we last ran full verification of all files. |
| `last_db_sync_time` | datetime | When database was last synced to remote. |
| `total_files_tracked` | int | Count of files in database. |
| `total_bytes_tracked` | int | Sum of all file sizes. |

---

## Example Record

```json
{
  "file_path": "2025-01-15/experiment_001.tdms",
  "file_name": "experiment_001.tdms",
  "file_extension": ".tdms",
  "file_size_bytes": 209715200,
  "file_mtime": "2025-01-15T14:32:15",
  "file_ctime": "2025-01-15T14:30:00",
  "filename_timestamp": "2025-01-15T00:00:00",
  "tdms_start_time": "2025-01-15T14:30:00.123456",
  "tdms_end_time": "2025-01-15T14:32:15.789012",
  "local_checksum": "a1b2c3d4e5f67890",
  "remote_checksum": "a1b2c3d4e5f67890",
  "checksum_algorithm": "xxh64",
  "transfer_status": "verified",
  "transfer_attempts": 1,
  "last_transfer_time": "2025-01-15T14:35:00",
  "failed_checksum_count": 0,
  "first_seen": "2025-01-15T14:33:00",
  "last_verified_local": "2025-01-16T03:00:00",
  "last_verified_remote": "2025-01-16T03:00:05",
  "data_quality": "unknown",
  "data_quality_notes": null,
  "last_error": null,
  "last_error_time": null
}
```

---

## Notes

### Why multiple timestamps?

Experimental data often has timing information in multiple places:
- **Filesystem times** are always available but can be modified by file operations
- **Filename times** are human-readable and often used for organization
- **TDMS metadata times** are the most accurate for when data was actually acquired

By capturing all of them, we:
1. Enable flexible querying by any time field
2. Can detect inconsistencies (e.g., filename doesn't match metadata)
3. Preserve information even if one source is unavailable

### Checksum considerations

- xxHash64 is non-cryptographic but extremely fast (~10 GB/s)
- For data integrity (not security), this is ideal
- 64-bit hash has negligible collision probability for our dataset size
- If cryptographic hashing needed later, add `sha256_checksum` field

### Migration to MongoDB

This schema maps directly to MongoDB documents. Migration steps:
1. Export TinyDB JSON
2. Import to MongoDB collection
3. Create indexes on: `file_path`, `transfer_status`, `last_verified_remote`
