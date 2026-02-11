# Polonaise Data Pipeline

A smart data transfer tool for experimental physics that tracks files in a database and only transfers new/modified files. Designed for syncing large TDMS datasets from local storage to remote servers.

**Author:** tunnell ([github.com/tunnell](https://github.com/tunnell))

## Features

- **Incremental sync** - Only processes new/modified files (no full directory scans)
- **Checksum verification** - Uses xxHash64 for fast integrity checking (~10 GB/s)
- **Parallel transfers** - Configurable concurrent SFTP transfers
- **Database tracking** - TinyDB (JSON) with easy MongoDB migration path
- **Slack notifications** - Alerts for errors, completions, and status
- **Resume support** - Tracks transfer state, resumes after interruptions
- **Cross-platform** - Works on Windows and Linux

## Installation

```bash
cd C:\Users\lion\polonaise_data_pipeline
pip install -r requirements.txt
```

### Remote Server Setup

Install xxHash on the remote server (required for checksum verification):

```bash
# Ubuntu/Debian
sudo apt install xxhash

# Verify installation
xxh64sum --version
```

## Configuration

Copy `.env.example` to `.env` and edit:

```bash
cp .env.example .env
```

### Required Settings

| Variable | Description | Example |
|----------|-------------|---------|
| `SSH_HOST` | Remote server hostname | `fried.rice.edu` |
| `SSH_USERNAME` | SSH username | `polonaise` |
| `SSH_KEY_PATH` | Path to SSH private key | `C:\Users\lion\.ssh\id_ed25519` |
| `LOCAL_SOURCE_PATH` | Local directory to sync | `Z:\MLMP\Data\Run45` |
| `REMOTE_DEST_PATH` | Remote destination path | `/stor2/polonaise/Run45` |
| `DATABASE_PATH` | Path to pipeline database | `C:\Users\lion\Documents\pipeline.json` |

### Optional Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_PARALLEL_TRANSFERS` | `3` | Number of concurrent transfers |
| `SKIP_RECENT_MINUTES` | `15` | Skip files modified within this time (safety for files being written) |
| `SLACK_WEBHOOK_URL` | - | Slack webhook for notifications |
| `FULL_VERIFY_HOUR` | `3` | Hour (24h) to run daily verification |

## Usage

### Incremental Sync (Normal)

```bash
python scripts/sync.py
```

Only syncs files modified since the last run.

### Full Sync

```bash
python scripts/sync.py --full
```

Checks all files, ignoring last scan time. Use for:
- First sync of existing data
- Re-verifying entire dataset
- After manual changes to remote

### Dry Run

```bash
python scripts/sync.py --dry-run
```

Shows what would be transferred without actually transferring.

### Verbose Mode

```bash
python scripts/sync.py --verbose
```

Shows why files are skipped (too recent, not modified, etc.)

### Check Status

```bash
python scripts/sync.py --status
```

Shows pipeline status: files tracked, transfer states, last sync time.

### Full Verification

```bash
python scripts/verify.py
```

Verifies checksums of all tracked files (local and remote).

### Command Line Options

| Option | Description |
|--------|-------------|
| `--full` | Full sync - check all files |
| `--dry-run` | Preview only, no transfers |
| `--verbose`, `-v` | Show debug info |
| `--status` | Show pipeline status |
| `--no-log` | Don't write to transfer.log |
| `--config PATH` | Use alternate .env file |

## How It Works

### Sync Process

```
Phase 1: Check files
├── Scan local directory for new/modified files
├── Skip files modified < 15 min ago (configurable)
├── For each file:
│   ├── Check database for existing record
│   ├── If new: compute local xxHash
│   ├── Check if exists on remote
│   ├── If exists: compare checksums
│   └── Mark for transfer if needed

Phase 2: Transfer files (parallel)
├── Transfer files via SFTP
├── Verify remote checksum after transfer
└── Update database with status

Phase 3: Cleanup
├── Sync database to remote
└── Send Slack notification
```

### File States

| Status | Description |
|--------|-------------|
| `pending` | New file, needs transfer |
| `transferring` | Transfer in progress |
| `transferred` | Transferred, awaiting verification |
| `verified` | Transfer complete, checksums match |
| `failed` | Transfer failed |
| `checksum_mismatch` | Remote checksum doesn't match |

## Architecture

```
polonaise_data_pipeline/
├── .env                    # Configuration (secrets)
├── .env.example            # Template
├── requirements.txt        # Dependencies
├── transfer.log            # Sync log (auto-generated)
│
├── pipeline/
│   ├── config.py           # Load settings from .env
│   ├── database.py         # TinyDB file tracking
│   ├── checksum.py         # xxHash computation
│   ├── scanner.py          # File discovery
│   ├── transfer.py         # Paramiko SFTP
│   ├── slack.py            # Notifications
│   └── orchestrator.py     # Main sync logic
│
├── scripts/
│   ├── sync.py             # Incremental sync entry point
│   └── verify.py           # Full verification entry point
│
└── tests/
    └── test_*.py           # Unit tests
```

## Database Schema

See [SCHEMA.md](SCHEMA.md) for full field documentation.

Key fields per file:
- `file_path` - Relative path (primary key)
- `local_checksum` - xxHash64 of local file
- `remote_checksum` - xxHash64 of remote file
- `transfer_status` - Current state
- `failed_checksum_count` - Mismatch counter

## Logging

All sync output is written to both:
- Terminal (stdout)
- `transfer.log` (in project directory)

Each session is timestamped:
```
============================================================
Session started: 2026-02-11T14:30:00
============================================================
Starting incremental sync...
...
Session ended: 2026-02-11T14:35:00
```

Use `--no-log` to disable file logging.

## Running as Scheduled Task

### Windows Task Scheduler

1. Open Task Scheduler
2. Create Basic Task
3. Trigger: Every 10 minutes
4. Action: Start a program
   - Program: `python`
   - Arguments: `C:\Users\lion\polonaise_data_pipeline\scripts\sync.py`
   - Start in: `C:\Users\lion\polonaise_data_pipeline`

### Linux Cron

```bash
*/10 * * * * cd /path/to/polonaise_data_pipeline && python scripts/sync.py >> /var/log/pipeline.log 2>&1
```

## Troubleshooting

### Files not being synced

Run with `--verbose` to see why files are skipped:
```bash
python scripts/sync.py --full --verbose
```

Common reasons:
- File modified < 15 min ago (configurable via `SKIP_RECENT_MINUTES`)
- File not modified since last scan (use `--full` to force check)

### Slow transfers

Check `transfer.log` for transfer speeds. Optimizations applied:
- SSH compression enabled
- Large SFTP buffer (1MB)
- Pipelining enabled
- Parallel transfers

### Connection errors

Test SSH connection:
```bash
ssh -i <key_path> user@host "echo OK"
```

Test xxHash on remote:
```bash
ssh user@host "xxh64sum --version"
```

### Reset database

To start fresh, delete the database file:
```bash
del C:\Users\lion\Documents\pipeline.json
```

## Migration to MongoDB

The TinyDB database stores JSON documents with the same structure as MongoDB. To migrate:

```python
from tinydb import TinyDB
from pymongo import MongoClient

tiny = TinyDB('pipeline.json')
mongo = MongoClient('mongodb://...')['pipeline']['files']

for record in tiny.table('files').all():
    mongo.insert_one(record)
```

## Running Tests

```bash
cd C:\Users\lion\polonaise_data_pipeline
pytest
```

With coverage:
```bash
pytest --cov=pipeline
```
