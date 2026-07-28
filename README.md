# Polonaise Data Pipeline

A smart data transfer tool for experimental physics that tracks files in a database and only transfers new/modified files. Designed for syncing large TDMS datasets from local storage to remote servers.

**Author:** tunnell ([github.com/tunnell](https://github.com/tunnell))

## Features

- **Whole-archive sync** - Points at the archive root; new run folders are picked up automatically
- **Incremental sync** - Only processes new/modified files (no full directory scans)
- **Checksum verification** - Uses xxHash64 for fast integrity checking (~10 GB/s)
- **Parallel transfers** - Configurable concurrent SFTP transfers
- **Database tracking** - TinyDB (JSON) with easy MongoDB migration path
- **Slack notifications** - Alerts for errors, completions, and status
- **Daemon mode** - Long-lived process that scans on a schedule, with keyboard controls
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

The pipeline uses **one** configuration file that points at the **archive root**, plus an
ignore list naming the top-level folders that should be left alone.

Copy the template to `.env` in the project root and edit it:

```bash
cp runs/template.env .env
```

The paths that matter:

```ini
LOCAL_SOURCE_PATH=Z:\MLMP\Zeppelin - DataArchive
REMOTE_DEST_PATH=/stor2/polonaise
DATABASE_PATH=C:\Users\lion\Documents\pipeline_archive.json
IGNORE_LIST_FILE=archive_ignore.txt
SKIP_ROOT_FILES=true
```

Everything under the archive root is synced **except** the folders listed in
`archive_ignore.txt`. New run folders are picked up automatically — starting a run
requires no config change.

`archive_ignore.txt` lives in the project root. One top-level folder name per line,
`#` starts a comment, blank lines are ignored:

```
# Folders NOT synced to /stor2/polonaise
Run12_2020_August
Run13_2020_Oktober
```

Remote paths are unchanged by this layout: archive root + `/stor2/polonaise` produces
exactly the same remote paths as the older one-config-per-run setup, so nothing already
uploaded is re-sent.

### Adding or retiring a run folder

- **New run folder** — nothing to do. It appears under the archive root and the next scan
  picks it up.
- **Stop syncing a folder** — add its exact folder name to `archive_ignore.txt`. The
  scanner skips it entirely from then on.

**Caveat when retiring a folder that has already been synced:** its records stay in the
database. `verify.py` iterates the database, not the filesystem, so it keeps checking those
files. Any mismatch marks them `pending`, and they stay `pending` forever because the
scanner no longer sees them. Those stale `pending` rows are counted as problems in the
Slack summary. There is currently no automatic purge — remove such records by hand if the
noise matters.

### Required Settings

| Variable | Description | Example |
|----------|-------------|---------|
| `SSH_HOST` | Remote server hostname | `fried.rice.edu` |
| `SSH_USERNAME` | SSH username | `polonaise` |
| `SSH_KEY_PATH` | Path to SSH private key | `C:\Users\lion\.ssh\id_ed25519` |
| `LOCAL_SOURCE_PATH` | Local archive root to sync | `Z:\MLMP\Zeppelin - DataArchive` |
| `REMOTE_DEST_PATH` | Remote archive root | `/stor2/polonaise` |
| `DATABASE_PATH` | Archive-wide pipeline database | `C:\Users\lion\Documents\pipeline_archive.json` |

### Optional Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_PARALLEL_TRANSFERS` | `3` | Number of concurrent transfers |
| `SKIP_RECENT_MINUTES` | `15` | Skip files modified within this time (safety for files being written) |
| `IGNORE_LIST_FILE` | - | Path to the ignore list. One top-level folder name per line, `#` for comments. Relative paths resolve against the project root. |
| `IGNORE_DIRS` | - | Comma-separated top-level folder names to skip. Alternative to `IGNORE_LIST_FILE`; the file wins if both are set. |
| `SKIP_ROOT_FILES` | `true` | Ignore loose files sitting directly in the archive root (Excel `~$` lock files, `Thumbs.db`, `desktop.ini` and similar cruft) |
| `SLACK_WEBHOOK_URL` | - | Slack webhook for notifications |
| `FULL_VERIFY_HOUR` | `3` | Hour (24h) to run daily verification |

## Usage

### Daemon Mode (Normal)

```bash
python scripts/sync.py --daemon
```

Runs as a long-lived process, scanning the whole archive every 10 minutes. This is the
normal way to run the pipeline — no arguments beyond `--daemon` are needed.

| Option | Description |
|--------|-------------|
| `--interval N` | Minutes between scans (default: 10) |
| `--continuous` | Scan back-to-back with a 1 minute pause |

While it runs:

```
q  - Quit (finishes current scan first)
p  - Pause / resume scanning
v  - Toggle verbose mode (show checksums and debug info)
f  - Run full sync with verification
s  - Trigger immediate scan
?  - Show this help
```

### One-Off Incremental Sync

```bash
python scripts/sync.py
```

Scans once and exits. Only syncs files modified since the last run.

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

Verifies checksums of all tracked files (local and remote). A remote copy that fails
verification is quarantined to `_mismatched/` and marked for re-transfer.

### Command Line Options

| Option | Description |
|--------|-------------|
| `--daemon` | Run in daemon mode (loop forever) |
| `--interval N` | Minutes between scans in daemon mode (default: 10) |
| `--continuous` | Daemon scans back-to-back with a 1 minute pause |
| `--full` | Full sync - check all files |
| `--dry-run` | Preview only, no transfers |
| `--verbose`, `-v` | Show debug info |
| `--status` | Show pipeline status |
| `--no-log` | Don't write to transfer.log |
| `--ignore-dirs NAMES` | Override the ignore list (comma-separated folder names) |
| `--skip-root-files BOOL` | Override `SKIP_ROOT_FILES` (`true`/`false`) |
| `--config PATH` | Use an alternate .env file |
| `--run NAME` | Load `runs/<name>.env` instead of `.env` |

Every setting in the tables above also has a matching `--long-option` override; run
`python scripts/sync.py --help` for the full list.

`--run` and `--config` are generic .env loaders. They still work for one-off jobs against
a different source or database, but normal operation needs neither.

## How It Works

### Sync Process

```
Phase 1: Check files
├── Walk the archive root for new/modified files
├── Skip top-level folders listed in archive_ignore.txt
├── Skip loose files in the archive root (SKIP_ROOT_FILES)
├── Skip files modified < 15 min ago (configurable)
├── For each file:
│   ├── Check database for existing record
│   ├── If new: compute local xxHash
│   ├── Check if exists on remote
│   ├── If exists: compare checksums
│   └── Mark for transfer if needed

Phase 2: Transfer files (parallel)
├── Transfer files via SFTP (persistent connection per worker thread)
├── Verify remote checksum after transfer
└── Update database with status

Phase 3: Cleanup
├── Sync database to remote
└── Send Slack notification
```

### Compressed Transfers (COMPRESSION_ENABLED=true)

Large single-channel `.tdms` files travel compressed (~12x smaller on the
wire) and land on the remote in two forms:

```
REMOTE_COMPRESSED_PATH/<run>/x.flac + x.json    faithful restoration bundle
REMOTE_DEST_PATH/<run>/x.tdms                   readable rebuilt TDMS
```

Per file: encode TDMS -> FLAC + JSON sidecar locally (integer-code
transform from PolonaiseExperiment/compression; the encoder decodes its own
output and bit-compares before anything ships) -> upload both, wire-verified
with xxh64 -> `scripts/remote_decode.py` runs on the remote (venv) to
rebuild the `.tdms` at the normal destination path and re-verify the
float64 sample hash end-to-end.

The rebuilt file holds bit-identical data and channel properties but a
different segment layout, so its *file* hash legitimately differs from the
original's — records track `data_checksum` (samples), `local_checksum`
(original file) and `remote_checksum` (rebuilt file) separately, and full
verification checks the rebuilt tdms, the .flac and the .json against their
recorded hashes.

Not everything compresses: multi-channel/odd TDMS, files under
`COMPRESSION_MIN_BYTES`, and any file the codec can't reproduce bit-exactly
fall back to plain byte-copy transfers automatically. `.tdms_index` files
ship (uncompressed) to the *compressed* tree: they describe the original
segment layout, which does not match rebuilt files.

Check a machine's codec against real archive files any time with:

```bash
python scripts/verify_compression.py --dir "Z:\path\to\RunXX" --sample 5
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
├── .env                    # Archive configuration (secrets, gitignored)
├── .env.example            # Bare-bones template (no ignore settings)
├── archive_ignore.txt      # Top-level folders NOT synced
├── requirements.txt        # Dependencies
├── transfer.log            # Sync log (auto-generated)
│
├── pipeline/
│   ├── config.py           # Load settings from .env, CLI overrides
│   ├── database.py         # TinyDB file tracking
│   ├── checksum.py         # xxHash computation
│   ├── scanner.py          # File discovery, ignore list handling
│   ├── transfer.py         # Paramiko SFTP (tuned socket, pipelined writes)
│   ├── flaccodec.py        # Lossless TDMS <-> FLAC codec (also runs remotely)
│   ├── slack.py            # Notifications
│   ├── output.py           # Structured logging (terminal + log file)
│   ├── daemon.py           # Daemon loop and keyboard controls
│   └── orchestrator.py     # Main sync logic
│
├── runs/
│   └── template.env        # Config template
│
├── scripts/
│   ├── sync.py             # Incremental sync entry point
│   ├── verify.py           # Full verification entry point
│   ├── verify_compression.py  # Prove codec bit-exactness on real files
│   └── remote_decode.py    # Runs ON THE REMOTE: rebuild .tdms from .flac
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
- `transfer.log` in the project directory — one log for the whole archive
  (change it with `LOG_FILE_PATH`)

Lines are prefixed `[LEVEL - HH:MM:SS]`:

```
[INFO - 14:30:00] Scanning Z:\MLMP\Zeppelin - DataArchive (may take a while on network drives)...
[INFO - 14:31:12] Found 8 files to process
[INFO - 14:33:40]   shot_0142.tdms  512.0 MB  74s  OK
[INFO - 14:35:02] Sync complete: 8 transferred, 0 failed, 4096.0 MB in 302s
```

`DEBUG` lines (checksums, skip reasons) appear only with `--verbose`, or after pressing
`v` in daemon mode. When a config is loaded with `--run NAME`, that label is added to the
prefix: `[run46 - INFO - 14:30:00]`.

The log file additionally gets a banner at the start and end of each session:

```
============================================================
Session started: 2026-02-11T14:30:00
============================================================
```

Use `--no-log` to disable file logging.

## Running Unattended

Daemon mode (`python scripts/sync.py --daemon`) is the simplest option and needs no
scheduler at all. If you prefer a scheduler, **exactly one** task covers the entire
archive — new run folders need no additional tasks.

### Windows Task Scheduler

1. Open Task Scheduler
2. Create Basic Task
3. Trigger: Every 10 minutes
4. Action: Start a program
   - Program: `python`
   - Arguments: `C:\Users\lion\polonaise_data_pipeline\scripts\sync.py`
   - Start in: `C:\Users\lion\polonaise_data_pipeline`

To keep the daemon alive across reboots instead, use one task with the trigger "At startup"
and arguments `...\scripts\sync.py --daemon`.

### Linux Cron

```bash
*/10 * * * * cd /path/to/polonaise_data_pipeline && python scripts/sync.py >> /var/log/pipeline.log 2>&1
```

## Troubleshooting

### Files not being synced

**Check `archive_ignore.txt` first** — if the file's top-level folder is listed there, the
scanner skips the whole folder. The name must match the folder exactly. This is by far the
most common cause.

Then run with `--verbose` to see why the remaining files are skipped:
```bash
python scripts/sync.py --full --verbose
```

Common reasons:
- The top-level folder is listed in `archive_ignore.txt` (or in `IGNORE_DIRS`)
- The file sits loose in the archive root and `SKIP_ROOT_FILES=true`
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
del C:\Users\lion\Documents\pipeline_archive.json
```

**Warning:** there is one database for the entire archive. Deleting it discards tracking
state for *every* run folder, not just the one you are troubleshooting. The next sync will
re-scan and re-checksum everything under the archive root. Nothing is re-uploaded that
already matches on the remote, but the first scan afterwards is slow.

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
