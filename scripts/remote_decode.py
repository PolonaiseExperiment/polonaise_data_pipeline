#!/usr/bin/env python3
"""
Remote-side decompressor: rebuild a .tdms from an uploaded .flac + .json.

Runs ON THE REMOTE SERVER (invoked by the pipeline over SSH), from a git
clone of this repo. Prints exactly one JSON line to stdout:

    {"ok": true,  "data_xxh64": ..., "file_xxh64": ..., "file_bytes": N, "out": ...}
    {"ok": false, "error": "..."}

Exit code 0 iff ok. Needs numpy + soundfile + nptdms + xxhash (see the
pipeline venv described in README).

Usage:
    remote_decode.py --flac /path/to/x.flac --out /path/to/x.tdms \
                     --expect-data-hash <xxh64hex>

Author: tunnell (https://github.com/tunnell)
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def main():
    parser = argparse.ArgumentParser(description="Rebuild a TDMS from FLAC+sidecar")
    parser.add_argument("--flac", required=True, help="Path to the uploaded .flac (sidecar .json must sit next to it)")
    parser.add_argument("--out", required=True, help="Final path for the rebuilt .tdms")
    parser.add_argument("--expect-data-hash", default=None,
                        help="xxh64 of the original float64 sample bytes; decode must match")
    args = parser.parse_args()

    try:
        from pipeline.flaccodec import rebuild_tdms

        result = rebuild_tdms(args.flac, args.out,
                              expect_data_xxh64=args.expect_data_hash)
        print(json.dumps({"ok": True, "out": args.out, **result}))
        sys.exit(0)
    except Exception as e:
        print(json.dumps({"ok": False, "error": f"{type(e).__name__}: {e}"}))
        sys.exit(1)


if __name__ == "__main__":
    main()
