#!/usr/bin/env python3
"""
Local compression verification: prove on THIS machine that
decode(encode(tdms)) reproduces the sample data bit-for-bit before trusting
the codec with real transfers. Mirrors the audit notebooks in
PolonaiseExperiment/compression, but runs the exact production codec
(pipeline/flaccodec.py) including the TDMS rebuild step.

Usage:
    python scripts/verify_compression.py --dir "Z:\\...\\Run48_2026_May" --sample 5
    python scripts/verify_compression.py --files a.tdms b.tdms

For each file: encode -> decode -> compare arrays bitwise; rebuild a .tdms
from the FLAC and re-read it to confirm data + properties survive. Exits 0
only if every sampled file is bit-exact.

Author: tunnell (https://github.com/tunnell)
"""

import argparse
import shutil
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np

from pipeline.flaccodec import (
    CodecUnsupported, data_xxh64, decode_flac, encode_tdms_to_flac,
    file_xxh64, read_tdms, rebuild_tdms,
)


def verify_one(tdms_path: Path, tmpdir: Path) -> bool:
    size_mb = tdms_path.stat().st_size / 1024 / 1024
    print(f"\n{tdms_path.name}  ({size_mb:.1f} MB)")

    data, props, group, channel = read_tdms(tdms_path)
    d_hash = data_xxh64(data)

    t0 = time.perf_counter()
    rep = encode_tdms_to_flac(tdms_path, tmpdir, exact=True,
                              original_file_xxh64=file_xxh64(tdms_path))
    t_enc = time.perf_counter() - t0

    comp_mb = (rep["flac_bytes"] + rep["sidecar_bytes"]) / 1024 / 1024
    ratio = tdms_path.stat().st_size / (rep["flac_bytes"] + rep["sidecar_bytes"])
    print(f"  encoding={rep['encoding']}  {comp_mb:.1f} MB  ratio={ratio:.1f}x  "
          f"encode {t_enc:.1f}s ({size_mb / t_enc:.0f} MB/s)")

    t0 = time.perf_counter()
    decoded, _ = decode_flac(rep["flac"])
    t_dec = time.perf_counter() - t0
    bit_exact = bool(np.array_equal(decoded, data))
    print(f"  decode {t_dec:.1f}s  bit-exact array: {bit_exact}  "
          f"data_xxh64 {'match' if data_xxh64(decoded) == d_hash else 'MISMATCH'}")

    # Rebuild a .tdms the way the remote will, and re-read it
    rebuilt_path = tmpdir / (tdms_path.stem + ".rebuilt.tdms")
    rb = rebuild_tdms(rep["flac"], rebuilt_path, expect_data_xxh64=d_hash)
    rdata, rprops, _, _ = read_tdms(rebuilt_path)
    rebuilt_exact = bool(np.array_equal(rdata, data))
    props_equal = set(props) == set(rprops) and all(
        np.array_equal(props[k], rprops[k]) if isinstance(props[k], np.ndarray)
        else props[k] == rprops[k]
        for k in props)
    print(f"  rebuilt tdms: data bit-exact: {rebuilt_exact}  "
          f"properties preserved: {props_equal}  "
          f"({rb['file_bytes']} B vs original {tdms_path.stat().st_size} B)")

    ok = rep["exact"] and bit_exact and rebuilt_exact and props_equal
    print(f"  => {'PASS' if ok else 'FAIL'}")
    return ok


def main():
    parser = argparse.ArgumentParser(description="Verify TDMS<->FLAC codec on real files")
    parser.add_argument("--dir", type=Path, help="Directory to sample .tdms files from (recursive)")
    parser.add_argument("--sample", type=int, default=3, help="How many files to sample from --dir")
    parser.add_argument("--min-mb", type=float, default=10, help="Ignore files smaller than this")
    parser.add_argument("--files", nargs="*", type=Path, help="Explicit .tdms files to verify")
    args = parser.parse_args()

    targets = list(args.files or [])
    if args.dir:
        candidates = [p for p in args.dir.rglob("*.tdms")
                      if p.stat().st_size >= args.min_mb * 1024 * 1024]
        # spread the sample across the directory, not just the first files
        step = max(1, len(candidates) // args.sample) if candidates else 1
        targets.extend(candidates[::step][:args.sample])

    if not targets:
        print("No .tdms files found to verify")
        sys.exit(1)

    tmpdir = Path(tempfile.mkdtemp(prefix="polonaise_verify_"))
    results = []
    try:
        for t in targets:
            try:
                results.append(verify_one(t, tmpdir))
            except CodecUnsupported as e:
                print(f"\n{t.name}\n  SKIP (codec unsupported, would transfer plainly): {e}")
            except Exception as e:
                print(f"\n{t.name}\n  FAIL with {type(e).__name__}: {e}")
                results.append(False)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    passed = sum(results)
    print(f"\n{passed}/{len(results)} verified bit-exact")
    sys.exit(0 if passed == len(results) else 1)


if __name__ == "__main__":
    main()
