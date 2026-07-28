"""
Lossless TDMS <-> FLAC codec for compressed transfers.

Ported from PolonaiseExperiment/compression (Andrew Gingerich, Summer 2026):
TDMS channel data is float64 samples sitting on an ADC grid, so each sample
maps to a small integer code that FLAC stores at ~10-18x compression. The
JSON sidecar carries everything needed to reproduce the float64 array
bit-for-bit, plus (our addition) the full TDMS channel properties so the
remote end can rebuild a readable .tdms file.

Byte-exactness contract: decode always reproduces the original float64
sample array bit-for-bit for 'affine_codes' and 'indexed' encodings.
'fixed_point' is lossy and is REJECTED by the pipeline (falls back to a
plain byte-copy transfer). A rebuilt .tdms contains identical data and
properties but is NOT byte-identical to the original file (different
segment layout), so file-level hashes of original and rebuilt differ.

This module must run on the remote side too: Python 3.8+, and decode needs
only numpy + soundfile + xxhash (nptdms is imported lazily, encode/rebuild
only).

Author: tunnell (https://github.com/tunnell)
"""

import base64
import json
import pathlib
import zlib
from typing import Optional, Tuple

import numpy as np
import soundfile as sf
import xxhash

CODEC_NAME = "polonaise-flac"
CODEC_VERSION = 2  # 1 = upstream notebook sidecar; 2 = + properties/hashes

DEFAULT_GROUP = "Group"
DEFAULT_CHANNEL = "Channel"

GRID_TOL = 1e-4  # max deviation (in steps) of an observed level from the grid


class CodecUnsupported(Exception):
    """File shape this codec cannot handle losslessly; transfer it plainly."""


def _bits_equal(a: np.ndarray, b: np.ndarray) -> bool:
    """Bit-level float equality. np.array_equal treats -0.0 == +0.0, but the
    end-to-end reference hash (data_xxh64) is over raw bytes where they
    differ — 'exact' must mean the same thing the hash means."""
    if a.size != b.size:
        return False
    return (np.ascontiguousarray(a, dtype="<f8").tobytes()
            == np.ascontiguousarray(b, dtype="<f8").tobytes())


def data_xxh64(data: np.ndarray) -> str:
    """xxHash64 of the sample array as little-endian float64 bytes.

    This is the end-to-end integrity reference: computed locally before
    encoding and again on the remote after decoding.
    """
    return xxhash.xxh64(
        np.ascontiguousarray(data, dtype="<f8").tobytes()
    ).hexdigest()


def file_xxh64(path, chunk_size: int = 8 * 1024 * 1024) -> str:
    """xxHash64 of a file's bytes (same algorithm as pipeline.checksum)."""
    hasher = xxhash.xxh64()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


# -- TDMS channel properties <-> JSON ----------------------------------------
# nptdms property values are numpy scalars / datetime64 / str. Preserve the
# numpy dtype through JSON so the rebuilt file's properties re-read identical.

def serialize_props(props: dict) -> dict:
    out = {}
    for key, v in props.items():
        if isinstance(v, np.datetime64):
            out[key] = {"t": "dt64", "v": str(v.astype("datetime64[us]"))}
        elif isinstance(v, np.timedelta64):
            out[key] = {"t": "td64", "v": int(v.astype("timedelta64[us]").astype(np.int64))}
        elif isinstance(v, np.generic):
            out[key] = {"t": "np", "dt": str(v.dtype), "v": v.item()}
        elif isinstance(v, (bool, int, float, str)):
            out[key] = {"t": "py", "v": v}
        else:
            out[key] = {"t": "str", "v": str(v)}
    return out


def deserialize_props(d: dict) -> dict:
    out = {}
    for key, e in d.items():
        t = e["t"]
        if t == "dt64":
            out[key] = np.datetime64(e["v"])
        elif t == "td64":
            out[key] = np.timedelta64(e["v"], "us")
        elif t == "np":
            out[key] = np.dtype(e["dt"]).type(e["v"])
        else:
            out[key] = e["v"]
    return out


# -- float grid recovery (verbatim from the compression repo) ----------------

def recover_codes(data, u=None):
    """Model samples as a uniform grid `data == offset + code*step`.

    Returns dict(offset, step, codes int64, max_error, exact) when the data
    sits on a clean grid, else None. `exact` says whether offset+code*step
    reproduces the float64 bit-for-bit.
    """
    data = np.asarray(data, dtype=np.float64)
    if u is None:
        u = np.unique(data)

    if u.size == 1:  # constant signal
        return dict(offset=float(u[0]), step=1.0,
                    codes=np.zeros(data.size, np.int64),
                    max_error=0.0, exact=True)

    diffs = np.diff(u)
    step = diffs.min()
    if step <= 0:
        return None

    # every observed gap between levels must be an integer number of steps
    if np.max(np.abs(diffs / step - np.round(diffs / step))) > GRID_TOL:
        return None

    # least-squares refine of step over all observed levels
    k = np.round((u - u[0]) / step)
    step = float(k @ (u - u[0]) / (k @ k))
    offset = float(u[0])
    codes = np.round((data - offset) / step).astype(np.int64)

    recon = offset + codes * step
    max_error = float(np.max(np.abs(recon - data)))
    return dict(offset=offset, step=step, codes=codes,
                max_error=max_error, exact=_bits_equal(recon, data))


def bits_for_codes(codes) -> int:
    """Smallest signed-int bit width that holds `codes` (minimum 2)."""
    if codes.size == 0:
        return 2
    cmin, cmax = int(codes.min()), int(codes.max())
    b = 2
    while cmin < -2 ** (b - 1) or cmax > 2 ** (b - 1) - 1:
        b += 1
    return b


def _encode_levels(levels) -> str:
    """Distinct float64 levels -> compact, exact, JSON-safe string."""
    raw = np.ascontiguousarray(levels, dtype="<f8").tobytes()
    return base64.b64encode(zlib.compress(raw, 9)).decode("ascii")


def _decode_levels(b64: str) -> np.ndarray:
    return np.frombuffer(zlib.decompress(base64.b64decode(b64)), dtype="<f8")


def _reconstruct(meta: dict, codes: np.ndarray) -> np.ndarray:
    """Integer codes -> float64 volts, per the sidecar's encoding."""
    enc = meta["encoding"]
    if enc == "affine_codes":
        return meta["offset"] + codes.astype(np.float64) * meta["step"]
    if enc == "indexed":
        return _decode_levels(meta["levels_b64"])[codes]
    full = 2 ** (meta["bits_per_sample"] - 1) - 1  # 'fixed_point'
    return codes.astype(np.float64) / full * meta["scale"]


# -- encode / decode ---------------------------------------------------------

def read_tdms(tdms_path,
              group: Optional[str] = None,
              channel: Optional[str] = None) -> Tuple[np.ndarray, dict, str, str]:
    """Read the single data channel of a TDMS file.

    Returns (data_float64, channel_properties, group_name, channel_name).
    Raises CodecUnsupported if the file doesn't have exactly one channel
    (multi-channel files must be transferred plainly).
    """
    from nptdms import TdmsFile

    tdms = TdmsFile.read(str(tdms_path))
    groups = tdms.groups()
    if group is not None:
        chans = tdms[group].channels()
        if channel is not None:
            chans = [tdms[group][channel]]
    else:
        if len(groups) != 1:
            raise CodecUnsupported(
                f"{tdms_path}: expected 1 group, found {len(groups)}")
        chans = groups[0].channels()
    if len(chans) != 1:
        raise CodecUnsupported(
            f"{tdms_path}: expected 1 channel, found {len(chans)}")

    ch = chans[0]
    props = dict(ch.properties)

    # nptdms returns SCALED data for channels carrying NI scaling properties.
    # Rebuilding such a file with the scaling properties intact would apply
    # the scale a second time on read — refuse and transfer plainly.
    if any(k.startswith("NI_Scale") or k in ("NI_Scaling_Status", "NI_Number_Of_Scales")
           for k in props):
        raise CodecUnsupported(f"{tdms_path}: channel carries NI scaling properties")

    data = np.asarray(ch[:], dtype=np.float64)
    return data, props, ch.group_name, ch.name


def encode_array_to_flac(data, flac_path, *, wf_increment, wf_start_time,
                         exact: bool = True, extra_meta: Optional[dict] = None) -> dict:
    """Lossless encode of an in-memory float array -> FLAC + JSON sidecar,
    with a full decode-and-compare self-verification.

    exact=True (bit-for-bit): 'affine_codes' only if reconstruction is exact,
    else 'indexed' (exact by construction). 'fixed_point' (lossy) only when
    there are too many distinct values (>2^24); flagged via exact=False in
    the returned report — callers must then fall back to a plain transfer.
    """
    data = np.ascontiguousarray(np.asarray(data, dtype=np.float64)).ravel()
    flac_path = pathlib.Path(flac_path)

    # 1. choose encoding + integer codes (one np.unique, shared with grid fit)
    # Several workers encode concurrently and each intermediate here is
    # ~240 MB for a production file — drop each as soon as it's consumed so
    # peak RSS stays ~1 GB per encode.
    levels, inv = np.unique(data, return_inverse=True)
    idx = inv.ravel().astype(np.int64)  # rank of each sample
    del inv
    g = recover_codes(data, u=levels)
    use_affine = (g is not None and bits_for_codes(g["codes"]) <= 24
                  and (g["exact"] if exact else True))
    if use_affine:
        encoding, codes = "affine_codes", g["codes"]
        meta_enc = dict(encoding=encoding, offset=g["offset"], step=g["step"])
        del idx
    elif bits_for_codes(idx) <= 24:  # bit-exact level index
        encoding, codes = "indexed", idx
        meta_enc = dict(encoding=encoding,
                        levels_b64=_encode_levels(levels),
                        n_levels=int(levels.size))
    else:  # too many levels for integer codes
        encoding = "fixed_point"
        scale = float(np.abs(data).max()) or 1.0
        full = 2 ** 23 - 1
        codes = np.round(data / scale * full).astype(np.int64)
        meta_enc = dict(encoding=encoding, scale=scale)
        del idx
    del g, levels

    bps = 16 if bits_for_codes(codes) <= 16 else 24
    shift = 32 - bps

    # 2. write FLAC (samples left-justified in int32) + JSON sidecar
    samples = np.ascontiguousarray((codes << shift).astype(np.int32).reshape(-1, 1))
    del codes
    fs = int(round(1.0 / wf_increment))
    sf.write(str(flac_path), samples, fs, format="FLAC", subtype=f"PCM_{bps}")
    del samples
    n_written = int(sf.info(str(flac_path)).frames)

    start = wf_start_time
    start = str(start.astype("datetime64[us]")) if hasattr(start, "astype") else str(start)
    meta = dict(meta_enc,
                bits_per_sample=bps,
                sample_rate_hz=fs,
                wf_increment=float(wf_increment),
                wf_start_time=start,
                n_samples=int(data.size),
                codec=CODEC_NAME,
                codec_version=CODEC_VERSION)
    if extra_meta:
        meta.update(extra_meta)
    with open(flac_path.with_suffix(".json"), "w") as f:
        json.dump(meta, f, indent=2)

    # 3. VERIFY: decode what we just wrote and bit-compare to the in-memory
    # array (bitwise, not ==: -0.0 vs +0.0 must count as different)
    decoded, _ = decode_flac(flac_path)
    if decoded.size == data.size:
        max_err = float(np.max(np.abs(decoded - data))) if data.size else 0.0
        exact_rt = _bits_equal(decoded, data)
    else:
        max_err, exact_rt = float("inf"), False  # frame loss = NOT lossless
    return dict(flac=str(flac_path), encoding=encoding, bits_per_sample=bps,
                n_samples=int(data.size), n_written=n_written,
                n_decoded=int(decoded.size),
                exact=exact_rt, max_abs_error_V=max_err,
                flac_bytes=flac_path.stat().st_size,
                sidecar_bytes=flac_path.with_suffix(".json").stat().st_size)


def encode_tdms_to_flac(tdms_path, flac_dir, exact: bool = True,
                        original_file_xxh64: Optional[str] = None) -> dict:
    """Read one TDMS file and losslessly encode it to <stem>.flac + .json.

    The sidecar gets the full channel properties and integrity hashes so the
    remote end can rebuild and verify a .tdms without seeing the original.
    Returns the encode report plus data_xxh64 / tdms_bytes.
    Raises CodecUnsupported for multi-group/channel files.
    """
    tdms_path = pathlib.Path(tdms_path)
    flac_path = pathlib.Path(flac_dir) / (tdms_path.stem + ".flac")

    data, props, group_name, channel_name = read_tdms(tdms_path)
    d_hash = data_xxh64(data)

    wf_increment = props.get("wf_increment")
    wf_start_time = props.get("wf_start_time")
    if wf_increment is None or wf_start_time is None:
        raise CodecUnsupported(
            f"{tdms_path}: no waveform timing properties (wf_increment/wf_start_time)")
    if not (float(wf_increment) > 0):
        raise CodecUnsupported(f"{tdms_path}: wf_increment={wf_increment}")
    fs = int(round(1.0 / float(wf_increment)))
    if not (1 <= fs <= 655350):  # libsndfile's FLAC sample-rate ceiling
        raise CodecUnsupported(
            f"{tdms_path}: sample rate {fs} Hz outside FLAC's supported range")

    extra = dict(
        group_name=group_name,
        channel_name=channel_name,
        channel_properties=serialize_props(props),
        data_xxh64=d_hash,
        original_file_xxh64=original_file_xxh64,
        original_file_bytes=tdms_path.stat().st_size,
        original_file_name=tdms_path.name,
    )
    rep = encode_array_to_flac(
        data, flac_path, exact=exact,
        wf_increment=props["wf_increment"],
        wf_start_time=props["wf_start_time"],
        extra_meta=extra)
    rep["tdms_bytes"] = tdms_path.stat().st_size
    rep["data_xxh64"] = d_hash
    rep["sidecar"] = str(flac_path.with_suffix(".json"))
    return rep


def decode_flac(flac_path) -> Tuple[np.ndarray, dict]:
    """Decode a FLAC + sidecar back to (data_float64, meta).

    Needs only numpy + soundfile; works for v1 (upstream) and v2 sidecars.
    """
    flac_path = pathlib.Path(flac_path)
    with open(flac_path.with_suffix(".json")) as f:
        meta = json.load(f)

    samples, _ = sf.read(str(flac_path), dtype="int32", always_2d=True)
    shift = 32 - meta["bits_per_sample"]  # undo left-justify
    codes = (samples[:, 0] >> shift).astype(np.int64)
    return _reconstruct(meta, codes), meta


def rebuild_tdms(flac_path, out_path,
                 expect_data_xxh64: Optional[str] = None) -> dict:
    """Decode a FLAC+sidecar and write a readable .tdms at out_path.

    The write is atomic (temp file + rename). The rebuilt file holds the
    bit-exact sample data and the original channel properties, but not the
    original's segment layout — its file hash differs from the original's.

    Returns dict(data_xxh64, file_xxh64, file_bytes, n_samples).
    Raises ValueError if the decoded data hash doesn't match expect_data_xxh64
    or the sidecar's own data_xxh64 (nothing is written in that case).
    """
    import os

    from nptdms import ChannelObject, TdmsWriter

    data, meta = decode_flac(flac_path)
    d_hash = data_xxh64(data)

    expected = expect_data_xxh64 or meta.get("data_xxh64")
    if expected and d_hash != expected:
        raise ValueError(
            f"decoded data hash {d_hash} != expected {expected} for {flac_path}")

    if "channel_properties" not in meta:
        raise ValueError(
            f"{flac_path}: sidecar has no channel_properties (v1 sidecar?) — "
            f"cannot rebuild a TDMS")
    props = deserialize_props(meta["channel_properties"])
    group = meta.get("group_name", DEFAULT_GROUP)
    channel = meta.get("channel_name", DEFAULT_CHANNEL)

    out_path = pathlib.Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(out_path.name + f".rebuild.{os.getpid()}.tmp")
    try:
        ch = ChannelObject(group, channel, data, properties=props)
        with TdmsWriter(str(tmp_path)) as w:
            w.write_segment([ch])
        os.replace(str(tmp_path), str(out_path))
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    # Prove the file we wrote reads back to the same bits before anyone
    # trusts it; a bad rebuild must not survive at the destination
    rdata, _, _, _ = read_tdms(out_path)
    r_hash = data_xxh64(rdata)
    if r_hash != d_hash:
        out_path.unlink()
        raise ValueError(
            f"rebuilt {out_path} re-reads to {r_hash}, expected {d_hash}")

    # A stale original-layout index next to a rebuilt file corrupts reads
    # that trust it; the original index lives on in the compressed tree
    index_path = pathlib.Path(str(out_path) + "_index")
    if index_path.exists():
        index_path.unlink()

    return dict(data_xxh64=d_hash,
                file_xxh64=file_xxh64(out_path),
                file_bytes=out_path.stat().st_size,
                n_samples=int(data.size))
