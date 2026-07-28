"""
Tests for the TDMS <-> FLAC codec.

Author: tunnell (https://github.com/tunnell)
"""

import numpy as np
import pytest

from pipeline.flaccodec import (
    CodecUnsupported, data_xxh64, decode_flac, encode_array_to_flac,
    encode_tdms_to_flac, deserialize_props, read_tdms, rebuild_tdms,
    serialize_props,
)


WF_START = np.datetime64("2026-05-28T15:56:21.123456")


def _grid_signal(n=50_000, step=1.25e-4, offset=-0.31):
    """Synthetic ADC-like data: float64 samples on a uniform grid."""
    rng = np.random.default_rng(42)
    codes = rng.integers(-2000, 2000, size=n)
    return offset + codes * step


@pytest.fixture
def tdms_file(tmp_path):
    """A small single-channel TDMS file like the real archive's."""
    from nptdms import ChannelObject, TdmsWriter

    data = _grid_signal()
    props = {
        "wf_increment": np.float64(2e-5),
        "wf_start_time": WF_START,
        "wf_samples": np.int32(5000),
        "NI_ChannelName": "Dev2/ai13",
        "unit_string": "Volts",
    }
    path = tmp_path / "TDMS_20260528_155621.tdms"
    ch = ChannelObject("Group", "Channel", data, properties=props)
    with TdmsWriter(str(path)) as w:
        w.write_segment([ch])
    return path, data, props


class TestArrayCodec:
    def test_grid_data_roundtrips_bit_exact(self, tmp_path):
        data = _grid_signal()
        rep = encode_array_to_flac(
            data, tmp_path / "x.flac", wf_increment=2e-5, wf_start_time=WF_START)
        assert rep["exact"] is True
        decoded, meta = decode_flac(tmp_path / "x.flac")
        assert np.array_equal(decoded, data)
        assert data_xxh64(decoded) == data_xxh64(data)

    def test_arbitrary_floats_roundtrip_bit_exact_via_indexed(self, tmp_path):
        # Not on any grid: indexed encoding must still be bit-exact
        rng = np.random.default_rng(7)
        data = rng.standard_normal(10_000)
        rep = encode_array_to_flac(
            data, tmp_path / "x.flac", wf_increment=2e-5, wf_start_time=WF_START)
        assert rep["encoding"] == "indexed"
        assert rep["exact"] is True
        decoded, _ = decode_flac(tmp_path / "x.flac")
        assert np.array_equal(decoded, data)

    def test_constant_signal(self, tmp_path):
        data = np.full(1000, 0.125)
        rep = encode_array_to_flac(
            data, tmp_path / "x.flac", wf_increment=2e-5, wf_start_time=WF_START)
        assert rep["exact"] is True
        decoded, _ = decode_flac(tmp_path / "x.flac")
        assert np.array_equal(decoded, data)


class TestProps:
    def test_numpy_types_survive_json(self):
        props = {
            "wf_increment": np.float64(2e-5),
            "wf_start_time": WF_START,
            "wf_samples": np.int32(5000),
            "name": "Dev2/ai13",
            "flag": True,
        }
        import json
        restored = deserialize_props(json.loads(json.dumps(serialize_props(props))))
        assert restored["wf_increment"] == props["wf_increment"]
        assert restored["wf_increment"].dtype == np.float64
        assert restored["wf_start_time"] == WF_START
        assert restored["wf_samples"] == props["wf_samples"]
        assert restored["wf_samples"].dtype == np.int32
        assert restored["name"] == "Dev2/ai13"
        assert restored["flag"] is True


class TestTdmsRoundtrip:
    def test_encode_decode_rebuild(self, tmp_path, tdms_file):
        path, data, props = tdms_file
        rep = encode_tdms_to_flac(path, tmp_path, original_file_xxh64="abc123")
        assert rep["exact"] is True
        assert rep["data_xxh64"] == data_xxh64(data)

        # Sidecar carries identity + integrity info
        _, meta = decode_flac(rep["flac"])
        assert meta["original_file_xxh64"] == "abc123"
        assert meta["group_name"] == "Group"
        assert meta["channel_name"] == "Channel"

        # Rebuild like the remote does, and re-read
        rebuilt = tmp_path / "rebuilt.tdms"
        rb = rebuild_tdms(rep["flac"], rebuilt, expect_data_xxh64=rep["data_xxh64"])
        assert rb["data_xxh64"] == rep["data_xxh64"]
        rdata, rprops, group, channel = read_tdms(rebuilt)
        assert np.array_equal(rdata, data)
        assert group == "Group" and channel == "Channel"
        for key, value in props.items():
            assert rprops[key] == value

    def test_rebuild_rejects_wrong_data_hash(self, tmp_path, tdms_file):
        path, _, _ = tdms_file
        rep = encode_tdms_to_flac(path, tmp_path)
        out = tmp_path / "rebuilt.tdms"
        with pytest.raises(ValueError, match="hash"):
            rebuild_tdms(rep["flac"], out, expect_data_xxh64="0" * 16)
        assert not out.exists()  # nothing written on failure

    def test_multi_channel_is_unsupported(self, tmp_path):
        from nptdms import ChannelObject, TdmsWriter

        path = tmp_path / "multi.tdms"
        chans = [ChannelObject("Group", name, np.arange(10.0),
                               properties={"wf_increment": np.float64(1.0),
                                           "wf_start_time": WF_START})
                 for name in ("A", "B")]
        with TdmsWriter(str(path)) as w:
            w.write_segment(chans)

        with pytest.raises(CodecUnsupported):
            encode_tdms_to_flac(path, tmp_path)
