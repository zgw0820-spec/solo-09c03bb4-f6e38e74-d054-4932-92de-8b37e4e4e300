"""Unit tests for artifact manifest and Range parsing."""

from __future__ import annotations

import hashlib

import pytest

from app.artifact import (
    CHUNK_SIZE,
    MANIFEST_PATH,
    build_manifest,
    load_manifest,
    package_bytes,
)
from app.gateway import SEGMENT_CHUNKS, build_segments
from app.ranges import RangeError, parse_range


def test_package_is_deterministic_and_matches_manifest():
    payload = package_bytes()
    manifest = load_manifest(MANIFEST_PATH)
    assert len(payload) == manifest.size
    assert hashlib.sha256(payload).hexdigest() == manifest.sha256
    assert build_manifest(payload) == {
        "name": manifest.name,
        "version": manifest.version,
        "size": manifest.size,
        "chunk_size": manifest.chunk_size,
        "chunk_count": manifest.chunk_count,
        "sha256": manifest.sha256,
        "chunks": [
            {
                "index": c.index,
                "offset": c.offset,
                "length": c.length,
                "sha256": c.sha256,
            }
            for c in manifest.chunks
        ],
    }


def test_manifest_chunks_tile_the_package():
    manifest = load_manifest(MANIFEST_PATH)
    payload = package_bytes()
    assert manifest.chunk_count == -(-manifest.size // CHUNK_SIZE)
    for chunk in manifest.chunks:
        assert chunk.length == min(CHUNK_SIZE, manifest.size - chunk.offset)
        assert (
            hashlib.sha256(payload[chunk.offset : chunk.offset + chunk.length]).hexdigest()
            == chunk.sha256
        )


def test_manifest_rejects_wrong_chunk_size(tmp_path):
    import json

    raw = json.loads(MANIFEST_PATH.read_text())
    raw["chunk_size"] = 1024
    broken = tmp_path / "manifest.json"
    broken.write_text(json.dumps(raw))
    with pytest.raises(ValueError):
        load_manifest(broken)


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("bytes=0-65535", (0, 65535)),
        ("bytes=65536-", (65536, 6303800)),
        ("bytes=-12345", (6291456, 6303800)),
        ("bytes=6291456-99999999", (6291456, 6303800)),
    ],
)
def test_parse_range_accepts_valid_ranges(header, expected):
    assert parse_range(header, 6303801) == expected


@pytest.mark.parametrize(
    "header",
    [
        "bytes=6303801-",
        "bytes=10-5",
        "bytes=0-1,4-5",
        "items=0-5",
        "bytes=-0",
        "bytes=",
        "bytes=a-b",
    ],
)
def test_parse_range_rejects_invalid_ranges(header):
    with pytest.raises(RangeError):
        parse_range(header, 6303801)


def test_parse_range_without_header():
    assert parse_range(None, 100) is None


def test_build_segments_merges_adjacent_chunks():
    manifest = load_manifest(MANIFEST_PATH)
    segments = build_segments(manifest, SEGMENT_CHUNKS)
    # 97 chunks merged by 4 -> 25 segments, i.e. 25 Range requests per download.
    assert len(segments) == 25
    assert all(len(segment.chunks) == SEGMENT_CHUNKS for segment in segments[:-1])
    assert len(segments[-1].chunks) == 1

    # Segments tile the package contiguously and match the manifest ranges.
    assert segments[0].start == 0
    assert segments[-1].end == manifest.size - 1
    for previous, following in zip(segments, segments[1:]):
        assert following.start == previous.end + 1
    for segment in segments:
        assert segment.length == sum(chunk.length for chunk in segment.chunks)
        assert segment.start == segment.chunks[0].offset
        last = segment.chunks[-1]
        assert segment.end == last.offset + last.length - 1


def test_build_segments_of_one_mirrors_plain_chunks():
    manifest = load_manifest(MANIFEST_PATH)
    segments = build_segments(manifest, 1)
    assert len(segments) == manifest.chunk_count
    for segment, chunk in zip(segments, manifest.chunks):
        assert segment.chunks == (chunk,)
        assert segment.start == chunk.offset
        assert segment.length == chunk.length
