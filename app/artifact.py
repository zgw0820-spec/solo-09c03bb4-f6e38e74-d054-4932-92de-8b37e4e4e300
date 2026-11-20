"""Deterministic upgrade-package artifact and its 64 KiB chunk manifest.

The package bytes are generated from a fixed seed so that the gateway
manifest and both mirror copies are reproducible without committing a
multi-megabyte binary to the repository.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path

PACKAGE_NAME = "gateway-upgrade-1.0.0.bin"
PACKAGE_VERSION = "1.0.0"
CHUNK_SIZE = 64 * 1024
# 96 full chunks plus a short final chunk, so the last-chunk path is exercised.
PACKAGE_SIZE = 96 * CHUNK_SIZE + 12_345
PACKAGE_SEED = b"gsb-upgrade-package:v1.0.0:2026-11-20"

MANIFEST_PATH = Path(__file__).with_name("manifest.json")


@dataclass(frozen=True)
class Chunk:
    index: int
    offset: int
    length: int
    sha256: str


@dataclass(frozen=True)
class Manifest:
    name: str
    version: str
    size: int
    chunk_size: int
    chunk_count: int
    sha256: str
    chunks: tuple[Chunk, ...]

    def chunk(self, index: int) -> Chunk:
        return self.chunks[index]


def package_bytes() -> bytes:
    """Return the deterministic package payload."""
    return hashlib.shake_256(PACKAGE_SEED).digest(PACKAGE_SIZE)


def build_manifest(payload: bytes | None = None) -> dict:
    if payload is None:
        payload = package_bytes()
    if len(payload) != PACKAGE_SIZE:
        raise ValueError(f"package length {len(payload)} != {PACKAGE_SIZE}")

    chunks = []
    for index, offset in enumerate(range(0, len(payload), CHUNK_SIZE)):
        block = payload[offset : offset + CHUNK_SIZE]
        chunks.append(
            {
                "index": index,
                "offset": offset,
                "length": len(block),
                "sha256": hashlib.sha256(block).hexdigest(),
            }
        )

    return {
        "name": PACKAGE_NAME,
        "version": PACKAGE_VERSION,
        "size": len(payload),
        "chunk_size": CHUNK_SIZE,
        "chunk_count": len(chunks),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "chunks": chunks,
    }


def load_manifest(path: Path = MANIFEST_PATH) -> Manifest:
    raw = json.loads(path.read_text(encoding="utf-8"))
    expected_count = math.ceil(raw["size"] / raw["chunk_size"])
    if raw["chunk_count"] != expected_count or len(raw["chunks"]) != expected_count:
        raise ValueError("manifest chunk count is inconsistent with package size")
    if raw["chunk_size"] != CHUNK_SIZE:
        raise ValueError(f"manifest chunk size must be {CHUNK_SIZE}")

    chunks: list[Chunk] = []
    for index, entry in enumerate(raw["chunks"]):
        expected_offset = index * raw["chunk_size"]
        expected_length = min(raw["chunk_size"], raw["size"] - expected_offset)
        if entry["index"] != index or entry["offset"] != expected_offset:
            raise ValueError(f"manifest chunk {index} has an unexpected offset")
        if entry["length"] != expected_length:
            raise ValueError(f"manifest chunk {index} has an unexpected length")
        digest = entry["sha256"]
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError(f"manifest chunk {index} has an invalid sha256")
        chunks.append(Chunk(index, expected_offset, expected_length, digest))

    return Manifest(
        name=raw["name"],
        version=raw["version"],
        size=raw["size"],
        chunk_size=raw["chunk_size"],
        chunk_count=raw["chunk_count"],
        sha256=raw["sha256"],
        chunks=tuple(chunks),
    )


def write_artifact(destination: Path, manifest_path: Path = MANIFEST_PATH) -> None:
    """Write the package file and (re)generate the manifest JSON."""
    payload = package_bytes()
    manifest = build_manifest(payload)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(payload)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def ensure_artifact(path: Path, manifest: Manifest) -> None:
    """Create the mirror payload if absent; refuse an inconsistent copy."""
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(package_bytes())
    stat = path.stat()
    if stat.st_size != manifest.size:
        raise RuntimeError(
            f"artifact {path} has {stat.st_size} bytes, expected {manifest.size}"
        )
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != manifest.sha256:
        raise RuntimeError(f"artifact {path} does not match the built-in manifest")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the deterministic upgrade package")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    args = parser.parse_args()
    write_artifact(args.output, args.manifest)
    print(f"wrote {args.output} ({PACKAGE_SIZE} bytes, {PACKAGE_SIZE // CHUNK_SIZE + 1} chunks)")
    print(f"wrote {args.manifest}")


if __name__ == "__main__":
    main()
