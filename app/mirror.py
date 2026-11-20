"""Local upgrade-package mirror with HTTP Range support and fault injection.

Each mirror serves one package file and can be told, through its admin API,
to fail from a given chunk onward in one of two ways:

``truncate``
    Declare the full requested range in ``Content-Length`` but send only part
    of it, then drop the connection.

``corrupt``
    Send a full-range body whose bytes no longer match the manifest digest.
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from starlette.concurrency import iterate_in_threadpool

from .artifact import CHUNK_SIZE, MANIFEST_PATH, Manifest, ensure_artifact, load_manifest
from .ranges import RangeError, parse_range

logger = logging.getLogger("mirror")

FAULT_MODES = ("none", "truncate", "corrupt")


class FaultRequest(BaseModel):
    mode: str = Field(description="none, truncate or corrupt")
    from_chunk: int = Field(default=1, ge=0)


class MirrorStreamDropped(Exception):
    """Raised on purpose after a partial body to simulate a dropped mirror."""


class _ExpectedDropFilter(logging.Filter):
    """Keep intentional stream drops out of uvicorn's error log."""

    def filter(self, record: logging.LogRecord) -> bool:
        if "Response content shorter than Content-Length" in record.getMessage():
            return False
        exc = record.exc_info[1] if record.exc_info else None
        while exc is not None:
            if isinstance(exc, MirrorStreamDropped):
                return False
            exc = exc.__cause__ or exc.__context__
        return True


def _read_range_sync(path: Path, start: int, length: int) -> bytes:
    with path.open("rb") as handle:
        handle.seek(start)
        data = handle.read(length)
    if len(data) != length:
        raise IOError(f"short read from {path}: {len(data)} != {length}")
    return data


def _iter_file_sync(path: Path, start: int, length: int):
    remaining = length
    with path.open("rb") as handle:
        handle.seek(start)
        while remaining > 0:
            block = handle.read(min(CHUNK_SIZE, remaining))
            if not block:
                raise IOError(f"unexpected EOF in {path}")
            remaining -= len(block)
            yield block


def _corrupt_block(block: bytes, global_offset: int, boundary: int) -> bytes:
    """Flip one byte per chunk region so the block length stays the same."""
    data = bytearray(block)
    block_end = global_offset + len(block)
    position = max(global_offset, boundary)
    changed = False
    while position < block_end:
        data[position - global_offset] ^= 0xFF
        changed = True
        position += CHUNK_SIZE
    return bytes(data) if changed else block


async def _dropped_after(data: bytes, declared: int, role: str) -> AsyncIterator[bytes]:
    """Yield ``data``, log the drop, then abort the response mid-body."""
    yield data
    logger.warning(
        "intentional drop role=%s declared=%d sent=%d", role, declared, len(data)
    )
    raise MirrorStreamDropped("simulated mirror stream drop")


async def _full_body(
    path: Path, start: int, length: int, corrupt: bool, boundary: int
) -> AsyncIterator[bytes]:
    offset = start
    async for block in iterate_in_threadpool(_iter_file_sync(path, start, length)):
        if corrupt:
            block = _corrupt_block(block, offset, boundary)
        offset += len(block)
        yield block


def create_app(
    role: str | None = None,
    data_dir: Path | None = None,
    manifest: Manifest | None = None,
    start_fault: tuple[str, int] = ("none", 1),
) -> FastAPI:
    role = role or os.environ.get("MIRROR_ROLE", "primary")
    data_dir = Path(data_dir or os.environ.get("MIRROR_DATA_DIR", "/data"))
    manifest = manifest or load_manifest(MANIFEST_PATH)
    package_path = data_dir / manifest.name

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logging.getLogger("uvicorn.error").addFilter(_ExpectedDropFilter())
        ensure_artifact(package_path, manifest)
        logger.info(
            "mirror role=%s package=%s size=%d chunks=%d fault=%s@%d",
            role,
            manifest.name,
            manifest.size,
            manifest.chunk_count,
            app.state.fault_mode,
            app.state.fault_from_chunk,
        )
        yield

    app = FastAPI(title=f"Package mirror ({role})", lifespan=lifespan)
    app.state.role = role
    app.state.manifest = manifest
    app.state.package_path = package_path
    app.state.fault_mode = start_fault[0]
    app.state.fault_from_chunk = start_fault[1]
    app.state.fault_updated_at = time.time()

    def fault_payload(request: Request) -> dict:
        return {
            "mode": request.app.state.fault_mode,
            "from_chunk": request.app.state.fault_from_chunk,
        }

    @app.get("/health")
    async def health():
        return {"role": role, "status": "ok", "package": manifest.name}

    @app.get("/admin/state")
    async def admin_state(request: Request):
        return {
            "role": role,
            "online": True,
            "fault": fault_payload(request),
            "package": {
                "name": manifest.name,
                "version": manifest.version,
                "size": manifest.size,
                "chunk_size": manifest.chunk_size,
                "chunk_count": manifest.chunk_count,
                "sha256": manifest.sha256,
            },
        }

    @app.post("/admin/fault")
    async def set_fault(body: FaultRequest, request: Request):
        if body.mode not in FAULT_MODES:
            raise HTTPException(
                status_code=422, detail=f"mode must be one of {FAULT_MODES}"
            )
        if not 0 <= body.from_chunk < manifest.chunk_count:
            raise HTTPException(
                status_code=422,
                detail=f"from_chunk must be 0..{manifest.chunk_count - 1}",
            )
        request.app.state.fault_mode = body.mode
        request.app.state.fault_from_chunk = body.from_chunk
        request.app.state.fault_updated_at = time.time()
        logger.info(
            "fault updated role=%s mode=%s from_chunk=%d",
            role,
            body.mode,
            body.from_chunk,
        )
        return {"role": role, "fault": fault_payload(request)}

    @app.delete("/admin/fault")
    async def clear_fault(request: Request):
        request.app.state.fault_mode = "none"
        request.app.state.fault_from_chunk = 1
        request.app.state.fault_updated_at = time.time()
        return {"role": role, "fault": fault_payload(request)}

    @app.get("/files/{name}")
    async def download(name: str, request: Request):
        if name != manifest.name:
            raise HTTPException(status_code=404, detail="package not found")

        header = request.headers.get("range")
        try:
            parsed = parse_range(header, manifest.size)
        except RangeError as exc:
            return JSONResponse(
                status_code=416,
                content={"detail": str(exc)},
                headers={"Content-Range": f"bytes */{manifest.size}"},
            )

        if parsed is None:
            start, end = 0, manifest.size - 1
            status_code = 200
        else:
            start, end = parsed
            status_code = 206

        length = end - start + 1
        mode = request.app.state.fault_mode
        boundary = request.app.state.fault_from_chunk * CHUNK_SIZE
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Length": str(length),
            "Content-Type": "application/octet-stream",
            "ETag": f'"{manifest.sha256}"',
            "X-Package-Sha256": manifest.sha256,
            "X-Mirror-Role": role,
        }
        if status_code == 206:
            headers["Content-Range"] = f"bytes {start}-{end}/{manifest.size}"

        if mode == "truncate":
            # Content-Length announces the whole range, but for a range that
            # begins at or after the fault boundary only half of it is sent;
            # a range that straddles the boundary is cut at the boundary.
            # Either way the connection is dropped mid-body. A range that lies
            # entirely before the boundary is delivered normally.
            if start >= boundary:
                send_length = max(1, length // 2)
            else:
                send_length = max(0, boundary - start)
            if 0 < send_length < length:
                body: AsyncIterator[bytes] = _dropped_after(
                    _read_range_sync(package_path, start, send_length), length, role
                )
            else:
                body = _full_body(package_path, start, length, False, boundary)
        else:
            body = _full_body(package_path, start, length, mode == "corrupt", boundary)

        return StreamingResponse(
            body,
            status_code=status_code,
            headers=headers,
            media_type="application/octet-stream",
        )

    return app


_initial_fault = (
    os.environ.get("MIRROR_FAULT_MODE", "none"),
    int(os.environ.get("MIRROR_FAULT_FROM_CHUNK", "1")),
)
app = create_app(start_fault=_initial_fault)
