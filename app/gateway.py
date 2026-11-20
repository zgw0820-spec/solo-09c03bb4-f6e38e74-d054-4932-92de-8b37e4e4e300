"""Chunk-verified resumable download gateway with merged range reads.

Adjacent 64 KiB chunks are merged into multi-block segments: one HTTP Range
request to the primary mirror fetches up to ``SEGMENT_CHUNKS`` blocks, so a
healthy primary serves the whole 97-block package in 25 requests. As the
segment body streams in, each block is checked against the manifest (range,
length and SHA-256) and released to the client in order, without waiting for
the rest of the segment to arrive.

If the merged response drops mid-stream or a block fails verification, the
verified prefix is kept and the remainder of that segment is fetched block by
block under the original primary->backup fallback rule. A block that neither
mirror can deliver correctly aborts the download; the response declares the
full package length up front, so an aborted stream is observably shorter than
declared and the client can recognise the failure.

At most two blocks are buffered per download inside the gateway — the block
being received and the block being sent both count — and when the client goes
away mid-download the in-flight mirror response is closed.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncIterator

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from starlette.staticfiles import StaticFiles

from .artifact import CHUNK_SIZE, MANIFEST_PATH, Chunk, Manifest, load_manifest

logger = logging.getLogger("gateway")

MAX_BUFFERED_BLOCKS = 2
MAX_FALLBACK_DETAIL = 8
MIRROR_ROLES = ("primary", "backup")
# Merge factor: ceil(97 chunks / 4) = 25 mirror Range requests per download.
SEGMENT_CHUNKS = 4
STATIC_DIR = Path(__file__).with_name("static")


class FaultBody(BaseModel):
    mode: str = Field(pattern="^(none|truncate|corrupt)$")
    from_chunk: int = Field(default=1, ge=0)


class ChunkError(Exception):
    """A mirror failed to deliver a verifiable block."""

    def __init__(self, reason: str, detail: str = ""):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


class DownloadAborted(Exception):
    """No mirror could supply a verified block; the response must be cut short."""


class _ExpectedAbortFilter(logging.Filter):
    """Hide the intentional mid-body abort from uvicorn's error log."""

    def filter(self, record: logging.LogRecord) -> bool:
        if "Response content shorter than Content-Length" in record.getMessage():
            return False
        exc = record.exc_info[1] if record.exc_info else None
        while exc is not None:
            if isinstance(exc, DownloadAborted):
                return False
            exc = exc.__cause__ or exc.__context__
        return True


@dataclass
class Counters:
    started: int = 0
    completed: int = 0
    failed: int = 0
    chunks_primary: int = 0
    chunks_backup: int = 0
    primary_errors: int = 0
    backup_errors: int = 0
    range_requests_primary: int = 0
    range_requests_backup: int = 0
    mirror_streams_open: int = 0
    last_error: str | None = None
    last_download: dict | None = None


@dataclass
class Settings:
    primary_url: str
    backup_url: str
    manifest: Manifest
    connect_timeout: float
    read_timeout: float
    first_byte_timeout: float
    segment_chunks: int = SEGMENT_CHUNKS

    @property
    def mirror_urls(self) -> dict[str, str]:
        return {"primary": self.primary_url, "backup": self.backup_url}


def settings_from_env(manifest: Manifest | None = None) -> Settings:
    def env_float(name: str, default: float) -> float:
        return float(os.environ.get(name, str(default)))

    connect = env_float("MIRROR_CONNECT_TIMEOUT", 2.0)
    read = env_float("MIRROR_READ_TIMEOUT", 10.0)
    return Settings(
        primary_url=os.environ.get("PRIMARY_URL", "http://127.0.0.1:8101").rstrip("/"),
        backup_url=os.environ.get("BACKUP_URL", "http://127.0.0.1:8102").rstrip("/"),
        manifest=manifest or load_manifest(MANIFEST_PATH),
        connect_timeout=connect,
        read_timeout=read,
        first_byte_timeout=env_float(
            "FIRST_BYTE_TIMEOUT", 2 * (connect + read) + 2.0
        ),
        segment_chunks=max(1, int(os.environ.get("SEGMENT_CHUNKS", str(SEGMENT_CHUNKS)))),
    )


@dataclass(frozen=True)
class Segment:
    """A run of consecutive manifest chunks fetched with one Range request."""

    chunks: tuple[Chunk, ...]

    @property
    def start(self) -> int:
        return self.chunks[0].offset

    @property
    def end(self) -> int:
        """Inclusive offset of the last byte in the segment."""
        last = self.chunks[-1]
        return last.offset + last.length - 1

    @property
    def length(self) -> int:
        return self.end - self.start + 1

    @property
    def label(self) -> str:
        return f"{self.chunks[0].index}..{self.chunks[-1].index}"


def build_segments(manifest: Manifest, segment_chunks: int) -> list[Segment]:
    """Group adjacent manifest chunks into merge-read segments."""
    size = max(1, segment_chunks)
    return [
        Segment(tuple(manifest.chunks[i : i + size]))
        for i in range(0, len(manifest.chunks), size)
    ]


@dataclass
class MirrorMetrics:
    """Per-download Range-request counters plus process-wide gauges."""

    counters: Counters
    requests: dict[str, int] = field(
        default_factory=lambda: {"primary": 0, "backup": 0}
    )

    def count_request(self, mirror: str) -> None:
        self.requests[mirror] += 1
        if mirror == "primary":
            self.counters.range_requests_primary += 1
        else:
            self.counters.range_requests_backup += 1

    @asynccontextmanager
    async def stream(self, client: httpx.AsyncClient, url: str, headers: dict):
        """Open one mirror response and track it until it is closed.

        The gauge drops back to zero only after the response (and its
        connection, when the body was not fully consumed) has been closed,
        which is what a cancelled download relies on.
        """
        self.counters.mirror_streams_open += 1
        try:
            async with client.stream("GET", url, headers=headers) as response:
                yield response
        finally:
            self.counters.mirror_streams_open -= 1


async def fetch_chunk(
    client: httpx.AsyncClient,
    settings: Settings,
    metrics: MirrorMetrics,
    mirror: str,
    chunk: Chunk,
) -> bytes:
    """Fetch one block from one mirror and return it only if fully verified."""
    manifest = settings.manifest
    url = f"{settings.mirror_urls[mirror]}/files/{manifest.name}"
    range_end = chunk.offset + chunk.length - 1
    expected_range = f"bytes {chunk.offset}-{range_end}/{manifest.size}"

    try:
        async with metrics.stream(
            client, url, headers={"Range": f"bytes={chunk.offset}-{range_end}"}
        ) as response:
            metrics.count_request(mirror)
            if response.status_code != 206:
                raise ChunkError("bad_status", f"HTTP {response.status_code}")
            content_range = response.headers.get("content-range")
            if content_range != expected_range:
                raise ChunkError(
                    "range_mismatch", f"{content_range!r} != {expected_range!r}"
                )
            declared = response.headers.get("content-length")
            if declared is not None and int(declared) != chunk.length:
                raise ChunkError(
                    "length_mismatch", f"Content-Length {declared} != {chunk.length}"
                )

            body = bytearray()
            async for piece in response.aiter_bytes():
                body.extend(piece)
                if len(body) > chunk.length:
                    raise ChunkError("length_mismatch", "more bytes than requested")
            if len(body) != chunk.length:
                raise ChunkError("short_body", f"{len(body)} of {chunk.length} bytes")
    except ChunkError:
        raise
    except (httpx.HTTPError, OSError) as exc:
        raise ChunkError("transport", f"{type(exc).__name__}: {exc}") from exc

    if hashlib.sha256(body).hexdigest() != chunk.sha256:
        raise ChunkError("digest_mismatch", f"chunk {chunk.index}")
    return bytes(body)


async def fetch_chunk_with_fallback(
    client: httpx.AsyncClient,
    settings: Settings,
    metrics: MirrorMetrics,
    chunk: Chunk,
) -> tuple[bytes, str, list[str]]:
    """Fetch one block; on any primary failure, refetch the block from backup."""
    counters = metrics.counters
    rejected: list[str] = []
    for mirror in MIRROR_ROLES:
        try:
            data = await fetch_chunk(client, settings, metrics, mirror, chunk)
        except ChunkError as exc:
            rejected.append(f"{mirror}:{exc.reason}")
            if mirror == "primary":
                counters.primary_errors += 1
            else:
                counters.backup_errors += 1
            logger.warning(
                "chunk %d rejected from %s (%s)", chunk.index, mirror, exc
            )
            continue
        if rejected:
            logger.info(
                "chunk %d served by %s after rejection on %s",
                chunk.index,
                mirror,
                ", ".join(rejected),
            )
        if mirror == "primary":
            counters.chunks_primary += 1
        else:
            counters.chunks_backup += 1
        return data, mirror, rejected
    raise DownloadAborted(
        f"chunk {chunk.index} unavailable: {'; '.join(rejected) or 'no mirrors'}"
    )


class _SegmentReader:
    """Reads exact block-sized pieces from one streaming mirror response.

    httpx re-chunks the body to at most ``CHUNK_SIZE`` bytes per piece and the
    reader only ever keeps the remainder of the block currently being
    received, so the block under assembly is the only application-layer
    buffer here.
    """

    def __init__(self, response: httpx.Response):
        self._pieces = response.aiter_bytes(chunk_size=CHUNK_SIZE)
        self._buffer = bytearray()
        self.received = 0

    async def read_block(self, length: int) -> bytes:
        """Return exactly ``length`` bytes or raise ``ChunkError``."""
        while len(self._buffer) < length:
            try:
                piece = await self._pieces.__anext__()
            except StopAsyncIteration:
                raise ChunkError(
                    "short_body",
                    f"segment body ended after {self.received} bytes",
                ) from None
            except (httpx.HTTPError, OSError) as exc:
                raise ChunkError("transport", f"{type(exc).__name__}: {exc}") from exc
            self._buffer.extend(piece)
            self.received += len(piece)
        data = bytes(self._buffer[:length])
        del self._buffer[:length]
        return data

    async def expect_end(self) -> None:
        """Fail if the mirror sent more bytes than the requested segment."""
        try:
            async for piece in self._pieces:
                if piece:
                    raise ChunkError(
                        "length_mismatch", "body longer than the requested segment"
                    )
        except (httpx.HTTPError, OSError) as exc:
            raise ChunkError("transport", f"{type(exc).__name__}: {exc}") from exc
        if self._buffer:
            raise ChunkError("length_mismatch", "trailing bytes after last block")


@dataclass
class _Block:
    chunk: Chunk
    data: bytes
    mirror: str


@dataclass
class _End:
    error: DownloadAborted | None = None


class ChunkPipeline:
    """Bounded producer/consumer holding at most two blocks per download.

    The producer merges adjacent chunks into one Range request per segment.
    A slot is taken before a block is received and returned only once the
    ASGI server has accepted that block for writing to the client, so the
    gateway never buffers more than ``MAX_BUFFERED_BLOCKS`` blocks for one
    download — including the block being received and the one being sent.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        settings: Settings,
        counters: Counters,
        download_id: str,
    ):
        self._client = client
        self._settings = settings
        self._counters = counters
        self.download_id = download_id
        self._queue: asyncio.Queue[_Block | _End] = asyncio.Queue()
        self._slots = asyncio.Semaphore(MAX_BUFFERED_BLOCKS)
        self._task: asyncio.Task | None = None
        self._closed = False
        self._resident = 0
        self.max_resident = 0
        self.sources = {"primary": 0, "backup": 0}
        self.metrics = MirrorMetrics(counters)
        self.fallbacks: list[dict] = []
        self.fallback_count = 0
        self._fallback_recorded = 0
        self.segment_fallbacks: list[dict] = []
        self.segment_fallback_count = 0
        self._segment_fallback_recorded = 0
        self.sent = 0
        self._segments = build_segments(settings.manifest, settings.segment_chunks)

    def start(self) -> None:
        self._task = asyncio.create_task(
            self._produce(), name=f"prefetch-{self.download_id}"
        )

    def _release_slot(self) -> None:
        self._resident -= 1
        self._slots.release()

    async def _stream_segment(self, segment: Segment) -> int | None:
        """Stream one merged segment from the primary mirror.

        Every verified block is queued for the client as soon as its digest
        passes, without waiting for the rest of the segment. Returns ``None``
        when the whole segment verified, otherwise the index of the first
        block that still needs trustworthy bytes.
        """
        manifest = self._settings.manifest
        url = f"{self._settings.primary_url}/files/{manifest.name}"
        expected_range = f"bytes {segment.start}-{segment.end}/{manifest.size}"
        failed_at: int | None = None
        try:
            async with self.metrics.stream(
                self._client,
                url,
                headers={"Range": f"bytes={segment.start}-{segment.end}"},
            ) as response:
                self.metrics.count_request("primary")
                if response.status_code != 206:
                    raise ChunkError("bad_status", f"HTTP {response.status_code}")
                content_range = response.headers.get("content-range")
                if content_range != expected_range:
                    raise ChunkError(
                        "range_mismatch", f"{content_range!r} != {expected_range!r}"
                    )
                declared = response.headers.get("content-length")
                if declared is not None and int(declared) != segment.length:
                    raise ChunkError(
                        "length_mismatch",
                        f"Content-Length {declared} != {segment.length}",
                    )

                reader = _SegmentReader(response)
                for chunk in segment.chunks:
                    failed_at = chunk.index
                    await self._slots.acquire()
                    self._resident += 1
                    self.max_resident = max(self.max_resident, self._resident)
                    try:
                        data = await reader.read_block(chunk.length)
                    except (ChunkError, asyncio.CancelledError):
                        self._release_slot()
                        raise
                    if hashlib.sha256(data).hexdigest() != chunk.sha256:
                        self._release_slot()
                        raise ChunkError("digest_mismatch", f"chunk {chunk.index}")
                    self.sources["primary"] += 1
                    self._counters.chunks_primary += 1
                    await self._queue.put(_Block(chunk, data, "primary"))
                try:
                    await reader.expect_end()
                except ChunkError as exc:
                    # Every block in the segment already verified, so the
                    # bytes are trusted; the talkative mirror is only noted.
                    self._counters.primary_errors += 1
                    logger.warning("segment %s trailing-data check: %s", segment.label, exc)
                return None
        except ChunkError as exc:
            reason, detail = exc.reason, exc.detail
        except (httpx.HTTPError, OSError) as exc:
            reason, detail = "transport", f"{type(exc).__name__}: {exc}"

        # The verified prefix stays; only the unverified tail is refetched.
        resume = failed_at if failed_at is not None else segment.chunks[0].index
        self._counters.primary_errors += 1
        self.segment_fallback_count += 1
        if self._segment_fallback_recorded < MAX_FALLBACK_DETAIL:
            self._segment_fallback_recorded += 1
            self.segment_fallbacks.append(
                {
                    "segment": segment.label,
                    "rejected": f"primary:{reason}" + (f" ({detail})" if detail else ""),
                    "resume_chunk": resume,
                }
            )
        logger.warning(
            "segment %s rejected from primary (%s%s); refilling from chunk %d",
            segment.label,
            reason,
            f": {detail}" if detail else "",
            resume,
        )
        return resume

    async def _refill(self, segment: Segment, resume: int) -> None:
        """Fetch the unverified tail of a failed segment block by block."""
        for chunk in segment.chunks:
            if chunk.index < resume:
                continue
            await self._slots.acquire()
            self._resident += 1
            self.max_resident = max(self.max_resident, self._resident)
            try:
                data, mirror, rejected = await fetch_chunk_with_fallback(
                    self._client, self._settings, self.metrics, chunk
                )
            except (asyncio.CancelledError, DownloadAborted):
                self._release_slot()
                raise
            except Exception as exc:  # defensive: never wedge a download
                self._release_slot()
                raise DownloadAborted(
                    f"internal error on chunk {chunk.index}: {exc!r}"
                ) from exc
            if rejected:
                self.fallback_count += 1
                if self._fallback_recorded < MAX_FALLBACK_DETAIL:
                    self._fallback_recorded += 1
                    self.fallbacks.append(
                        {"chunk": chunk.index, "rejected": rejected, "served_by": mirror}
                    )
            self.sources[mirror] += 1
            await self._queue.put(_Block(chunk, data, mirror))

    async def _produce(self) -> None:
        error: DownloadAborted | None = None
        try:
            for segment in self._segments:
                try:
                    resume = await self._stream_segment(segment)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # defensive: never wedge a download
                    error = DownloadAborted(
                        f"internal error in segment {segment.label}: {exc!r}"
                    )
                    break
                if resume is None:
                    continue
                try:
                    await self._refill(segment, resume)
                except asyncio.CancelledError:
                    raise
                except DownloadAborted as exc:
                    error = exc
                    break
        finally:
            self._queue.put_nowait(_End(error))

    async def next_block(self, timeout: float | None = None) -> _Block | None:
        """Return the next verified block, or ``None`` at end of stream."""
        getter = self._queue.get()
        item = (
            await getter
            if timeout is None
            else await asyncio.wait_for(getter, timeout=timeout)
        )
        if isinstance(item, _End):
            if item.error is not None:
                raise item.error
            return None
        return item

    def release(self, block: _Block) -> None:
        self._release_slot()

    def stats(self) -> dict:
        return {
            "requests": dict(self.metrics.requests),
            "sources": dict(self.sources),
            "fallback_count": self.fallback_count,
            "fallbacks": list(self.fallbacks),
            "segment_fallback_count": self.segment_fallback_count,
            "segment_fallbacks": list(self.segment_fallbacks),
            "max_resident_blocks": self.max_resident,
        }

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._task is not None and not self._task.done():
            self._task.cancel()
        if self._task is not None:
            # Always await so a cancelled or failed producer is reaped instead
            # of leaking an unretrieved-exception warning. Cancelling the
            # producer also closes any mirror response it was reading.
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        while not self._queue.empty():
            self._queue.get_nowait()


def create_app(
    settings: Settings | None = None,
    static_dir: Path = STATIC_DIR,
) -> FastAPI:
    settings = settings or settings_from_env()
    manifest = settings.manifest
    counters = Counters()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logging.getLogger("uvicorn.error").addFilter(_ExpectedAbortFilter())
        app.state.http = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=settings.connect_timeout,
                read=settings.read_timeout,
                write=settings.read_timeout,
                pool=settings.connect_timeout,
            ),
            limits=httpx.Limits(max_connections=16, max_keepalive_connections=8),
            follow_redirects=False,
        )
        logger.info(
            "gateway manifest=%s size=%d chunks=%d segment_chunks=%d primary=%s backup=%s",
            manifest.name,
            manifest.size,
            manifest.chunk_count,
            settings.segment_chunks,
            settings.primary_url,
            settings.backup_url,
        )
        try:
            yield
        finally:
            await app.state.http.aclose()

    app = FastAPI(title="Upgrade package download gateway", lifespan=lifespan)

    def base_headers(download_id: str) -> dict[str, str]:
        return {
            "Content-Length": str(manifest.size),
            "Content-Type": "application/octet-stream",
            "Content-Disposition": f'attachment; filename="{manifest.name}"',
            "Accept-Ranges": "none",
            "ETag": f'"{manifest.sha256}"',
            "X-Package-Sha256": manifest.sha256,
            "X-Chunk-Size": str(CHUNK_SIZE),
            "X-Chunk-Count": str(manifest.chunk_count),
            "X-Segment-Chunks": str(settings.segment_chunks),
            "X-Download-Id": download_id,
        }

    def record_failure(
        download_id: str, pipeline: ChunkPipeline, error: Exception
    ) -> None:
        counters.failed += 1
        counters.last_error = str(error)
        counters.last_download = {
            "id": download_id,
            "outcome": "failed",
            "received": pipeline.sent,
            "expected": manifest.size,
            **pipeline.stats(),
            "error": str(error),
            "at": time.time(),
        }

    @app.get("/download/{name}")
    async def download(name: str, request: Request):
        if name != manifest.name:
            raise HTTPException(status_code=404, detail="package not found")

        download_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        pipeline = ChunkPipeline(
            request.app.state.http, settings, counters, download_id
        )
        pipeline.start()
        started_at = time.monotonic()

        try:
            first = await pipeline.next_block(timeout=settings.first_byte_timeout)
        except (DownloadAborted, asyncio.TimeoutError) as exc:
            error = (
                exc
                if isinstance(exc, DownloadAborted)
                else DownloadAborted(
                    f"first chunk timed out after {settings.first_byte_timeout:.0f}s"
                )
            )
            await pipeline.close()
            record_failure(download_id, pipeline, error)
            logger.error("download %s failed before first byte: %s", download_id, error)
            return JSONResponse(
                status_code=502,
                content={
                    "detail": "升级包下载失败：两台镜像均无法提供完整且校验通过的块",
                    "download_id": download_id,
                    "error": str(error),
                },
                headers={"X-Download-Id": download_id},
            )

        if first is None:  # pragma: no cover - manifest always has chunks
            await pipeline.close()
            raise HTTPException(status_code=500, detail="empty package manifest")

        counters.started += 1

        async def body() -> AsyncIterator[bytes]:
            try:
                block: _Block | None = first
                while block is not None:
                    try:
                        yield block.data
                        pipeline.sent += len(block.data)
                    finally:
                        pipeline.release(block)
                    block = await pipeline.next_block()

                if pipeline.sent != manifest.size:
                    raise DownloadAborted(
                        f"stream ended after {pipeline.sent} of {manifest.size} bytes"
                    )

                counters.completed += 1
                counters.last_download = {
                    "id": download_id,
                    "outcome": "completed",
                    "received": pipeline.sent,
                    "expected": manifest.size,
                    **pipeline.stats(),
                    "elapsed_ms": round((time.monotonic() - started_at) * 1000, 1),
                    "at": time.time(),
                }
                logger.info(
                    "download %s complete: %d bytes, primary=%d backup=%d, "
                    "requests=%d/%d, fallbacks=%d, segment_fallbacks=%d, max_buffered=%d",
                    download_id,
                    pipeline.sent,
                    pipeline.sources["primary"],
                    pipeline.sources["backup"],
                    pipeline.metrics.requests["primary"],
                    pipeline.metrics.requests["backup"],
                    pipeline.fallback_count,
                    pipeline.segment_fallback_count,
                    pipeline.max_resident,
                )
            except DownloadAborted as exc:
                record_failure(download_id, pipeline, exc)
                logger.error(
                    "download %s aborted after %d of %d bytes: %s",
                    download_id,
                    pipeline.sent,
                    manifest.size,
                    exc,
                )
                # Fewer bytes than the declared Content-Length are sent and the
                # connection is closed, which is how the client detects failure.
                raise RuntimeError(f"download aborted: {exc}") from exc
            except (asyncio.CancelledError, OSError) as exc:
                # Raised when the client goes away mid-stream.
                canceled = RuntimeError(f"client disconnected after {pipeline.sent} bytes")
                record_failure(download_id, pipeline, canceled)
                logger.warning(
                    "download %s cancelled by client after %d of %d bytes (%s)",
                    download_id,
                    pipeline.sent,
                    manifest.size,
                    type(exc).__name__,
                )
                raise
            finally:
                await pipeline.close()

        return StreamingResponse(
            body(),
            status_code=200,
            headers=base_headers(download_id),
            media_type="application/octet-stream",
        )

    @app.get("/api/manifest")
    async def api_manifest():
        return {
            "name": manifest.name,
            "version": manifest.version,
            "size": manifest.size,
            "sha256": manifest.sha256,
            "chunk_size": manifest.chunk_size,
            "chunk_count": manifest.chunk_count,
            "segment_chunks": settings.segment_chunks,
            "max_buffered_blocks": MAX_BUFFERED_BLOCKS,
            "download_path": f"/download/{manifest.name}",
        }

    @app.get("/api/stats")
    async def api_stats():
        return {
            "downloads": {
                "started": counters.started,
                "completed": counters.completed,
                "failed": counters.failed,
            },
            "chunks": {
                "primary": counters.chunks_primary,
                "backup": counters.chunks_backup,
                "primary_errors": counters.primary_errors,
                "backup_errors": counters.backup_errors,
            },
            "range_requests": {
                "primary": counters.range_requests_primary,
                "backup": counters.range_requests_backup,
            },
            "mirror_streams_open": counters.mirror_streams_open,
            "last_error": counters.last_error,
            "last_download": counters.last_download,
        }

    async def mirror_state(request: Request, role: str) -> dict:
        url = settings.mirror_urls[role]
        try:
            response = await request.app.state.http.get(
                f"{url}/admin/state", timeout=settings.connect_timeout
            )
            response.raise_for_status()
            state = response.json()
            return {
                "role": role,
                "url": url,
                "online": True,
                "fault": state["fault"],
                "package": state["package"],
            }
        except (httpx.HTTPError, ValueError, KeyError) as exc:
            return {
                "role": role,
                "url": url,
                "online": False,
                "fault": None,
                "error": f"{type(exc).__name__}: {exc}",
            }

    @app.get("/api/mirrors")
    async def api_mirrors(request: Request):
        return {
            "mirrors": [await mirror_state(request, role) for role in MIRROR_ROLES],
            "chunk_size": manifest.chunk_size,
            "chunk_count": manifest.chunk_count,
            "segment_chunks": settings.segment_chunks,
            "max_buffered_blocks": MAX_BUFFERED_BLOCKS,
        }

    async def forward(request: Request, role: str, method: str, **kwargs):
        if role not in MIRROR_ROLES:
            raise HTTPException(status_code=404, detail="unknown mirror")
        try:
            response = await request.app.state.http.request(
                method,
                f"{settings.mirror_urls[role]}/admin/fault",
                timeout=settings.connect_timeout,
                **kwargs,
            )
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=502, detail=f"mirror unreachable: {exc}"
            ) from exc
        if response.status_code != 200:
            raise HTTPException(status_code=502, detail=response.text)
        return response.json()

    @app.post("/api/mirrors/{role}/fault")
    async def api_set_fault(role: str, payload: FaultBody, request: Request):
        if payload.from_chunk >= manifest.chunk_count:
            raise HTTPException(status_code=422, detail="from_chunk out of range")
        return await forward(
            request,
            role,
            "POST",
            json={"mode": payload.mode, "from_chunk": payload.from_chunk},
        )

    @app.delete("/api/mirrors/{role}/fault")
    async def api_clear_fault(role: str, request: Request):
        return await forward(request, role, "DELETE")

    @app.get("/health")
    async def health():
        return {
            "status": "ok",
            "package": manifest.name,
            "size": manifest.size,
            "primary": settings.primary_url,
            "backup": settings.backup_url,
        }

    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
    return app


app = create_app()
