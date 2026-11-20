"""Mirror fault-injection semantics and gateway validation of mirror replies."""

from __future__ import annotations

import hashlib

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import Response

from app import gateway as gateway_module
from tests.conftest import (
    MANIFEST,
    RunningServer,
    free_port,
    run_stack,
    sha256,
)

pytestmark = pytest.mark.anyio


async def get_chunk(client: httpx.AsyncClient, base: str, index: int) -> httpx.Response:
    chunk = MANIFEST.chunks[index]
    return await client.get(
        f"{base}/files/{MANIFEST.name}",
        headers={"Range": f"bytes={chunk.offset}-{chunk.offset + chunk.length - 1}"},
    )


async def set_fault(client, base, mode, from_chunk):
    response = await client.post(
        f"{base}/admin/fault", json={"mode": mode, "from_chunk": from_chunk}
    )
    assert response.status_code == 200


async def test_truncate_delivers_chunks_before_boundary_whole(tmp_path):
    async with run_stack(tmp_path) as stack:
        await set_fault(stack["client"], stack["primary"], "truncate", 3)
        before = await get_chunk(stack["client"], stack["primary"], 2)
        assert before.status_code == 206
        assert hashlib.sha256(before.content).hexdigest() == MANIFEST.chunks[2].sha256


async def test_truncate_drops_from_boundary_onward(tmp_path):
    async with run_stack(tmp_path) as stack:
        await set_fault(stack["client"], stack["primary"], "truncate", 3)
        with pytest.raises(httpx.HTTPError):
            await get_chunk(stack["client"], stack["primary"], 3)


async def test_corrupt_returns_equal_length_bad_block(tmp_path):
    async with run_stack(tmp_path) as stack:
        await set_fault(stack["client"], stack["backup"], "corrupt", 4)
        response = await get_chunk(stack["client"], stack["backup"], 4)
        chunk = MANIFEST.chunks[4]
        assert response.status_code == 206
        assert len(response.content) == chunk.length
        assert response.headers["content-length"] == str(chunk.length)
        assert hashlib.sha256(response.content).hexdigest() != chunk.sha256
        # Bytes before the boundary remain intact.
        untouched = await get_chunk(stack["client"], stack["backup"], 3)
        assert hashlib.sha256(untouched.content).hexdigest() == MANIFEST.chunks[3].sha256


async def test_gateway_rejects_mirror_with_wrong_range_or_length(tmp_path):
    """A misleading mirror must be rejected even if the bytes are correct."""
    body = hashlib.shake_256(b"gsb-upgrade-package:v1.0.0:2026-11-20").digest(MANIFEST.size)

    fake = FastAPI()

    @fake.get("/files/{name}")
    async def files(name: str, request: Request):
        # Serves correct bytes but lies about the range and length.
        return Response(
            body,
            status_code=200,
            headers={"Content-Type": "application/octet-stream"},
        )

    @fake.get("/admin/state")
    async def state():
        return {"fault": {"mode": "none", "from_chunk": 1}, "package": {}}

    good_port, fake_port, gateway_port = free_port(), free_port(), free_port()
    from app import mirror as mirror_module

    good_dir = tmp_path / "good"
    good_dir.mkdir()
    good = RunningServer(mirror_module.create_app("good", good_dir, MANIFEST), good_port)
    bad = RunningServer(fake, fake_port)
    settings = gateway_module.Settings(
        primary_url=f"http://127.0.0.1:{fake_port}",
        backup_url=f"http://127.0.0.1:{good_port}",
        manifest=MANIFEST,
        connect_timeout=2.0,
        read_timeout=10.0,
        first_byte_timeout=6.0,
    )
    gateway = RunningServer(gateway_module.create_app(settings), gateway_port)
    await good.start()
    await bad.start()
    await gateway.start()
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(
                f"http://127.0.0.1:{gateway_port}/download/{MANIFEST.name}"
            )
            assert response.status_code == 200
            assert sha256(response.content) == MANIFEST.sha256
            stats = (await client.get(f"http://127.0.0.1:{gateway_port}/api/stats")).json()
            assert stats["chunks"]["backup"] == MANIFEST.chunk_count
            # 25 merged segment requests plus 97 single-block retries, all lied about.
            assert stats["chunks"]["primary_errors"] == 25 + MANIFEST.chunk_count
            assert stats["last_download"]["outcome"] == "completed"
    finally:
        await gateway.stop()
        await bad.stop()
        await good.stop()
