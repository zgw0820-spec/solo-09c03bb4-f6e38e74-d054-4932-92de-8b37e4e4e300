"""End-to-end gateway behaviour against real mirror servers."""

from __future__ import annotations

import asyncio
import hashlib

import httpx
import pytest

from app import gateway as gateway_module
from tests.conftest import PACKAGE_NAME, run_stack, sha256

pytestmark = pytest.mark.anyio


async def set_fault(client: httpx.AsyncClient, base: str, mode: str, from_chunk: int = 0):
    response = await client.post(
        f"{base}/admin/fault", json={"mode": mode, "from_chunk": from_chunk}
    )
    assert response.status_code == 200, response.text
    return response.json()


async def fetch_via_gateway(stack) -> tuple[int, bytes, dict]:
    client: httpx.AsyncClient = stack["client"]
    async with client.stream(
        "GET", f"{stack['gateway']}/download/{PACKAGE_NAME}"
    ) as response:
        body = b"".join([piece async for piece in response.aiter_bytes()])
        return response.status_code, body, dict(response.headers)


async def stats(stack) -> dict:
    response = await stack["client"].get(f"{stack['gateway']}/api/stats")
    assert response.status_code == 200
    return response.json()


async def test_happy_path_serves_exact_package(tmp_path):
    async with run_stack(tmp_path) as stack:
        status, body, headers = await fetch_via_gateway(stack)
        manifest = stack["manifest"]
        assert status == 200
        assert len(body) == manifest.size
        assert sha256(body) == manifest.sha256
        assert headers["content-length"] == str(manifest.size)
        assert headers["x-chunk-size"] == "65536"
        assert headers["x-segment-chunks"] == "4"

        counters = await stats(stack)
        assert counters["downloads"] == {"started": 1, "completed": 1, "failed": 0}
        last = counters["last_download"]
        assert last["sources"] == {"primary": manifest.chunk_count, "backup": 0}
        # 97 chunks merged into ceil(97 / 4) = 25 Range requests, all to primary.
        assert last["requests"] == {"primary": 25, "backup": 0}
        assert last["segment_fallback_count"] == 0
        assert last["max_resident_blocks"] <= 2
        assert counters["range_requests"] == {"primary": 25, "backup": 0}
        assert counters["mirror_streams_open"] == 0


async def test_primary_truncation_falls_back_per_block(tmp_path):
    """Merged segments drop mid-stream; the unverified tail is refilled per block."""
    async with run_stack(tmp_path) as stack:
        await set_fault(stack["client"], stack["primary"], "truncate", 5)
        status, body, _ = await fetch_via_gateway(stack)
        manifest = stack["manifest"]
        assert status == 200
        assert sha256(body) == manifest.sha256

        last = (await stats(stack))["last_download"]
        assert last["outcome"] == "completed"
        # Segment 0 is whole; later segments deliver a verified prefix (one or
        # two blocks) before the mirror drops the stream, and the rest of each
        # segment is refetched block by block from the backup.
        assert last["sources"] == {"primary": 49, "backup": 48}
        assert last["requests"] == {"primary": 73, "backup": 48}
        assert last["fallback_count"] == 48
        assert last["segment_fallback_count"] == 24
        assert last["max_resident_blocks"] <= 2


async def test_primary_corruption_falls_back_per_block(tmp_path):
    async with run_stack(tmp_path) as stack:
        await set_fault(stack["client"], stack["primary"], "corrupt", 40)
        status, body, _ = await fetch_via_gateway(stack)
        manifest = stack["manifest"]
        assert status == 200
        assert sha256(body) == manifest.sha256

        last = (await stats(stack))["last_download"]
        assert last["sources"]["primary"] == 40
        assert last["sources"]["backup"] == manifest.chunk_count - 40
        # 25 merged requests plus one single-block retry per corrupted block.
        assert last["requests"] == {"primary": 25 + 57, "backup": 57}
        assert last["segment_fallback_count"] == 15


async def test_bad_block_mid_segment_keeps_verified_prefix(tmp_path):
    """A digest failure inside a segment keeps the blocks already verified."""
    async with run_stack(tmp_path) as stack:
        # Chunk 6 sits in the middle of segment 4..7.
        await set_fault(stack["client"], stack["primary"], "corrupt", 6)
        status, body, _ = await fetch_via_gateway(stack)
        manifest = stack["manifest"]
        assert status == 200
        assert sha256(body) == manifest.sha256

        last = (await stats(stack))["last_download"]
        assert last["outcome"] == "completed"
        # Segment 0 (4 blocks) plus the verified prefix chunks 4 and 5 came
        # from the primary; everything from chunk 6 on fell back to backup.
        assert last["sources"] == {"primary": 6, "backup": manifest.chunk_count - 6}
        assert last["segment_fallbacks"][0]["segment"] == "4..7"
        assert last["segment_fallbacks"][0]["resume_chunk"] == 6
        assert last["fallbacks"][0]["chunk"] == 6
        assert last["fallbacks"][0]["served_by"] == "backup"
        assert last["max_resident_blocks"] <= 2


async def test_broken_primary_never_leaks_corrupt_bytes(tmp_path):
    """Every primary block is corrupt; the delivered file must still be exact."""
    async with run_stack(tmp_path) as stack:
        await set_fault(stack["client"], stack["primary"], "corrupt", 0)
        status, body, headers = await fetch_via_gateway(stack)
        manifest = stack["manifest"]
        assert status == 200
        assert len(body) == manifest.size
        assert hashlib.sha256(body).hexdigest() == manifest.sha256
        last = (await stats(stack))["last_download"]
        assert last["sources"] == {"primary": 0, "backup": manifest.chunk_count}
        assert last["requests"] == {"primary": 25 + 97, "backup": 97}
        assert last["max_resident_blocks"] <= 2


async def test_dual_corruption_aborts_mid_stream(tmp_path):
    async with run_stack(tmp_path) as stack:
        await set_fault(stack["client"], stack["primary"], "corrupt", 7)
        await set_fault(stack["client"], stack["backup"], "truncate", 7)

        client: httpx.AsyncClient = stack["client"]
        received = 0
        with pytest.raises(httpx.HTTPError):
            async with client.stream(
                "GET", f"{stack['gateway']}/download/{PACKAGE_NAME}"
            ) as response:
                assert response.status_code == 200
                declared = int(response.headers["content-length"])
                async for piece in response.aiter_bytes():
                    received += len(piece)
                assert received == declared

        manifest = stack["manifest"]
        assert received == 7 * manifest.chunk_size
        assert received < manifest.size

        counters = await stats(stack)
        assert counters["downloads"]["failed"] == 1
        last = counters["last_download"]
        assert last["outcome"] == "failed"
        assert last["received"] == 7 * manifest.chunk_size
        assert "chunk 7 unavailable" in last["error"]


async def test_dual_failure_before_first_byte_returns_502(tmp_path):
    # The first block itself is corrupt on the primary and truncated on the
    # backup, so the gateway can verify nothing and must fail before streaming.
    async with run_stack(tmp_path) as stack:
        await set_fault(stack["client"], stack["primary"], "corrupt", 0)
        await set_fault(stack["client"], stack["backup"], "truncate", 0)

        response = await stack["client"].get(
            f"{stack['gateway']}/download/{PACKAGE_NAME}"
        )
        assert response.status_code == 502
        payload = response.json()
        assert "download_id" in payload
        assert "chunk 0 unavailable" in payload["error"]
        assert (await stats(stack))["last_download"]["outcome"] == "failed"


async def test_mirror_range_and_digest_contract(tmp_path):
    async with run_stack(tmp_path) as stack:
        manifest = stack["manifest"]
        chunk = manifest.chunks[3]
        response = await stack["client"].get(
            f"{stack['primary']}/files/{manifest.name}",
            headers={"Range": f"bytes={chunk.offset}-{chunk.offset + chunk.length - 1}"},
        )
        assert response.status_code == 206
        assert (
            response.headers["content-range"]
            == f"bytes {chunk.offset}-{chunk.offset + chunk.length - 1}/{manifest.size}"
        )
        assert hashlib.sha256(response.content).hexdigest() == chunk.sha256

        unsatisfiable = await stack["client"].get(
            f"{stack['primary']}/files/{manifest.name}",
            headers={"Range": f"bytes={manifest.size}-"},
        )
        assert unsatisfiable.status_code == 416
        assert unsatisfiable.headers["content-range"] == f"bytes */{manifest.size}"


async def test_primary_offline_serves_entire_file_from_backup(tmp_path):
    async with run_stack(tmp_path, primary_offline=True) as stack:
        status, body, _ = await fetch_via_gateway(stack)
        manifest = stack["manifest"]
        assert status == 200
        assert sha256(body) == manifest.sha256

        last = (await stats(stack))["last_download"]
        assert last["outcome"] == "completed"
        assert last["sources"] == {"primary": 0, "backup": manifest.chunk_count}
        # The primary never answered a single request (connection refused), so
        # no Range request reached it; every block came from the backup.
        assert last["requests"] == {"primary": 0, "backup": manifest.chunk_count}

        mirrors = (await stack["client"].get(f"{stack['gateway']}/api/mirrors")).json()
        roles = {m["role"]: m["online"] for m in mirrors["mirrors"]}
        assert roles == {"primary": False, "backup": True}


async def test_primary_dies_mid_download_and_backup_finishes(tmp_path):
    async with run_stack(tmp_path) as stack:
        manifest = stack["manifest"]
        client: httpx.AsyncClient = stack["client"]
        received = 0
        pieces: list[bytes] = []
        started = False

        async with client.stream(
            "GET", f"{stack['gateway']}/download/{PACKAGE_NAME}"
        ) as response:
            assert response.status_code == 200
            async for piece in response.aiter_bytes():
                pieces.append(piece)
                received += len(piece)
                if not started and received >= 4 * manifest.chunk_size:
                    started = True
                    await stack["primary_server"].stop()

        assert received == manifest.size
        assert sha256(b"".join(pieces)) == manifest.sha256

        last = (await stats(stack))["last_download"]
        assert last["outcome"] == "completed"
        assert last["sources"]["primary"] >= 1
        assert last["sources"]["backup"] >= 1
        assert last["sources"]["primary"] + last["sources"]["backup"] == manifest.chunk_count
        assert last["max_resident_blocks"] <= 2


async def test_concurrent_downloads_keep_bounded_buffers(tmp_path):
    async with run_stack(tmp_path) as stack:
        await set_fault(stack["client"], stack["primary"], "corrupt", 3)
        manifest = stack["manifest"]

        async def one() -> bytes:
            async with httpx.AsyncClient(timeout=60) as client:
                response = await client.get(
                    f"{stack['gateway']}/download/{PACKAGE_NAME}"
                )
                assert response.status_code == 200
                return response.content

        bodies = await asyncio.gather(*[one() for _ in range(6)])
        assert all(len(body) == manifest.size for body in bodies)
        assert all(sha256(body) == manifest.sha256 for body in bodies)

        counters = await stats(stack)
        assert counters["downloads"]["completed"] == 6
        assert counters["downloads"]["failed"] == 0
        assert counters["last_download"]["max_resident_blocks"] <= 2
        assert counters["mirror_streams_open"] == 0


async def test_client_disconnect_mid_download_closes_mirror_stream(tmp_path):
    """A cancelled download must close the mirror response it was reading."""
    async with run_stack(tmp_path) as stack:
        client: httpx.AsyncClient = stack["client"]
        received = 0
        async with client.stream(
            "GET", f"{stack['gateway']}/download/{PACKAGE_NAME}"
        ) as response:
            assert response.status_code == 200
            async for piece in response.aiter_bytes():
                received += len(piece)
                break  # walk away after the first piece
        assert 0 < received < stack["manifest"].size

        counters = {}
        for _ in range(500):
            counters = await stats(stack)
            if counters["downloads"]["failed"] >= 1 and counters["mirror_streams_open"] == 0:
                break
            await asyncio.sleep(0.02)
        assert counters["downloads"]["failed"] == 1
        # The in-flight merged segment response was closed, not leaked.
        assert counters["mirror_streams_open"] == 0
        last = counters["last_download"]
        assert last["outcome"] == "failed"
        assert "client disconnected" in last["error"]


async def test_pipeline_close_cancels_inflight_segment(tmp_path):
    """White-box: closing a download mid-segment closes the mirror response."""
    async with run_stack(tmp_path) as stack:
        counters = gateway_module.Counters()
        pipeline = gateway_module.ChunkPipeline(
            stack["client"], stack["settings"], counters, "whitebox"
        )
        pipeline.start()
        block = await pipeline.next_block(timeout=5)
        assert block is not None and block.chunk.index == 0
        # The producer is stalled inside segment 0 (chunks 0..3): both buffer
        # slots are taken, so the merged mirror response is still open with
        # chunks left unread.
        for _ in range(250):
            if counters.mirror_streams_open == 1:
                break
            await asyncio.sleep(0.02)
        assert counters.mirror_streams_open == 1

        await pipeline.close()
        assert counters.mirror_streams_open == 0


async def test_ui_and_manifest_endpoints(tmp_path):
    async with run_stack(tmp_path) as stack:
        client = stack["client"]
        page = await client.get(f"{stack['gateway']}/")
        assert page.status_code == 200
        assert "升级包下载网关" in page.text
        script = await client.get(f"{stack['gateway']}/app.js")
        assert script.status_code == 200

        manifest = await client.get(f"{stack['gateway']}/api/manifest")
        assert manifest.json()["size"] == stack["manifest"].size
        assert manifest.json()["max_buffered_blocks"] == 2
        assert manifest.json()["segment_chunks"] == 4

        mirrors = await client.get(f"{stack['gateway']}/api/mirrors")
        payload = mirrors.json()
        assert [m["role"] for m in payload["mirrors"]] == ["primary", "backup"]
        assert all(m["online"] for m in payload["mirrors"])
