"""End-to-end fixtures: two real mirror servers plus the gateway."""

from __future__ import annotations

import asyncio
import hashlib
import socket
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
import uvicorn

from app import gateway as gateway_module
from app import mirror as mirror_module
from app.artifact import MANIFEST_PATH, load_manifest

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST = load_manifest(MANIFEST_PATH)
PACKAGE_NAME = MANIFEST.name


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class RunningServer:
    def __init__(self, app, port: int):
        self.server = uvicorn.Server(
            uvicorn.Config(
                app,
                host="127.0.0.1",
                port=port,
                log_level="warning",
                lifespan="on",
                access_log=False,
            )
        )
        self.port = port
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self.server.serve())
        for _ in range(250):
            if self.server.started:
                return
            await asyncio.sleep(0.02)
        raise RuntimeError(f"server on port {self.port} did not start")

    async def stop(self) -> None:
        self.server.should_exit = True
        if self._task is not None:
            await self._task


@asynccontextmanager
async def run_stack(tmp_path: Path, primary_offline: bool = False):
    primary_dir = tmp_path / "primary"
    backup_dir = tmp_path / "backup"
    primary_dir.mkdir()
    backup_dir.mkdir()

    primary_port, backup_port, gateway_port = free_port(), free_port(), free_port()

    primary = RunningServer(
        mirror_module.create_app("primary", primary_dir, MANIFEST), primary_port
    )
    backup = RunningServer(
        mirror_module.create_app("backup", backup_dir, MANIFEST), backup_port
    )
    settings = gateway_module.Settings(
        primary_url=f"http://127.0.0.1:{primary_port}",
        backup_url=f"http://127.0.0.1:{backup_port}",
        manifest=MANIFEST,
        connect_timeout=2.0,
        read_timeout=10.0,
        first_byte_timeout=6.0,
        segment_chunks=4,
    )
    gateway = RunningServer(gateway_module.create_app(settings), gateway_port)

    if not primary_offline:
        await primary.start()
    await backup.start()
    await gateway.start()
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            yield {
                "client": client,
                "primary": f"http://127.0.0.1:{primary_port}",
                "backup": f"http://127.0.0.1:{backup_port}",
                "gateway": f"http://127.0.0.1:{gateway_port}",
                "primary_server": primary,
                "manifest": MANIFEST,
                "settings": settings,
            }
    finally:
        await gateway.stop()
        if not primary_offline:
            await primary.stop()
        await backup.stop()


@pytest.fixture
def anyio_backend():
    return "asyncio"


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
