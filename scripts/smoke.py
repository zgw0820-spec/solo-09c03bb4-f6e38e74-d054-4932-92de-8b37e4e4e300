#!/usr/bin/env python3
"""Exercise the running Compose stack: baseline, failover, dual failure.

Usage (after `docker compose up --build -d`):
    python3 scripts/smoke.py --base http://127.0.0.1:8000

The script changes mirror fault state through the gateway admin API and
restores both mirrors to normal at the end.
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

PACKAGE_PATH = "/download/gateway-upgrade-1.0.0.bin"
READ_SIZE = 65536


def request(method: str, url: str, body: dict | None = None) -> tuple[int, bytes]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def download(base: str) -> dict:
    """Stream the package and describe exactly how the transfer ended."""
    parsed = urllib.parse.urlsplit(base + PACKAGE_PATH)
    connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=120)
    result = {
        "status": None,
        "received": 0,
        "declared": None,
        "sha256": None,
        "error": "",
    }
    digest = hashlib.sha256()
    try:
        connection.request("GET", parsed.path)
        response = connection.getresponse()
        result["status"] = response.status
        declared = response.getheader("Content-Length")
        result["declared"] = int(declared) if declared else None
        while True:
            block = response.read(READ_SIZE)
            if not block:
                break
            result["received"] += len(block)
            digest.update(block)
        result["sha256"] = digest.hexdigest()
        if result["declared"] is not None and result["received"] != result["declared"]:
            result["error"] = (
                f"body ended at {result['received']} of {result['declared']} declared bytes"
            )
        elif result["status"] != 200:
            result["error"] = f"HTTP {result['status']}"
    except (http.client.HTTPException, OSError) as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        connection.close()
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    base = args.base.rstrip("/")
    failures = 0

    status, body = request("GET", base + "/api/manifest")
    manifest = json.loads(body)
    expected_sha = manifest["sha256"]
    size = manifest["size"]
    print(
        f"manifest: {manifest['name']} {size} bytes "
        f"in {manifest['chunk_count']} chunks of {manifest['chunk_size']}"
    )

    def set_fault(role: str, mode: str, chunk: int) -> None:
        code, payload = request(
            "POST",
            f"{base}/api/mirrors/{role}/fault",
            {"mode": mode, "from_chunk": chunk},
        )
        if code != 200:
            raise SystemExit(f"cannot set fault on {role}: {code} {payload!r}")

    def clear(role: str) -> None:
        request("DELETE", f"{base}/api/mirrors/{role}/fault")

    def report(label: str, ok: bool, result: dict) -> None:
        nonlocal failures
        detail = (
            f"HTTP {result['status']}, {result['received']}/{result['declared']} bytes, "
            f"sha256 {(result['sha256'] or '?')[:16]}"
        )
        if result["error"]:
            detail += f", {result['error']}"
        print(f"[{'PASS' if ok else 'FAIL'}] {label}: {detail}")
        if not ok:
            failures += 1

    def exact(result: dict) -> bool:
        return (
            result["status"] == 200
            and result["received"] == size
            and result["sha256"] == expected_sha
        )

    # A previous run may have left faults behind.
    clear("primary")
    clear("backup")

    report("baseline", exact(result := download(base)), result)

    set_fault("primary", "corrupt", 2)
    report("primary corrupt blocks -> backup", exact(result := download(base)), result)
    clear("primary")

    set_fault("primary", "truncate", 2)
    report("primary truncated blocks -> backup", exact(result := download(base)), result)
    clear("primary")

    set_fault("primary", "corrupt", 10)
    set_fault("backup", "truncate", 10)
    result = download(base)
    detected = not exact(result)
    report("dual failure is visible to the client", detected, result)
    clear("primary")
    clear("backup")

    code, body = request("GET", base + "/api/stats")
    stats = json.loads(body)
    print("downloads:", json.dumps(stats["downloads"], ensure_ascii=False))
    last = stats["last_download"] or {}
    if last.get("outcome") == "completed":
        print(
            "last completed download:",
            json.dumps(
                {
                    "sources": last["sources"],
                    "fallback_count": last["fallback_count"],
                    "max_resident_blocks": last["max_resident_blocks"],
                    "elapsed_ms": last["elapsed_ms"],
                },
                ensure_ascii=False,
            ),
        )
    print(f"{'OK' if failures == 0 else 'FAILURES'}: {failures} failing check(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
