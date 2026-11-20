"""Strict single-range parsing shared by the mirrors and the gateway."""

from __future__ import annotations

import re


class RangeError(ValueError):
    pass


_RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$")


def parse_range(header: str | None, size: int) -> tuple[int, int] | None:
    """Parse an RFC 9110 single byte range into an inclusive (start, end).

    Returns ``None`` when no Range header was supplied. Raises ``RangeError``
    for malformed, multi-range or unsatisfiable requests.
    """
    if header is None:
        return None
    match = _RANGE_RE.fullmatch(header.strip())
    if match is None:
        raise RangeError("only one bytes range is supported")
    first, last = match.groups()
    if first == "" and last == "":
        raise RangeError("empty range")
    if first == "":
        suffix = int(last)
        if suffix <= 0:
            raise RangeError("invalid suffix range")
        return max(size - suffix, 0), size - 1
    start = int(first)
    end = int(last) if last else size - 1
    if start >= size or end < start:
        raise RangeError("range not satisfiable")
    return start, min(end, size - 1)
