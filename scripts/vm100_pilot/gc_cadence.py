"""Rewrite GC cadence values without changing unrelated TOML text."""
from __future__ import annotations

import re
import tomllib
from collections.abc import Iterator

CADENCE_KEYS = ("interval_secs", "idle_interval_secs", "busy_backlog_interval_secs")


def _statements(text: str) -> Iterator[tuple[str, dict]]:
    """Let tomllib decide where multiline values end, not a second TOML lexer."""
    start = 0
    for line in re.finditer(r"[^\n]*\n|[^\n]+\Z", text):
        fragment = text[start : line.end()]
        try:
            value = tomllib.loads(fragment)
        except tomllib.TOMLDecodeError:
            continue  # The complete input is validated by the caller.
        yield fragment, value
        start = line.end()
    if start != len(text):
        raise ValueError("cannot locate a complete TOML statement")


def _assignment(statement: str) -> tuple[str, str, dict]:
    # An equals sign in a quoted key is not the assignment delimiter.
    for delimiter in re.finditer("=", statement):
        prefix, suffix = statement[: delimiter.end()], statement[delimiter.end() :]
        try:
            path = tomllib.loads(prefix + "0")
        except tomllib.TOMLDecodeError:
            continue
        return prefix, suffix, path
    raise ValueError("cannot locate a TOML assignment")


def rewrite_gc_cadence(text: str, interval_secs: int) -> str:
    """Set the three cadence tiers, preserving key spelling, comments and layout."""
    if type(interval_secs) is not int or not 300 <= interval_secs <= 86400:
        raise ValueError("maintenance GC interval must be between 300 and 86400")
    settings = tomllib.loads(text)
    gc = settings.get("gc", {})
    if not isinstance(gc, dict):
        raise ValueError("GC configuration must be a table")
    for key in CADENCE_KEYS:
        if key in gc and type(gc[key]) is not int:
            raise ValueError(f"GC cadence {key} must be an integer")

    chunks: list[str] = []
    scope = "root"
    anchor: int | None = None
    dotted = False
    replaced: set[str] = set()
    for statement, value in _statements(text):
        if statement.lstrip(" \t").startswith("["):
            scope = "gc" if value == {"gc": {}} else "other"
            if scope == "gc":
                anchor = len(chunks)
                dotted = False
        elif value and scope != "other":
            prefix, suffix, path = _assignment(statement)
            if scope == "root" and "gc" in path:
                if path["gc"] == 0:
                    raise ValueError("cannot safely rewrite an inline GC table")
                anchor = len(chunks)
                dotted = True
            for key in CADENCE_KEYS:
                expected = {key: 0} if scope == "gc" else {"gc": {key: 0}}
                if path == expected:
                    # The parsed value is an integer, so only its token changes.
                    suffix = re.sub(
                        r"\A([ \t]*)[^ \t\r\n#]+",
                        rf"\g<1>{interval_secs}",
                        suffix,
                        count=1,
                    )
                    statement = prefix + suffix
                    replaced.add(key)
                    break
        chunks.append(statement)

    missing = [key for key in CADENCE_KEYS if key not in replaced]
    if missing:
        newline = "\r\n" if "\r\n" in text else "\n"
        prefix = "gc." if dotted else ""
        additions = "".join(
            f"{prefix}{key} = {interval_secs}{newline}" for key in missing
        )
        if anchor is None:
            if text.endswith(newline * 2):
                separator = ""
            elif text.endswith(newline):
                separator = newline
            else:
                separator = newline * 2
            chunks.append(f"{separator}[gc]{newline}{additions}")
        else:
            if not chunks[anchor].endswith("\n"):
                chunks[anchor] += newline
            chunks.insert(anchor + 1, additions)

    rewritten = "".join(chunks)
    rewritten_gc = tomllib.loads(rewritten).get("gc", {})
    if any(rewritten_gc.get(key) != interval_secs for key in CADENCE_KEYS):
        raise RuntimeError("rewritten [gc] cadence did not validate")
    return rewritten
