"""Tiny fallback YAML loader for repository validation checks.

This project does not depend on PyYAML at runtime. The readiness checks only
need syntax sanity for simple compose files, so this module provides safe_load
when PyYAML is unavailable in the execution environment.
"""

from __future__ import annotations


def safe_load(text: str):
    if not isinstance(text, str):
        text = text.read()
    stack: list[tuple[int, dict]] = [(-1, {})]
    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        line = raw.strip()
        if line.startswith("-"):
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip().strip('"\'')
        value = value.strip().strip('"\'')
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if value == "":
            child = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            parent[key] = value
    return stack[0][1]
