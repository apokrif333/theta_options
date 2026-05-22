from __future__ import annotations

from typing import Any


def normalize_option_right(value: Any) -> str:
    raw = str(value).strip().lower()
    if raw in {"c", "call"}:
        return "call"
    if raw in {"p", "put"}:
        return "put"
    raise ValueError(f"Unsupported option right: {value}")


def normalize_option_strike(value: Any) -> float:
    strike = float(value)
    return strike / 1_000.0 if abs(strike) > 10_000 else strike

