from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import pandas as pd
import polars as pl

from .settings import Settings


def load_tiingo_dividend_ttm(symbol: str, settings: Settings) -> pl.DataFrame:
    symbol = symbol.upper()
    if settings.tiingo_usa_root is None:
        return pl.DataFrame({"date": [], "div_ttm": []}, schema={"date": pl.Date, "div_ttm": pl.Float64})

    csv_path = _find_ticker_csv(symbol, str(settings.tiingo_usa_root))
    if csv_path is None:
        return pl.DataFrame({"date": [], "div_ttm": []}, schema={"date": pl.Date, "div_ttm": pl.Float64})
    print(f"{symbol}. Tiingo source file: {csv_path}")

    try:
        pd_df = pd.read_csv(csv_path, usecols=["date", "divCash"], parse_dates=["date"])
    except Exception:
        return pl.DataFrame({"date": [], "div_ttm": []}, schema={"date": pl.Date, "div_ttm": pl.Float64})

    if pd_df.empty:
        return pl.DataFrame({"date": [], "div_ttm": []}, schema={"date": pl.Date, "div_ttm": pl.Float64})

    pd_df = pd_df.sort_values("date")
    pd_df["divCash"] = pd.to_numeric(pd_df["divCash"], errors="coerce").fillna(0.0)
    pd_df = pd_df.set_index("date")
    pd_df["div_ttm"] = pd_df["divCash"].rolling("365D", min_periods=1).sum()
    pd_df = pd_df.reset_index()[["date", "div_ttm"]]
    dates = [value.date() for value in pd_df["date"].tolist()]
    div_ttm = [float(value) for value in pd_df["div_ttm"].tolist()]
    return pl.DataFrame({"date": dates, "div_ttm": div_ttm}, schema={"date": pl.Date, "div_ttm": pl.Float64})


@lru_cache(maxsize=2048)
def _find_ticker_csv(symbol: str, tiingo_root: str) -> str | None:
    root = Path(tiingo_root)
    if not root.exists():
        return None

    # Recursive search inside USA root across any nested folders/exchanges.
    matches = [
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() == ".csv" and path.stem.upper() == symbol.upper()
    ]
    if not matches:
        return None

    # Prefer ETF source, then non-"nan" folder, then shorter path depth.
    matches.sort(
        key=lambda path: (
            int("ETF" not in [part.upper() for part in path.parts]),
            int("nan" in str(path).lower()),
            len(path.parts),
            str(path),
        )
    )
    return str(matches[0])
