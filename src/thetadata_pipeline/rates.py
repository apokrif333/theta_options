from __future__ import annotations

import datetime as dt
from io import StringIO
from pathlib import Path

import polars as pl
import requests

from .settings import Settings


DGS1_SERIES_ID = "DGS1"
DGS1_HISTORY_START = dt.date(1962, 1, 2)


def ensure_local_dgs1_history(
    settings: Settings,
    required_start: dt.date,
    required_end: dt.date,
) -> pl.DataFrame:
    path = settings.risk_free_rates_path
    path.parent.mkdir(parents=True, exist_ok=True)

    curve = _read_curve(path)
    target_end = required_end

    if curve.is_empty():
        print(f"DGS1. Local file is missing, downloading full history to {path}")
        curve = _download_dgs1(DGS1_HISTORY_START, target_end)
        _write_curve(path, curve)
        return curve

    current_max = curve["date"].max()
    if current_max is None:
        print(f"DGS1. Local file has no valid rows, redownloading full history to {path}")
        curve = _download_dgs1(DGS1_HISTORY_START, target_end)
        _write_curve(path, curve)
        return curve

    if current_max >= target_end:
        print(f"DGS1. Local history is up-to-date through {current_max}")
        return curve

    download_start = current_max + dt.timedelta(days=1)
    print(f"DGS1. Updating local history {download_start}..{target_end}")
    new_rows = _download_dgs1(download_start, target_end)
    if new_rows.is_empty():
        raise RuntimeError(f"DGS1 update returned no rows for {download_start}..{target_end}")

    combined = (
        pl.concat([curve, new_rows], how="vertical_relaxed")
        .unique(subset=["date"], keep="last")
        .sort("date")
    )
    _write_curve(path, combined)
    return combined


def _read_curve(path: Path) -> pl.DataFrame:
    if not path.exists():
        return _empty_curve()
    try:
        frame = pl.read_parquet(path)
    except Exception:
        return _empty_curve()
    if frame.is_empty():
        return _empty_curve()
    expected = {"date", "risk_free_rate"}
    if not expected.issubset(set(frame.columns)):
        return _empty_curve()
    return (
        frame.select(
            pl.col("date").cast(pl.Date),
            pl.col("risk_free_rate").cast(pl.Float64),
        )
        .filter(pl.col("date").is_not_null())
        .filter(pl.col("risk_free_rate").is_not_null())
        .sort("date")
        .unique(subset=["date"], keep="last")
    )


def _write_curve(path: Path, frame: pl.DataFrame) -> None:
    tmp_path = path.with_name(f"{path.stem}.tmp{path.suffix}")
    if tmp_path.exists():
        tmp_path.unlink()
    frame.sort("date").write_parquet(tmp_path)
    tmp_path.replace(path)


def _download_dgs1(start: dt.date, end: dt.date) -> pl.DataFrame:
    if start > end:
        return _empty_curve()

    try:
        response = requests.get(
            "https://fred.stlouisfed.org/graph/fredgraph.csv",
            params={"id": DGS1_SERIES_ID, "cosd": start.isoformat(), "coed": end.isoformat()},
            timeout=60,
        )
        response.raise_for_status()
    except Exception as exc:
        raise RuntimeError(f"DGS1 download failed for {start}..{end}: {exc}") from exc

    csv_text = response.text.strip()
    if not csv_text:
        return _empty_curve()
    raw = pl.read_csv(StringIO(csv_text))
    if raw.is_empty():
        return _empty_curve()
    date_column = "DATE" if "DATE" in raw.columns else "observation_date"
    if date_column not in raw.columns or DGS1_SERIES_ID not in raw.columns:
        return _empty_curve()

    return (
        raw.select(
            pl.col(date_column).str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("date"),
            pl.col(DGS1_SERIES_ID).cast(pl.Float64, strict=False).alias("risk_free_rate_raw"),
        )
        .filter(pl.col("date").is_not_null())
        .sort("date")
        .with_columns((pl.col("risk_free_rate_raw").fill_null(strategy="forward") / 100.0).alias("risk_free_rate"))
        .drop("risk_free_rate_raw")
        .filter(pl.col("risk_free_rate").is_not_null())
        .unique(subset=["date"], keep="last")
    )


def _empty_curve() -> pl.DataFrame:
    return pl.DataFrame({"date": [], "risk_free_rate": []}, schema={"date": pl.Date, "risk_free_rate": pl.Float64})


def _previous_business_day(today: dt.date | None = None) -> dt.date:
    day = today or dt.date.today()
    day -= dt.timedelta(days=1)
    while day.weekday() >= 5:
        day -= dt.timedelta(days=1)
    return day
