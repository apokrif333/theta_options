from __future__ import annotations

import csv
import datetime as dt
import re

import pandas as pd
import polars as pl

from ..loaders.m1 import download_stock_m1_data
from ..loaders.tick import (
    StockTickWindow,
    ensure_stock_tick_windows,
)
from ..settings import Settings
from ..time_utils import format_time_ms, ms_of_day, parse_ib_expiration, time_to_ms_expr, to_date


OPEN_MS = 34_560_000
STOP_OPEN_MS = 35_400_000
CLOSE_MS = 57_060_000
TICK_LOOKAHEAD_MS = 61_000 * 2

OPTION_RE = re.compile(r"^(?P<ticker>\S+)\s+(?P<expiration>\d{2}[A-Z]{3}\d{2})\s+(?P<strike>\d+(?:\.\d+)?)\s+(?P<right>[CP])$")


def trades_collector(account: str, settings: Settings) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for path in settings.ib_states_dir.glob(f"{account}*.csv"):
        if "lattency" in path.stem.lower() or "latency" in path.stem.lower():
            continue
        header: list[str] | None = None
        with path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.reader(handle):
                if len(row) < 4:
                    continue
                if row[0] == "Trades" and row[1] == "Header":
                    header = row
                    continue
                if row[0] == "Trades" and row[1] == "Data" and row[3] == "Equity and Index Options":
                    if header is None:
                        raise ValueError(f"{path}: Trades header not found before data row")
                    rows.append(dict(zip(header, row)))

    trades = pd.DataFrame(rows)
    if trades.empty:
        return trades

    parsed_contracts = trades["Symbol"].apply(_parse_option_symbol).apply(pd.Series)
    trades = pd.concat([trades, parsed_contracts], axis=1)
    trades["trade_dt"] = pd.to_datetime(trades["Date/Time"], format="%Y-%m-%d, %H:%M:%S")
    trades["trade_date"] = trades["trade_dt"].dt.date
    trades["trade_ms"] = trades["trade_dt"].apply(ms_of_day)
    trades["quantity"] = pd.to_numeric(trades["Quantity"], errors="coerce")
    trades["ticker"] = trades["ticker"].str.upper()
    trades["code"] = trades["Code"].fillna("").astype(str)
    return trades


def trades_latency_analyzer(
    trades: pd.DataFrame,
    settings: Settings,
    batch_mode: str = "weekly",
    stock_concurrency: int = 2,
    tick_concurrency: int = 4,
    dry_run: bool = False,
) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()

    trades = trades.dropna(subset=["ticker", "strike", "right", "trade_dt", "quantity"]).copy()
    print(f"Latency. Trades selected for analysis: {len(trades)}")
    print(f"Latency. Tick concurrency: {max(1, int(tick_concurrency))}")
    _download_stock_m1_for_trades(
        trades,
        settings,
        batch_mode=batch_mode,
        stock_concurrency=stock_concurrency,
        dry_run=dry_run,
    )

    frames: list[pd.DataFrame] = []
    for ticker in sorted(trades["ticker"].dropna().unique()):
        ticker_trades = trades[trades["ticker"] == ticker].copy()
        frame = analyze_ticker_lattency(
            ticker=ticker,
            trades=ticker_trades,
            settings=settings,
            tick_concurrency=tick_concurrency,
            dry_run=dry_run,
        )
        if not frame.empty:
            frames.append(frame)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, sort=False).sort_values("trade_dt").reset_index(drop=True)


def analyze_ticker_lattency(
    ticker: str,
    trades: pd.DataFrame,
    settings: Settings,
    tick_concurrency: int = 4,
    dry_run: bool = False,
) -> pd.DataFrame:
    print(f"{ticker}. Latency ticker trades={len(trades)}")

    after_close_trades = trades[trades["trade_ms"] > CLOSE_MS].copy()
    _mark_after_close(after_close_trades)

    open_trades = trades[(trades["quantity"] < 0) & (trades["trade_ms"] < STOP_OPEN_MS)].copy()
    _mark_open(open_trades)

    close_trades = trades[(trades["quantity"] > 0) & (trades["trade_ms"] < CLOSE_MS)].copy()
    crossed = _find_m1_crosses(settings, ticker, close_trades)
    print(
        f"{ticker}. Latency groups: open={len(open_trades)}, "
        f"after_close={len(after_close_trades)}, close_candidates={len(close_trades)}, "
        f"m1_crossed={len(crossed)}"
    )

    stopped = pd.DataFrame()
    if not crossed.empty and not dry_run:
        triggers = _resolve_tick_triggers(
            settings=settings,
            ticker=ticker,
            crossed=crossed,
            tick_concurrency=tick_concurrency,
        )
        if not triggers.empty:
            stopped = crossed.drop(columns=["ms_of_day", "high", "low"], errors="ignore").merge(
                triggers,
                left_index=True,
                right_index=True,
            )
            stopped["trigger_time"] = stopped["trigger_time_ms"].apply(format_time_ms)
            stopped["lattency"] = (stopped["trade_ms"] - stopped["trigger_time_ms"]) / 1000
            stopped["lable"] = "stop"
    elif dry_run and not crossed.empty:
        stopped = crossed.drop(columns=["ms_of_day", "high", "low"], errors="ignore").copy()
        stopped["lable"] = "dry_run_stop"

    stop_label = "Tick stop candidates" if dry_run else "Tick stop triggers"
    print(f"{ticker}. {stop_label}={len(stopped)}")
    frames = [frame for frame in [after_close_trades, open_trades, stopped] if not frame.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, sort=False).sort_values("trade_dt")


def run(
    account: str,
    settings: Settings,
    batch_mode: str = "weekly",
    stock_concurrency: int = 2,
    tick_concurrency: int = 4,
    dry_run: bool = False,
) -> int:
    if not account:
        raise ValueError("analysis.account_id is empty in pipeline.toml")

    trades = trades_collector(account, settings)
    result = trades_latency_analyzer(
        trades=trades,
        settings=settings,
        batch_mode=batch_mode,
        stock_concurrency=stock_concurrency,
        tick_concurrency=tick_concurrency,
        dry_run=dry_run,
    )
    output_path = settings.ib_states_dir / f"{account}_lattency.csv"
    if not dry_run:
        result.to_csv(output_path, index=False)
    print(f"trades={len(trades)}")
    print(f"lattency_rows={len(result)}")
    print(f"output_path={output_path}")
    if not result.empty and "lable" in result.columns:
        print(result["lable"].value_counts(dropna=False).to_string())
    return 0


def _download_stock_m1_for_trades(
    trades: pd.DataFrame,
    settings: Settings,
    batch_mode: str,
    stock_concurrency: int,
    dry_run: bool,
) -> None:
    for ticker, group in trades.groupby("ticker"):
        days = sorted(set(group["trade_date"]))
        if not days:
            continue
        print(f"{ticker}. Latency stock M1 ensure: {days[0]}..{days[-1]} ({len(days)} day(s))")
        download_stock_m1_data(
            settings=settings,
            symbol=ticker,
            from_date=days[0],
            end_date=days[-1],
            batch_mode=batch_mode,
            stock_concurrency=stock_concurrency,
            dry_run=dry_run,
        )


def _mark_after_close(trades: pd.DataFrame) -> None:
    if trades.empty:
        return
    trades["lable"] = "after_close"
    trades["lattency"] = 0.0
    trades["trigger_time_ms"] = trades["trade_ms"]
    trades["trigger_time"] = trades["trigger_time_ms"].apply(format_time_ms)


def _mark_open(trades: pd.DataFrame) -> None:
    if trades.empty:
        return
    trades["lattency"] = (trades["trade_ms"] - OPEN_MS) / 1000
    trades["trigger_time_ms"] = OPEN_MS
    trades["trigger_time"] = format_time_ms(OPEN_MS)
    trades["lable"] = "open"


def _find_m1_crosses(settings: Settings, ticker: str, trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()

    cur_trades = pl.from_pandas(trades.reset_index().rename(columns={"index": "idx"})).with_columns(
        pl.col("trade_date").cast(pl.Date),
        pl.col("strike").cast(pl.Float64),
    )
    start_date = trades["trade_date"].min()
    end_date = trades["trade_date"].max()
    m1 = _load_stock_m1_window(settings, ticker, start_date, end_date)
    if m1.is_empty():
        return pd.DataFrame()

    crossed = (
        cur_trades.join(m1, left_on="trade_date", right_on="date", how="inner")
        .filter(
            ((pl.col("right") == "C") & (pl.col("strike") < pl.col("high")))
            | ((pl.col("right") == "P") & (pl.col("strike") > pl.col("low")))
        )
        .sort(["trade_date", "ms_of_day"])
        .unique(subset=["idx"], keep="first", maintain_order=True)
        .with_columns(pl.col("trade_date").cast(pl.Date))
    )
    if crossed.is_empty():
        return pd.DataFrame()

    return crossed.to_pandas().set_index("idx")


def _resolve_tick_triggers(
    settings: Settings,
    ticker: str,
    crossed: pd.DataFrame,
    tick_concurrency: int,
) -> pd.DataFrame:
    windows = [
        StockTickWindow(
            ticker=ticker,
            day=to_date(row["trade_date"]),
            start_ms=int(row["ms_of_day"]),
            end_ms=int(row["ms_of_day"]) + TICK_LOOKAHEAD_MS,
        )
        for _, row in crossed.iterrows()
    ]
    tick_concurrency = max(1, int(tick_concurrency))
    print(f"{ticker}. Tick trigger jobs={len(crossed)}, concurrency={tick_concurrency}")

    tick_pool = ensure_stock_tick_windows(
        settings=settings,
        windows=windows,
        concurrency=tick_concurrency,
        interval="tick",
    )
    if tick_pool.is_empty():
        return pd.DataFrame()
    return _tick_triggers_from_pool(tick_pool, crossed)


def _tick_triggers_from_pool(ticks: pl.DataFrame, crossed: pd.DataFrame) -> pd.DataFrame:
    job_frame = (
        pl.from_pandas(crossed.reset_index()[["idx", "trade_date", "right", "strike"]])
        .with_columns(
            pl.col("trade_date").cast(pl.Date).alias("date"),
            pl.col("strike").cast(pl.Float64),
            pl.col("right").cast(pl.String),
            pl.col("idx").cast(pl.Int64),
        )
        .select(["idx", "date", "right", "strike"])
    )
    total = ticks.join(job_frame, on="date", how="inner")
    total = total.filter(
        ((pl.col("right") == "C") & (pl.col("strike") < pl.col("ask")))
        | ((pl.col("right") == "P") & (pl.col("strike") > pl.col("bid")))
    )
    if total.is_empty():
        return pd.DataFrame()

    total = (
        total.sort(["date", "time"])
        .unique(subset=["idx"], keep="first", maintain_order=True)
        .with_columns(time_to_ms_expr().alias("trigger_time_ms"))
        .select(["idx", "time", "trigger_time_ms"])
        .rename({"time": "trigger_time"})
    )
    return total.to_pandas().set_index("idx")


def _load_stock_m1_window(settings: Settings, ticker: str, start_date: dt.date, end_date: dt.date) -> pl.DataFrame:
    paths = sorted(settings.stock_m1_dir.glob(f"*{ticker}_m1_stock.parquet"))
    if not paths:
        raise FileNotFoundError(f"{ticker}. No stock M1 parquet files found in {settings.stock_m1_dir}")

    lf = pl.scan_parquet([str(path) for path in paths])
    schema = lf.collect_schema()
    return (
        lf.with_columns(_stock_m1_date_expr(schema).alias("date"))
        .filter((pl.col("date") >= start_date) & (pl.col("date") <= end_date))
        .filter(pl.col("ms_of_day") > OPEN_MS)
        .filter(pl.col("close") != 0)
        .select(
            pl.col("date").cast(pl.Date),
            pl.col("ms_of_day").cast(pl.Int32),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
        )
        .collect()
    )


def _stock_m1_date_expr(schema: pl.Schema) -> pl.Expr:
    dtype = schema.get("date")
    if dtype == pl.Date:
        return pl.col("date").cast(pl.Date)
    if dtype == pl.Datetime:
        return pl.col("date").dt.date()
    date_text = pl.col("date").cast(pl.String)
    return pl.coalesce(
        [
            date_text.str.strptime(pl.Date, "%Y%m%d", strict=False),
            date_text.str.strptime(pl.Date, "%Y-%m-%d", strict=False),
        ]
    )


def _parse_option_symbol(value: str) -> dict:
    match = OPTION_RE.match(str(value).strip())
    if match is None:
        return {"ticker": None, "expiration": None, "strike": None, "right": None}
    raw_expiration = match.group("expiration")
    return {
        "ticker": match.group("ticker"),
        "expiration": parse_ib_expiration(raw_expiration),
        "strike": float(match.group("strike")),
        "right": match.group("right"),
    }
