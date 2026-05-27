from __future__ import annotations

import datetime as dt
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
from tqdm import tqdm

from ..dates import coerce_date, format_theta_date, format_theta_time
from ..options import normalize_option_right, normalize_option_strike
from ..settings import Settings
from ..theta_client import ThetaClient
from ..time_utils import datetime_to_ms_expr, time_to_ms_expr


@dataclass(frozen=True)
class StockTickWindow:
    ticker: str
    day: dt.date
    start_ms: int
    end_ms: int


@dataclass(frozen=True)
class OptionQuoteWindow:
    ticker: str
    day: dt.date
    expiration: dt.date
    strike: float
    right: str
    start_ms: int
    end_ms: int


OPTION_QUOTE_COLUMNS = [
    "ticker",
    "date",
    "expiration",
    "strike",
    "right",
    "time",
    "bid_size",
    "bid_exchange",
    "bid",
    "bid_condition",
    "ask_size",
    "ask_exchange",
    "ask",
    "ask_condition",
]

def fetch_stock_quote_window(
    settings: Settings,
    symbol: str,
    day: dt.date,
    start_ms: int,
    end_ms: int,
    interval: str = "1s",
    client: ThetaClient | None = None,
) -> pl.DataFrame:
    frame = _client(settings, client).get_csv_frame(
        "/stock/history/quote",
        _window_params(symbol, day, start_ms, end_ms) | {"interval": interval},
    )
    return _drop_empty_response_rows(frame)


def fetch_stock_trade_window(
    settings: Settings,
    symbol: str,
    day: dt.date,
    start_ms: int,
    end_ms: int,
    client: ThetaClient | None = None,
) -> pl.DataFrame:
    frame = _client(settings, client).get_csv_frame(
        "/stock/history/trade",
        _window_params(symbol, day, start_ms, end_ms),
    )
    return _drop_empty_response_rows(frame)


def fetch_option_quote_window(
    settings: Settings,
    symbol: str,
    day: dt.date,
    expiration: dt.date,
    strike: float,
    right: str,
    start_ms: int,
    end_ms: int,
    interval: str = "1s",
    client: ThetaClient | None = None,
) -> pl.DataFrame:
    params = _window_params(symbol, day, start_ms, end_ms)
    params.update(
        {
            "expiration": format_theta_date(expiration),
            "strike": f"{normalize_option_strike(strike):.3f}",
            "right": normalize_option_right(right),
            "interval": interval,
        }
    )
    frame = _client(settings, client).get_csv_frame("/option/history/quote", params)
    return _drop_empty_response_rows(frame)


def stock_tick_file_path(settings: Settings, ticker: str) -> Path:
    return settings.stock_ticks_dir / f"{ticker.upper()}_ticks.parquet"


def stock_tick_coverage_path(settings: Settings, ticker: str) -> Path:
    return settings.stock_ticks_dir / f"{ticker.upper()}_ticks_coverage.parquet"


def option_quote_file_path(settings: Settings, ticker: str, interval: str = "100ms") -> Path:
    interval_key = _interval_key(interval)
    return settings.data_dir / "options" / interval_key / f"{ticker.upper()}_{interval_key}_opts.parquet"


def option_quote_coverage_path(settings: Settings, ticker: str, interval: str = "100ms") -> Path:
    interval_key = _interval_key(interval)
    return settings.data_dir / "options" / interval_key / f"{ticker.upper()}_{interval_key}_opts_coverage.parquet"


def ensure_option_quote_window(
    settings: Settings,
    symbol: str,
    day: dt.date,
    expiration: dt.date,
    strike: float,
    right: str,
    start_ms: int,
    end_ms: int,
    interval: str = "100ms",
    option_concurrency: int = 1,
) -> pl.DataFrame:
    return ensure_option_quote_windows(
        settings=settings,
        windows=[
            OptionQuoteWindow(
                ticker=symbol.upper(),
                day=coerce_date(day),
                expiration=coerce_date(expiration),
                strike=normalize_option_strike(strike),
                right=_option_right_code(right),
                start_ms=int(start_ms),
                end_ms=int(end_ms),
            )
        ],
        interval=interval,
        option_concurrency=option_concurrency,
    )


def ensure_option_quote_windows(
    settings: Settings,
    windows,
    interval: str = "100ms",
    option_concurrency: int = 1,
) -> pl.DataFrame:
    """ Ensure option quote data is available for the specified windows and return the combined data.

    Downloads missing option quote data from Theta Data API and caches it locally.
    Returns the option quotes for all requested windows from the local cache.

    Args:
        settings:
        windows: Option quote windows to ensure. Can be:
            - Single OptionQuoteWindow instance
            - List/tuple of OptionQuoteWindow instances
            - pl.DataFrame with required columns
            - Dict with required keys
            - pandas DataFrame (via to_dict('records'))

            Required columns/keys for dict-like inputs:
            - ticker (str):
            - date (date/str): YYYY-MM-DD or YYYYMMDD
            - expiration (date/str): YYYY-MM-DD or YYYYMMDD
            - strike (float):
            - right (str): 'C'/'call' or 'P'/'put'
            - start_ms (int):
            - end_ms (int):

        interval: (e.g., '100ms', '1s', 'tick')
        option_concurrency:

    Returns:
        pl.DataFrame
    """
    normalized = _coerce_option_quote_windows(windows)
    if not normalized:
        return _empty_option_quote_frame()

    jobs: list[tuple[OptionQuoteWindow, int, int]] = []
    for window in normalized:
        missing = missing_option_quote_ranges(settings, window, interval=interval)
        jobs.extend((window, start, end) for start, end in missing)

    tickers = sorted({window.ticker for window in normalized})
    print(
        f"Option quotes {interval}: windows={len(normalized)}, "
        f"theta_ranges={len(jobs)}, concurrency={max(1, int(option_concurrency))}, "
        f"tickers={','.join(tickers)}"
    )
    for ticker in tickers:
        print(f"{ticker}. Option quotes cache: {option_quote_file_path(settings, ticker, interval=interval)}")

    if jobs:
        _download_and_store_option_quote_jobs(
            settings=settings,
            jobs=jobs,
            interval=interval,
            option_concurrency=option_concurrency,
        )
    return load_option_quote_windows(settings, normalized, interval=interval)


def load_option_quote_window(
    settings: Settings,
    window: OptionQuoteWindow,
    interval: str = "100ms",
) -> pl.DataFrame:
    path = option_quote_file_path(settings, window.ticker, interval=interval)
    if not path.exists():
        return _empty_option_quote_frame()
    lf = pl.scan_parquet(path)
    schema = lf.collect_schema()
    return (
        lf.with_columns(
            _option_quote_date_expr(schema, "date").alias("date"),
            _option_quote_date_expr(schema, "expiration").alias("expiration"),
            _option_quote_time_expr(schema).alias("time"),
            _option_quote_ms_expr(schema).alias("ms_of_day"),
            pl.col("strike").cast(pl.Float64),
            _option_quote_right_expr().alias("right"),
        )
        .filter(
            (pl.col("date") == window.day)
            & (pl.col("expiration") == window.expiration)
            & (pl.col("strike") == window.strike)
            & (pl.col("right") == window.right)
            & pl.col("ms_of_day").is_between(window.start_ms, window.end_ms, closed="both")
        )
        .select(OPTION_QUOTE_COLUMNS)
        .sort(["date", "expiration", "strike", "right", "time"])
        .collect()
    )


def load_option_quote_windows(
    settings: Settings,
    windows,
    interval: str = "100ms",
) -> pl.DataFrame:
    windows = _coerce_option_quote_windows(windows)
    frames: list[pl.DataFrame] = []
    for window in windows:
        frame = load_option_quote_window(settings, window, interval=interval)
        if not frame.is_empty():
            frames.append(frame)
    if not frames:
        return _empty_option_quote_frame()
    return (
        pl.concat(frames)
        .sort(["ticker", "date", "expiration", "strike", "right", "time"])
        .unique(subset=["ticker", "date", "expiration", "strike", "right", "time"], keep="last", maintain_order=True)
    )


def missing_option_quote_ranges(
    settings: Settings,
    window: OptionQuoteWindow,
    interval: str = "100ms",
) -> list[tuple[int, int]]:
    coverage = _load_option_quote_coverage(settings, window.ticker, interval=interval)
    if coverage.is_empty():
        return [(window.start_ms, window.end_ms)]
    rows = (
        coverage.filter(
            (pl.col("date") == window.day)
            & (pl.col("expiration") == window.expiration)
            & (pl.col("strike") == window.strike)
            & (pl.col("right") == window.right)
        )
        .select(["start_ms", "end_ms"])
        .sort("start_ms")
        .to_dicts()
    )
    return _subtract_covered_ranges(window.start_ms, window.end_ms, [(row["start_ms"], row["end_ms"]) for row in rows])


def _coerce_option_quote_windows(windows) -> list[OptionQuoteWindow]:
    if isinstance(windows, OptionQuoteWindow):
        raw_windows = [windows]
    elif isinstance(windows, pl.DataFrame):
        raw_windows = windows.to_dicts()
    elif isinstance(windows, dict):
        raw_windows = [windows]
    elif hasattr(windows, "to_dict") and not isinstance(windows, list | tuple):
        raw_windows = windows.to_dict("records")
    else:
        raw_windows = list(windows)

    normalized: list[OptionQuoteWindow] = []
    for raw in raw_windows:
        window = _normalize_option_quote_window(raw)
        if window.start_ms <= window.end_ms:
            normalized.append(window)
    return normalized


def _normalize_option_quote_window(window) -> OptionQuoteWindow:
    if isinstance(window, OptionQuoteWindow):
        return _normalize_option_quote_window_values(
            ticker=window.ticker,
            day=window.day,
            expiration=window.expiration,
            strike=window.strike,
            right=window.right,
            start_ms=window.start_ms,
            end_ms=window.end_ms,
        )
    if isinstance(window, dict):
        return _option_quote_window_from_mapping(window)
    raise TypeError(f"Unsupported option quote window row type: {type(window).__name__}")


def _option_quote_window_from_mapping(row: dict) -> OptionQuoteWindow:
    required = ["ticker", "date", "expiration", "strike", "right", "start_ms", "end_ms"]
    missing = [column for column in required if column not in row or row[column] is None]
    if missing:
        raise KeyError(
            "Option quote windows require fixed columns: "
            f"{', '.join(required)}. Missing: {', '.join(missing)}"
        )
    return _normalize_option_quote_window_values(
        ticker=row["ticker"],
        day=row["date"],
        expiration=row["expiration"],
        strike=row["strike"],
        right=row["right"],
        start_ms=row["start_ms"],
        end_ms=row["end_ms"],
    )


def _normalize_option_quote_window_values(
    ticker,
    day,
    expiration,
    strike,
    right,
    start_ms,
    end_ms,
) -> OptionQuoteWindow:
    return OptionQuoteWindow(
        ticker=str(ticker).upper(),
        day=coerce_date(day),
        expiration=coerce_date(expiration),
        strike=normalize_option_strike(strike),
        right=_option_right_code(right),
        start_ms=int(start_ms),
        end_ms=int(end_ms),
    )


def load_stock_tick_window(
    settings: Settings,
    ticker: str,
    day: dt.date,
    start_ms: int,
    end_ms: int,
) -> pl.DataFrame | None:
    path = stock_tick_file_path(settings, ticker)
    if not path.exists():
        return None

    lf = pl.scan_parquet(path)
    schema = lf.collect_schema()
    window = (
        lf.with_columns(
            _stock_tick_date_expr(schema).alias("date"),
            _stock_tick_ms_expr(schema).alias("ms_of_day"),
        )
        .filter(
            (pl.col("date") == day)
            & pl.col("ms_of_day").is_between(start_ms, end_ms, closed="both")
        )
        .collect()
    )
    if window.is_empty():
        return None
    return add_tick_time_columns(window).sort(["date", "time"])


def missing_stock_tick_ranges(
    settings: Settings,
    ticker: str,
    day: dt.date,
    start_ms: int,
    end_ms: int,
) -> list[tuple[int, int]]:
    coverage = _load_stock_tick_coverage(settings, ticker)
    if coverage.is_empty():
        return [(start_ms, end_ms)]

    rows = (
        coverage.filter(pl.col("date") == day)
        .select(["start_ms", "end_ms"])
        .sort("start_ms")
        .to_dicts()
    )
    return _subtract_covered_ranges(start_ms, end_ms, [(row["start_ms"], row["end_ms"]) for row in rows])


def ensure_stock_tick_window(
    settings: Settings,
    ticker: str,
    day: dt.date,
    start_ms: int,
    end_ms: int,
    interval: str = "tick",
) -> pl.DataFrame:
    return ensure_stock_tick_windows(
        settings=settings,
        windows=[StockTickWindow(ticker=ticker, day=day, start_ms=start_ms, end_ms=end_ms)],
        concurrency=1,
        interval=interval,
    )


def ensure_stock_tick_windows(
    settings: Settings,
    windows,
    concurrency: int = 1,
    interval: str = "tick",
) -> pl.DataFrame:
    """Ensure stock tick data is available for the specified windows and return the combined data.

    Downloads missing stock quote ticks from Theta Data API and caches them locally.
    `windows` accepts the same input shapes as option quote windows: a single
    StockTickWindow, an iterable of windows, a mapping, a Polars DataFrame, or
    a pandas-like DataFrame with ticker/date/start_ms/end_ms columns.
    """
    normalized = _coerce_stock_tick_windows(windows)
    if not normalized:
        return _empty_stock_tick_frame()

    requested = _merge_stock_tick_windows(normalized)
    missing = _missing_stock_tick_windows(settings, requested)
    print(f"Stock ticks. requested_ranges={len(requested)}, theta_ranges={len(missing)}")
    if missing:
        _download_and_store_stock_tick_windows(
            settings=settings,
            windows=missing,
            concurrency=concurrency,
            interval=interval,
        )
    return load_stock_tick_windows(settings, requested)


def load_stock_tick_windows(settings: Settings, windows) -> pl.DataFrame:
    windows = _coerce_stock_tick_windows(windows)
    frames: list[pl.DataFrame] = []
    for window in windows:
        ticks = load_stock_tick_window(settings, window.ticker, window.day, window.start_ms, window.end_ms)
        if ticks is not None and not ticks.is_empty():
            frames.append(ticks.select(["date", "time", "bid", "ask"]))
    if not frames:
        return _empty_stock_tick_frame()
    return (
        pl.concat(frames)
        .sort(["date", "time"])
        .unique(subset=["date", "bid", "ask"], keep="first", maintain_order=True)
    )


def _coerce_stock_tick_windows(windows) -> list[StockTickWindow]:
    if isinstance(windows, StockTickWindow):
        raw_windows = [windows]
    elif isinstance(windows, pl.DataFrame):
        raw_windows = windows.to_dicts()
    elif isinstance(windows, dict):
        raw_windows = [windows]
    elif hasattr(windows, "to_dict") and not isinstance(windows, list | tuple):
        raw_windows = windows.to_dict("records")
    else:
        raw_windows = list(windows)

    normalized: list[StockTickWindow] = []
    for raw in raw_windows:
        window = _normalize_stock_tick_window(raw)
        if window.start_ms <= window.end_ms:
            normalized.append(window)
    return normalized


def _normalize_stock_tick_window(window) -> StockTickWindow:
    if isinstance(window, StockTickWindow):
        return _normalize_stock_tick_window_values(
            ticker=window.ticker,
            day=window.day,
            start_ms=window.start_ms,
            end_ms=window.end_ms,
        )
    if isinstance(window, dict):
        return _stock_tick_window_from_mapping(window)
    raise TypeError(f"Unsupported stock tick window row type: {type(window).__name__}")


def _stock_tick_window_from_mapping(row: dict) -> StockTickWindow:
    required = ["ticker", "date", "start_ms", "end_ms"]
    values = {
        "ticker": row.get("ticker"),
        "date": row.get("date") if row.get("date") is not None else row.get("day"),
        "start_ms": row.get("start_ms"),
        "end_ms": row.get("end_ms"),
    }
    missing = [column for column in required if values[column] is None]
    if missing:
        raise KeyError(
            "Stock tick windows require fixed columns: "
            f"{', '.join(required)}. Missing: {', '.join(missing)}"
        )
    return _normalize_stock_tick_window_values(
        ticker=values["ticker"],
        day=values["date"],
        start_ms=values["start_ms"],
        end_ms=values["end_ms"],
    )


def _normalize_stock_tick_window_values(
    ticker,
    day,
    start_ms,
    end_ms,
) -> StockTickWindow:
    return StockTickWindow(
        ticker=str(ticker).upper(),
        day=coerce_date(day),
        start_ms=int(start_ms),
        end_ms=int(end_ms),
    )


def record_stock_tick_coverage(
    settings: Settings,
    ticker: str,
    ranges: list[tuple[dt.date, int, int]],
) -> Path:
    path = stock_tick_coverage_path(settings, ticker)
    path.parent.mkdir(parents=True, exist_ok=True)

    rows = _load_stock_tick_coverage(settings, ticker).to_dicts()
    rows.extend(
        {"date": day, "start_ms": int(start_ms), "end_ms": int(end_ms)}
        for day, start_ms, end_ms in ranges
        if start_ms <= end_ms
    )
    merged = _merge_coverage_rows(rows)
    frame = pl.DataFrame(
        merged,
        schema={"date": pl.Date, "start_ms": pl.Int32, "end_ms": pl.Int32},
        orient="row",
    )
    tmp_path = path.with_name(f"{path.stem}.tmp{path.suffix}")
    if tmp_path.exists():
        tmp_path.unlink()
    frame.write_parquet(tmp_path)
    tmp_path.replace(path)
    return path


def stage_stock_tick_frame(settings: Settings, ticker: str, ticks: pl.DataFrame) -> Path:
    ticker = ticker.upper()
    staging_dir = settings.stock_ticks_dir / "_staging" / ticker
    staging_dir.mkdir(parents=True, exist_ok=True)
    path = staging_dir / f"{time.time_ns()}.parquet"
    normalize_stock_tick_frame(ticks).write_parquet(path)
    return path


def merge_stock_tick_file(settings: Settings, ticker: str, staged_paths: list[Path]) -> Path:
    output_path = stock_tick_file_path(settings, ticker)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    inputs = ([output_path] if output_path.exists() else []) + [path for path in staged_paths if path.exists()]
    if not inputs:
        return output_path

    tmp_path = output_path.with_name(f"{output_path.stem}.tmp{output_path.suffix}")
    if tmp_path.exists():
        tmp_path.unlink()
    (
        pl.concat([_scan_compact_stock_tick_file(path) for path in inputs])
        .sort(["date", "time"])
        .unique(subset=["date", "bid", "ask"], keep="first", maintain_order=True)
        .sink_parquet(tmp_path)
    )
    tmp_path.replace(output_path)
    for path in staged_paths:
        path.unlink(missing_ok=True)
    return output_path


def append_stock_ticks(settings: Settings, ticker: str, ticks: pl.DataFrame) -> Path:
    ticks = normalize_stock_tick_frame(ticks)
    if ticks.is_empty():
        return stock_tick_file_path(settings, ticker)

    staged_path = stage_stock_tick_frame(settings, ticker, ticks)
    return merge_stock_tick_file(settings, ticker, [staged_path])


def stage_option_quote_frame(
    settings: Settings,
    window: OptionQuoteWindow,
    quotes: pl.DataFrame,
    interval: str = "100ms",
) -> Path:
    interval_key = _interval_key(interval)
    staging_dir = settings.data_dir / "options" / interval_key / "_staging" / window.ticker
    staging_dir.mkdir(parents=True, exist_ok=True)
    path = staging_dir / f"{time.time_ns()}.parquet"
    normalize_option_quote_frame(quotes, window).write_parquet(path)
    return path


def merge_option_quote_file(
    settings: Settings,
    ticker: str,
    staged_paths: list[Path],
    interval: str = "100ms",
) -> Path:
    output_path = option_quote_file_path(settings, ticker, interval=interval)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    inputs = ([output_path] if output_path.exists() else []) + [path for path in staged_paths if path.exists()]
    if not inputs:
        return output_path

    tmp_path = output_path.with_name(f"{output_path.stem}.tmp{output_path.suffix}")
    if tmp_path.exists():
        tmp_path.unlink()
    (
        pl.concat([_scan_option_quote_file(path) for path in inputs])
        .sort(["date", "expiration", "strike", "right", "time"])
        .unique(subset=["date", "expiration", "strike", "right", "time"], keep="last", maintain_order=True)
        .sink_parquet(tmp_path)
    )
    tmp_path.replace(output_path)
    for path in staged_paths:
        path.unlink(missing_ok=True)
    return output_path


def record_option_quote_coverage(
    settings: Settings,
    ticker: str,
    ranges: list[tuple[dt.date, dt.date, float, str, int, int]],
    interval: str = "100ms",
) -> Path:
    path = option_quote_coverage_path(settings, ticker, interval=interval)
    path.parent.mkdir(parents=True, exist_ok=True)

    rows = _load_option_quote_coverage(settings, ticker, interval=interval).to_dicts()
    rows.extend(
        {
            "date": day,
            "expiration": expiration,
            "strike": normalize_option_strike(strike),
            "right": _option_right_code(right),
            "start_ms": int(start_ms),
            "end_ms": int(end_ms),
        }
        for day, expiration, strike, right, start_ms, end_ms in ranges
        if int(start_ms) <= int(end_ms)
    )
    frame = pl.DataFrame(
        _merge_option_coverage_rows(rows),
        schema={
            "date": pl.Date,
            "expiration": pl.Date,
            "strike": pl.Float64,
            "right": pl.String,
            "start_ms": pl.Int32,
            "end_ms": pl.Int32,
        },
        orient="row",
    )
    tmp_path = path.with_name(f"{path.stem}.tmp{path.suffix}")
    if tmp_path.exists():
        tmp_path.unlink()
    frame.write_parquet(tmp_path)
    tmp_path.replace(path)
    return path


def _download_and_store_option_quote_jobs(
    settings: Settings,
    jobs: list[tuple[OptionQuoteWindow, int, int]],
    interval: str,
    option_concurrency: int,
) -> None:
    staged_by_ticker: dict[str, list[Path]] = {}
    covered_by_ticker: dict[str, list[tuple[dt.date, dt.date, float, str, int, int]]] = {}
    concurrency = max(1, int(option_concurrency))
    print(f"Option quotes downloading ranges={len(jobs)}, concurrency={concurrency}")

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(
                fetch_option_quote_window,
                settings,
                window.ticker,
                window.day,
                window.expiration,
                window.strike,
                window.right,
                start,
                end,
                interval,
            ): (window, start, end)
            for window, start, end in jobs
        }
        with tqdm(total=len(futures), desc="Option quotes download", unit="range") as pbar:
            for future in as_completed(futures):
                window, start, end = futures[future]
                quotes = future.result()
                if not quotes.is_empty():
                    staged_by_ticker.setdefault(window.ticker, []).append(
                        stage_option_quote_frame(settings, window, quotes, interval=interval)
                    )
                covered_by_ticker.setdefault(window.ticker, []).append(
                    (window.day, window.expiration, window.strike, window.right, start, end)
                )
                pbar.update(1)
                pbar.set_postfix(
                    ticker=window.ticker,
                    date=str(window.day),
                    start=str(start),
                    end=str(end),
                    rows=quotes.height,
                )

    for ticker, staged_paths in staged_by_ticker.items():
        print(f"{ticker}. Option quotes cache merge: staged_files={len(staged_paths)}")
        merge_option_quote_file(settings, ticker, staged_paths, interval=interval)
    for ticker, ranges in covered_by_ticker.items():
        record_option_quote_coverage(settings, ticker, ranges, interval=interval)


def _download_and_store_stock_tick_windows(
    settings: Settings,
    windows: list[StockTickWindow],
    concurrency: int,
    interval: str,
) -> None:
    staged_by_ticker: dict[str, list[Path]] = {}
    covered_by_ticker: dict[str, list[tuple[dt.date, int, int]]] = {}
    concurrency = max(1, int(concurrency))
    print(f"Stock ticks. Downloading theta_ranges={len(windows)}, concurrency={concurrency}, interval={interval}")

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(
                fetch_stock_quote_window,
                settings,
                window.ticker,
                window.day,
                window.start_ms,
                window.end_ms,
                interval,
            ): window
            for window in windows
        }
        with tqdm(total=len(futures), desc="Stock ticks download", unit="window") as pbar:
            for future in as_completed(futures):
                window = futures[future]
                ticks = future.result()
                if not ticks.is_empty():
                    staged_by_ticker.setdefault(window.ticker, []).append(
                        stage_stock_tick_frame(settings, window.ticker, ticks)
                    )
                covered_by_ticker.setdefault(window.ticker, []).append((window.day, window.start_ms, window.end_ms))
                pbar.update(1)
                pbar.set_postfix(
                    ticker=window.ticker,
                    date=str(window.day),
                    rows=ticks.height,
                )

    for ticker, staged_paths in staged_by_ticker.items():
        print(f"{ticker}. Tick cache merge: staged_files={len(staged_paths)}")
        merge_stock_tick_file(settings, ticker, staged_paths)
    for ticker, ranges in covered_by_ticker.items():
        print(f"{ticker}. Tick coverage update: ranges={len(ranges)}")
        record_stock_tick_coverage(settings, ticker, ranges)


def _missing_stock_tick_windows(settings: Settings, windows: list[StockTickWindow]) -> list[StockTickWindow]:
    missing: list[StockTickWindow] = []
    for window in windows:
        for start_ms, end_ms in missing_stock_tick_ranges(
            settings,
            window.ticker,
            window.day,
            window.start_ms,
            window.end_ms,
        ):
            missing.append(StockTickWindow(window.ticker, window.day, start_ms, end_ms))
    return missing


def _merge_stock_tick_windows(windows: list[StockTickWindow]) -> list[StockTickWindow]:
    by_key: dict[tuple[str, dt.date], list[tuple[int, int]]] = {}
    for window in windows:
        by_key.setdefault((window.ticker.upper(), window.day), []).append((window.start_ms, window.end_ms))

    merged: list[StockTickWindow] = []
    for (ticker, day), ranges in sorted(by_key.items()):
        for start_ms, end_ms in _merge_ms_ranges(ranges):
            merged.append(StockTickWindow(ticker, day, start_ms, end_ms))
    return merged


def normalize_stock_tick_frame(ticks: pl.DataFrame) -> pl.DataFrame:
    ticks = add_tick_time_columns(ticks)
    if ticks.is_empty():
        return ticks

    return (
        ticks.with_columns(
            pl.col("date").cast(pl.Date),
            pl.col("time").cast(pl.Time),
            pl.col("bid").cast(pl.Float64),
            pl.col("ask").cast(pl.Float64),
        )
        .select(["date", "time", "bid", "ask"])
        .sort(["date", "time"])
        .unique(subset=["date", "bid", "ask"], keep="first", maintain_order=True)
    )


def normalize_option_quote_frame(quotes: pl.DataFrame, window: OptionQuoteWindow) -> pl.DataFrame:
    if quotes.is_empty():
        return _empty_option_quote_frame()

    quotes = add_tick_time_columns(quotes)
    columns = set(quotes.columns)
    return (
        quotes.with_columns(
            pl.lit(window.ticker).alias("ticker"),
            pl.lit(window.day).cast(pl.Date).alias("date"),
            pl.lit(window.expiration).cast(pl.Date).alias("expiration"),
            pl.lit(window.strike).cast(pl.Float64).alias("strike"),
            pl.lit(window.right).alias("right"),
            pl.col("time").cast(pl.Time),
            _optional_col(columns, "bid_size", pl.Int32),
            _optional_col(columns, "bid_exchange", pl.Int16),
            _optional_col(columns, "bid", pl.Float64),
            _optional_col(columns, "bid_condition", pl.Int16),
            _optional_col(columns, "ask_size", pl.Int32),
            _optional_col(columns, "ask_exchange", pl.Int16),
            _optional_col(columns, "ask", pl.Float64),
            _optional_col(columns, "ask_condition", pl.Int16),
        )
        .select(OPTION_QUOTE_COLUMNS)
        .sort(["date", "expiration", "strike", "right", "time"])
        .unique(subset=["date", "expiration", "strike", "right", "time"], keep="last", maintain_order=True)
    )


def add_tick_time_columns(ticks: pl.DataFrame) -> pl.DataFrame:
    expressions = []
    has_timestamp = "timestamp" in ticks.columns
    has_time = "time" in ticks.columns
    timestamp = pl.col("timestamp").cast(pl.String).str.to_datetime(strict=False) if has_timestamp else None

    if "date" in ticks.columns:
        date_dtype = ticks.schema["date"]
        if date_dtype == pl.Date:
            expressions.append(pl.col("date").cast(pl.Date).alias("date"))
        elif date_dtype == pl.Datetime:
            expressions.append(pl.col("date").dt.date().alias("date"))
        else:
            date_text = pl.col("date").cast(pl.String)
            candidates = [
                date_text.str.strptime(pl.Date, "%Y-%m-%d", strict=False),
                date_text.str.strptime(pl.Date, "%Y%m%d", strict=False),
            ]
            if timestamp is not None:
                candidates.append(timestamp.dt.date())
            expressions.append(
                pl.coalesce(candidates).alias("date")
            )
    elif timestamp is not None:
        expressions.append(timestamp.dt.date().alias("date"))
    else:
        raise ValueError("Tick frame must contain timestamp or date column")

    if has_time:
        expressions.append(pl.col("time").cast(pl.Time).alias("time"))
    elif timestamp is not None:
        expressions.append(timestamp.dt.time().alias("time"))

    if "ms_of_day" in ticks.columns:
        expressions.append(pl.col("ms_of_day").cast(pl.Int32).alias("ms_of_day"))
    elif timestamp is not None:
        expressions.append(datetime_to_ms_expr(timestamp).alias("ms_of_day"))
    elif has_time:
        expressions.append(time_to_ms_expr().alias("ms_of_day"))
    else:
        raise ValueError("Tick frame must contain timestamp, time, or ms_of_day column")
    return ticks.with_columns(*expressions)


def _window_params(symbol: str, day: dt.date, start_ms: int, end_ms: int) -> dict[str, Any]:
    return {
        "symbol": symbol.upper(),
        "date": format_theta_date(day),
        "start_time": format_theta_time(start_ms),
        "end_time": format_theta_time(end_ms),
    }


def _client(settings: Settings, client: ThetaClient | None) -> ThetaClient:
    return client or ThetaClient(
        base_url=settings.theta_base_url,
        timeout_seconds=settings.request_timeout_seconds,
        max_retries=settings.max_retries,
        retry_sleep_seconds=settings.retry_sleep_seconds,
    )


def _drop_empty_response_rows(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty() or "timestamp" not in frame.columns:
        return frame
    return frame.drop_nulls("timestamp")


def _load_stock_tick_coverage(settings: Settings, ticker: str) -> pl.DataFrame:
    path = stock_tick_coverage_path(settings, ticker)
    schema = {"date": pl.Date, "start_ms": pl.Int32, "end_ms": pl.Int32}
    if not path.exists():
        return pl.DataFrame(schema=schema)

    return (
        pl.read_parquet(path)
        .with_columns(
            pl.col("date").cast(pl.Date),
            pl.col("start_ms").cast(pl.Int32),
            pl.col("end_ms").cast(pl.Int32),
        )
        .select(["date", "start_ms", "end_ms"])
    )


def _load_option_quote_coverage(settings: Settings, ticker: str, interval: str = "100ms") -> pl.DataFrame:
    path = option_quote_coverage_path(settings, ticker, interval=interval)
    schema = {
        "date": pl.Date,
        "expiration": pl.Date,
        "strike": pl.Float64,
        "right": pl.String,
        "start_ms": pl.Int32,
        "end_ms": pl.Int32,
    }
    if not path.exists():
        return pl.DataFrame(schema=schema)
    return (
        pl.read_parquet(path)
        .with_columns(
            pl.col("date").cast(pl.Date),
            pl.col("expiration").cast(pl.Date),
            pl.col("strike").cast(pl.Float64),
            _option_quote_right_expr().alias("right"),
            pl.col("start_ms").cast(pl.Int32),
            pl.col("end_ms").cast(pl.Int32),
        )
        .select(["date", "expiration", "strike", "right", "start_ms", "end_ms"])
    )


def _subtract_covered_ranges(start_ms: int, end_ms: int, covered: list[tuple[int, int]]) -> list[tuple[int, int]]:
    missing: list[tuple[int, int]] = []
    cursor = int(start_ms)
    end_ms = int(end_ms)
    for covered_start, covered_end in _merge_ms_ranges(covered):
        if covered_end < cursor:
            continue
        if covered_start > end_ms:
            break
        if covered_start > cursor:
            missing.append((cursor, min(covered_start - 1, end_ms)))
        cursor = max(cursor, covered_end + 1)
        if cursor > end_ms:
            break
    if cursor <= end_ms:
        missing.append((cursor, end_ms))
    return missing


def _merge_coverage_rows(rows: list[dict]) -> list[dict]:
    by_day: dict[dt.date, list[tuple[int, int]]] = {}
    for row in rows:
        day = row["date"]
        if isinstance(day, dt.datetime):
            day = day.date()
        by_day.setdefault(day, []).append((int(row["start_ms"]), int(row["end_ms"])))

    merged_rows: list[dict] = []
    for day in sorted(by_day):
        for start_ms, end_ms in _merge_ms_ranges(by_day[day]):
            merged_rows.append({"date": day, "start_ms": start_ms, "end_ms": end_ms})
    return merged_rows


def _merge_option_coverage_rows(rows: list[dict]) -> list[dict]:
    by_contract: dict[tuple[dt.date, dt.date, float, str], list[tuple[int, int]]] = {}
    for row in rows:
        key = (
            coerce_date(row["date"]),
            coerce_date(row["expiration"]),
            normalize_option_strike(row["strike"]),
            _option_right_code(row["right"]),
        )
        by_contract.setdefault(key, []).append((int(row["start_ms"]), int(row["end_ms"])))

    merged_rows: list[dict] = []
    for date, expiration, strike, right in sorted(by_contract):
        for start_ms, end_ms in _merge_ms_ranges(by_contract[(date, expiration, strike, right)]):
            merged_rows.append(
                {
                    "date": date,
                    "expiration": expiration,
                    "strike": strike,
                    "right": right,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                }
            )
    return merged_rows


def _merge_ms_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for start_ms, end_ms in sorted((int(start), int(end)) for start, end in ranges if start <= end):
        if not merged or start_ms > merged[-1][1] + 1:
            merged.append((start_ms, end_ms))
            continue
        merged[-1] = (merged[-1][0], max(merged[-1][1], end_ms))
    return merged


def _interval_key(interval: str) -> str:
    return str(interval).strip().lower().replace("/", "_").replace("\\", "_")


def _option_right_code(value: Any) -> str:
    normalized = normalize_option_right(value)
    return "C" if normalized == "call" else "P"


def _optional_col(columns: set[str], name: str, dtype: pl.DataType) -> pl.Expr:
    return (pl.col(name) if name in columns else pl.lit(None)).cast(dtype).alias(name)


def _stock_tick_date_expr(schema) -> pl.Expr:
    has_timestamp = schema.get("timestamp") is not None
    timestamp = pl.col("timestamp").cast(pl.String).str.to_datetime(strict=False) if has_timestamp else None
    date_dtype = schema.get("date")
    if date_dtype == pl.Date:
        return pl.col("date").cast(pl.Date)
    if date_dtype == pl.Datetime:
        return pl.col("date").dt.date()
    if date_dtype is not None:
        date_text = pl.col("date").cast(pl.String)
        candidates = [
            date_text.str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            date_text.str.strptime(pl.Date, "%Y%m%d", strict=False),
        ]
        if timestamp is not None:
            candidates.append(timestamp.dt.date())
        return pl.coalesce(candidates)
    if timestamp is None:
        raise ValueError("Tick parquet must contain date or timestamp column")
    return timestamp.dt.date()


def _option_quote_date_expr(schema, column: str) -> pl.Expr:
    dtype = schema.get(column)
    if dtype == pl.Date:
        return pl.col(column).cast(pl.Date)
    if dtype == pl.Datetime:
        return pl.col(column).dt.date()
    if dtype is not None:
        date_text = pl.col(column).cast(pl.String)
        return pl.coalesce(
            [
                date_text.str.strptime(pl.Date, "%Y-%m-%d", strict=False),
                date_text.str.strptime(pl.Date, "%Y%m%d", strict=False),
            ]
        )
    raise ValueError(f"Option quote parquet must contain {column} column")


def _stock_tick_ms_expr(schema) -> pl.Expr:
    if schema.get("ms_of_day") is not None:
        return pl.col("ms_of_day").cast(pl.Int32)
    if schema.get("time") is not None:
        return time_to_ms_expr()
    if schema.get("timestamp") is not None:
        timestamp = pl.col("timestamp").cast(pl.String).str.to_datetime(strict=False)
        return datetime_to_ms_expr(timestamp)
    raise ValueError("Tick parquet must contain ms_of_day, time, or timestamp column")


def _option_quote_ms_expr(schema) -> pl.Expr:
    if schema.get("ms_of_day") is not None:
        return pl.col("ms_of_day").cast(pl.Int32)
    if schema.get("time") is not None:
        return time_to_ms_expr()
    if schema.get("timestamp") is not None:
        timestamp = pl.col("timestamp").cast(pl.String).str.to_datetime(strict=False)
        return datetime_to_ms_expr(timestamp)
    raise ValueError("Option quote parquet must contain ms_of_day, time, or timestamp column")


def _stock_tick_time_expr(schema) -> pl.Expr:
    if schema.get("time") is not None:
        return pl.col("time").cast(pl.Time)
    if schema.get("timestamp") is not None:
        return pl.col("timestamp").cast(pl.String).str.to_datetime(strict=False).dt.time()
    raise ValueError("Tick parquet must contain time or timestamp column")


def _option_quote_time_expr(schema) -> pl.Expr:
    if schema.get("time") is not None:
        return pl.col("time").cast(pl.Time)
    if schema.get("timestamp") is not None:
        return pl.col("timestamp").cast(pl.String).str.to_datetime(strict=False).dt.time()
    raise ValueError("Option quote parquet must contain time or timestamp column")


def _option_quote_right_expr() -> pl.Expr:
    return pl.col("right").cast(pl.String).str.to_uppercase().str.slice(0, 1)


def _scan_compact_stock_tick_file(path: Path) -> pl.LazyFrame:
    lf = pl.scan_parquet(path)
    schema = lf.collect_schema()
    return (
        lf.with_columns(
            _stock_tick_date_expr(schema).alias("date"),
            _stock_tick_time_expr(schema).alias("time"),
            pl.col("bid").cast(pl.Float64),
            pl.col("ask").cast(pl.Float64),
        )
        .select(["date", "time", "bid", "ask"])
        .sort(["date", "time"])
        .unique(subset=["date", "bid", "ask"], keep="first", maintain_order=True)
    )


def _scan_option_quote_file(path: Path) -> pl.LazyFrame:
    lf = pl.scan_parquet(path)
    schema = lf.collect_schema()
    return (
        lf.with_columns(
            pl.col("ticker").cast(pl.String),
            _option_quote_date_expr(schema, "date").alias("date"),
            _option_quote_date_expr(schema, "expiration").alias("expiration"),
            pl.col("strike").cast(pl.Float64),
            _option_quote_right_expr().alias("right"),
            _option_quote_time_expr(schema).alias("time"),
            pl.col("bid_size").cast(pl.Int32),
            pl.col("bid_exchange").cast(pl.Int16),
            pl.col("bid").cast(pl.Float64),
            pl.col("bid_condition").cast(pl.Int16),
            pl.col("ask_size").cast(pl.Int32),
            pl.col("ask_exchange").cast(pl.Int16),
            pl.col("ask").cast(pl.Float64),
            pl.col("ask_condition").cast(pl.Int16),
        )
        .select(OPTION_QUOTE_COLUMNS)
        .sort(["date", "expiration", "strike", "right", "time"])
        .unique(subset=["date", "expiration", "strike", "right", "time"], keep="last", maintain_order=True)
    )


def _empty_stock_tick_frame() -> pl.DataFrame:
    return pl.DataFrame(schema={"date": pl.Date, "time": pl.Time, "bid": pl.Float64, "ask": pl.Float64})


def _empty_option_quote_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "ticker": pl.String,
            "date": pl.Date,
            "expiration": pl.Date,
            "strike": pl.Float64,
            "right": pl.String,
            "time": pl.Time,
            "bid_size": pl.Int32,
            "bid_exchange": pl.Int16,
            "bid": pl.Float64,
            "bid_condition": pl.Int16,
            "ask_size": pl.Int32,
            "ask_exchange": pl.Int16,
            "ask": pl.Float64,
            "ask_condition": pl.Int16,
        }
    )
