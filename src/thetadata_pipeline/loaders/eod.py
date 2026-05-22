from __future__ import annotations

import datetime as dt
import gc
import time
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from ..constants import EOD_FINAL_COLUMNS, EOD_FLOAT32_COLUMNS
from ..dates import business_days, format_theta_date, parse_date, previous_business_day
from ..greeks import add_risk_free_rate, calculate_iv_greeks
from ..rates import ensure_local_dgs1_history
from ..settings import Settings
from ..theta_client import ThetaClient, ThetaDataError, ThetaNoData
from ..time_utils import datetime_to_ms_expr
from ..tiingo import load_tiingo_dividend_ttm


class TiingoDataCoverageError(RuntimeError):
    pass


@dataclass(frozen=True)
class EodUpdateSummary:
    symbol: str
    final_path: Path
    candidates: int
    requests: int
    downloaded_days: int
    skipped_days: int
    rows_added: int
    start_date: dt.date | None
    end_date: dt.date | None
    dry_run: bool = False


def update_eod_with_greeks(
    settings: Settings,
    symbol: str = "SPY",
    from_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
    max_days: int | None = None,
    max_dte: int | None = None,
    strike_range: int | None = None,
    batch_mode: str = "monthly",
    dry_run: bool = False,
) -> EodUpdateSummary:
    symbol = symbol.upper()
    options_dir = settings.eod_final_path(symbol).parent
    if not dry_run:
        _migrate_legacy_eod_options_file(options_dir, symbol)
    existing_option_dates = _read_existing_option_eod_dates(options_dir, symbol)
    existing_stock_dates = _read_existing_stock_eod_dates(settings.stock_eod_dir, symbol)
    client = ThetaClient(
        base_url=settings.theta_base_url,
        timeout_seconds=settings.request_timeout_seconds,
        max_retries=settings.max_retries,
        retry_sleep_seconds=settings.retry_sleep_seconds,
    )

    parsed_from = parse_date(from_date)
    parsed_end = parse_date(end_date) or previous_business_day()
    if parsed_from is None:
        max_option = max(existing_option_dates) if existing_option_dates else None
        max_stock = max(existing_stock_dates) if existing_stock_dates else None
        if max_option and max_stock:
            max_existing = min(max_option, max_stock)
        else:
            max_existing = max_option or max_stock
        parsed_from = (max_existing + dt.timedelta(days=1)) if max_existing else dt.date(2016, 1, 1)

    trading_days = _fetch_trading_days(
        client,
        symbol,
        parsed_from,
        parsed_end,
        allow_business_day_fallback=False,
    )
    option_missing_set = {day for day in trading_days if day not in existing_option_dates}
    stock_missing_set = {day for day in trading_days if day not in existing_stock_dates}
    candidates = sorted(option_missing_set | stock_missing_set)
    if max_days is not None:
        candidates = candidates[:max_days]
        limited_set = set(candidates)
        option_missing_set = option_missing_set & limited_set
        stock_missing_set = stock_missing_set & limited_set
    year_groups = _group_days_by_year(candidates)
    requests = sum(len(build_batch_ranges(days, batch_mode=batch_mode)) for days in year_groups.values())
    preview_paths = _preview_year_output_paths(options_dir, symbol, "_etf_greeks.parquet", year_groups)

    print(f"{symbol}. Candidate trading days: {len(candidates)}")
    print(f"{symbol}. Missing option EOD days: {len(option_missing_set)}")
    print(f"{symbol}. Missing stock EOD days: {len(stock_missing_set)}")
    if candidates:
        print(f"{symbol}. Date range: {candidates[0]}..{candidates[-1]}")
    print(f"{symbol}. EOD request batches: {requests} ({batch_mode})")
    if preview_paths:
        print(f"{symbol}. EOD option outputs:")
        for path in preview_paths:
            print(path)

    if dry_run:
        if option_missing_set:
            _load_dividend_curve_for_dates(
                symbol=symbol,
                settings=settings,
                expected_days=sorted(option_missing_set),
            )
        return EodUpdateSummary(
            symbol=symbol,
            final_path=preview_paths[-1] if preview_paths else (_latest_year_file(options_dir, symbol, "_etf_greeks.parquet") or settings.eod_final_path(symbol)),
            candidates=len(candidates),
            requests=requests,
            downloaded_days=0,
            skipped_days=0,
            rows_added=0,
            start_date=candidates[0] if candidates else None,
            end_date=candidates[-1] if candidates else None,
            dry_run=True,
        )

    if not candidates:
        return EodUpdateSummary(
            symbol=symbol,
            final_path=_latest_year_file(options_dir, symbol, "_etf_greeks.parquet") or settings.eod_final_path(symbol),
            candidates=0,
            requests=0,
            downloaded_days=0,
            skipped_days=0,
            rows_added=0,
            start_date=None,
            end_date=None,
        )

    settings.staging_eod_dir.mkdir(parents=True, exist_ok=True)
    options_dir.mkdir(parents=True, exist_ok=True)
    settings.stock_eod_dir.mkdir(parents=True, exist_ok=True)
    if option_missing_set:
        dividend_curve = _load_dividend_curve_for_dates(
            symbol=symbol,
            settings=settings,
            expected_days=sorted(option_missing_set),
        )
        risk_free_curve = _load_risk_free_curve_for_dates(
            settings=settings,
            expected_days=sorted(option_missing_set),
        )
    else:
        dividend_curve = pl.DataFrame({"date": [], "div_ttm": []}, schema={"date": pl.Date, "div_ttm": pl.Float64})
        risk_free_curve = pl.DataFrame({"date": [], "risk_free_rate": []}, schema={"date": pl.Date, "risk_free_rate": pl.Float64})

    downloaded_days = 0
    skipped_days = 0
    rows_added = 0
    last_output_path: Path | None = None

    for year in sorted(year_groups):
        year_days = year_groups[year]
        option_year_paths = _find_year_files(options_dir, symbol, "_etf_greeks.parquet", year)
        stock_year_paths = _find_year_files(settings.stock_eod_dir, symbol, "_eod_stock.parquet", year)
        batch_ranges = build_batch_ranges(year_days, batch_mode=batch_mode)
        for start_day, end_day, range_days in batch_ranges:
            print(f"{symbol}. Processing {start_day}..{end_day} ({len(range_days)} trading days)")
            keep_dates = set(range_days)
            option_keep_dates = keep_dates & option_missing_set
            stock_keep_dates = keep_dates & stock_missing_set
            try:
                stock_closes = _fetch_stock_closes(client, symbol, start_day, end_day)
            except ThetaNoData:
                print(f"{symbol}. No stock EOD data for {start_day}..{end_day}; skipped")
                skipped_days += len(range_days)
                continue

            stock_rows = (
                stock_closes.filter(pl.col("date").is_in(stock_keep_dates))
                .with_columns(pl.lit(symbol).alias("ticker"))
            )
            if not stock_rows.is_empty():
                stock_output_path = _merge_stock_eod_year_file(
                    directory=settings.stock_eod_dir,
                    symbol=symbol,
                    year=year,
                    staged=stock_rows,
                    existing_paths=stock_year_paths,
                )
                stock_year_paths = [stock_output_path]
                print(f"{symbol}. Stock EOD merged: {stock_output_path}")

            if option_keep_dates:
                try:
                    batch_df = fetch_eod_range_with_greeks(
                        client=client,
                        settings=settings,
                        symbol=symbol,
                        start_day=start_day,
                        end_day=end_day,
                        keep_dates=option_keep_dates,
                        max_dte=max_dte,
                        strike_range=strike_range,
                        dividend_curve=dividend_curve,
                        risk_free_curve=risk_free_curve,
                        stock_closes=stock_closes,
                    )
                except ThetaNoData:
                    print(f"{symbol}. No option EOD data for {start_day}..{end_day}; skipped")
                    skipped_days += len(option_keep_dates)
                    continue

                if batch_df.is_empty():
                    print(f"{symbol}. Empty normalized batch for {start_day}..{end_day}; skipped")
                    skipped_days += len(option_keep_dates)
                    continue

                print(f"{symbol}. Batch rows with greeks: {len(batch_df)}")
                unique_dates = batch_df.select(pl.col("date").n_unique()).item()
                downloaded_days += unique_dates
                skipped_days += max(0, len(option_keep_dates) - unique_dates)
                rows_added += len(batch_df)
                option_output_path = _merge_option_eod_year_file(
                    directory=options_dir,
                    symbol=symbol,
                    year=year,
                    staged=batch_df,
                    existing_paths=option_year_paths,
                )
                option_year_paths = [option_output_path]
                last_output_path = option_output_path
                print(f"{symbol}. Option EOD merged: {option_output_path}")
                del batch_df
                gc.collect()

    return EodUpdateSummary(
        symbol=symbol,
        final_path=last_output_path or _latest_year_file(options_dir, symbol, "_etf_greeks.parquet") or settings.eod_final_path(symbol),
        candidates=len(candidates),
        requests=requests,
        downloaded_days=downloaded_days,
        skipped_days=skipped_days,
        rows_added=rows_added,
        start_date=candidates[0] if candidates else None,
        end_date=candidates[-1] if candidates else None,
    )


def fetch_eod_day_with_greeks(
    client: ThetaClient,
    settings: Settings,
    symbol: str,
    day: dt.date,
    max_dte: int | None = None,
    strike_range: int | None = None,
) -> pl.DataFrame:
    dividend_curve = _load_dividend_curve_for_dates(symbol, settings, [day])
    risk_free_curve = _load_risk_free_curve_for_dates(settings, [day])
    return fetch_eod_range_with_greeks(
        client=client,
        settings=settings,
        symbol=symbol,
        start_day=day,
        end_day=day,
        keep_dates={day},
        max_dte=max_dte,
        strike_range=strike_range,
        dividend_curve=dividend_curve,
        risk_free_curve=risk_free_curve,
    )


def fetch_eod_range_with_greeks(
    client: ThetaClient,
    settings: Settings,
    symbol: str,
    start_day: dt.date,
    end_day: dt.date,
    keep_dates: set[dt.date],
    max_dte: int | None = None,
    strike_range: int | None = None,
    dividend_curve: pl.DataFrame | None = None,
    risk_free_curve: pl.DataFrame | None = None,
    stock_closes: pl.DataFrame | None = None,
) -> pl.DataFrame:
    if stock_closes is None:
        print(f"{symbol}. Waiting for stock EOD {start_day}..{end_day}")
        stock_closes = _fetch_stock_closes(client, symbol, start_day, end_day)

    print(f"{symbol}. Waiting for option EOD {start_day}..{end_day}")
    start_time = time.time()
    raw_options = _fetch_option_eod(client, symbol, start_day, end_day, max_dte=max_dte, strike_range=strike_range)
    print(f"{symbol}. Option EOD fetch took {time.time() - start_time:.2f}s")
    print(f"{symbol}. Normalizing option EOD")
    normalized = normalize_option_eod(raw_options, symbol=symbol).filter(pl.col("date").is_in(keep_dates))
    del raw_options
    normalized = normalized.join(stock_closes, how="inner", on="date")
    del stock_closes

    print(f"{symbol}. Adding dividend yield")
    normalized = _attach_dividend_yield(normalized, dividend_curve)
    print(f"{symbol}. Adding risk-free rate")
    normalized = add_risk_free_rate(
        normalized,
        risk_free_curve,
    )
    print(f"{symbol}. Calculating IV and greeks")
    with_greeks = calculate_iv_greeks(normalized)
    del normalized
    result = align_final_schema(with_greeks)
    del with_greeks
    gc.collect()

    return result


def normalize_option_eod(df: pl.DataFrame, symbol: str) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(schema={column: pl.Null for column in EOD_FINAL_COLUMNS})

    parsed_created = pl.col("created").cast(pl.String).str.to_datetime(strict=False)
    parsed_last_trade = pl.col("last_trade").cast(pl.String).str.to_datetime(strict=False)
    result = (
        df.with_columns(
            pl.col("expiration").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            pl.col("right").str.to_lowercase().str.slice(0, 1),
            parsed_created.alias("_created_dt"),
            parsed_last_trade.alias("_last_trade_dt"),
            pl.lit(symbol.upper()).alias("ticker"),
            pl.lit(1.0).cast(pl.Float32).alias("splitFactor"),
        )
        .with_columns(pl.coalesce(["_created_dt", "_last_trade_dt"]).dt.date().alias("date"))
        .filter(pl.col("date").is_not_null())
        .filter(pl.col("expiration") >= pl.col("date"))
        .with_columns(
            datetime_to_ms_expr("_last_trade_dt").fill_null(0).cast(pl.Int64).alias("ms_of_day2"),
            (pl.col("expiration") - pl.col("date")).dt.total_days().truediv(365).cast(pl.Float32).alias("timeToExp"),
        )
        .with_columns(
            pl.when(pl.col("timeToExp") <= 0).then(1e-3).otherwise(pl.col("timeToExp")).alias("timeToExp")
        )
        .select(
            "expiration",
            pl.col("strike").cast(pl.Float64),
            "right",
            "ms_of_day2",
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.Int64),
            pl.col("count").cast(pl.Int64),
            pl.col("bid_size").cast(pl.Int64),
            pl.col("bid").cast(pl.Float64),
            pl.col("ask_size").cast(pl.Int64),
            pl.col("ask").cast(pl.Float64),
            "date",
            "ticker",
            "timeToExp",
            "splitFactor",
        )
    )
    return result


def align_final_schema(df: pl.DataFrame) -> pl.DataFrame:
    result = df
    for column in EOD_FINAL_COLUMNS:
        if column not in result.columns:
            result = result.with_columns(pl.lit(None).alias(column))

    result = result.with_columns(
        pl.col("expiration").cast(pl.Date),
        pl.col("strike").cast(pl.Float64),
        pl.col("right").cast(pl.String),
        pl.col("ms_of_day2").cast(pl.Int64),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Int64),
        pl.col("count").cast(pl.Int64),
        pl.col("bid_size").cast(pl.Int64),
        pl.col("bid").cast(pl.Float64),
        pl.col("ask_size").cast(pl.Int64),
        pl.col("ask").cast(pl.Float64),
        pl.col("date").cast(pl.Date),
        pl.col("ticker").cast(pl.String),
        *[pl.col(column).cast(pl.Float32) for column in EOD_FLOAT32_COLUMNS],
    )
    return result.select(EOD_FINAL_COLUMNS)


def _fetch_stock_closes(client: ThetaClient, symbol: str, start_day: dt.date, end_day: dt.date) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for chunk_start, chunk_end in _iter_date_chunks(start_day, end_day, max_days=365):
        frame = client.get_csv_frame(
            "/stock/history/eod",
            {"symbol": symbol, "start_date": format_theta_date(chunk_start), "end_date": format_theta_date(chunk_end)},
        )
        if not frame.is_empty():
            frames.append(frame)

    if not frames:
        raise ThetaNoData(f"No stock EOD for {symbol} {start_day}..{end_day}")

    frame = pl.concat(frames, how="vertical_relaxed")

    return (
        frame.with_columns(
            pl.coalesce(
                [
                    pl.col("created").cast(pl.String).str.to_datetime(strict=False),
                    pl.col("last_trade").cast(pl.String).str.to_datetime(strict=False),
                ]
            )
            .dt.date()
            .alias("date")
        )
        .filter(pl.col("date").is_not_null())
        .select("date", pl.col("close").cast(pl.Float32).alias("base_close"))
        .unique(subset=["date"], keep="last")
        .sort("date")
    )


def _fetch_option_eod(
    client: ThetaClient,
    symbol: str,
    start_day: dt.date,
    end_day: dt.date,
    max_dte: int | None = None,
    strike_range: int | None = None,
) -> pl.DataFrame:
    params: dict[str, str | int] = {
        "symbol": symbol,
        "expiration": "*",
        "start_date": format_theta_date(start_day),
        "end_date": format_theta_date(end_day),
    }
    if max_dte is not None:
        params["max_dte"] = max_dte
    if strike_range is not None:
        params["strike_range"] = strike_range
    return client.get_csv_frame("/option/history/eod", params)


def _attach_dividend_yield(df: pl.DataFrame, dividend_curve: pl.DataFrame | None) -> pl.DataFrame:
    if df.is_empty():
        return df

    joined = df.join(dividend_curve, how="left", on="date")
    result = (
        joined.with_columns((pl.col("div_ttm") / pl.col("base_close")).cast(pl.Float64).alias("dividend_yield"))
        .drop("div_ttm")
    )
    if result["dividend_yield"].null_count() > 0:
        first_date = result.filter(pl.col("dividend_yield").is_null())["date"].min()
        raise ValueError(f"{first_date}: dividend_yield is missing after Tiingo join")
    return result


def _load_dividend_curve_for_dates(
    symbol: str,
    settings: Settings,
    expected_days: list[dt.date],
) -> pl.DataFrame:
    expected_days = sorted(set(expected_days))
    if not expected_days:
        return load_tiingo_dividend_ttm(symbol, settings)

    start_day = expected_days[0]
    end_day = expected_days[-1]
    print(f"{symbol}. Checking local Tiingo dividend data for {start_day}..{end_day}")
    dividend_curve = load_tiingo_dividend_ttm(symbol, settings)
    _print_dividend_curve_status(symbol, dividend_curve)
    if dividend_curve.is_empty():
        raise TiingoDataCoverageError(
            f"{symbol}. Tiingo dividend data not found. Set THETA_TIINGO_USA_ROOT or update Tiingo manually."
        )

    tiingo_days = set(
        dividend_curve.filter(pl.col("date").is_between(start_day, end_day))["date"].to_list()
    )
    expected_set = set(expected_days)
    missing_days = sorted(expected_set - tiingo_days)
    print(f"{symbol}. Tiingo matching dates: {len(expected_set) - len(missing_days)}/{len(expected_set)}")
    if not missing_days:
        return dividend_curve

    raise TiingoDataCoverageError(
        f"{symbol}. Tiingo dates do not cover requested EOD dates {start_day}..{end_day}. "
        f"Missing {len(missing_days)} date(s); first missing: {_format_sample_dates(missing_days)}. "
        f"Update Tiingo manually and rerun."
    )


def _load_risk_free_curve_for_dates(
    settings: Settings,
    expected_days: list[dt.date],
) -> pl.DataFrame:
    expected_days = sorted(set(expected_days))
    if not expected_days:
        return pl.DataFrame({"date": [], "risk_free_rate": []}, schema={"date": pl.Date, "risk_free_rate": pl.Float64})

    start_day = expected_days[0]
    end_day = expected_days[-1]
    print(f"DGS1. Preparing local history for {start_day}..{end_day}")
    curve = ensure_local_dgs1_history(settings, required_start=start_day, required_end=end_day)
    if curve.is_empty():
        raise ValueError("DGS1 local history is empty")

    matched = curve.filter(pl.col("date").is_between(start_day, end_day)).height
    print(f"DGS1. Local rows in range: {matched}")
    return curve


def _print_dividend_curve_status(symbol: str, dividend_curve: pl.DataFrame | None) -> None:
    bounds = _dividend_curve_bounds(dividend_curve)
    if bounds is None:
        print(f"{symbol}. Tiingo dividend data: no local rows found")
        return
    start_day, end_day, rows = bounds
    print(f"{symbol}. Tiingo dividend data: rows={rows}, range={start_day}..{end_day}")


def _dividend_curve_bounds(dividend_curve: pl.DataFrame | None) -> tuple[dt.date, dt.date, int] | None:
    if dividend_curve is None or dividend_curve.is_empty():
        return None

    row = dividend_curve.select(
        pl.col("date").min().alias("start_day"),
        pl.col("date").max().alias("end_day"),
        pl.len().alias("rows"),
    ).row(0, named=True)
    start_day = row["start_day"]
    end_day = row["end_day"]
    if start_day is None or end_day is None:
        return None
    return start_day, end_day, int(row["rows"])


def _format_sample_dates(days: list[dt.date], limit: int = 10) -> str:
    sample = ", ".join(day.isoformat() for day in days[:limit])
    if len(days) > limit:
        return f"{sample}, ..."
    return sample


def _fetch_trading_days(
    client: ThetaClient,
    symbol: str,
    start_day: dt.date,
    end_day: dt.date,
    allow_business_day_fallback: bool = True,
) -> list[dt.date]:
    if start_day > end_day:
        return []

    try:
        closes = _fetch_stock_closes(client, symbol, start_day, end_day)
    except (ThetaNoData, ThetaDataError):
        if not allow_business_day_fallback:
            raise
        return list(business_days(start_day, end_day))

    days = [day for day in closes["date"].to_list() if day is not None]
    if not days:
        if not allow_business_day_fallback:
            raise ThetaNoData(f"No stock EOD trading days for {symbol} {start_day}..{end_day}")
        return list(business_days(start_day, end_day))
    return sorted(days)


@dataclass(frozen=True)
class EodBenchmarkResult:
    batch_mode: str
    requests: int
    seconds: float
    rows: int
    megabytes: float


def benchmark_eod_download(
    settings: Settings,
    symbol: str,
    start_date: str | dt.date,
    end_date: str | dt.date,
    batch_modes: list[str],
    max_dte: int | None = None,
    strike_range: int | None = None,
) -> list[EodBenchmarkResult]:
    start = parse_date(start_date)
    end = parse_date(end_date)
    if start is None or end is None:
        raise ValueError("start_date and end_date are required for benchmark")

    client = ThetaClient(
        base_url=settings.theta_base_url,
        timeout_seconds=settings.request_timeout_seconds,
        max_retries=settings.max_retries,
        retry_sleep_seconds=settings.retry_sleep_seconds,
    )
    days = _fetch_trading_days(client, symbol.upper(), start, end)
    if not days:
        days = list(business_days(start, end))
    results: list[EodBenchmarkResult] = []

    for mode in batch_modes:
        ranges = build_batch_ranges(days, batch_mode=mode)
        total_rows = 0
        total_bytes = 0
        started = time.perf_counter()
        for range_start, range_end, _ in ranges:
            params: dict[str, str | int] = {
                "symbol": symbol.upper(),
                "expiration": "*",
                "start_date": format_theta_date(range_start),
                "end_date": format_theta_date(range_end),
            }
            if max_dte is not None:
                params["max_dte"] = max_dte
            if strike_range is not None:
                params["strike_range"] = strike_range
            try:
                csv_path = client.get_csv_file("/option/history/eod", params)
            except ThetaNoData:
                continue
            try:
                total_rows += max(0, _count_file_lines(csv_path) - 1)
                total_bytes += csv_path.stat().st_size
            finally:
                csv_path.unlink(missing_ok=True)
        elapsed = time.perf_counter() - started
        results.append(
            EodBenchmarkResult(
                batch_mode=mode,
                requests=len(ranges),
                seconds=elapsed,
                rows=total_rows,
                megabytes=total_bytes / 1024 / 1024,
            )
        )
    return results


def build_batch_ranges(days: list[dt.date], batch_mode: str) -> list[tuple[dt.date, dt.date, list[dt.date]]]:
    if not days:
        return []

    mode = batch_mode.lower()
    if mode == "daily":
        return [(day, day, [day]) for day in days]
    if mode == "all":
        return [(min(days), max(days), days)]

    grouped: dict[tuple[int, ...], list[dt.date]] = {}
    for day in days:
        if mode == "weekly":
            iso = day.isocalendar()
            key = (iso.year, iso.week)
        elif mode == "monthly":
            key = (day.year, day.month)
        elif mode == "yearly":
            key = (day.year,)
        else:
            raise ValueError(f"Unsupported EOD batch_mode: {batch_mode}")
        grouped.setdefault(key, []).append(day)

    return [(min(group_days), max(group_days), group_days) for _, group_days in sorted(grouped.items())]


def _migrate_legacy_eod_options_file(directory: Path, symbol: str) -> None:
    legacy_path = (directory / f"{symbol}_etf_greeks.parquet").resolve()
    if not legacy_path.exists():
        return

    print(f"{symbol}. Migrating legacy EOD file to yearly layout: {legacy_path}")
    lf = pl.scan_parquet(legacy_path).with_columns(pl.col("date").cast(pl.Date))
    years = (
        lf.select(pl.col("date").dt.year().alias("year"))
        .filter(pl.col("year").is_not_null())
        .unique()
        .sort("year")
        .collect()
    )
    for row in years.iter_rows(named=True):
        year = int(row["year"])
        part = lf.filter(pl.col("date").dt.year() == year).collect()
        if part.is_empty():
            continue
        existing_paths = _find_year_files(directory, symbol, "_etf_greeks.parquet", year)
        out_path = _merge_option_eod_year_file(
            directory=directory,
            symbol=symbol,
            year=year,
            staged=part,
            existing_paths=existing_paths,
        )
        print(f"{symbol}. Legacy year {year} merged into {out_path}")

    legacy_path.unlink(missing_ok=True)


def _read_existing_option_eod_dates(directory: Path, symbol: str) -> set[dt.date]:
    paths = _all_year_files(directory, symbol, "_etf_greeks.parquet")
    legacy_path = (directory / f"{symbol}_etf_greeks.parquet").resolve()
    if legacy_path.exists():
        paths.append(legacy_path)
    paths = sorted(set(paths))
    if not paths:
        return set()
    rows = (
        pl.scan_parquet([str(path) for path in paths])
        .select(pl.col("date").cast(pl.Date).unique())
        .collect()
    )
    return set(rows["date"].to_list())


def _read_existing_stock_eod_dates(directory: Path, symbol: str) -> set[dt.date]:
    paths = _all_year_files(directory, symbol, "_eod_stock.parquet")
    if not paths:
        return set()
    rows = (
        pl.scan_parquet([str(path) for path in paths])
        .select(pl.col("date").cast(pl.Date).unique())
        .collect()
    )
    return set(rows["date"].to_list())


def _group_days_by_year(days: list[dt.date]) -> dict[int, list[dt.date]]:
    groups: dict[int, list[dt.date]] = {}
    for day in sorted(set(days)):
        groups.setdefault(day.year, []).append(day)
    return groups


def _all_year_files(directory: Path, symbol: str, suffix: str) -> list[Path]:
    pattern = f"*_{symbol}{suffix}"
    files = [path.resolve() for path in directory.glob(pattern) if path.is_file()]
    files.sort()
    return files


def _find_year_files(directory: Path, symbol: str, suffix: str, year: int) -> list[Path]:
    matched: list[tuple[dt.date, dt.date, Path]] = []
    for path in _all_year_files(directory, symbol, suffix):
        bounds = _parse_named_bounds(path, symbol, suffix)
        if bounds is None:
            continue
        start_day, end_day = bounds
        if start_day.year <= year <= end_day.year:
            matched.append((start_day, end_day, path))
    matched.sort(key=lambda item: (item[0], item[1], item[2].name))
    return [path for _, _, path in matched]


def _latest_year_file(directory: Path, symbol: str, suffix: str) -> Path | None:
    files = _all_year_files(directory, symbol, suffix)
    if not files:
        return None
    return files[-1]


def _preview_year_output_paths(
    directory: Path,
    symbol: str,
    suffix: str,
    year_groups: dict[int, list[dt.date]],
) -> list[Path]:
    outputs: list[Path] = []
    for year in sorted(year_groups):
        outputs.append(_build_year_output_path(directory, symbol, suffix, year_groups[year], _find_year_files(directory, symbol, suffix, year)))
    return outputs


def _build_year_output_path(
    directory: Path,
    symbol: str,
    suffix: str,
    days: list[dt.date],
    existing_paths: list[Path],
) -> Path:
    if not days and not existing_paths:
        raise ValueError(f"{symbol}. cannot build yearly output path for empty inputs: {suffix}")
    min_day = min(days) if days else dt.date.max
    max_day = max(days) if days else dt.date.min
    for path in existing_paths:
        bounds = _parse_named_bounds(path, symbol, suffix)
        if bounds is None:
            continue
        start_day, end_day = bounds
        min_day = min(min_day, start_day)
        max_day = max(max_day, end_day)
    if min_day == dt.date.max or max_day == dt.date.min:
        raise ValueError(f"{symbol}. could not determine yearly output bounds for {suffix}")
    return (directory / f"{format_theta_date(min_day)}_{format_theta_date(max_day)}_{symbol}{suffix}").resolve()


def _parse_named_bounds(path: Path, symbol: str, suffix: str) -> tuple[dt.date, dt.date] | None:
    tail = f"_{symbol}{suffix}"
    name = path.name
    if not name.endswith(tail):
        return None
    prefix = name[: -len(tail)]
    if len(prefix) < 17 or prefix[8] != "_":
        return None
    start_raw = prefix[:8]
    end_raw = prefix[9:17]
    if not (start_raw.isdigit() and end_raw.isdigit()):
        return None
    start_day = parse_date(start_raw)
    end_day = parse_date(end_raw)
    if start_day is None or end_day is None:
        return None
    return start_day, end_day


def _unlink_all(paths: list[Path], keep: Path | None = None) -> None:
    keep_resolved = keep.resolve() if keep is not None else None
    for path in paths:
        try:
            resolved = path.resolve()
        except OSError:
            resolved = path
        if keep_resolved is not None and resolved == keep_resolved:
            continue
        path.unlink(missing_ok=True)


def _merge_option_eod_year_file(
    directory: Path,
    symbol: str,
    year: int,
    staged: pl.DataFrame,
    existing_paths: list[Path],
) -> Path:
    bounds = staged.select(pl.col("date").min().alias("start_day"), pl.col("date").max().alias("end_day")).row(0, named=True)
    start_day = bounds["start_day"]
    end_day = bounds["end_day"]
    if start_day is None or end_day is None:
        raise ValueError(f"{symbol}. empty staged option EOD rows for year {year}")
    target_path = _build_year_output_path(
        directory,
        symbol,
        "_etf_greeks.parquet",
        [start_day, end_day],
        existing_paths,
    )
    inputs = [path for path in existing_paths if path.exists()]
    if inputs:
        combined = pl.concat([pl.scan_parquet([str(path) for path in inputs]), staged.lazy()], how="vertical_relaxed")
    else:
        combined = staged.lazy()
    merged = (
        combined.with_columns(
            pl.col("date").cast(pl.Date),
            pl.col("expiration").cast(pl.Date),
            pl.col("right").cast(pl.String).str.to_lowercase().str.slice(0, 1),
        )
        .filter(pl.col("date").is_not_null())
        .filter(pl.col("expiration").is_not_null())
        .unique(subset=["date", "expiration", "strike", "right", "ticker"], keep="last")
        .sort(["ticker", "date", "expiration", "strike", "right"])
        .with_columns(
            pl.col("expiration").cast(pl.Date),
            pl.col("strike").cast(pl.Float64),
            pl.col("right").cast(pl.String),
            pl.col("ms_of_day2").cast(pl.Int64),
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.Int64),
            pl.col("count").cast(pl.Int64),
            pl.col("bid_size").cast(pl.Int64),
            pl.col("bid").cast(pl.Float64),
            pl.col("ask_size").cast(pl.Int64),
            pl.col("ask").cast(pl.Float64),
            pl.col("date").cast(pl.Date),
            pl.col("ticker").cast(pl.String),
            *[pl.col(column).cast(pl.Float32) for column in EOD_FLOAT32_COLUMNS],
        )
        .select(EOD_FINAL_COLUMNS)
    )
    final_bounds = merged.select(pl.col("date").min().alias("start_day"), pl.col("date").max().alias("end_day")).collect().row(0, named=True)
    start_day = final_bounds["start_day"]
    end_day = final_bounds["end_day"]
    if start_day is None or end_day is None:
        raise ValueError(f"{symbol}. empty option EOD merge for year {year}")
    final_path = (
        directory
        / f"{format_theta_date(start_day)}_{format_theta_date(end_day)}_{symbol}_etf_greeks.parquet"
    ).resolve()
    tmp_path = final_path.with_name(f"{final_path.stem}.tmp{final_path.suffix}")
    if tmp_path.exists():
        tmp_path.unlink()
    merged.sink_parquet(tmp_path)
    tmp_path.replace(final_path)
    _unlink_all(existing_paths, keep=final_path)
    if target_path.exists() and target_path.resolve() != final_path.resolve():
        target_path.unlink(missing_ok=True)
    return final_path


def _merge_stock_eod_year_file(
    directory: Path,
    symbol: str,
    year: int,
    staged: pl.DataFrame,
    existing_paths: list[Path],
) -> Path:
    bounds = staged.select(pl.col("date").min().alias("start_day"), pl.col("date").max().alias("end_day")).row(0, named=True)
    start_day = bounds["start_day"]
    end_day = bounds["end_day"]
    if start_day is None or end_day is None:
        raise ValueError(f"{symbol}. empty staged stock EOD rows for year {year}")
    target_path = _build_year_output_path(
        directory,
        symbol,
        "_eod_stock.parquet",
        [start_day, end_day],
        existing_paths,
    )
    staged_lf = (
        staged.lazy()
        .select(
            pl.col("ticker").cast(pl.String).str.to_uppercase().alias("ticker"),
            pl.col("date").cast(pl.Date).alias("date"),
            pl.col("base_close").cast(pl.Float32).alias("base_close"),
        )
    )
    inputs = [path for path in existing_paths if path.exists()]
    if inputs:
        existing_lf = (
            pl.scan_parquet([str(path) for path in inputs])
            .select(
                pl.col("ticker").cast(pl.String).str.to_uppercase().alias("ticker"),
                pl.col("date").cast(pl.Date).alias("date"),
                pl.col("base_close").cast(pl.Float32).alias("base_close"),
            )
        )
        combined = pl.concat([existing_lf, staged_lf], how="vertical_relaxed")
    else:
        combined = staged_lf
    merged = (
        combined.filter(pl.col("date").is_not_null())
        .unique(subset=["ticker", "date"], keep="last")
        .sort(["ticker", "date"])
        .select(
            pl.col("ticker").cast(pl.String),
            pl.col("date").cast(pl.Date),
            pl.col("base_close").cast(pl.Float32),
        )
    )
    final_bounds = merged.select(pl.col("date").min().alias("start_day"), pl.col("date").max().alias("end_day")).collect().row(0, named=True)
    start_day = final_bounds["start_day"]
    end_day = final_bounds["end_day"]
    if start_day is None or end_day is None:
        raise ValueError(f"{symbol}. empty stock EOD merge for year {year}")
    final_path = (
        directory
        / f"{format_theta_date(start_day)}_{format_theta_date(end_day)}_{symbol}_eod_stock.parquet"
    ).resolve()
    tmp_path = final_path.with_name(f"{final_path.stem}.tmp{final_path.suffix}")
    if tmp_path.exists():
        tmp_path.unlink()
    merged.sink_parquet(tmp_path)
    tmp_path.replace(final_path)
    _unlink_all(existing_paths, keep=final_path)
    if target_path.exists() and target_path.resolve() != final_path.resolve():
        target_path.unlink(missing_ok=True)
    return final_path


def _count_file_lines(path: Path) -> int:
    with path.open("rb") as file:
        return sum(1 for _ in file)


def _iter_date_chunks(
    start_day: dt.date,
    end_day: dt.date,
    max_days: int,
) -> list[tuple[dt.date, dt.date]]:
    if start_day > end_day:
        return []
    if max_days <= 0:
        raise ValueError("max_days must be positive")

    result: list[tuple[dt.date, dt.date]] = []
    current = start_day
    step = dt.timedelta(days=max_days - 1)
    while current <= end_day:
        chunk_end = min(current + step, end_day)
        result.append((current, chunk_end))
        current = chunk_end + dt.timedelta(days=1)
    return result
