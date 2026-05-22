from __future__ import annotations

import datetime as dt
import gc
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from ..constants import M1_FINAL_COLUMNS, M1_FLOAT32_COLUMNS, M1_INT16_COLUMNS, M1_INT32_COLUMNS
from ..dates import format_theta_date, parse_date, previous_business_day
from ..greeks import add_risk_free_rate, calculate_iv_greeks
from ..rates import ensure_local_dgs1_history
from ..settings import Settings
from ..theta_client import ThetaClient, ThetaDataError, ThetaNoData
from ..time_utils import datetime_to_ms_expr
from ..tiingo import load_tiingo_dividend_ttm


MARKET_CLOSE_MS = 16 * 60 * 60 * 1000
MS_PER_YEAR = 365 * 24 * 60 * 60 * 1000
ALLOWED_CONCURRENCY = {1, 2, 4, 8}
ALLOWED_BATCH_MODES = {"daily", "weekly", "monthly", "yearly", "all"}


@dataclass(frozen=True)
class M1FileResult:
    input_path: Path
    output_path: Path
    days: int
    rows_raw: int
    rows_filtered_before_stock: int
    rows_non_positive_tte: int
    stock_days_redownloaded: int
    rows_read: int
    rows_written: int
    rows_without_stock: int


@dataclass(frozen=True)
class M1EnrichSummary:
    symbol: str
    input_files: int
    processed_files: int
    skipped_files: int
    processed_days: int
    rows_raw: int
    rows_filtered_before_stock: int
    rows_non_positive_tte: int
    stock_days_redownloaded: int
    rows_read: int
    rows_written: int
    rows_without_stock: int
    output_dir: Path
    dry_run: bool = False


@dataclass(frozen=True)
class M1DownloadSummary:
    symbol: str
    trading_days: int
    stock_candidate_days: int
    option_candidate_days: int
    stock_request_batches: int
    option_request_batches: int
    stock_downloaded_days: int
    option_downloaded_days: int
    option_no_data_days: int
    stock_rows: int
    option_rows: int
    stock_output_path: Path | None
    option_output_path: Path | None
    downloaded_option_paths: list[Path]
    dry_run: bool = False


@dataclass(frozen=True)
class M1UpdateSummary:
    symbol: str
    download: M1DownloadSummary
    enrich: M1EnrichSummary | None


@dataclass(frozen=True)
class _RawDownloadResult:
    output_path: Path | None
    downloaded_days: int
    no_data_days: int
    rows: int


def update_m1_data_with_greeks(
    settings: Settings,
    symbol: str = "SPY",
    from_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
    max_days: int | None = None,
    batch_mode: str = "weekly",
    stock_concurrency: int = 1,
    option_concurrency: int = 1,
    option_expiration_mode: str = "same_day",
    overwrite_stock_raw: bool = False,
    overwrite_option_raw: bool = False,
    overwrite_greeks: bool = False,
    skip_enrich: bool = False,
    dry_run: bool = False,
) -> M1UpdateSummary:
    download = download_m1_data(
        settings=settings,
        symbol=symbol,
        from_date=from_date,
        end_date=end_date,
        max_days=max_days,
        batch_mode=batch_mode,
        stock_concurrency=stock_concurrency,
        option_concurrency=option_concurrency,
        option_expiration_mode=option_expiration_mode,
        overwrite_stock_raw=overwrite_stock_raw,
        overwrite_option_raw=overwrite_option_raw,
        dry_run=dry_run,
    )
    enrich_summary: M1EnrichSummary | None = None
    if not skip_enrich and not dry_run:
        input_paths = download.downloaded_option_paths or None
        enrich_summary = enrich_m1_with_greeks(
            settings=settings,
            symbol=symbol,
            input_paths=input_paths,
            from_date=from_date,
            end_date=end_date,
            max_days=max_days if input_paths is None else None,
            overwrite=overwrite_greeks,
            dry_run=False,
        )
    return M1UpdateSummary(symbol=symbol.upper(), download=download, enrich=enrich_summary)


def download_m1_data(
    settings: Settings,
    symbol: str = "SPY",
    from_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
    max_days: int | None = None,
    batch_mode: str = "weekly",
    stock_concurrency: int = 1,
    option_concurrency: int = 1,
    option_expiration_mode: str = "same_day",
    overwrite_stock_raw: bool = False,
    overwrite_option_raw: bool = False,
    dry_run: bool = False,
) -> M1DownloadSummary:
    return _download_m1_data_parts(
        settings=settings,
        symbol=symbol,
        from_date=from_date,
        end_date=end_date,
        max_days=max_days,
        batch_mode=batch_mode,
        stock_concurrency=stock_concurrency,
        option_concurrency=option_concurrency,
        option_expiration_mode=option_expiration_mode,
        overwrite_stock_raw=overwrite_stock_raw,
        overwrite_option_raw=overwrite_option_raw,
        download_stock=True,
        download_options=True,
        dry_run=dry_run,
    )


def download_stock_m1_data(
    settings: Settings,
    symbol: str = "SPY",
    from_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
    max_days: int | None = None,
    batch_mode: str = "weekly",
    stock_concurrency: int = 1,
    overwrite_stock_raw: bool = False,
    dry_run: bool = False,
) -> M1DownloadSummary:
    return _download_m1_data_parts(
        settings=settings,
        symbol=symbol,
        from_date=from_date,
        end_date=end_date,
        max_days=max_days,
        batch_mode=batch_mode,
        stock_concurrency=stock_concurrency,
        option_concurrency=1,
        option_expiration_mode="same_day",
        overwrite_stock_raw=overwrite_stock_raw,
        overwrite_option_raw=False,
        download_stock=True,
        download_options=False,
        dry_run=dry_run,
    )


def download_option_m1_data(
    settings: Settings,
    symbol: str = "SPY",
    from_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
    max_days: int | None = None,
    batch_mode: str = "weekly",
    option_concurrency: int = 1,
    option_expiration_mode: str = "same_day",
    overwrite_option_raw: bool = False,
    dry_run: bool = False,
) -> M1DownloadSummary:
    return _download_m1_data_parts(
        settings=settings,
        symbol=symbol,
        from_date=from_date,
        end_date=end_date,
        max_days=max_days,
        batch_mode=batch_mode,
        stock_concurrency=1,
        option_concurrency=option_concurrency,
        option_expiration_mode=option_expiration_mode,
        overwrite_stock_raw=False,
        overwrite_option_raw=overwrite_option_raw,
        download_stock=False,
        download_options=True,
        dry_run=dry_run,
    )


def _download_m1_data_parts(
    settings: Settings,
    symbol: str,
    from_date: str | dt.date | None,
    end_date: str | dt.date | None,
    max_days: int | None,
    batch_mode: str,
    stock_concurrency: int,
    option_concurrency: int,
    option_expiration_mode: str,
    overwrite_stock_raw: bool,
    overwrite_option_raw: bool,
    download_stock: bool,
    download_options: bool,
    dry_run: bool,
) -> M1DownloadSummary:
    symbol = symbol.upper()
    if not download_stock and not download_options:
        raise ValueError("At least one M1 source must be selected")
    batch_mode = _validate_batch_mode(batch_mode)
    stock_concurrency = _validate_concurrency(stock_concurrency, "stock_concurrency")
    option_concurrency = _validate_concurrency(option_concurrency, "option_concurrency")
    option_expiration_mode = option_expiration_mode.lower()
    if option_expiration_mode not in {"same_day", "all"}:
        raise ValueError(f"Unsupported M1 option_expiration_mode: {option_expiration_mode}")

    existing_stock_dates = _read_existing_stock_m1_dates(settings, symbol) if download_stock else set()
    existing_option_dates = _read_existing_option_m1_dates(settings, symbol) if download_options else set()
    parsed_from = parse_date(from_date)
    parsed_end = parse_date(end_date) or previous_business_day()

    if parsed_from is None:
        existing_maxes = []
        missing_selected_source = False
        if download_stock:
            if existing_stock_dates:
                existing_maxes.append(max(existing_stock_dates))
            else:
                missing_selected_source = True
        if download_options:
            if existing_option_dates:
                existing_maxes.append(max(existing_option_dates))
            else:
                missing_selected_source = True
        max_existing = None if missing_selected_source or not existing_maxes else min(existing_maxes)
        parsed_from = (max_existing + dt.timedelta(days=1)) if max_existing else dt.date(2020, 1, 1)

    if parsed_from > parsed_end:
        if from_date is None:
            print(f"{symbol}. M1 is up to date through {parsed_end}")
            return M1DownloadSummary(
                symbol=symbol,
                trading_days=0,
                stock_candidate_days=0,
                option_candidate_days=0,
                stock_request_batches=0,
                option_request_batches=0,
                stock_downloaded_days=0,
                option_downloaded_days=0,
                option_no_data_days=0,
                stock_rows=0,
                option_rows=0,
                stock_output_path=None,
                option_output_path=None,
                downloaded_option_paths=[],
                dry_run=dry_run,
            )
        raise ValueError(f"M1 from_date must be <= end_date, got {parsed_from}..{parsed_end}")

    client = _make_theta_client(settings)
    trading_days = _fetch_stock_trading_days(client, symbol, parsed_from, parsed_end)
    if max_days is not None:
        trading_days = trading_days[:max_days]

    stock_candidates = [
        day for day in trading_days if download_stock and (overwrite_stock_raw or day not in existing_stock_dates)
    ]
    option_candidates = [
        day for day in trading_days if download_options and (overwrite_option_raw or day not in existing_option_dates)
    ]
    stock_batch_ranges = _build_batch_ranges(stock_candidates, batch_mode=batch_mode)
    option_batch_ranges = _build_batch_ranges(option_candidates, batch_mode=batch_mode)

    print(f"{symbol}. M1 date range: {parsed_from}..{parsed_end}")
    print(f"{symbol}. M1 trading days from Theta stock EOD: {len(trading_days)}")
    print(f"{symbol}. M1 sources: stock={download_stock}, options={download_options}")
    print(f"{symbol}. M1 stock candidate days: {len(stock_candidates)}")
    print(f"{symbol}. M1 option candidate days: {len(option_candidates)}")
    print(f"{symbol}. M1 batch mode: {batch_mode}")
    if download_stock:
        print(f"{symbol}. M1 stock concurrency: {stock_concurrency}")
        print(f"{symbol}. M1 overwrite stock raw: {overwrite_stock_raw}")
    if download_options:
        print(f"{symbol}. M1 option concurrency: {option_concurrency}")
        print(f"{symbol}. M1 option expiration mode: {option_expiration_mode}")
        print(f"{symbol}. M1 overwrite option raw: {overwrite_option_raw}")
    print(f"{symbol}. M1 stock request batches: {len(stock_batch_ranges)}")
    print(f"{symbol}. M1 option request batches: {len(option_batch_ranges)}")

    stock_year_groups = _group_days_by_year(stock_candidates)
    option_year_groups = _group_days_by_year(option_candidates)
    stock_preview = _preview_year_output_paths(settings.stock_m1_dir, symbol, "_m1_stock.parquet", stock_year_groups)
    option_preview = _preview_year_output_paths(settings.m1_options_dir, symbol, "_m1_opts.parquet", option_year_groups)
    if stock_preview:
        print(f"{symbol}. M1 stock outputs:")
        for path in stock_preview:
            print(path)
    if option_preview:
        print(f"{symbol}. M1 option outputs:")
        for path in option_preview:
            print(path)

    if dry_run:
        return M1DownloadSummary(
            symbol=symbol,
            trading_days=len(trading_days),
            stock_candidate_days=len(stock_candidates),
            option_candidate_days=len(option_candidates),
            stock_request_batches=len(stock_batch_ranges),
            option_request_batches=len(option_batch_ranges),
            stock_downloaded_days=0,
            option_downloaded_days=0,
            option_no_data_days=0,
            stock_rows=0,
            option_rows=0,
            stock_output_path=stock_preview[-1] if stock_preview else None,
            option_output_path=option_preview[-1] if option_preview else None,
            downloaded_option_paths=[],
            dry_run=True,
        )

    if download_options:
        settings.m1_options_dir.mkdir(parents=True, exist_ok=True)
    if download_stock:
        settings.stock_m1_dir.mkdir(parents=True, exist_ok=True)
    settings.staging_m1_dir.mkdir(parents=True, exist_ok=True)

    stock_downloaded_days = 0
    option_downloaded_days = 0
    option_no_data_days = 0
    stock_rows = 0
    option_rows = 0
    stock_output_path: Path | None = None
    option_output_path: Path | None = None
    downloaded_option_paths: list[Path] = []

    for year in sorted(stock_year_groups):
        year_days = stock_year_groups[year]
        existing_paths = _find_year_files(settings.stock_m1_dir, symbol, "_m1_stock.parquet", year)
        target_path = _build_year_output_path(
            settings.stock_m1_dir,
            symbol,
            "_m1_stock.parquet",
            year_days,
            existing_paths,
        )
        stock_result = _download_stock_m1_days(
            settings=settings,
            symbol=symbol,
            days=year_days,
            batch_mode=batch_mode,
            output_path=target_path,
            existing_paths=existing_paths,
            concurrency=stock_concurrency,
        )
        stock_downloaded_days += stock_result.downloaded_days
        stock_rows += stock_result.rows
        if stock_result.output_path is not None:
            stock_output_path = stock_result.output_path

    for year in sorted(option_year_groups):
        year_days = option_year_groups[year]
        existing_paths = _find_year_files(settings.m1_options_dir, symbol, "_m1_opts.parquet", year)
        target_path = _build_year_output_path(
            settings.m1_options_dir,
            symbol,
            "_m1_opts.parquet",
            year_days,
            existing_paths,
        )
        option_result = _download_option_m1_days(
            settings=settings,
            symbol=symbol,
            days=year_days,
            batch_mode=batch_mode,
            output_path=target_path,
            existing_paths=existing_paths,
            concurrency=option_concurrency,
            expiration_mode=option_expiration_mode,
        )
        option_downloaded_days += option_result.downloaded_days
        option_no_data_days += option_result.no_data_days
        option_rows += option_result.rows
        if option_result.output_path is not None:
            option_output_path = option_result.output_path
            downloaded_option_paths.append(option_result.output_path)

    downloaded_option_paths = sorted(set(downloaded_option_paths))

    return M1DownloadSummary(
        symbol=symbol,
        trading_days=len(trading_days),
        stock_candidate_days=len(stock_candidates),
        option_candidate_days=len(option_candidates),
        stock_request_batches=len(stock_batch_ranges),
        option_request_batches=len(option_batch_ranges),
        stock_downloaded_days=stock_downloaded_days,
        option_downloaded_days=option_downloaded_days,
        option_no_data_days=option_no_data_days,
        stock_rows=stock_rows,
        option_rows=option_rows,
        stock_output_path=stock_output_path,
        option_output_path=option_output_path,
        downloaded_option_paths=downloaded_option_paths,
    )


def enrich_m1_with_greeks(
    settings: Settings,
    symbol: str = "SPY",
    input_paths: list[str | Path] | None = None,
    from_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
    max_days: int | None = None,
    overwrite: bool = False,
    dry_run: bool = False,
) -> M1EnrichSummary:
    symbol = symbol.upper()
    parsed_from = parse_date(from_date)
    parsed_end = parse_date(end_date)
    raw_paths = _resolve_option_m1_paths(settings, symbol, input_paths)
    if not raw_paths:
        raise FileNotFoundError(f"{symbol}. No option M1 parquet files found in {settings.m1_options_dir}")

    settings.m1_with_greeks_dir.mkdir(parents=True, exist_ok=True)
    settings.staging_m1_dir.mkdir(parents=True, exist_ok=True)

    remaining_days = max_days
    processed_files = 0
    skipped_files = 0
    processed_days = 0
    rows_raw = 0
    rows_filtered_before_stock = 0
    rows_non_positive_tte = 0
    stock_days_redownloaded = 0
    rows_read = 0
    rows_written = 0
    rows_without_stock = 0

    print(f"{symbol}. M1 input files: {len(raw_paths)}")
    print(f"{symbol}. M1 options dir: {settings.m1_options_dir}")
    print(f"{symbol}. M1 with greeks dir: {settings.m1_with_greeks_dir}")

    for raw_path in raw_paths:
        all_days = _read_m1_dates(raw_path)
        selected_days = _filter_days(all_days, parsed_from, parsed_end)
        if remaining_days is not None:
            selected_days = selected_days[:remaining_days]
            remaining_days -= len(selected_days)

        if not selected_days:
            print(f"{symbol}. {raw_path.name}: no selected dates")
            if remaining_days == 0:
                break
            continue

        year_groups = _group_days_by_year(selected_days)
        for year in sorted(year_groups):
            year_days = year_groups[year]
            existing_paths = _find_year_files(
                settings.m1_with_greeks_dir,
                symbol,
                "_m1_greeks_opts.parquet",
                year,
            )
            process_days = year_days
            if not overwrite:
                existing_dates = _collect_dates_from_paths(existing_paths)
                process_days = [day for day in year_days if day not in existing_dates]

            if not process_days:
                print(
                    f"{symbol}. {raw_path.name}: year {year} fully covered in with_greeks, skipped"
                )
                skipped_files += 1
                continue

            output_path = _build_year_output_path(
                settings.m1_with_greeks_dir,
                symbol,
                "_m1_greeks_opts.parquet",
                process_days,
                existing_paths,
            )
            print(
                f"{symbol}. {raw_path.name}: selected {process_days[0]}..{process_days[-1]} "
                f"({len(process_days)} days)"
            )
            print(f"{symbol}. Output: {output_path}")
            if dry_run:
                processed_files += 1
                processed_days += len(process_days)
                continue

            result = _enrich_m1_file(
                settings=settings,
                symbol=symbol,
                raw_path=raw_path,
                output_path=output_path,
                selected_days=process_days,
                existing_output_paths=existing_paths,
            )
            processed_files += 1
            processed_days += result.days
            rows_raw += result.rows_raw
            rows_filtered_before_stock += result.rows_filtered_before_stock
            rows_non_positive_tte += result.rows_non_positive_tte
            stock_days_redownloaded += result.stock_days_redownloaded
            rows_read += result.rows_read
            rows_written += result.rows_written
            rows_without_stock += result.rows_without_stock

        if remaining_days == 0:
            break

    return M1EnrichSummary(
        symbol=symbol,
        input_files=len(raw_paths),
        processed_files=processed_files,
        skipped_files=skipped_files,
        processed_days=processed_days,
        rows_raw=rows_raw,
        rows_filtered_before_stock=rows_filtered_before_stock,
        rows_non_positive_tte=rows_non_positive_tte,
        stock_days_redownloaded=stock_days_redownloaded,
        rows_read=rows_read,
        rows_written=rows_written,
        rows_without_stock=rows_without_stock,
        output_dir=settings.m1_with_greeks_dir,
        dry_run=dry_run,
    )


def normalize_option_m1(df: pl.DataFrame, symbol: str) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(schema={column: pl.Null for column in M1_FINAL_COLUMNS})

    schema = df.schema
    expiration_expr = _normalized_date_expr(schema, "expiration")
    date_expr = _normalized_date_expr(schema, "date")
    strike = pl.col("strike").cast(pl.Float64)
    strike_expr = pl.when(strike.abs() > 10_000).then(strike / 1000.0).otherwise(strike)

    result = (
        df.with_columns(
            pl.lit(symbol.upper()).alias("ticker"),
            expiration_expr.alias("expiration"),
            date_expr.alias("date"),
            strike_expr.alias("strike"),
            pl.col("right").cast(pl.String).str.to_lowercase().str.slice(0, 1).alias("right"),
            pl.col("ms_of_day").cast(pl.Int32),
            pl.col("bid_size").cast(pl.Int32),
            pl.col("bid_exchange").cast(pl.Int16),
            pl.col("bid").cast(pl.Float64),
            pl.col("bid_condition").cast(pl.Int16),
            pl.col("ask_size").cast(pl.Int32),
            pl.col("ask_exchange").cast(pl.Int16),
            pl.col("ask").cast(pl.Float64),
            pl.col("ask_condition").cast(pl.Int16),
        )
        .filter(pl.col("date").is_not_null())
        .filter(pl.col("expiration").is_not_null())
        .filter(pl.col("expiration") >= pl.col("date"))
        .with_columns(
            (
                (
                    (pl.col("expiration") - pl.col("date")).dt.total_days().cast(pl.Float64)
                    * 24
                    * 60
                    * 60
                    * 1000
                )
                + (pl.lit(MARKET_CLOSE_MS) - pl.col("ms_of_day").cast(pl.Int64)).cast(pl.Float64)
            )
            .truediv(MS_PER_YEAR)
            .alias("timeToExp")
        )
        .select(
            "ticker",
            "expiration",
            "strike",
            "right",
            "ms_of_day",
            "bid_size",
            "bid_exchange",
            "bid",
            "bid_condition",
            "ask_size",
            "ask_exchange",
            "ask",
            "ask_condition",
            "date",
            "timeToExp",
        )
    )
    return result


def align_m1_schema(df: pl.DataFrame) -> pl.DataFrame:
    result = df
    if "baseClose" not in result.columns and "base_close" in result.columns:
        result = result.rename({"base_close": "baseClose"})

    for column in M1_FINAL_COLUMNS:
        if column not in result.columns:
            result = result.with_columns(pl.lit(None).alias(column))

    result = result.with_columns(
        pl.col("ticker").cast(pl.String),
        pl.col("expiration").cast(pl.Date),
        pl.col("right").cast(pl.String),
        pl.col("date").cast(pl.Date),
        *[pl.col(column).cast(pl.Int16) for column in M1_INT16_COLUMNS],
        *[pl.col(column).cast(pl.Int32) for column in M1_INT32_COLUMNS],
        *[pl.col(column).cast(pl.Float32) for column in M1_FLOAT32_COLUMNS],
    )
    return result.select(M1_FINAL_COLUMNS)


def _enrich_m1_file(
    settings: Settings,
    symbol: str,
    raw_path: Path,
    output_path: Path,
    selected_days: list[dt.date],
    existing_output_paths: list[Path],
) -> M1FileResult:
    started = time.perf_counter()

    stock_m1 = _load_stock_m1_for_dates(settings, symbol, selected_days)
    dividend_curve = _load_dividend_curve_for_dates(symbol, settings, selected_days)
    risk_free_curve = _load_risk_free_curve_for_dates(settings, selected_days)

    rows_read = 0
    rows_raw = 0
    rows_filtered_before_stock = 0
    rows_non_positive_tte = 0
    stock_days_redownloaded = 0
    rows_written = 0
    rows_without_stock = 0

    print(f"{symbol}. M1 bulk processing {selected_days[0]}..{selected_days[-1]} ({len(selected_days)} days)")
    raw_selected = _read_m1_days(raw_path, selected_days)
    rows_raw = raw_selected.height
    normalized = normalize_option_m1(raw_selected, symbol=symbol)
    del raw_selected
    gc.collect()
    if normalized.is_empty():
        raise ValueError(f"{symbol}. No normalized option M1 rows for selected range in {raw_path}")

    rows_filtered_before_stock = rows_raw - normalized.height
    if rows_filtered_before_stock:
        print(f"{symbol}. Rows dropped before stock join: {rows_filtered_before_stock}")
    rows_non_positive_tte = int(normalized.select((pl.col("timeToExp") <= 0).sum()).item())
    if rows_non_positive_tte:
        print(f"{symbol}. Rows with timeToExp <= 0 kept; greeks will be NaN: {rows_non_positive_tte}")
    rows_read = normalized.height

    joined, missing_stock_rows = _join_stock_m1_exact(normalized, stock_m1)
    if missing_stock_rows:
        missing_days = _missing_stock_days(normalized, stock_m1)
        print(
            f"{symbol}. Exact stock minutes missing for {missing_stock_rows} option rows across "
            f"{len(missing_days)} day(s). Redownloading those stock M1 days"
        )
        for day in missing_days:
            stock_day = _redownload_stock_m1_day(settings, symbol, day)
            stock_m1 = _replace_stock_day(stock_m1, stock_day, day)
            stock_days_redownloaded += 1
        joined, missing_stock_rows = _join_stock_m1_exact(normalized, stock_m1)

    if missing_stock_rows:
        rows_without_stock = missing_stock_rows
        missing_minutes = _format_missing_stock_minutes(normalized, stock_m1)
        raise ValueError(
            f"{symbol}. Exact stock M1 minutes are still missing after redownload. "
            f"Rows={missing_stock_rows}; minutes={missing_minutes}"
        )

    normalized = joined
    normalized = _attach_dividend_yield(normalized, dividend_curve)
    normalized = add_risk_free_rate(normalized, risk_free_curve)
    normalized = normalized.with_columns(pl.col("risk_free_rate").cast(pl.Float32).alias("dgs1"))

    with_greeks = calculate_iv_greeks(normalized, include_probability=False)
    del normalized
    result = align_m1_schema(with_greeks)
    del with_greeks
    rows_written = result.height

    print(f"{symbol}. M1 writing final file: {output_path}")
    output_path = _merge_m1_with_greeks_year_file(
        output_path=output_path,
        symbol=symbol,
        staged=result,
        existing_paths=existing_output_paths,
        replace_days=set(selected_days),
    )
    del result
    gc.collect()

    print(f"{symbol}. M1 file done in {time.perf_counter() - started:.2f}s; rows={rows_written}")
    return M1FileResult(
        input_path=raw_path,
        output_path=output_path,
        days=len(selected_days),
        rows_raw=rows_raw,
        rows_filtered_before_stock=rows_filtered_before_stock,
        rows_non_positive_tte=rows_non_positive_tte,
        stock_days_redownloaded=stock_days_redownloaded,
        rows_read=rows_read,
        rows_written=rows_written,
        rows_without_stock=rows_without_stock,
    )


def _download_stock_m1_days(
    settings: Settings,
    symbol: str,
    days: list[dt.date],
    batch_mode: str,
    output_path: Path | None,
    existing_paths: list[Path],
    concurrency: int,
) -> _RawDownloadResult:
    if not days or output_path is None:
        return _RawDownloadResult(output_path=None, downloaded_days=0, no_data_days=0, rows=0)

    started = time.perf_counter()
    staging_run_dir = settings.staging_m1_dir / f"{symbol}_stock_m1_download_{time.time_ns()}"
    staging_run_dir.mkdir(parents=True, exist_ok=False)
    staged_paths: list[Path] = []
    rows = 0
    no_data_days = 0
    downloaded_days = 0
    batch_ranges = _build_batch_ranges(days, batch_mode=batch_mode)

    try:
        print(
            f"{symbol}. Downloading stock M1: {days[0]}..{days[-1]} ({len(days)} days), "
            f"batches={len(batch_ranges)} ({batch_mode})"
        )
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {
                executor.submit(_download_stock_m1_range, settings, symbol, start_day, end_day, staging_run_dir): (
                    start_day,
                    end_day,
                    range_days,
                )
                for start_day, end_day, range_days in batch_ranges
            }
            for future in as_completed(futures):
                start_day, end_day, range_days = futures[future]
                try:
                    staged_path, batch_rows, batch_downloaded_days = future.result()
                except ThetaNoData:
                    no_data_days += len(range_days)
                    print(f"{symbol}. Stock M1 no data: {start_day}..{end_day}")
                    continue
                staged_paths.append(staged_path)
                rows += batch_rows
                downloaded_days += batch_downloaded_days
                print(
                    f"{symbol}. Stock M1 downloaded {start_day}..{end_day}: "
                    f"rows={batch_rows}, days={batch_downloaded_days}"
                )

        if not staged_paths:
            return _RawDownloadResult(output_path=None, downloaded_days=0, no_data_days=no_data_days, rows=0)

        output_path = _merge_stock_m1_year_file(
            output_path=output_path,
            symbol=symbol,
            staged_paths=staged_paths,
            existing_paths=existing_paths,
        )
        print(f"{symbol}. Stock M1 written: {output_path}")
        print(f"{symbol}. Stock M1 download done in {time.perf_counter() - started:.2f}s")
        return _RawDownloadResult(
            output_path=output_path,
            downloaded_days=downloaded_days,
            no_data_days=no_data_days,
            rows=rows,
        )
    finally:
        for path in staged_paths:
            path.unlink(missing_ok=True)
        try:
            staging_run_dir.rmdir()
        except OSError:
            pass


def _download_option_m1_days(
    settings: Settings,
    symbol: str,
    days: list[dt.date],
    batch_mode: str,
    output_path: Path | None,
    existing_paths: list[Path],
    concurrency: int,
    expiration_mode: str,
) -> _RawDownloadResult:
    if not days or output_path is None:
        return _RawDownloadResult(output_path=None, downloaded_days=0, no_data_days=0, rows=0)

    started = time.perf_counter()
    staging_run_dir = settings.staging_m1_dir / f"{symbol}_option_m1_download_{time.time_ns()}"
    staging_run_dir.mkdir(parents=True, exist_ok=False)
    staged_paths: list[Path] = []
    rows = 0
    no_data_days = 0
    downloaded_days = 0
    batch_ranges = _build_batch_ranges(days, batch_mode=batch_mode)
    use_range_mode = expiration_mode == "all"
    if not use_range_mode and batch_mode != "daily":
        print(
            f"{symbol}. Option M1 same_day mode: batch_mode={batch_mode} requested, "
            "falling back to daily requests for accuracy."
        )

    try:
        print(
            f"{symbol}. Downloading option M1: {days[0]}..{days[-1]} ({len(days)} days), "
            f"batches={len(batch_ranges)} ({batch_mode})"
        )
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            if use_range_mode:
                futures = {
                    executor.submit(
                        _download_option_m1_range,
                        settings,
                        symbol,
                        start_day,
                        end_day,
                        staging_run_dir,
                        expiration_mode,
                    ): (start_day, end_day, range_days)
                    for start_day, end_day, range_days in batch_ranges
                }
            else:
                futures = {
                    executor.submit(_download_option_m1_day, settings, symbol, day, staging_run_dir, expiration_mode): (
                        day,
                        day,
                        [day],
                    )
                    for day in days
                }
            for future in as_completed(futures):
                start_day, end_day, range_days = futures[future]
                try:
                    staged_path, batch_rows, batch_downloaded_days = future.result()
                except ThetaNoData:
                    no_data_days += len(range_days)
                    print(f"{symbol}. Option M1 no data: {start_day}..{end_day}")
                    continue
                staged_paths.append(staged_path)
                rows += batch_rows
                downloaded_days += batch_downloaded_days
                print(
                    f"{symbol}. Option M1 downloaded {start_day}..{end_day}: "
                    f"rows={batch_rows}, days={batch_downloaded_days}"
                )

        if not staged_paths:
            return _RawDownloadResult(output_path=None, downloaded_days=0, no_data_days=no_data_days, rows=0)

        output_path = _merge_option_m1_year_file(
            output_path=output_path,
            symbol=symbol,
            staged_paths=staged_paths,
            existing_paths=existing_paths,
        )
        print(f"{symbol}. Option M1 written: {output_path}")
        print(f"{symbol}. Option M1 download done in {time.perf_counter() - started:.2f}s")
        return _RawDownloadResult(
            output_path=output_path,
            downloaded_days=downloaded_days,
            no_data_days=no_data_days,
            rows=rows,
        )
    finally:
        for path in staged_paths:
            path.unlink(missing_ok=True)
        try:
            staging_run_dir.rmdir()
        except OSError:
            pass


def _download_stock_m1_day(
    settings: Settings,
    symbol: str,
    day: dt.date,
    staging_dir: Path,
) -> tuple[Path, int]:
    client = _make_theta_client(settings)
    frame = client.get_csv_frame(
        "/stock/history/ohlc",
        {
            "symbol": symbol,
            "date": format_theta_date(day),
            "interval": "1m",
        },
    )
    normalized = normalize_stock_m1(frame, symbol=symbol)
    if normalized.is_empty():
        raise ThetaNoData(f"No stock M1 rows for {symbol} {day}")
    staged_path = staging_dir / f"{symbol}_stock_m1_{format_theta_date(day)}.parquet"
    normalized.write_parquet(staged_path)
    return staged_path, normalized.height


def _download_stock_m1_range(
    settings: Settings,
    symbol: str,
    start_day: dt.date,
    end_day: dt.date,
    staging_dir: Path,
) -> tuple[Path, int, int]:
    client = _make_theta_client(settings)
    frame = client.get_csv_frame(
        "/stock/history/ohlc",
        {
            "symbol": symbol,
            "start_date": format_theta_date(start_day),
            "end_date": format_theta_date(end_day),
            "interval": "1m",
        },
    )
    normalized = normalize_stock_m1(frame, symbol=symbol)
    if normalized.is_empty():
        raise ThetaNoData(f"No stock M1 rows for {symbol} {start_day}..{end_day}")
    staged_path = staging_dir / f"{symbol}_stock_m1_{format_theta_date(start_day)}_{format_theta_date(end_day)}.parquet"
    normalized.write_parquet(staged_path)
    unique_days = normalized.select(pl.col("date").n_unique()).item()
    return staged_path, normalized.height, int(unique_days)


def _download_option_m1_day(
    settings: Settings,
    symbol: str,
    day: dt.date,
    staging_dir: Path,
    expiration_mode: str,
) -> tuple[Path, int, int]:
    client = _make_theta_client(settings)
    expiration = format_theta_date(day) if expiration_mode == "same_day" else "*"
    frame = client.get_csv_frame(
        "/option/history/quote",
        {
            "symbol": symbol,
            "expiration": expiration,
            "date": format_theta_date(day),
            "interval": "1m",
        },
    )
    normalized = normalize_downloaded_option_m1(frame, symbol=symbol)
    if normalized.is_empty():
        raise ThetaNoData(f"No option M1 rows for {symbol} {day}")
    staged_path = staging_dir / f"{symbol}_option_m1_{format_theta_date(day)}.parquet"
    normalized.write_parquet(staged_path)
    unique_days = normalized.select(pl.col("date").n_unique()).item()
    return staged_path, normalized.height, int(unique_days)


def _download_option_m1_range(
    settings: Settings,
    symbol: str,
    start_day: dt.date,
    end_day: dt.date,
    staging_dir: Path,
    expiration_mode: str,
) -> tuple[Path, int, int]:
    client = _make_theta_client(settings)
    expiration = "*" if expiration_mode == "all" else format_theta_date(start_day)
    frame = client.get_csv_frame(
        "/option/history/quote",
        {
            "symbol": symbol,
            "expiration": expiration,
            "start_date": format_theta_date(start_day),
            "end_date": format_theta_date(end_day),
            "interval": "1m",
        },
    )
    normalized = normalize_downloaded_option_m1(frame, symbol=symbol)
    if normalized.is_empty():
        raise ThetaNoData(f"No option M1 rows for {symbol} {start_day}..{end_day}")
    staged_path = staging_dir / f"{symbol}_option_m1_{format_theta_date(start_day)}_{format_theta_date(end_day)}.parquet"
    normalized.write_parquet(staged_path)
    unique_days = normalized.select(pl.col("date").n_unique()).item()
    return staged_path, normalized.height, int(unique_days)


def normalize_stock_m1(df: pl.DataFrame, symbol: str) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(
            schema={
                "ms_of_day": pl.Int64,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "count": pl.Int64,
                "date": pl.Int64,
                "ticker": pl.String,
            }
        )

    timestamp = pl.col("timestamp").cast(pl.String).str.to_datetime(strict=False)
    return (
        df.with_columns(
            timestamp.alias("_timestamp"),
            pl.lit(symbol.upper()).alias("ticker"),
        )
        .filter(pl.col("_timestamp").is_not_null())
        .with_columns(
            datetime_to_ms_expr("_timestamp").cast(pl.Int64).alias("ms_of_day"),
            pl.col("_timestamp").dt.strftime("%Y%m%d").cast(pl.Int64).alias("date"),
        )
        .select(
            "ms_of_day",
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.Int64),
            pl.col("count").cast(pl.Int64),
            "date",
            "ticker",
        )
    )


def normalize_downloaded_option_m1(df: pl.DataFrame, symbol: str) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "expiration": pl.Int64,
                "strike": pl.Int64,
                "right": pl.String,
                "ms_of_day": pl.Int64,
                "bid_size": pl.Int64,
                "bid_exchange": pl.Int64,
                "bid": pl.Float64,
                "bid_condition": pl.Int64,
                "ask_size": pl.Int64,
                "ask_exchange": pl.Int64,
                "ask": pl.Float64,
                "ask_condition": pl.Int64,
                "date": pl.Date,
            }
        )

    timestamp = pl.col("timestamp").cast(pl.String).str.to_datetime(strict=False)
    root_expr = pl.col("symbol").cast(pl.String) if "symbol" in df.columns else pl.lit(symbol.upper())
    return (
        df.with_columns(
            timestamp.alias("_timestamp"),
            root_expr.alias("ticker"),
            pl.col("expiration").cast(pl.String).str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("_expiration"),
            pl.col("right").cast(pl.String).str.to_uppercase().str.slice(0, 1).alias("right"),
        )
        .filter(pl.col("_timestamp").is_not_null())
        .filter(pl.col("_expiration").is_not_null())
        .with_columns(
            datetime_to_ms_expr("_timestamp").cast(pl.Int64).alias("ms_of_day"),
            pl.col("_timestamp").dt.date().alias("date"),
            pl.col("_expiration").dt.strftime("%Y%m%d").cast(pl.Int64).alias("expiration"),
            (pl.col("strike").cast(pl.Float64) * 1000.0).round(0).cast(pl.Int64).alias("strike"),
        )
        .select(
            "ticker",
            "expiration",
            "strike",
            "right",
            "ms_of_day",
            pl.col("bid_size").cast(pl.Int64),
            pl.col("bid_exchange").cast(pl.Int64),
            pl.col("bid").cast(pl.Float64),
            pl.col("bid_condition").cast(pl.Int64),
            pl.col("ask_size").cast(pl.Int64),
            pl.col("ask_exchange").cast(pl.Int64),
            pl.col("ask").cast(pl.Float64),
            pl.col("ask_condition").cast(pl.Int64),
            "date",
        )
    )


def _join_stock_m1_exact(
    options: pl.DataFrame,
    stock: pl.DataFrame,
) -> tuple[pl.DataFrame, int]:
    stock_for_join = (
        stock.select(
            pl.col("date").cast(pl.Date),
            pl.col("ms_of_day").cast(pl.Int32),
            pl.col("base_close"),
        )
        .unique(subset=["date", "ms_of_day"], keep="last")
    )
    joined = options.join(stock_for_join, how="left", on=["date", "ms_of_day"])
    missing_rows = joined["base_close"].null_count()
    return joined, missing_rows


def _redownload_stock_m1_day(settings: Settings, symbol: str, day: dt.date) -> pl.DataFrame:
    settings.stock_m1_dir.mkdir(parents=True, exist_ok=True)
    settings.staging_m1_dir.mkdir(parents=True, exist_ok=True)
    staging_dir = settings.staging_m1_dir / f"{symbol}_stock_m1_refresh_{format_theta_date(day)}_{time.time_ns()}"
    staging_dir.mkdir(parents=True, exist_ok=False)
    staged_path: Path | None = None
    try:
        staged_path, rows = _download_stock_m1_day(settings, symbol, day, staging_dir)
        existing_paths = _find_year_files(settings.stock_m1_dir, symbol, "_m1_stock.parquet", day.year)
        output_path = _build_year_output_path(
            settings.stock_m1_dir,
            symbol,
            "_m1_stock.parquet",
            [day],
            existing_paths,
        )
        final_path = _merge_stock_m1_year_file(
            output_path=output_path,
            symbol=symbol,
            staged_paths=[staged_path],
            existing_paths=existing_paths,
        )
        print(f"{symbol}. {day}: refreshed stock M1 written: {final_path}; rows={rows}")
        return _stock_raw_to_join_frame(pl.read_parquet(final_path))
    finally:
        if staged_path is not None:
            staged_path.unlink(missing_ok=True)
        try:
            staging_dir.rmdir()
        except OSError:
            pass


def _stock_raw_to_join_frame(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame({"date": [], "ms_of_day": [], "base_close": []}, schema={"date": pl.Date, "ms_of_day": pl.Int32, "base_close": pl.Float64})
    schema = df.schema
    if schema["date"] == pl.Date:
        date_expr = pl.col("date").cast(pl.Date)
    else:
        date_expr = pl.col("date").cast(pl.String).str.strptime(pl.Date, "%Y%m%d", strict=False)
    return (
        df.with_columns(date_expr.alias("date"))
        .select(
            pl.col("date").cast(pl.Date),
            pl.col("ms_of_day").cast(pl.Int32),
            pl.col("close").cast(pl.Float64).alias("base_close"),
        )
        .filter(pl.col("base_close").is_not_null())
        .unique(subset=["date", "ms_of_day"], keep="last")
        .sort(["date", "ms_of_day"])
    )


def _replace_stock_day(stock_m1: pl.DataFrame, stock_day: pl.DataFrame, day: dt.date) -> pl.DataFrame:
    return pl.concat(
        [stock_m1.filter(pl.col("date") != day), stock_day],
        how="vertical_relaxed",
    ).sort(["date", "ms_of_day"])


def _format_missing_stock_minutes(options: pl.DataFrame, stock: pl.DataFrame, limit: int = 10) -> str:
    missing = (
        options.select("date", "ms_of_day")
        .join(stock.select("date", "ms_of_day").unique(), how="anti", on=["date", "ms_of_day"])
        .group_by("ms_of_day")
        .agg(pl.len().alias("rows"))
        .sort("rows", descending=True)
        .head(limit)
    )
    if missing.is_empty():
        return ""
    return ", ".join(f"{row['ms_of_day']} ({row['rows']} rows)" for row in missing.iter_rows(named=True))


def _missing_stock_days(options: pl.DataFrame, stock: pl.DataFrame) -> list[dt.date]:
    missing = (
        options.select("date", "ms_of_day")
        .join(stock.select("date", "ms_of_day").unique(), how="anti", on=["date", "ms_of_day"])
        .select(pl.col("date").unique().sort())
    )
    return missing["date"].to_list()


def _read_m1_day(path: Path, day: dt.date) -> pl.DataFrame:
    lf = pl.scan_parquet(path)
    schema = lf.collect_schema()
    return lf.filter(_date_equals_expr(schema, "date", day)).collect()


def _read_m1_days(path: Path, days: list[dt.date]) -> pl.DataFrame:
    lf = pl.scan_parquet(path)
    schema = lf.collect_schema()
    return (
        lf.with_columns(_normalized_date_expr(schema, "date").alias("date"))
        .filter(pl.col("date").is_in(days))
        .collect()
    )


def _read_m1_dates(path: Path) -> list[dt.date]:
    lf = pl.scan_parquet(path)
    schema = lf.collect_schema()
    dates = (
        lf.select(_normalized_date_expr(schema, "date").alias("date"))
        .filter(pl.col("date").is_not_null())
        .select(pl.col("date").unique().sort())
        .collect()
    )
    return dates["date"].to_list()


def _read_existing_option_m1_dates(settings: Settings, symbol: str) -> set[dt.date]:
    paths = _resolve_option_m1_paths(settings, symbol, input_paths=None)
    days: set[dt.date] = set()
    for path in paths:
        days.update(_read_m1_dates(path))
    return days


def _read_existing_stock_m1_dates(settings: Settings, symbol: str) -> set[dt.date]:
    paths = _find_stock_m1_paths(settings, symbol)
    days: set[dt.date] = set()
    for path in paths:
        lf = pl.scan_parquet(path)
        schema = lf.collect_schema()
        frame = (
            lf.select(_normalized_date_expr(schema, "date").alias("date"))
            .filter(pl.col("date").is_not_null())
            .select(pl.col("date").unique())
            .collect()
        )
        days.update(frame["date"].to_list())
    return days


def _load_stock_m1_for_dates(settings: Settings, symbol: str, days: list[dt.date]) -> pl.DataFrame:
    paths = _find_stock_m1_paths(settings, symbol)
    if not paths:
        raise FileNotFoundError(f"{symbol}. No stock M1 parquet files found in {settings.stock_m1_dir}")

    print(f"{symbol}. Stock M1 source files: {len(paths)}")
    lf = pl.scan_parquet([str(path) for path in paths])
    schema = lf.collect_schema()
    stock = (
        lf.with_columns(_normalized_date_expr(schema, "date").alias("date"))
        .filter(pl.col("date").is_in(days))
        .select(
            pl.col("date").cast(pl.Date),
            pl.col("ms_of_day").cast(pl.Int32),
            pl.col("close").cast(pl.Float64).alias("base_close"),
        )
        .filter(pl.col("base_close").is_not_null())
        .unique(subset=["date", "ms_of_day"], keep="last")
        .sort(["date", "ms_of_day"])
        .collect()
    )
    stock_days = set(stock["date"].to_list())
    missing_days = sorted(set(days) - stock_days)
    if missing_days:
        raise ValueError(
            f"{symbol}. Stock M1 dates do not cover selected option M1 dates. "
            f"Missing {len(missing_days)} date(s); first missing: {_format_sample_dates(missing_days)}"
        )
    print(f"{symbol}. Stock M1 rows matched by date range: {stock.height}")
    return stock


def _fetch_stock_trading_days(
    client: ThetaClient,
    symbol: str,
    start_day: dt.date,
    end_day: dt.date,
) -> list[dt.date]:
    if start_day > end_day:
        return []

    frames: list[pl.DataFrame] = []
    for chunk_start, chunk_end in _iter_date_chunks(start_day, end_day, max_days=365):
        try:
            frame = client.get_csv_frame(
                "/stock/history/eod",
                {
                    "symbol": symbol,
                    "start_date": format_theta_date(chunk_start),
                    "end_date": format_theta_date(chunk_end),
                },
            )
        except (ThetaNoData, ThetaDataError):
            continue
        if not frame.is_empty():
            frames.append(frame)

    if not frames:
        raise ThetaNoData(f"No stock EOD trading days for {symbol} {start_day}..{end_day}")

    frame = pl.concat(frames, how="vertical_relaxed")
    days = (
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
        .select(pl.col("date").unique().sort())
    )
    return days["date"].to_list()


def _make_theta_client(settings: Settings) -> ThetaClient:
    return ThetaClient(
        base_url=settings.theta_base_url,
        timeout_seconds=settings.request_timeout_seconds,
        max_retries=settings.max_retries,
        retry_sleep_seconds=settings.retry_sleep_seconds,
    )


def _validate_concurrency(value: int, name: str) -> int:
    if value not in ALLOWED_CONCURRENCY:
        raise ValueError(f"{name} must be one of {sorted(ALLOWED_CONCURRENCY)}, got {value}")
    return value


def _validate_batch_mode(value: str) -> str:
    mode = value.lower()
    if mode not in ALLOWED_BATCH_MODES:
        raise ValueError(f"batch_mode must be one of {sorted(ALLOWED_BATCH_MODES)}, got {value}")
    return mode


def _load_dividend_curve_for_dates(symbol: str, settings: Settings, expected_days: list[dt.date]) -> pl.DataFrame:
    expected_days = sorted(set(expected_days))
    start_day = expected_days[0]
    end_day = expected_days[-1]
    print(f"{symbol}. Checking local Tiingo dividend data for M1 {start_day}..{end_day}")
    dividend_curve = load_tiingo_dividend_ttm(symbol, settings)
    if dividend_curve.is_empty():
        raise ValueError(f"{symbol}. Tiingo dividend data not found. Update Tiingo manually and rerun.")

    tiingo_days = set(dividend_curve.filter(pl.col("date").is_between(start_day, end_day))["date"].to_list())
    missing_days = sorted(set(expected_days) - tiingo_days)
    print(f"{symbol}. Tiingo matching dates for M1: {len(expected_days) - len(missing_days)}/{len(expected_days)}")
    if missing_days:
        raise ValueError(
            f"{symbol}. Tiingo dates do not cover selected M1 dates {start_day}..{end_day}. "
            f"Missing {len(missing_days)} date(s); first missing: {_format_sample_dates(missing_days)}"
        )
    return dividend_curve


def _load_risk_free_curve_for_dates(settings: Settings, expected_days: list[dt.date]) -> pl.DataFrame:
    expected_days = sorted(set(expected_days))
    start_day = expected_days[0]
    end_day = expected_days[-1]
    print(f"DGS1. Preparing local history for M1 {start_day}..{end_day}")
    curve = ensure_local_dgs1_history(settings, required_start=start_day, required_end=end_day)
    if curve.is_empty():
        raise ValueError("DGS1 local history is empty")
    return curve


def _attach_dividend_yield(df: pl.DataFrame, dividend_curve: pl.DataFrame) -> pl.DataFrame:
    result = (
        df.join(dividend_curve, how="left", on="date")
        .with_columns((pl.col("div_ttm") / pl.col("base_close")).cast(pl.Float64).alias("dividend_yield"))
        .drop("div_ttm")
    )
    if result["dividend_yield"].null_count() > 0:
        first_date = result.filter(pl.col("dividend_yield").is_null())["date"].min()
        raise ValueError(f"{first_date}: dividend_yield is missing after Tiingo join")
    return result


def _resolve_option_m1_paths(
    settings: Settings,
    symbol: str,
    input_paths: list[str | Path] | None,
) -> list[Path]:
    if input_paths:
        return [Path(path).resolve() for path in input_paths]

    pattern = f"*_{symbol}_m1_opts.parquet"
    return sorted(path.resolve() for path in settings.m1_options_dir.glob(pattern) if path.is_file())


def _find_stock_m1_paths(settings: Settings, symbol: str) -> list[Path]:
    pattern = f"*_{symbol}_m1_stock.parquet"
    return sorted(path.resolve() for path in settings.stock_m1_dir.glob(pattern) if path.is_file())


def _find_year_files(directory: Path, symbol: str, suffix: str, year: int) -> list[Path]:
    pattern = f"*_{symbol}{suffix}"
    matched: list[tuple[dt.date, dt.date, Path]] = []
    for path in directory.glob(pattern):
        if not path.is_file():
            continue
        bounds = _parse_named_bounds(path, symbol, suffix)
        if bounds is None:
            continue
        start_day, end_day = bounds
        if start_day.year <= year <= end_day.year:
            matched.append((start_day, end_day, path.resolve()))
    matched.sort(key=lambda item: (item[0], item[1], item[2].name))
    return [path for _, _, path in matched]


def _build_year_output_path(
    directory: Path,
    symbol: str,
    suffix: str,
    days: list[dt.date],
    existing_paths: list[Path] | Path | None,
) -> Path:
    if not days and not existing_paths:
        raise ValueError(f"{symbol}. cannot build yearly output path for empty day set and empty existing set: {suffix}")

    if isinstance(existing_paths, Path):
        existing_list = [existing_paths]
    else:
        existing_list = existing_paths or []

    min_day = min(days) if days else dt.date.max
    max_day = max(days) if days else dt.date.min
    for path in existing_list:
        bounds = _parse_named_bounds(path, symbol, suffix)
        if bounds is None:
            continue
        start_day, end_day = bounds
        min_day = min(min_day, start_day)
        max_day = max(max_day, end_day)

    if min_day == dt.date.max or max_day == dt.date.min:
        raise ValueError(f"{symbol}. could not determine date bounds for {suffix}")
    return (directory / f"{format_theta_date(min_day)}_{format_theta_date(max_day)}_{symbol}{suffix}").resolve()


def _preview_year_output_paths(
    directory: Path,
    symbol: str,
    suffix: str,
    year_groups: dict[int, list[dt.date]],
) -> list[Path]:
    paths: list[Path] = []
    for year in sorted(year_groups):
        days = year_groups[year]
        existing_paths = _find_year_files(directory, symbol, suffix, year)
        paths.append(_build_year_output_path(directory, symbol, suffix, days, existing_paths))
    return paths


def _group_days_by_year(days: list[dt.date]) -> dict[int, list[dt.date]]:
    groups: dict[int, list[dt.date]] = {}
    for day in sorted(set(days)):
        groups.setdefault(day.year, []).append(day)
    return groups


def _collect_dates_from_paths(paths: list[Path]) -> set[dt.date]:
    result: set[dt.date] = set()
    for path in paths:
        result.update(_read_m1_dates(path))
    return result


def _filter_days(
    days: list[dt.date],
    from_date: dt.date | None,
    end_date: dt.date | None,
) -> list[dt.date]:
    result = days
    if from_date is not None:
        result = [day for day in result if day >= from_date]
    if end_date is not None:
        result = [day for day in result if day <= end_date]
    return result


def _normalized_date_expr(schema: pl.Schema, column: str) -> pl.Expr:
    dtype = schema[column]
    if dtype == pl.Date:
        return pl.col(column).cast(pl.Date)
    if dtype == pl.Datetime:
        return pl.col(column).dt.date()
    return pl.col(column).cast(pl.String).str.strptime(pl.Date, "%Y%m%d", strict=False)


def _date_equals_expr(schema: pl.Schema, column: str, day: dt.date) -> pl.Expr:
    dtype = schema[column]
    if dtype == pl.Date:
        return pl.col(column) == pl.lit(day)
    if dtype == pl.Datetime:
        return pl.col(column).dt.date() == pl.lit(day)
    return pl.col(column) == int(format_theta_date(day))


def _format_sample_dates(days: list[dt.date], limit: int = 10) -> str:
    sample = ", ".join(day.isoformat() for day in days[:limit])
    if len(days) > limit:
        return f"{sample}, ..."
    return sample


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


def _build_batch_ranges(days: list[dt.date], batch_mode: str) -> list[tuple[dt.date, dt.date, list[dt.date]]]:
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
            raise ValueError(f"Unsupported M1 batch_mode: {batch_mode}")
        grouped.setdefault(key, []).append(day)

    return [(min(group_days), max(group_days), group_days) for _, group_days in sorted(grouped.items())]


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


def _merge_stock_m1_year_file(
    output_path: Path,
    symbol: str,
    staged_paths: list[Path],
    existing_paths: list[Path],
) -> Path:
    inputs = [path for path in existing_paths if path.exists()] + [path for path in staged_paths if path.exists()]
    if not inputs:
        raise ValueError(f"{symbol}. no stock M1 inputs to merge for {output_path.name}")

    lf = pl.scan_parquet([str(path) for path in inputs])
    schema = lf.collect_schema()
    merged = (
        lf.with_columns(
            pl.col("ticker").cast(pl.String).str.to_uppercase().alias("ticker"),
            _normalized_date_expr(schema, "date").alias("_date"),
            pl.col("ms_of_day").cast(pl.Int64).alias("ms_of_day"),
        )
        .filter(pl.col("_date").is_not_null())
        .unique(subset=["ticker", "_date", "ms_of_day"], keep="last")
        .sort(["ticker", "_date", "ms_of_day"])
    )
    bounds = merged.select(
        pl.col("_date").min().alias("start_day"),
        pl.col("_date").max().alias("end_day"),
    ).collect().row(0, named=True)
    start_day = bounds["start_day"]
    end_day = bounds["end_day"]
    if start_day is None or end_day is None:
        raise ValueError(f"{symbol}. stock M1 merge produced empty result for {output_path.name}")
    final_path = (
        output_path.parent
        / f"{format_theta_date(start_day)}_{format_theta_date(end_day)}_{symbol}_m1_stock.parquet"
    ).resolve()
    tmp_path = final_path.with_name(f"{final_path.stem}.tmp{final_path.suffix}")
    if tmp_path.exists():
        tmp_path.unlink()
    (
        merged.select(
            pl.col("ms_of_day").cast(pl.Int64),
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.Int64),
            pl.col("count").cast(pl.Int64),
            pl.col("_date").dt.strftime("%Y%m%d").cast(pl.Int64).alias("date"),
            pl.col("ticker").cast(pl.String),
        )
        .sink_parquet(tmp_path)
    )
    tmp_path.replace(final_path)
    _unlink_all(existing_paths, keep=final_path)
    return final_path


def _merge_option_m1_year_file(
    output_path: Path,
    symbol: str,
    staged_paths: list[Path],
    existing_paths: list[Path],
) -> Path:
    inputs = [path for path in existing_paths if path.exists()] + [path for path in staged_paths if path.exists()]
    if not inputs:
        raise ValueError(f"{symbol}. no option M1 inputs to merge for {output_path.name}")

    lf = pl.scan_parquet([str(path) for path in inputs])
    schema = lf.collect_schema()
    merged = (
        lf.with_columns(
            pl.col("ticker").cast(pl.String).str.to_uppercase().alias("ticker"),
            _normalized_date_expr(schema, "date").alias("date"),
            _normalized_date_expr(schema, "expiration").alias("expiration"),
            pl.col("strike").cast(pl.Int64).alias("strike"),
            pl.col("right").cast(pl.String).str.to_uppercase().str.slice(0, 1).alias("right"),
            pl.col("ms_of_day").cast(pl.Int64).alias("ms_of_day"),
        )
        .filter(pl.col("date").is_not_null())
        .filter(pl.col("expiration").is_not_null())
        .unique(subset=["ticker", "expiration", "strike", "right", "date", "ms_of_day"], keep="last")
        .sort(["ticker", "date", "ms_of_day", "expiration", "strike", "right"])
    )
    bounds = merged.select(
        pl.col("date").min().alias("start_day"),
        pl.col("date").max().alias("end_day"),
    ).collect().row(0, named=True)
    start_day = bounds["start_day"]
    end_day = bounds["end_day"]
    if start_day is None or end_day is None:
        raise ValueError(f"{symbol}. option M1 merge produced empty result for {output_path.name}")
    final_path = (
        output_path.parent
        / f"{format_theta_date(start_day)}_{format_theta_date(end_day)}_{symbol}_m1_opts.parquet"
    ).resolve()
    tmp_path = final_path.with_name(f"{final_path.stem}.tmp{final_path.suffix}")
    if tmp_path.exists():
        tmp_path.unlink()
    (
        merged.select(
            pl.col("ticker").cast(pl.String),
            pl.col("expiration").dt.strftime("%Y%m%d").cast(pl.Int64).alias("expiration"),
            pl.col("strike").cast(pl.Int64),
            pl.col("right").cast(pl.String),
            pl.col("ms_of_day").cast(pl.Int64),
            pl.col("bid_size").cast(pl.Int64),
            pl.col("bid_exchange").cast(pl.Int64),
            pl.col("bid").cast(pl.Float64),
            pl.col("bid_condition").cast(pl.Int64),
            pl.col("ask_size").cast(pl.Int64),
            pl.col("ask_exchange").cast(pl.Int64),
            pl.col("ask").cast(pl.Float64),
            pl.col("ask_condition").cast(pl.Int64),
            pl.col("date").cast(pl.Date),
        )
        .sink_parquet(tmp_path)
    )
    tmp_path.replace(final_path)
    _unlink_all(existing_paths, keep=final_path)
    return final_path


def _merge_m1_with_greeks_year_file(
    output_path: Path,
    symbol: str,
    staged: pl.DataFrame,
    existing_paths: list[Path],
    replace_days: set[dt.date],
) -> Path:
    staged_lf = staged.lazy()
    if existing_paths:
        existing_lf = pl.scan_parquet([str(path) for path in existing_paths])
        existing_schema = existing_lf.collect_schema()
        existing_lf = existing_lf.with_columns(_normalized_date_expr(existing_schema, "date").alias("date"))
        if replace_days:
            existing_lf = existing_lf.filter(~pl.col("date").is_in(sorted(replace_days)))
        combined = pl.concat([existing_lf, staged_lf], how="vertical_relaxed")
    else:
        combined = staged_lf

    merged = (
        combined.with_columns(
            pl.col("ticker").cast(pl.String).str.to_uppercase().alias("ticker"),
            pl.col("expiration").cast(pl.Date),
            pl.col("right").cast(pl.String).str.to_lowercase().str.slice(0, 1).alias("right"),
            pl.col("date").cast(pl.Date),
            pl.col("ms_of_day").cast(pl.Int32),
        )
        .filter(pl.col("date").is_not_null())
        .filter(pl.col("expiration").is_not_null())
        .unique(subset=["ticker", "expiration", "strike", "right", "date", "ms_of_day"], keep="last")
        .sort(["ticker", "date", "ms_of_day", "expiration", "strike", "right"])
        .with_columns(
            *[pl.col(column).cast(pl.Int16) for column in M1_INT16_COLUMNS],
            *[pl.col(column).cast(pl.Int32) for column in M1_INT32_COLUMNS],
            *[pl.col(column).cast(pl.Float32) for column in M1_FLOAT32_COLUMNS],
            pl.col("ticker").cast(pl.String),
            pl.col("expiration").cast(pl.Date),
            pl.col("right").cast(pl.String),
            pl.col("date").cast(pl.Date),
        )
        .select(M1_FINAL_COLUMNS)
    )
    bounds = merged.select(
        pl.col("date").min().alias("start_day"),
        pl.col("date").max().alias("end_day"),
    ).collect().row(0, named=True)
    start_day = bounds["start_day"]
    end_day = bounds["end_day"]
    if start_day is None or end_day is None:
        raise ValueError(f"{symbol}. M1 with-greeks merge produced empty result for {output_path.name}")
    final_path = (
        output_path.parent
        / f"{format_theta_date(start_day)}_{format_theta_date(end_day)}_{symbol}_m1_greeks_opts.parquet"
    ).resolve()
    tmp_path = final_path.with_name(f"{final_path.stem}.tmp{final_path.suffix}")
    if tmp_path.exists():
        tmp_path.unlink()
    merged.sink_parquet(tmp_path)
    tmp_path.replace(final_path)
    _unlink_all(existing_paths, keep=final_path)
    return final_path
