from __future__ import annotations

import datetime as dt
import gc
import time
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from ..constants import M1_FINAL_COLUMNS, M1_FLOAT32_COLUMNS, M1_INT16_COLUMNS, M1_INT32_COLUMNS
from ..dates import format_theta_date, parse_date
from ..greeks import add_risk_free_rate, calculate_iv_greeks
from ..rates import ensure_local_dgs1_history
from ..settings import Settings
from ..tiingo import load_tiingo_dividend_ttm


MARKET_CLOSE_MS = 16 * 60 * 60 * 1000
MS_PER_YEAR = 365 * 24 * 60 * 60 * 1000


@dataclass(frozen=True)
class M1FileResult:
    input_path: Path
    output_path: Path
    days: int
    rows_raw: int
    rows_filtered_before_stock: int
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
    rows_read: int
    rows_written: int
    rows_without_stock: int
    output_dir: Path
    dry_run: bool = False


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

        output_path = _m1_output_path(settings, symbol, raw_path, all_days, selected_days)
        if output_path.exists() and not overwrite:
            print(f"{symbol}. {raw_path.name}: output exists, skipped: {output_path}")
            skipped_files += 1
            if remaining_days == 0:
                break
            continue

        print(f"{symbol}. {raw_path.name}: selected {selected_days[0]}..{selected_days[-1]} ({len(selected_days)} days)")
        print(f"{symbol}. Output: {output_path}")
        if dry_run:
            processed_files += 1
            processed_days += len(selected_days)
            if remaining_days == 0:
                break
            continue

        result = _enrich_m1_file(
            settings=settings,
            symbol=symbol,
            raw_path=raw_path,
            output_path=output_path,
            selected_days=selected_days,
        )
        processed_files += 1
        processed_days += result.days
        rows_raw += result.rows_raw
        rows_filtered_before_stock += result.rows_filtered_before_stock
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
        .filter(pl.col("timeToExp") > 0)
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
) -> M1FileResult:
    started = time.perf_counter()
    staging_run_dir = settings.staging_m1_dir / f"{output_path.stem}_{time.time_ns()}"
    staging_run_dir.mkdir(parents=True, exist_ok=False)
    staged_paths: list[Path] = []

    stock_m1 = _load_stock_m1_for_dates(settings, symbol, selected_days)
    dividend_curve = _load_dividend_curve_for_dates(symbol, settings, selected_days)
    risk_free_curve = _load_risk_free_curve_for_dates(settings, selected_days)

    rows_read = 0
    rows_raw = 0
    rows_filtered_before_stock = 0
    rows_written = 0
    rows_without_stock = 0

    try:
        for day in selected_days:
            print(f"{symbol}. M1 processing {day}")
            raw_day = _read_m1_day(raw_path, day)
            raw_rows = raw_day.height
            rows_raw += raw_rows
            normalized = normalize_option_m1(raw_day, symbol=symbol)
            del raw_day
            if normalized.is_empty():
                print(f"{symbol}. {day}: empty normalized option M1, skipped; raw_rows={raw_rows}")
                continue

            filtered_before_stock = raw_rows - normalized.height
            rows_filtered_before_stock += filtered_before_stock
            if filtered_before_stock:
                print(f"{symbol}. {day}: rows filtered before stock join: {filtered_before_stock}")
            rows_read += normalized.height
            stock_day = stock_m1.filter(pl.col("date") == day)
            if stock_day.is_empty():
                raise ValueError(f"{symbol}. {day}: stock M1 close data is missing")

            normalized = normalized.join(stock_day, how="inner", on=["date", "ms_of_day"])
            missing_stock_rows = rows_read - rows_without_stock - rows_written - normalized.height
            if missing_stock_rows > 0:
                rows_without_stock += missing_stock_rows
            if normalized.is_empty():
                raise ValueError(f"{symbol}. {day}: all option M1 rows were dropped after stock M1 join")

            normalized = _attach_dividend_yield(normalized, dividend_curve)
            normalized = add_risk_free_rate(normalized, risk_free_curve)
            normalized = normalized.with_columns(pl.col("risk_free_rate").cast(pl.Float32).alias("dgs1"))

            with_greeks = calculate_iv_greeks(normalized, include_probability=False)
            del normalized
            result = align_m1_schema(with_greeks)
            del with_greeks

            staged_path = staging_run_dir / f"{symbol}_m1_greeks_{format_theta_date(day)}.parquet"
            result.write_parquet(staged_path)
            rows_written += result.height
            staged_paths.append(staged_path)
            del result
            gc.collect()

        if not staged_paths:
            raise ValueError(f"{symbol}. No staged M1 data was produced for {raw_path}")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = output_path.with_name(f"{output_path.stem}.tmp{output_path.suffix}")
        if tmp_path.exists():
            tmp_path.unlink()
        print(f"{symbol}. M1 writing final file: {output_path}")
        pl.scan_parquet([str(path) for path in staged_paths]).sink_parquet(tmp_path)
        tmp_path.replace(output_path)

        if rows_without_stock:
            print(f"{symbol}. M1 rows without stock minute close dropped: {rows_without_stock}")
        print(f"{symbol}. M1 file done in {time.perf_counter() - started:.2f}s; rows={rows_written}")

        return M1FileResult(
            input_path=raw_path,
            output_path=output_path,
            days=len(selected_days),
            rows_raw=rows_raw,
            rows_filtered_before_stock=rows_filtered_before_stock,
            rows_read=rows_read,
            rows_written=rows_written,
            rows_without_stock=rows_without_stock,
        )
    finally:
        for path in staged_paths:
            path.unlink(missing_ok=True)
        try:
            staging_run_dir.rmdir()
        except OSError:
            pass


def _read_m1_day(path: Path, day: dt.date) -> pl.DataFrame:
    lf = pl.scan_parquet(path)
    schema = lf.collect_schema()
    return lf.filter(_date_equals_expr(schema, "date", day)).collect()


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
    pattern = f"*{symbol}*.parquet"
    return sorted(path.resolve() for path in settings.stock_m1_dir.glob(pattern) if path.is_file())


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


def _m1_output_path(
    settings: Settings,
    symbol: str,
    raw_path: Path,
    all_days: list[dt.date],
    selected_days: list[dt.date],
) -> Path:
    if selected_days == all_days:
        output_name = raw_path.name.replace("_m1_opts.parquet", "_m1_greeks_opts.parquet")
    else:
        output_name = (
            f"{format_theta_date(selected_days[0])}_{format_theta_date(selected_days[-1])}_"
            f"{symbol}_m1_greeks_opts.parquet"
        )
    return (settings.m1_with_greeks_dir / output_name).resolve()


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
