from __future__ import annotations

import argparse

from .dates import parse_date, previous_business_day
from .ib.lattency import run as run_lattency_analysis
from .loaders.eod import benchmark_eod_download, update_eod_with_greeks
from .loaders.m1 import (
    download_m1_data,
    download_option_m1_data,
    download_stock_m1_data,
    enrich_m1_with_greeks,
    update_m1_data_with_greeks,
)
from .loaders.tick import (
    append_stock_ticks,
    ensure_option_quote_window,
    fetch_stock_quote_window,
    fetch_stock_trade_window,
)
from .pipeline_config import DEFAULT_CONFIG_PATH, load_pipeline_config
from .rates import DGS1_HISTORY_START, ensure_local_dgs1_history
from .runners.tick import run as run_tick_from_config
from .settings import get_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="thetadata-pipeline")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to pipeline TOML config")
    subparsers = parser.add_subparsers(dest="command", required=True)

    eod_parser = subparsers.add_parser("eod", help="EOD data commands")
    eod_subparsers = eod_parser.add_subparsers(dest="eod_command", required=True)

    update_parser = eod_subparsers.add_parser("update", help="Download missing EOD data and calculate greeks")
    update_parser.add_argument("--symbol", default=None)
    update_parser.add_argument("--symbols", nargs="+", default=None)
    update_parser.add_argument("--from-date", default=None, help="YYYYMMDD or YYYY-MM-DD")
    update_parser.add_argument("--end-date", default=None, help="YYYYMMDD or YYYY-MM-DD")
    update_parser.add_argument("--max-days", type=int, default=None)
    update_parser.add_argument("--max-dte", type=int, default=None)
    update_parser.add_argument("--strike-range", type=int, default=None)
    update_parser.add_argument("--batch-mode", choices=["daily", "weekly", "monthly", "yearly", "all"], default=None)
    update_parser.add_argument("--dry-run", action="store_true")
    update_parser.set_defaults(func=_run_eod_update)

    benchmark_parser = eod_subparsers.add_parser("benchmark", help="Benchmark EOD request batch sizes without writing files")
    benchmark_parser.add_argument("--symbol", default=None)
    benchmark_parser.add_argument("--from-date", required=True, help="YYYYMMDD or YYYY-MM-DD")
    benchmark_parser.add_argument("--end-date", required=True, help="YYYYMMDD or YYYY-MM-DD")
    benchmark_parser.add_argument("--modes", nargs="+", default=["daily", "weekly", "monthly"])
    benchmark_parser.add_argument("--max-dte", type=int, default=None)
    benchmark_parser.add_argument("--strike-range", type=int, default=None)
    benchmark_parser.set_defaults(func=_run_eod_benchmark)

    m1_parser = subparsers.add_parser("m1", help="M1 data commands")
    m1_subparsers = m1_parser.add_subparsers(dest="m1_command", required=True)

    m1_update_parser = m1_subparsers.add_parser("update", help="Download missing local M1 data and calculate greeks")
    _add_m1_download_arguments(m1_update_parser)
    m1_update_parser.add_argument("--skip-enrich", action="store_true")
    m1_update_parser.add_argument("--overwrite-greeks", action="store_true")
    m1_update_parser.set_defaults(func=_run_m1_update)

    m1_download_parser = m1_subparsers.add_parser("download", help="Download missing local M1 stock and option parquet files")
    _add_m1_download_arguments(m1_download_parser)
    m1_download_parser.set_defaults(func=_run_m1_download)

    m1_stock_download_parser = m1_subparsers.add_parser("download-stock", help="Download missing local M1 stock parquet files")
    _add_m1_download_arguments(m1_stock_download_parser)
    m1_stock_download_parser.set_defaults(func=_run_m1_download_stock)

    m1_option_download_parser = m1_subparsers.add_parser("download-options", help="Download missing local M1 option parquet files")
    _add_m1_download_arguments(m1_option_download_parser)
    m1_option_download_parser.set_defaults(func=_run_m1_download_options)

    m1_enrich_parser = m1_subparsers.add_parser("enrich", help="Calculate IV and greeks for local option M1 parquet files")
    m1_enrich_parser.add_argument("--symbol", default=None)
    m1_enrich_parser.add_argument("--symbols", nargs="+", default=None)
    m1_enrich_parser.add_argument("--input-files", nargs="+", default=None)
    m1_enrich_parser.add_argument("--from-date", default=None, help="YYYYMMDD or YYYY-MM-DD")
    m1_enrich_parser.add_argument("--end-date", default=None, help="YYYYMMDD or YYYY-MM-DD")
    m1_enrich_parser.add_argument("--max-days", type=int, default=None)
    m1_enrich_parser.add_argument("--overwrite", action="store_true")
    m1_enrich_parser.add_argument("--dry-run", action="store_true")
    m1_enrich_parser.set_defaults(func=_run_m1_enrich)

    rates_parser = subparsers.add_parser("rates", help="Reference rates commands")
    rates_subparsers = rates_parser.add_subparsers(dest="rates_command", required=True)
    rates_update_parser = rates_subparsers.add_parser("update", help="Update local DGS1 history from FRED")
    rates_update_parser.add_argument("--from-date", default=None, help="YYYYMMDD or YYYY-MM-DD")
    rates_update_parser.add_argument("--end-date", default=None, help="YYYYMMDD or YYYY-MM-DD")
    rates_update_parser.set_defaults(func=_run_rates_update)

    tick_parser = subparsers.add_parser("tick", help="Tick and second data commands")
    tick_subparsers = tick_parser.add_subparsers(dest="tick_command", required=True)

    tick_run_parser = tick_subparsers.add_parser("run", help="Run tick downloader from pipeline.toml")
    tick_run_parser.add_argument("--mode", choices=["stock_quote", "stock_trade", "option_quote"], default=None)
    tick_run_parser.add_argument("--date", default=None, help="YYYYMMDD or YYYY-MM-DD")
    tick_run_parser.add_argument("--start-ms", type=int, default=None)
    tick_run_parser.add_argument("--end-ms", type=int, default=None)
    tick_run_parser.add_argument("--interval", default=None)
    tick_run_parser.add_argument("--dry-run", action="store_true")
    tick_run_parser.set_defaults(func=_run_tick_config)

    stock_quote_parser = tick_subparsers.add_parser("stock-quote", help="Download stock quote rows for a time window")
    _add_tick_window_arguments(stock_quote_parser)
    stock_quote_parser.add_argument("--interval", default="1s", help="Theta interval: tick, 10ms, 100ms, 1s, ...")
    stock_quote_parser.set_defaults(func=_run_tick_stock_quote)

    stock_trade_parser = tick_subparsers.add_parser("stock-trade", help="Download stock trade rows for a time window")
    _add_tick_window_arguments(stock_trade_parser)
    stock_trade_parser.set_defaults(func=_run_tick_stock_trade)

    option_quote_parser = tick_subparsers.add_parser("option-quote", help="Download option quote rows for a time window")
    _add_tick_window_arguments(option_quote_parser)
    option_quote_parser.add_argument("--expiration", required=True, help="YYYYMMDD or YYYY-MM-DD")
    option_quote_parser.add_argument("--strike", type=float, required=True, help="Option strike in dollars or raw strike*1000")
    option_quote_parser.add_argument("--right", required=True, choices=["c", "p", "call", "put", "C", "P", "CALL", "PUT"])
    option_quote_parser.add_argument("--interval", default="100ms", help="Theta interval: tick, 10ms, 100ms, 1s, ...")
    option_quote_parser.set_defaults(func=_run_tick_option_quote)

    lattency_parser = subparsers.add_parser("lattency", help="IB trades lattency analysis")
    lattency_subparsers = lattency_parser.add_subparsers(dest="lattency_command", required=True)
    lattency_run_parser = lattency_subparsers.add_parser("run", help="Calculate entry/exit lattency from IB trades")
    lattency_run_parser.add_argument("--account", default=None, help="IB account id. Defaults to analysis.account_id")
    lattency_run_parser.add_argument("--stock-concurrency", type=int, choices=[1, 2, 4, 8], default=None)
    lattency_run_parser.add_argument("--tick-concurrency", type=int, choices=[1, 2, 4, 8], default=None)
    lattency_run_parser.add_argument("--dry-run", action="store_true")
    lattency_run_parser.set_defaults(func=_run_lattency)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


def _run_eod_update(args: argparse.Namespace) -> int:
    settings = get_settings()
    config = load_pipeline_config(args.config)
    symbols = _resolve_symbols(args, config.eod.symbols, settings.default_symbol)

    for symbol in symbols:
        summary = update_eod_with_greeks(
            settings=settings,
            symbol=symbol,
            from_date=args.from_date or config.eod.from_date,
            end_date=args.end_date or config.eod.end_date,
            max_days=args.max_days,
            max_dte=args.max_dte if args.max_dte is not None else config.eod.max_dte,
            strike_range=args.strike_range if args.strike_range is not None else config.eod.strike_range,
            batch_mode=args.batch_mode or config.eod.batch_mode,
            dry_run=args.dry_run,
        )
        mode = "dry-run" if summary.dry_run else "updated"
        print(f"EOD {mode}: symbol={summary.symbol}")
        print(f"final_path={summary.final_path}")
        print(f"candidate_days={summary.candidates}")
        print(f"requests={summary.requests}")
        print(f"downloaded_days={summary.downloaded_days}")
        print(f"skipped_days={summary.skipped_days}")
        print(f"rows_added={summary.rows_added}")
        if summary.start_date and summary.end_date:
            print(f"date_range={summary.start_date:%Y-%m-%d}..{summary.end_date:%Y-%m-%d}")

    return 0


def _run_eod_benchmark(args: argparse.Namespace) -> int:
    settings = get_settings()
    config = load_pipeline_config(args.config)
    if args.symbol:
        symbol = args.symbol.upper()
    elif config.eod.symbols:
        symbol = config.eod.symbols[0].upper()
    else:
        symbol = settings.default_symbol.upper()
    results = benchmark_eod_download(
        settings=settings,
        symbol=symbol,
        start_date=args.from_date,
        end_date=args.end_date,
        batch_modes=args.modes,
        max_dte=args.max_dte if args.max_dte is not None else config.eod.max_dte,
        strike_range=args.strike_range if args.strike_range is not None else config.eod.strike_range,
    )
    print(f"EOD benchmark: symbol={symbol} range={args.from_date}..{args.end_date}")
    for result in results:
        print(
            f"{result.batch_mode}: requests={result.requests} "
            f"seconds={result.seconds:.3f} rows={result.rows} MB={result.megabytes:.2f}"
        )
    return 0


def _add_m1_download_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--symbols", nargs="+", default=None)
    parser.add_argument("--from-date", default=None, help="YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--max-days", type=int, default=None)
    parser.add_argument("--batch-mode", choices=["daily", "weekly", "monthly", "yearly", "all"], default=None)
    parser.add_argument("--stock-concurrency", type=int, choices=[1, 2, 4, 8], default=None)
    parser.add_argument("--option-concurrency", type=int, choices=[1, 2, 4, 8], default=None)
    parser.add_argument("--option-expiration-mode", choices=["same_day", "all"], default=None)
    parser.add_argument(
        "--overwrite-raw",
        action="store_true",
        help="Backward-compatible alias: same as --overwrite-stock-raw --overwrite-option-raw",
    )
    parser.add_argument("--overwrite-stock-raw", action="store_true")
    parser.add_argument("--overwrite-option-raw", action="store_true")
    parser.add_argument("--dry-run", action="store_true")


def _add_tick_window_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--date", required=True, help="YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--start-ms", type=int, required=True)
    parser.add_argument("--end-ms", type=int, required=True)


def _run_m1_update(args: argparse.Namespace) -> int:
    settings = get_settings()
    config = load_pipeline_config(args.config)
    symbols = _resolve_symbols(args, config.m1.symbols, settings.default_symbol)

    overwrite_stock_raw = args.overwrite_raw or args.overwrite_stock_raw
    overwrite_option_raw = args.overwrite_raw or args.overwrite_option_raw

    for symbol in symbols:
        summary = update_m1_data_with_greeks(
            settings=settings,
            symbol=symbol,
            from_date=args.from_date or config.m1.from_date,
            end_date=args.end_date or config.m1.end_date,
            max_days=args.max_days,
            batch_mode=args.batch_mode or config.m1.batch_mode,
            stock_concurrency=args.stock_concurrency or config.m1.stock_concurrency,
            option_concurrency=args.option_concurrency or config.m1.option_concurrency,
            option_expiration_mode=args.option_expiration_mode or config.m1.option_expiration_mode,
            overwrite_stock_raw=overwrite_stock_raw,
            overwrite_option_raw=overwrite_option_raw,
            overwrite_greeks=args.overwrite_greeks,
            skip_enrich=args.skip_enrich,
            dry_run=args.dry_run,
        )
        _print_m1_download_summary(summary.download, label="M1 update download")
        if summary.enrich is not None:
            _print_m1_enrich_summary(summary.enrich, label="M1 update enrich")

    return 0


def _run_m1_download(args: argparse.Namespace) -> int:
    settings = get_settings()
    config = load_pipeline_config(args.config)
    symbols = _resolve_symbols(args, config.m1.symbols, settings.default_symbol)

    overwrite_stock_raw = args.overwrite_raw or args.overwrite_stock_raw
    overwrite_option_raw = args.overwrite_raw or args.overwrite_option_raw

    for symbol in symbols:
        summary = download_m1_data(
            settings=settings,
            symbol=symbol,
            from_date=args.from_date or config.m1.from_date,
            end_date=args.end_date or config.m1.end_date,
            max_days=args.max_days,
            batch_mode=args.batch_mode or config.m1.batch_mode,
            stock_concurrency=args.stock_concurrency or config.m1.stock_concurrency,
            option_concurrency=args.option_concurrency or config.m1.option_concurrency,
            option_expiration_mode=args.option_expiration_mode or config.m1.option_expiration_mode,
            overwrite_stock_raw=overwrite_stock_raw,
            overwrite_option_raw=overwrite_option_raw,
            dry_run=args.dry_run,
        )
        _print_m1_download_summary(summary, label="M1 download")

    return 0


def _run_m1_download_stock(args: argparse.Namespace) -> int:
    settings = get_settings()
    config = load_pipeline_config(args.config)
    symbols = _resolve_symbols(args, config.m1.symbols, settings.default_symbol)

    for symbol in symbols:
        summary = download_stock_m1_data(
            settings=settings,
            symbol=symbol,
            from_date=args.from_date or config.m1.from_date,
            end_date=args.end_date or config.m1.end_date,
            max_days=args.max_days,
            batch_mode=args.batch_mode or config.m1.batch_mode,
            stock_concurrency=args.stock_concurrency or config.m1.stock_concurrency,
            overwrite_stock_raw=args.overwrite_raw or args.overwrite_stock_raw,
            dry_run=args.dry_run,
        )
        _print_m1_download_summary(summary, label="M1 stock download")

    return 0


def _run_m1_download_options(args: argparse.Namespace) -> int:
    settings = get_settings()
    config = load_pipeline_config(args.config)
    symbols = _resolve_symbols(args, config.m1.symbols, settings.default_symbol)

    for symbol in symbols:
        summary = download_option_m1_data(
            settings=settings,
            symbol=symbol,
            from_date=args.from_date or config.m1.from_date,
            end_date=args.end_date or config.m1.end_date,
            max_days=args.max_days,
            batch_mode=args.batch_mode or config.m1.batch_mode,
            option_concurrency=args.option_concurrency or config.m1.option_concurrency,
            option_expiration_mode=args.option_expiration_mode or config.m1.option_expiration_mode,
            overwrite_option_raw=args.overwrite_raw or args.overwrite_option_raw,
            dry_run=args.dry_run,
        )
        _print_m1_download_summary(summary, label="M1 option download")

    return 0


def _run_m1_enrich(args: argparse.Namespace) -> int:
    settings = get_settings()
    config = load_pipeline_config(args.config)
    symbols = _resolve_symbols(args, config.m1.symbols, settings.default_symbol)

    for symbol in symbols:
        summary = enrich_m1_with_greeks(
            settings=settings,
            symbol=symbol,
            input_paths=args.input_files,
            from_date=args.from_date or config.m1.from_date,
            end_date=args.end_date or config.m1.end_date,
            max_days=args.max_days,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
        )
        _print_m1_enrich_summary(summary, label="M1 enrich")

    return 0


def _print_m1_download_summary(summary, label: str) -> None:
    mode = "dry-run" if summary.dry_run else "done"
    print(f"{label} {mode}: symbol={summary.symbol}")
    print(f"trading_days={summary.trading_days}")
    print(f"stock_candidate_days={summary.stock_candidate_days}")
    print(f"option_candidate_days={summary.option_candidate_days}")
    print(f"stock_request_batches={summary.stock_request_batches}")
    print(f"option_request_batches={summary.option_request_batches}")
    print(f"stock_downloaded_days={summary.stock_downloaded_days}")
    print(f"option_downloaded_days={summary.option_downloaded_days}")
    print(f"option_no_data_days={summary.option_no_data_days}")
    print(f"stock_rows={summary.stock_rows}")
    print(f"option_rows={summary.option_rows}")
    print(f"stock_output_path={summary.stock_output_path}")
    print(f"option_output_path={summary.option_output_path}")


def _print_m1_enrich_summary(summary, label: str) -> None:
    mode = "dry-run" if summary.dry_run else "done"
    print(f"{label} {mode}: symbol={summary.symbol}")
    print(f"input_files={summary.input_files}")
    print(f"processed_files={summary.processed_files}")
    print(f"skipped_files={summary.skipped_files}")
    print(f"processed_days={summary.processed_days}")
    print(f"rows_raw={summary.rows_raw}")
    print(f"rows_filtered_before_stock={summary.rows_filtered_before_stock}")
    print(f"rows_non_positive_tte={summary.rows_non_positive_tte}")
    print(f"stock_days_redownloaded={summary.stock_days_redownloaded}")
    print(f"rows_read={summary.rows_read}")
    print(f"rows_written={summary.rows_written}")
    print(f"rows_without_stock={summary.rows_without_stock}")
    print(f"output_dir={summary.output_dir}")


def _resolve_symbols(args: argparse.Namespace, config_symbols: list[str], default_symbol: str) -> list[str]:
    if args.symbols:
        raw_symbols = args.symbols
    elif args.symbol:
        raw_symbols = [args.symbol]
    else:
        raw_symbols = config_symbols or [default_symbol]

    symbols: list[str] = []
    for raw in raw_symbols:
        symbols.extend(part.strip().upper() for part in raw.split(",") if part.strip())
    return symbols


def _run_rates_update(args: argparse.Namespace) -> int:
    settings = get_settings()
    start = parse_date(args.from_date) or DGS1_HISTORY_START
    end = parse_date(args.end_date) or previous_business_day()
    if start > end:
        raise ValueError(f"from-date must be <= end-date, got {start}..{end}")

    curve = ensure_local_dgs1_history(settings, required_start=start, required_end=end)
    if curve.is_empty():
        print("rates_update_ok=False")
        print(f"path={settings.risk_free_rates_path}")
        print("rows=0")
        return 1

    print("rates_update_ok=True")
    print(f"path={settings.risk_free_rates_path}")
    print(f"rows={curve.height}")
    print(f"date_range={curve['date'].min()}..{curve['date'].max()}")
    return 0


def _run_tick_config(args: argparse.Namespace) -> int:
    return run_tick_from_config(
        mode=args.mode,
        date=args.date,
        start_ms=args.start_ms,
        end_ms=args.end_ms,
        interval=args.interval,
        dry_run=args.dry_run,
    )


def _run_lattency(args: argparse.Namespace) -> int:
    settings = get_settings()
    config = load_pipeline_config(args.config)
    return run_lattency_analysis(
        account=args.account or config.analysis.account_id,
        settings=settings,
        batch_mode=config.m1.batch_mode,
        stock_concurrency=args.stock_concurrency or config.m1.stock_concurrency,
        tick_concurrency=args.tick_concurrency or config.m1.option_concurrency,
        dry_run=args.dry_run,
    )


def _run_tick_stock_quote(args: argparse.Namespace) -> int:
    settings = get_settings()
    day = _required_date(args.date, "date")
    frame = fetch_stock_quote_window(
        settings=settings,
        symbol=(args.symbol or settings.default_symbol).upper(),
        day=day,
        start_ms=args.start_ms,
        end_ms=args.end_ms,
    )
    _print_tick_frame(frame, "stock_quote")
    if args.interval.lower() == "tick":
        output_path = append_stock_ticks(settings, (args.symbol or settings.default_symbol).upper(), frame)
        print(f"stock_tick_output_path={output_path}")
    return 0


def _run_tick_stock_trade(args: argparse.Namespace) -> int:
    settings = get_settings()
    day = _required_date(args.date, "date")
    frame = fetch_stock_trade_window(
        settings=settings,
        symbol=(args.symbol or settings.default_symbol).upper(),
        day=day,
        start_ms=args.start_ms,
        end_ms=args.end_ms,
    )
    _print_tick_frame(frame, "stock_trade")
    return 0


def _run_tick_option_quote(args: argparse.Namespace) -> int:
    settings = get_settings()
    day = _required_date(args.date, "date")
    expiration = _required_date(args.expiration, "expiration")
    frame = ensure_option_quote_window(
        settings=settings,
        symbol=(args.symbol or settings.default_symbol).upper(),
        day=day,
        expiration=expiration,
        strike=args.strike,
        right=args.right,
        start_ms=args.start_ms,
        end_ms=args.end_ms,
        interval=args.interval,
    )
    _print_tick_frame(frame, "option_quote")
    return 0


def _required_date(raw: str, name: str):
    parsed = parse_date(raw)
    if parsed is None:
        raise ValueError(f"{name} is required")
    return parsed


def _print_tick_frame(frame, label: str) -> None:
    print(f"{label}_ok=True")
    print(f"rows={frame.height}")
    if frame.height:
        print(f"columns={','.join(frame.columns)}")
        print(f"first={frame.row(0, named=True)}")
        print(f"last={frame.row(frame.height - 1, named=True)}")
