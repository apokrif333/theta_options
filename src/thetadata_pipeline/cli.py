from __future__ import annotations

import argparse

from .dates import parse_date, previous_business_day
from .loaders.eod import benchmark_eod_download, update_eod_with_greeks
from .loaders.m1 import enrich_m1_with_greeks
from .pipeline_config import DEFAULT_CONFIG_PATH, load_pipeline_config
from .rates import DGS1_HISTORY_START, ensure_local_dgs1_history
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
        mode = "dry-run" if summary.dry_run else "enriched"
        print(f"M1 {mode}: symbol={summary.symbol}")
        print(f"input_files={summary.input_files}")
        print(f"processed_files={summary.processed_files}")
        print(f"skipped_files={summary.skipped_files}")
        print(f"processed_days={summary.processed_days}")
        print(f"rows_raw={summary.rows_raw}")
        print(f"rows_filtered_before_stock={summary.rows_filtered_before_stock}")
        print(f"rows_read={summary.rows_read}")
        print(f"rows_written={summary.rows_written}")
        print(f"rows_without_stock={summary.rows_without_stock}")
        print(f"output_dir={summary.output_dir}")

    return 0


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
