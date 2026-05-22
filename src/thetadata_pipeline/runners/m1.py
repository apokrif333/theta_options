from __future__ import annotations

import argparse
import sys
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from thetadata_pipeline.loaders.m1 import (
    download_m1_data,
    download_option_m1_data,
    download_stock_m1_data,
    enrich_m1_with_greeks,
    update_m1_data_with_greeks,
)
from thetadata_pipeline.pipeline_config import load_pipeline_config
from thetadata_pipeline.settings import get_settings


def run(
    mode: str = "update",
    dry_run: bool = False,
    overwrite: bool = False,
    overwrite_raw: bool = False,
    overwrite_stock_raw: bool = False,
    overwrite_option_raw: bool = False,
    skip_enrich: bool = False,
    max_days: int | None = None,
    batch_mode: str | None = None,
) -> int:
    settings = get_settings()
    cfg = load_pipeline_config()
    symbols = cfg.m1.symbols or [settings.default_symbol]
    effective_batch_mode = batch_mode or cfg.m1.batch_mode

    effective_overwrite_stock_raw = overwrite_raw or overwrite_stock_raw
    effective_overwrite_option_raw = overwrite_raw or overwrite_option_raw

    for symbol in symbols:
        if mode == "update":
            summary = update_m1_data_with_greeks(
                settings=settings,
                symbol=symbol,
                from_date=cfg.m1.from_date,
                end_date=cfg.m1.end_date,
                max_days=max_days,
                batch_mode=effective_batch_mode,
                stock_concurrency=cfg.m1.stock_concurrency,
                option_concurrency=cfg.m1.option_concurrency,
                option_expiration_mode=cfg.m1.option_expiration_mode,
                overwrite_stock_raw=effective_overwrite_stock_raw,
                overwrite_option_raw=effective_overwrite_option_raw,
                overwrite_greeks=overwrite,
                skip_enrich=skip_enrich,
                dry_run=dry_run,
            )
            _print_download_summary(summary.download, "M1 update download")
            if summary.enrich is not None:
                _print_enrich_summary(summary.enrich, "M1 update enrich")
            continue

        if mode == "download":
            summary = download_m1_data(
                settings=settings,
                symbol=symbol,
                from_date=cfg.m1.from_date,
                end_date=cfg.m1.end_date,
                max_days=max_days,
                batch_mode=effective_batch_mode,
                stock_concurrency=cfg.m1.stock_concurrency,
                option_concurrency=cfg.m1.option_concurrency,
                option_expiration_mode=cfg.m1.option_expiration_mode,
                overwrite_stock_raw=effective_overwrite_stock_raw,
                overwrite_option_raw=effective_overwrite_option_raw,
                dry_run=dry_run,
            )
            _print_download_summary(summary, "M1 download")
            continue

        if mode == "download-stock":
            summary = download_stock_m1_data(
                settings=settings,
                symbol=symbol,
                from_date=cfg.m1.from_date,
                end_date=cfg.m1.end_date,
                max_days=max_days,
                batch_mode=effective_batch_mode,
                stock_concurrency=cfg.m1.stock_concurrency,
                overwrite_stock_raw=effective_overwrite_stock_raw,
                dry_run=dry_run,
            )
            _print_download_summary(summary, "M1 stock download")
            continue

        if mode == "download-options":
            summary = download_option_m1_data(
                settings=settings,
                symbol=symbol,
                from_date=cfg.m1.from_date,
                end_date=cfg.m1.end_date,
                max_days=max_days,
                batch_mode=effective_batch_mode,
                option_concurrency=cfg.m1.option_concurrency,
                option_expiration_mode=cfg.m1.option_expiration_mode,
                overwrite_option_raw=effective_overwrite_option_raw,
                dry_run=dry_run,
            )
            _print_download_summary(summary, "M1 option download")
            continue

        summary = enrich_m1_with_greeks(
            settings=settings,
            symbol=symbol,
            from_date=cfg.m1.from_date,
            end_date=cfg.m1.end_date,
            max_days=max_days,
            overwrite=overwrite,
            dry_run=dry_run,
        )
        _print_enrich_summary(summary, "M1 enrich")

    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python src/thetadata_pipeline/runners/m1.py")
    parser.add_argument("--mode", choices=["update", "download", "download-stock", "download-options", "enrich"], default="update")
    parser.add_argument("--dry-run", action="store_true", help="Do not write data, only show plan")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing M1 with-greeks parquet files")
    parser.add_argument(
        "--overwrite-raw",
        action="store_true",
        help="Backward-compatible alias: same as --overwrite-stock-raw --overwrite-option-raw",
    )
    parser.add_argument("--overwrite-stock-raw", action="store_true", help="Redownload raw stock M1 parquet files")
    parser.add_argument("--overwrite-option-raw", action="store_true", help="Redownload raw option M1 parquet files")
    parser.add_argument("--skip-enrich", action="store_true", help="Download raw M1 only")
    parser.add_argument("--max-days", type=int, default=None, help="Limit selected dates for smoke checks")
    parser.add_argument("--batch-mode", choices=["daily", "weekly", "monthly", "yearly", "all"], default=None)
    return parser


def _print_download_summary(summary, label: str) -> None:
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


def _print_enrich_summary(summary, label: str) -> None:
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


if __name__ == "__main__":
    args = _build_parser().parse_args()
    raise SystemExit(
        run(
            mode=args.mode,
            dry_run=args.dry_run,
            overwrite=args.overwrite,
            overwrite_raw=args.overwrite_raw,
            overwrite_stock_raw=args.overwrite_stock_raw,
            overwrite_option_raw=args.overwrite_option_raw,
            skip_enrich=args.skip_enrich,
            max_days=args.max_days,
            batch_mode=args.batch_mode,
        )
    )
