from __future__ import annotations

import argparse
import sys
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from thetadata_pipeline.loaders.m1 import enrich_m1_with_greeks
from thetadata_pipeline.pipeline_config import load_pipeline_config
from thetadata_pipeline.settings import get_settings


def run(dry_run: bool = False, overwrite: bool = False, max_days: int | None = None) -> int:
    settings = get_settings()
    cfg = load_pipeline_config()
    symbols = cfg.m1.symbols or [settings.default_symbol]

    for symbol in symbols:
        summary = enrich_m1_with_greeks(
            settings=settings,
            symbol=symbol,
            from_date=cfg.m1.from_date,
            end_date=cfg.m1.end_date,
            max_days=max_days,
            overwrite=overwrite,
            dry_run=dry_run,
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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python src/thetadata_pipeline/runners/m1.py")
    parser.add_argument("--dry-run", action="store_true", help="Do not write data, only show plan")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing M1 with-greeks parquet files")
    parser.add_argument("--max-days", type=int, default=None, help="Limit selected dates for smoke checks")
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()
    raise SystemExit(run(dry_run=args.dry_run, overwrite=args.overwrite, max_days=args.max_days))
