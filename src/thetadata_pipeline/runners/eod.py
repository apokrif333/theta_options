from __future__ import annotations

import argparse
import sys
from pathlib import Path

from tqdm import tqdm

SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from thetadata_pipeline.loaders.eod import update_eod_with_greeks
from thetadata_pipeline.pipeline_config import load_pipeline_config
from thetadata_pipeline.settings import get_settings


def run(dry_run: bool = False) -> int:
    settings = get_settings()
    cfg = load_pipeline_config()
    symbols = cfg.eod.symbols or [settings.default_symbol]

    for symbol in tqdm(symbols):
        summary = update_eod_with_greeks(
            settings=settings,
            symbol=symbol,
            from_date=cfg.eod.from_date,
            end_date=cfg.eod.end_date,
            max_dte=cfg.eod.max_dte,
            strike_range=cfg.eod.strike_range,
            batch_mode=cfg.eod.batch_mode,
            dry_run=dry_run,
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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python src/thetadata_pipeline/runners/eod.py")
    parser.add_argument("--dry-run", action="store_true", help="Do not write data, only show plan")
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()
    raise SystemExit(run(dry_run=args.dry_run))
