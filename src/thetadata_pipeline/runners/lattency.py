from __future__ import annotations

import argparse
import sys
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from thetadata_pipeline.ib.lattency import run
from thetadata_pipeline.pipeline_config import load_pipeline_config
from thetadata_pipeline.settings import get_settings


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python src/thetadata_pipeline/runners/lattency.py")
    parser.add_argument("--account", default=None, help="IB account id. Defaults to analysis.account_id in pipeline.toml")
    parser.add_argument("--tick-concurrency", type=int, default=None, help="Tick quote request concurrency")
    parser.add_argument("--stock-concurrency", type=int, default=None, help="Stock M1 request concurrency")
    parser.add_argument("--dry-run", action="store_true", help="Do not write lattency outputs or download tick data")
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()
    cfg = load_pipeline_config()
    raise SystemExit(
        run(
            account=args.account or cfg.analysis.account_id,
            settings=get_settings(),
            batch_mode=cfg.m1.batch_mode,
            stock_concurrency=args.stock_concurrency or cfg.m1.stock_concurrency,
            tick_concurrency=args.tick_concurrency or cfg.m1.option_concurrency,
            dry_run=args.dry_run,
        )
    )
