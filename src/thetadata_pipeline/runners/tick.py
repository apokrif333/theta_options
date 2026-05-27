from __future__ import annotations

import argparse
import sys
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from thetadata_pipeline.dates import parse_date
from thetadata_pipeline.loaders.tick import (
    append_stock_ticks,
    ensure_option_quote_window,
    fetch_stock_quote_window,
    fetch_stock_trade_window,
)
from thetadata_pipeline.pipeline_config import load_pipeline_config
from thetadata_pipeline.settings import get_settings


def run(
    mode: str | None = None,
    date: str | None = None,
    start_ms: int | None = None,
    end_ms: int | None = None,
    interval: str | None = None,
    option_concurrency: int | None = None,
    dry_run: bool = False,
) -> int:
    settings = get_settings()
    cfg = load_pipeline_config()
    tick = cfg.tick

    effective_mode = (mode or tick.mode).lower()
    effective_date = _required_date(date or tick.date, "tick.date")
    effective_start_ms = start_ms if start_ms is not None else tick.start_ms
    effective_end_ms = end_ms if end_ms is not None else tick.end_ms
    effective_interval = interval or tick.interval
    effective_option_concurrency = option_concurrency or cfg.m1.option_concurrency

    for symbol in tick.symbols or [settings.default_symbol]:
        if dry_run:
            _print_plan(
                symbol=symbol,
                mode=effective_mode,
                day=effective_date,
                start_ms=effective_start_ms,
                end_ms=effective_end_ms,
                interval=effective_interval,
                option_concurrency=effective_option_concurrency,
                expiration=tick.expiration,
                strike=tick.strike,
                right=tick.right,
            )
            continue

        if effective_mode == "stock_quote":
            frame = fetch_stock_quote_window(
                settings=settings,
                symbol=symbol,
                day=effective_date,
                start_ms=effective_start_ms,
                end_ms=effective_end_ms,
                interval=effective_interval,
            )
            _print_frame(frame, symbol, effective_mode)
            if effective_interval.lower() == "tick":
                output_path = append_stock_ticks(settings, symbol, frame)
                print(f"stock_tick_output_path={output_path}")
            continue

        if effective_mode == "stock_trade":
            frame = fetch_stock_trade_window(
                settings=settings,
                symbol=symbol,
                day=effective_date,
                start_ms=effective_start_ms,
                end_ms=effective_end_ms,
            )
            _print_frame(frame, symbol, effective_mode)
            continue

        if effective_mode == "option_quote":
            expiration = _required_date(tick.expiration, "tick.expiration")
            if tick.strike is None or tick.right is None:
                raise ValueError("tick.strike and tick.right are required for mode=option_quote")
            frame = ensure_option_quote_window(
                settings=settings,
                symbol=symbol,
                day=effective_date,
                expiration=expiration,
                strike=tick.strike,
                right=tick.right,
                start_ms=effective_start_ms,
                end_ms=effective_end_ms,
                interval=effective_interval,
                option_concurrency=effective_option_concurrency,
            )
            _print_frame(frame, symbol, effective_mode)
            continue

        raise ValueError(f"Unsupported tick.mode: {effective_mode}")

    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python src/thetadata_pipeline/runners/tick.py")
    parser.add_argument("--mode", choices=["stock_quote", "stock_trade", "option_quote"], default=None)
    parser.add_argument("--date", default=None, help="YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--start-ms", type=int, default=None)
    parser.add_argument("--end-ms", type=int, default=None)
    parser.add_argument("--interval", default=None)
    parser.add_argument("--option-concurrency", type=int, choices=[1, 2, 4, 8], default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def _required_date(raw: str | None, name: str):
    parsed = parse_date(raw)
    if parsed is None:
        raise ValueError(f"{name} is required")
    return parsed


def _print_frame(frame, symbol: str, mode: str) -> None:
    print(f"tick_{mode}_ok=True")
    print(f"symbol={symbol}")
    print(f"rows={frame.height}")
    if frame.height:
        print(f"columns={','.join(frame.columns)}")
        print(f"first={frame.row(0, named=True)}")
        print(f"last={frame.row(frame.height - 1, named=True)}")


def _print_plan(
    symbol: str,
    mode: str,
    day,
    start_ms: int,
    end_ms: int,
    interval: str,
    option_concurrency: int,
    expiration: str | None,
    strike: float | None,
    right: str | None,
) -> None:
    print("tick_dry_run=True")
    print(f"symbol={symbol}")
    print(f"mode={mode}")
    print(f"date={day:%Y-%m-%d}")
    print(f"start_ms={start_ms}")
    print(f"end_ms={end_ms}")
    print(f"interval={interval}")
    print(f"option_concurrency={option_concurrency}")
    if mode == "option_quote":
        print(f"expiration={expiration}")
        print(f"strike={strike}")
        print(f"right={right}")


if __name__ == "__main__":
    args = _build_parser().parse_args()
    raise SystemExit(
        run(
            mode=args.mode,
            date=args.date,
            start_ms=args.start_ms,
            end_ms=args.end_ms,
            interval=args.interval,
            option_concurrency=args.option_concurrency,
            dry_run=args.dry_run,
        )
    )
