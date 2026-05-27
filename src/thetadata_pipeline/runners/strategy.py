from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from thetadata_pipeline.pipeline_config import load_pipeline_config
from thetadata_pipeline.settings import get_settings
from thetadata_pipeline.strategies.strategy import (
    DEFAULT_CUT_DELTA_DOWN,
    DEFAULT_CUT_DELTA_UP,
    DEFAULT_DATE_CUT,
    DEFAULT_FALLBACK_EXIT_OPTION_PRICE,
    DEFAULT_LATENCY_ACCOUNT,
    DEFAULT_LATENCY_MAX_SECONDS,
    DEFAULT_NEED_DELTA,
    DEFAULT_NEED_TIME_HOURS,
    DEFAULT_OPTION_CONCURRENCY,
    DEFAULT_STOP_WINDOW_MS,
    DEFAULT_TICK_CONCURRENCY,
    DEFAULT_TICKER,
    StrategyConfig,
    run_strategy,
)


def run(
    ticker: str = DEFAULT_TICKER,
    date_cut: str | None = None,
    need_time: float = DEFAULT_NEED_TIME_HOURS,
    cut_delta_up: float = DEFAULT_CUT_DELTA_UP,
    cut_delta_down: float = DEFAULT_CUT_DELTA_DOWN,
    need_delta: float = DEFAULT_NEED_DELTA,
    account: str | None = None,
    latency_path: str | None = None,
    latency_max_seconds: float = DEFAULT_LATENCY_MAX_SECONDS,
    option_concurrency: int = DEFAULT_OPTION_CONCURRENCY,
    tick_concurrency: int = DEFAULT_TICK_CONCURRENCY,
    stop_window_ms: int = DEFAULT_STOP_WINDOW_MS,
    fallback_exit_option_price: float = DEFAULT_FALLBACK_EXIT_OPTION_PRICE,
    output: str | None = None,
    random_seed: int | None = None,
    dry_run: bool = False,
    no_write: bool = False,
) -> int:
    settings = get_settings()
    cfg = load_pipeline_config()
    effective_account = account or cfg.analysis.account_id or DEFAULT_LATENCY_ACCOUNT
    config = StrategyConfig(
        ticker=ticker.upper(),
        date_cut=_parse_date(date_cut) or DEFAULT_DATE_CUT,
        need_time_hours=need_time,
        cut_delta_up=cut_delta_up,
        cut_delta_down=cut_delta_down,
        need_delta=need_delta,
        latency_account=effective_account,
        latency_path=Path(latency_path).resolve() if latency_path else None,
        latency_max_seconds=latency_max_seconds,
        option_concurrency=option_concurrency,
        tick_concurrency=tick_concurrency,
        stop_window_ms=stop_window_ms,
        fallback_exit_option_price=fallback_exit_option_price,
        output_path=Path(output).resolve() if output else None,
        random_seed=random_seed,
    )
    result = run_strategy(settings, config, dry_run=dry_run, write_output=not no_write)
    _print_result(result, dry_run=dry_run)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python src/thetadata_pipeline/runners/strategy.py")
    parser.add_argument("--ticker", default=DEFAULT_TICKER)
    parser.add_argument("--date-cut", default=DEFAULT_DATE_CUT.isoformat(), help="YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--need-time", type=float, default=DEFAULT_NEED_TIME_HOURS, help="Start time in market-day hours")
    parser.add_argument("--cut-delta-up", type=float, default=DEFAULT_CUT_DELTA_UP)
    parser.add_argument("--cut-delta-down", type=float, default=DEFAULT_CUT_DELTA_DOWN)
    parser.add_argument("--need-delta", type=float, default=DEFAULT_NEED_DELTA)
    parser.add_argument("--account", default=None, help="IB account id for <account>_lattency.csv")
    parser.add_argument("--latency-path", default=None, help="Explicit lattency csv path")
    parser.add_argument("--latency-max-seconds", type=float, default=DEFAULT_LATENCY_MAX_SECONDS)
    parser.add_argument("--option-concurrency", type=int, choices=[1, 2, 4, 8], default=DEFAULT_OPTION_CONCURRENCY)
    parser.add_argument("--tick-concurrency", type=int, choices=[1, 2, 4, 8], default=DEFAULT_TICK_CONCURRENCY)
    parser.add_argument("--stop-window-ms", type=int, default=DEFAULT_STOP_WINDOW_MS)
    parser.add_argument("--fallback-exit-option-price", type=float, default=DEFAULT_FALLBACK_EXIT_OPTION_PRICE)
    parser.add_argument("--output", default=None, help="Output parquet path. Defaults to settings.strategy_strangle_trades_path(ticker)")
    parser.add_argument("--random-seed", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true", help="Build candidate trades only, without tick/quote downloads")
    parser.add_argument("--no-write", action="store_true", help="Do not write final strangle parquet")
    return parser


def _parse_date(raw: str | None) -> dt.date | None:
    if raw is None or not raw.strip():
        return None
    value = raw.strip()
    if "-" in value:
        return dt.date.fromisoformat(value)
    return dt.datetime.strptime(value, "%Y%m%d").date()


def _print_result(result, dry_run: bool) -> None:
    mode = "dry_run" if dry_run else "done"
    print(f"strategy_{mode}=True")
    print(f"ticker={result.config.ticker}")
    print(f"date_cut={result.config.date_cut:%Y-%m-%d}")
    print(f"options_rows={result.options_rows}")
    print(f"options_days={result.options_days}")
    print(f"start_days={result.start_days}")
    print(f"call_trades={result.call_trades}")
    print(f"put_trades={result.put_trades}")
    if dry_run:
        return
    print(f"call_final_trades={result.call_final_trades}")
    print(f"put_final_trades={result.put_final_trades}")
    print(f"strangle_trades={result.strangle_trades_rows}")
    if result.output_path is not None:
        print(f"output_path={result.output_path}")


if __name__ == "__main__":
    args = _build_parser().parse_args()
    raise SystemExit(
        run(
            ticker=args.ticker,
            date_cut=args.date_cut,
            need_time=args.need_time,
            cut_delta_up=args.cut_delta_up,
            cut_delta_down=args.cut_delta_down,
            need_delta=args.need_delta,
            account=args.account,
            latency_path=args.latency_path,
            latency_max_seconds=args.latency_max_seconds,
            option_concurrency=args.option_concurrency,
            tick_concurrency=args.tick_concurrency,
            stop_window_ms=args.stop_window_ms,
            fallback_exit_option_price=args.fallback_exit_option_price,
            output=args.output,
            random_seed=args.random_seed,
            dry_run=args.dry_run,
            no_write=args.no_write,
        )
    )
