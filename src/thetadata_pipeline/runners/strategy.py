from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from thetadata_pipeline.pipeline_config import DEFAULT_CONFIG_PATH, load_pipeline_config
from thetadata_pipeline.settings import get_settings
from thetadata_pipeline.strategies.strategy import (
    DEFAULT_DATE_CUT,
    DEFAULT_LATENCY_ACCOUNT,
    DEFAULT_TICKER,
    StrategyConfig,
    run_strategy,
)


def run(
    ticker: str | None = None,
    date_cut: str | None = None,
    need_time: float | None = None,
    cut_delta_up: float | None = None,
    cut_delta_down: float | None = None,
    need_delta: float | None = None,
    account: str | None = None,
    latency_path: str | None = None,
    latency_max_seconds: float | None = None,
    option_concurrency: int | None = None,
    tick_concurrency: int | None = None,
    stop_window_ms: int | None = None,
    fallback_exit_option_price: float | None = None,
    stock_tick_source: str | None = None,
    trade_exit_option_window_ms: int | None = None,
    output: str | None = None,
    random_seed: int | None = None,
    config_path: str | None = None,
    dry_run: bool = False,
    no_write: bool = False,
) -> int:
    settings = get_settings()
    cfg = load_pipeline_config(config_path)
    strategy_cfg = cfg.strategy
    effective_account = account or strategy_cfg.account_id or cfg.analysis.account_id or DEFAULT_LATENCY_ACCOUNT
    effective_latency_path = latency_path if latency_path is not None else strategy_cfg.latency_path
    effective_output = output if output is not None else strategy_cfg.output
    config = StrategyConfig(
        ticker=(ticker or strategy_cfg.ticker or DEFAULT_TICKER).upper(),
        date_cut=_parse_date(date_cut if date_cut is not None else strategy_cfg.date_cut) or DEFAULT_DATE_CUT,
        need_time_hours=need_time if need_time is not None else strategy_cfg.need_time,
        cut_delta_up=cut_delta_up if cut_delta_up is not None else strategy_cfg.cut_delta_up,
        cut_delta_down=cut_delta_down if cut_delta_down is not None else strategy_cfg.cut_delta_down,
        need_delta=need_delta if need_delta is not None else strategy_cfg.need_delta,
        latency_account=effective_account,
        latency_path=Path(effective_latency_path).resolve() if effective_latency_path else None,
        latency_max_seconds=latency_max_seconds if latency_max_seconds is not None else strategy_cfg.latency_max_seconds,
        option_concurrency=option_concurrency if option_concurrency is not None else strategy_cfg.option_concurrency,
        tick_concurrency=tick_concurrency if tick_concurrency is not None else strategy_cfg.tick_concurrency,
        stop_window_ms=stop_window_ms if stop_window_ms is not None else strategy_cfg.stop_window_ms,
        fallback_exit_option_price=(
            fallback_exit_option_price
            if fallback_exit_option_price is not None
            else strategy_cfg.fallback_exit_option_price
        ),
        stock_tick_source=stock_tick_source if stock_tick_source is not None else strategy_cfg.stock_tick_source,
        trade_exit_option_window_ms=(
            trade_exit_option_window_ms
            if trade_exit_option_window_ms is not None
            else strategy_cfg.trade_exit_option_window_ms
        ),
        output_path=Path(effective_output).resolve() if effective_output else None,
        random_seed=random_seed if random_seed is not None else strategy_cfg.random_seed,
    )
    result = run_strategy(settings, config, dry_run=dry_run, write_output=not no_write)
    _print_result(result, dry_run=dry_run)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python src/thetadata_pipeline/runners/strategy.py")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to pipeline TOML config")
    parser.add_argument("--ticker", default=None)
    parser.add_argument("--date-cut", default=None, help="YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--need-time", type=float, default=None, help="Start time in market-day hours")
    parser.add_argument("--cut-delta-up", type=float, default=None)
    parser.add_argument("--cut-delta-down", type=float, default=None)
    parser.add_argument("--need-delta", type=float, default=None)
    parser.add_argument("--account", default=None, help="IB account id for <account>_lattency.csv")
    parser.add_argument("--latency-path", default=None, help="Explicit lattency csv path")
    parser.add_argument("--latency-max-seconds", type=float, default=None)
    parser.add_argument("--option-concurrency", type=int, choices=[1, 2, 4, 8], default=None)
    parser.add_argument("--tick-concurrency", type=int, choices=[1, 2, 4, 8], default=None)
    parser.add_argument("--stop-window-ms", type=int, default=None)
    parser.add_argument("--fallback-exit-option-price", type=float, default=None)
    parser.add_argument("--stock-tick-source", choices=["quotes", "trades"], default=None)
    parser.add_argument("--trade-exit-option-window-ms", type=int, default=None)
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
    print(f"stock_tick_source={result.config.stock_tick_source}")
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
            stock_tick_source=args.stock_tick_source,
            trade_exit_option_window_ms=args.trade_exit_option_window_ms,
            output=args.output,
            random_seed=args.random_seed,
            config_path=args.config,
            dry_run=args.dry_run,
            no_write=args.no_write,
        )
    )
