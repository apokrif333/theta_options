from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field, replace
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

from ..loaders.tick import ensure_option_quote_windows, ensure_stock_quotes_windows, ensure_stock_trades_windows
from ..settings import Settings
from ..time_utils import ms_of_day


DEFAULT_DATE_CUT = dt.date(2021, 1, 1)
DEFAULT_TICKER = "SPY"
DEFAULT_NEED_TIME_HOURS = 9.6
DEFAULT_CUT_DELTA_UP = 0.40
DEFAULT_CUT_DELTA_DOWN = 0.10
DEFAULT_NEED_DELTA = 0.35
DEFAULT_LATENCY_ACCOUNT = "U1717377"
DEFAULT_LATENCY_MAX_SECONDS = 120.0
DEFAULT_OPTION_CONCURRENCY = 8
DEFAULT_TICK_CONCURRENCY = 8
DEFAULT_STOP_WINDOW_MS = 300_000
DEFAULT_FALLBACK_EXIT_OPTION_PRICE = 0.02
DEFAULT_STOCK_TICK_SOURCE = "quotes"
DEFAULT_TRADE_EXIT_OPTION_WINDOW_MS = 60_000
STOCK_TICK_SOURCES = {"quotes", "trades"}

EQUITY_RTH_STOP_TRIGGER_STRICT = {
    0,   # REGULAR
    10,  # BUNCHED
    14,  # RULE_127
    22,  # ACQUISITION
    25,  # BURST_BASKET
    29,  # RULE_155
    30,  # DISTRIBUTION
    31,  # SPLIT
    32,  # REGULAR_SETTLE
    45,  # MATCH_CROSS
    46,  # FAST_MARKET
    55,  # STOPPED_REGULAR
    75,  # BLOCK_TRADE
    79,  # YELLOW_FLAG
    95,  # INTERMARKET_SWEEP
}

TRADE_EXT_CONDITION_COLUMNS = [
    "ext_condition1",
    "ext_condition2",
    "ext_condition3",
    "ext_condition4",
]


DROP_OPTION_COLUMNS = [
    "bid_size",
    "bid_exchange",
    "bid_condition",
    "ask_size",
    "ask_exchange",
    "ask_condition",
    "dgs1",
    "timeToExp",
    "gamma_ask",
    "theta_ask",
    "vega_ask",
    "rho_ask",
    "IV_ask",
    "delta_ask",
    "gamma_bid",
    "theta_bid",
    "vega_bid",
    "rho_bid",
    "delta_diff",
]


@dataclass(frozen=True)
class StrategyConfig:
    ticker: str = DEFAULT_TICKER
    date_cut: dt.date = DEFAULT_DATE_CUT
    need_time_hours: float = DEFAULT_NEED_TIME_HOURS
    cut_delta_up: float = DEFAULT_CUT_DELTA_UP
    cut_delta_down: float = DEFAULT_CUT_DELTA_DOWN
    need_delta: float = DEFAULT_NEED_DELTA
    latency_account: str = DEFAULT_LATENCY_ACCOUNT
    latency_path: Path | None = None
    latency_max_seconds: float = DEFAULT_LATENCY_MAX_SECONDS
    option_concurrency: int = DEFAULT_OPTION_CONCURRENCY
    tick_concurrency: int = DEFAULT_TICK_CONCURRENCY
    stop_window_ms: int = DEFAULT_STOP_WINDOW_MS
    fallback_exit_option_price: float = DEFAULT_FALLBACK_EXIT_OPTION_PRICE
    stock_tick_source: str = DEFAULT_STOCK_TICK_SOURCE
    trade_exit_option_window_ms: int = DEFAULT_TRADE_EXIT_OPTION_WINDOW_MS
    output_path: Path | None = None
    random_seed: int | None = None


@dataclass
class StrategyResult:
    config: StrategyConfig
    options_rows: int
    options_days: int
    start_days: int
    call_trades: int
    put_trades: int
    call_final_trades: int = 0
    put_final_trades: int = 0
    strangle_trades_rows: int = 0
    output_path: Path | None = None
    trades: dict[str, pl.DataFrame] = field(default_factory=dict, repr=False)
    strangle_trades: pl.DataFrame | None = field(default=None, repr=False)
    rolling_iv: pd.DataFrame | None = field(default=None, repr=False)


def run_strategy(
    settings: Settings,
    config: StrategyConfig | None = None,
    *,
    dry_run: bool = False,
    write_output: bool = True,
) -> StrategyResult:
    config = normalize_strategy_config(config or StrategyConfig())
    if config.random_seed is not None:
        np.random.seed(config.random_seed)

    options_df = load_option_candidates(settings, config)
    options_rows = options_df.height
    options_days = options_df["date"].n_unique() if options_df.height else 0

    options_df, start_df = apply_start_filter(options_df, config.date_cut)
    trades = select_trade_candidates(options_df, config.need_delta)
    rolling_iv = build_rolling_iv(trades)

    result = StrategyResult(
        config=config,
        options_rows=options_rows,
        options_days=options_days,
        start_days=start_df.height,
        call_trades=trades["call"].height,
        put_trades=trades["put"].height,
        trades=trades,
        rolling_iv=rolling_iv,
    )
    if dry_run:
        return result

    ticker_m1 = load_stock_m1(settings, config)
    open_latency, stop_latency = load_latency(settings, config)
    final_trades = resolve_final_trades(
        settings=settings,
        trades=trades,
        ticker_m1=ticker_m1,
        open_latency=open_latency,
        stop_latency=stop_latency,
        config=config,
    )
    strangle_trades = build_strangle_trades(final_trades)

    output_path = config.output_path
    if write_output:
        output_path = output_path or default_output_path(settings, config.ticker)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        strangle_trades.write_parquet(output_path)

    result.trades = final_trades
    result.call_final_trades = final_trades["call_final"].height
    result.put_final_trades = final_trades["put_final"].height
    result.strangle_trades = strangle_trades
    result.strangle_trades_rows = strangle_trades.height
    result.output_path = output_path if write_output else None
    return result


def normalize_strategy_config(config: StrategyConfig) -> StrategyConfig:
    stock_tick_source = normalize_stock_tick_source(config.stock_tick_source)
    if stock_tick_source == config.stock_tick_source:
        return config
    return replace(config, stock_tick_source=stock_tick_source)


def normalize_stock_tick_source(source: str) -> str:
    normalized = str(source).strip().lower()
    aliases = {
        "quote": "quotes",
        "stock_quote": "quotes",
        "stock_quotes": "quotes",
        "trade": "trades",
        "stock_trade": "trades",
        "stock_trades": "trades",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in STOCK_TICK_SOURCES:
        raise ValueError(f"stock_tick_source must be one of {sorted(STOCK_TICK_SOURCES)}, got {source!r}")
    return normalized


def load_option_candidates(settings: Settings, config: StrategyConfig) -> pl.DataFrame:
    need_time = int(config.need_time_hours * 1_000 * 60 * 60)
    frames: list[pl.DataFrame] = []
    pattern = f"*{config.ticker.upper()}_m1_greeks_opts.parquet"
    for path in settings.m1_with_greeks_dir.glob(pattern):
        frame = pl.read_parquet(path).filter(
            (pl.col("ms_of_day") >= need_time)
            & (
                (
                    (pl.col("delta_bid") <= config.cut_delta_up)
                    & (pl.col("delta_bid") >= config.cut_delta_down)
                    & (pl.col("right") == "c")
                )
                | (
                    (pl.col("delta_bid") >= -config.cut_delta_up)
                    & (pl.col("delta_bid") <= -config.cut_delta_down)
                    & (pl.col("right") == "p")
                )
            )
        )
        frames.append(frame)
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal")


def apply_start_filter(options_df: pl.DataFrame, date_cut: dt.date) -> tuple[pl.DataFrame, pl.DataFrame]:
    if options_df.is_empty():
        return options_df, pl.DataFrame()

    options_df = (
        options_df.filter(pl.col("date") > date_cut)
        .with_columns(spread=(pl.col("ask") - pl.col("bid")) / pl.col("ask"))
        .filter(pl.col("spread") < 0.1)
    )
    start_df = (
        options_df.sort("date", "ms_of_day")
        .unique(["date", "ms_of_day", "right"], keep="first", maintain_order=True)
        .group_by(["date", "ms_of_day"])
        .agg(pl.col("ticker").count())
        .sort("date", "ms_of_day")
        .filter(pl.col("ticker") > 1)
        .unique("date", keep="first", maintain_order=True)
        .rename({"ms_of_day": "start_time"})
        .drop("ticker")
    )
    options_df = (
        options_df.join(start_df, on="date", how="inner")
        .filter(pl.col("ms_of_day") >= pl.col("start_time"))
        .drop("spread", "start_time")
    )
    return options_df, start_df


def select_trade_candidates(options_df: pl.DataFrame, need_delta: float) -> dict[str, pl.DataFrame]:
    return {
        "call": (
            options_df.filter(pl.col("right") == "c")
            .with_columns(delta_diff=(pl.col("delta_bid") - need_delta).abs())
            .sort(["date", "expiration", "ms_of_day", "delta_diff"])
            .unique(subset=["date", "expiration"], keep="first", maintain_order=True)
            .drop(DROP_OPTION_COLUMNS)
        ),
        "put": (
            options_df.filter(pl.col("right") == "p")
            .with_columns(delta_diff=(pl.col("delta_bid") + need_delta).abs())
            .sort(["date", "expiration", "ms_of_day", "delta_diff"])
            .unique(subset=["date", "expiration"], keep="first", maintain_order=True)
            .drop(DROP_OPTION_COLUMNS)
        ),
    }


def build_rolling_iv(trades: dict[str, pl.DataFrame]) -> pd.DataFrame:
    return (
        trades["call"][["date", "IV_bid"]]
        .join(
            trades["put"][["date", "IV_bid"]].rename({"IV_bid": "IV_bid_put"}),
            on="date",
        )
        .with_columns(avgIV=(pl.col("IV_bid") + pl.col("IV_bid_put")) / 2)[["date", "avgIV"]]
        .to_pandas()
        .set_index("date")
        .rolling(window="365D")
        .mean()
    )


def load_stock_m1(settings: Settings, config: StrategyConfig) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    pattern = f"*{config.ticker.upper()}_m1_stock.parquet"
    for path in settings.stock_m1_dir.glob(pattern):
        frames.append(pl.read_parquet(path))
    if not frames:
        return pl.DataFrame()
    return (
        pl.concat(frames)
        .with_columns(pl.col("date").cast(pl.String).str.to_datetime("%Y%m%d").cast(pl.Date))
        .filter((pl.col("date") > config.date_cut) & (pl.col("close") > 0))
        .sort("date", "ms_of_day")
    )


def load_latency(settings: Settings, config: StrategyConfig) -> tuple[pl.Series, pl.Series]:
    latency_path = config.latency_path or settings.ib_states_dir / f"{config.latency_account}_lattency.csv"
    latency = pl.read_csv(latency_path).filter(pl.col("lattency") < config.latency_max_seconds)
    return (
        latency.filter(pl.col("lable") == "open")["lattency"],
        latency.filter(pl.col("lable") == "stop")["lattency"],
    )


def latency_distribution(latency_col: pl.Series | pd.Series, length: int) -> np.ndarray:
    percentiles = np.linspace(0, 100, length)
    values = np.round(np.percentile(latency_col * 1000, percentiles), -2)
    values = np.maximum(values, 0)
    np.random.shuffle(values)
    return values


def resolve_final_trades(
    settings: Settings,
    trades: dict[str, pl.DataFrame],
    ticker_m1: pl.DataFrame,
    open_latency: pl.Series,
    stop_latency: pl.Series,
    config: StrategyConfig,
) -> dict[str, pl.DataFrame]:
    resolved = dict(trades)
    for right in list(trades.keys()):
        print(f"---------{right}---------")
        trade_df = add_entry_prices(
            settings=settings,
            trade_df=trades[right],
            open_latency=open_latency,
            config=config,
            right=right,
        )
        total_m1 = find_exit_windows(trade_df, ticker_m1, right, config.stop_window_ms)
        print(f"{total_m1.height} {right} - stop trades")
        total_m1 = add_stock_tick_exit_times(
            settings=settings,
            total_m1=total_m1,
            right=right,
            stop_latency=stop_latency,
            config=config,
        )
        total_m1 = add_exit_option_prices(
            settings=settings,
            total_m1=total_m1,
            config=config,
            right=right,
        )
        resolved[f"{right}_final"] = finalize_side_trades(trade_df, total_m1, right, config)
    return resolved


def add_entry_prices(
    settings: Settings,
    trade_df: pl.DataFrame,
    open_latency: pl.Series,
    config: StrategyConfig,
    right: str,
) -> pl.DataFrame:
    trade_df = (
        trade_df.with_columns(start_ms=pl.col("ms_of_day") + latency_distribution(open_latency, trade_df.height))
        .with_columns(end_ms=pl.col("start_ms"))
    )

    print(f"{trade_df.height} {right} - enter trades")
    quotes = (
        ensure_option_quote_windows(
            settings,
            trade_df,
            option_concurrency=config.option_concurrency,
        )
        .with_columns(pl.col("right").str.to_lowercase().alias("right"))
        .rename({"bid": "ent_opt_bid", "ask": "ent_opt_ask", "time": "ent_time"})
        .select(["date", "expiration", "strike", "right", "ent_time", "ent_opt_bid", "ent_opt_ask"])
    )

    return (
        trade_df.join(quotes, on=["date", "expiration", "strike", "right"], how="inner")
        .drop(["end_ms", "bid", "ask", "ms_of_day"])
        .rename({"start_ms": "ent_time_ms"})
    )


def find_exit_windows(
    trade_df: pl.DataFrame,
    ticker_m1: pl.DataFrame,
    right: str,
    stop_window_ms: int,
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for row in trade_df.iter_rows(named=True):
        need_date = row["date"]
        need_time = row["ent_time_ms"]

        if right == "call":
            current_m1 = ticker_m1.filter(
                (need_date == pl.col("date"))
                & (need_time < pl.col("ms_of_day"))
                & (row["strike"] < pl.col("high"))
            )
        elif right == "put":
            current_m1 = ticker_m1.filter(
                (need_date == pl.col("date"))
                & (need_time < pl.col("ms_of_day"))
                & (row["strike"] > pl.col("low"))
            )
        else:
            raise ValueError(f"Wrong right: {right}")

        if current_m1.is_empty():
            continue

        exit_row = (
            current_m1[0]
            .select(["ms_of_day", "high", "date"])
            .rename({"date": "date_exit"})
            .with_columns(
                expiration=pl.lit(row["expiration"]),
                strike=pl.lit(row["strike"]),
                date=pl.lit(row["date"]),
                right=pl.lit(row["right"]),
                ticker=pl.lit(row["ticker"]),
            )
        )
        frames.append(exit_row)

    if not frames:
        return pl.DataFrame()

    return (
        pl.concat(frames)
        .with_row_index(name="idx")
        .rename({"ms_of_day": "start_ms"})
        .with_columns(end_ms=pl.col("start_ms") + stop_window_ms)
    )


def add_stock_tick_exit_times(
    settings: Settings,
    total_m1: pl.DataFrame,
    right: str,
    stop_latency: pl.Series,
    config: StrategyConfig,
) -> pl.DataFrame:
    if config.stock_tick_source == "trades":
        return add_stock_trade_tick_exit_times(
            settings=settings,
            total_m1=total_m1,
            right=right,
            stop_latency=stop_latency,
            config=config,
        )
    return add_stock_quote_tick_exit_times(
        settings=settings,
        total_m1=total_m1,
        right=right,
        stop_latency=stop_latency,
        config=config,
    )


def add_stock_quote_tick_exit_times(
    settings: Settings,
    total_m1: pl.DataFrame,
    right: str,
    stop_latency: pl.Series,
    config: StrategyConfig,
) -> pl.DataFrame:
    tick_df = (
        ensure_stock_quotes_windows(
            settings,
            total_m1,
            concurrency=config.tick_concurrency,
            interval="tick",
        )
        .rename({"time": "exact_stop_time", "bid": "quote_bid", "ask": "quote_ask"})
    )

    if right == "call":
        total_m1_with_ticks = total_m1.join(tick_df, on="date", how="left").filter(pl.col("quote_ask") > pl.col("strike"))
    elif right == "put":
        total_m1_with_ticks = total_m1.join(tick_df, on="date", how="left").filter(pl.col("quote_bid") < pl.col("strike"))
    else:
        raise ValueError(f"Wrong right: {right}")

    total_m1_with_ticks = total_m1_with_ticks.sort("idx", "date", "exact_stop_time").unique(
        "idx",
        keep="first",
        maintain_order=True,
    )

    missing_idx = set(total_m1["idx"]) - set(total_m1_with_ticks["idx"])
    empty_data = total_m1.filter(pl.col("idx").is_in(missing_idx))

    print(f"{empty_data.height} {right} - empty ticks")
    empty_data = (
        empty_data.join(tick_df, on="date", how="left")
        .with_columns(
            diff=pl.when(right == "call")
            .then((pl.col("strike") - pl.col("quote_bid")).abs())
            .otherwise((pl.col("strike") - pl.col("quote_ask")).abs())
        )
        .sort("idx", "diff")
        .unique("idx", keep="first", maintain_order=True)
        .drop("diff")
    )
    total_m1 = (
        pl.concat([total_m1_with_ticks, empty_data])
        .sort("idx")
        .drop(["quote_bid", "quote_ask", "start_ms", "end_ms"])
    )
    return (
        total_m1.with_columns(ms_of_day(total_m1["exact_stop_time"]).alias("exact_stop_time_ms"))
        .with_columns(start_ms=pl.col("exact_stop_time_ms") + latency_distribution(stop_latency, total_m1.height))
        .with_columns(end_ms=pl.col("start_ms"))
    )


def add_stock_trade_tick_exit_times(
    settings: Settings,
    total_m1: pl.DataFrame,
    right: str,
    stop_latency: pl.Series,
    config: StrategyConfig,
) -> pl.DataFrame:
    tick_df = (
        ensure_stock_trades_windows(
            settings,
            total_m1,
            concurrency=config.tick_concurrency,
        )
        .rename({"time": "exact_stop_time"})
    )
    tick_df = filter_stock_trade_ticks(tick_df, load_trade_conditions(settings)).drop(
        ["sequence", *TRADE_EXT_CONDITION_COLUMNS, "condition", "size", "exchange"]
    )

    if right == "call":
        total_m1_with_ticks = total_m1.join(tick_df, on="date", how="left").filter(pl.col("price") > pl.col("strike"))
    elif right == "put":
        total_m1_with_ticks = total_m1.join(tick_df, on="date", how="left").filter(pl.col("price") < pl.col("strike"))
    else:
        raise ValueError(f"Wrong right: {right}")

    total_m1_with_ticks = total_m1_with_ticks.sort("idx", "date", "exact_stop_time").unique(
        "idx",
        keep="first",
        maintain_order=True,
    )

    missing_idx = set(total_m1["idx"]) - set(total_m1_with_ticks["idx"])
    empty_data = total_m1.filter(pl.col("idx").is_in(missing_idx))

    print(f"{empty_data.height} {right} - empty ticks")
    empty_data = (
        empty_data.join(tick_df, on="date", how="left")
        .with_columns(diff=(pl.col("strike") - pl.col("price")).abs())
        .sort("idx", "diff")
        .unique("idx", keep="first", maintain_order=True)
        .drop("diff")
    )
    total_m1 = (
        pl.concat([total_m1_with_ticks, empty_data])
        .sort("idx")
        .drop(["price", "start_ms", "end_ms"])
    )
    return (
        total_m1.with_columns(ms_of_day(total_m1["exact_stop_time"]).alias("exact_stop_time_ms"))
        .with_columns(start_ms=pl.col("exact_stop_time_ms") + latency_distribution(stop_latency, total_m1.height))
        .with_columns(end_ms=pl.col("start_ms") + config.trade_exit_option_window_ms)
    )


def filter_stock_trade_ticks(tick_df: pl.DataFrame, trade_conditions: set[int]) -> pl.DataFrame:
    ext_allowed = trade_conditions | {255}
    return tick_df.filter(
        pl.col("condition").is_in(trade_conditions)
        & pl.col("ext_condition1").is_in(ext_allowed)
        & pl.col("ext_condition2").is_in(ext_allowed)
        & pl.col("ext_condition3").is_in(ext_allowed)
        & pl.col("ext_condition4").is_in(ext_allowed)
    )


def load_trade_conditions(settings: Settings) -> set[int]:
    path = settings.project_root / "ThetaTerminal" / "TradeConditions.csv"
    with path.open("r", encoding="utf-8") as handle:
        header = handle.readline().strip().split(",")
        rows = [line.strip().split(",", 10) for line in handle if line.strip()]
    frame = pd.DataFrame(rows, columns=header)
    code = frame["Code"].astype(int)
    mask = (
        frame["High"].str.lower().eq("true")
        & frame["Low"].str.lower().eq("true")
        & frame["Volume"].str.lower().eq("true")
        & frame["Last"].str.lower().eq("true")
        & frame["Cancel"].str.lower().eq("false")
        & frame["LateReport"].str.lower().eq("false")
        & code.isin(EQUITY_RTH_STOP_TRIGGER_STRICT)
    )
    return set(code[mask])


def add_exit_option_prices(
    settings: Settings,
    total_m1: pl.DataFrame,
    config: StrategyConfig,
    right: str,
) -> pl.DataFrame:
    print(f"{total_m1.height} {right} - exit trades")
    quotes = ensure_option_quote_windows(
        settings,
        total_m1,
        option_concurrency=config.option_concurrency,
    )
    option_ms = (
        quotes.with_columns(
            pl.col("right").str.to_lowercase().alias("right"),
            ms_of_day(quotes["time"]).cast(pl.Float64).alias("time_ms"),
        )
        .rename({"bid": "ext_opt_bid", "ask": "ext_opt_ask"})
        .select(["date", "expiration", "strike", "right", "time", "time_ms", "ext_opt_bid", "ext_opt_ask"])
    )

    stop_trades = total_m1.height
    total_m1 = (
        total_m1.join(
            option_ms,
            left_on=["date_exit", "expiration", "strike", "right"],
            right_on=["date", "expiration", "strike", "right"],
            how="inner",
        )
        .filter(pl.col("time_ms") >= pl.col("start_ms"))
        .sort("idx", "date", "time_ms")
        .unique("idx", keep="first", maintain_order=True)
    )
    assert total_m1.height == stop_trades, f"Wrong length of total_m1: {total_m1.height} != {stop_trades}"
    return total_m1.rename({"time": "ext_time", "start_ms": "ext_time_ms"}).drop("end_ms")


def finalize_side_trades(
    trade_df: pl.DataFrame,
    total_m1: pl.DataFrame,
    right: str,
    config: StrategyConfig,
) -> pl.DataFrame:
    return (
        trade_df.join(
            total_m1.drop("high", "date_exit"),
            on=["expiration", "strike", "right", "date", "ticker"],
            how="left",
        )
        .with_columns(
            ext_opt_bid=pl.col("ext_opt_bid").fill_null(config.fallback_exit_option_price),
            ext_opt_ask=pl.col("ext_opt_ask").fill_null(config.fallback_exit_option_price),
        )
        .with_columns((pl.col("ent_opt_bid") - pl.col("ext_opt_ask")).alias(f"{right}_profit"))
        .drop(["idx"])
    )


def build_strangle_trades(trades: dict[str, pl.DataFrame]) -> pl.DataFrame:
    strangle_trades = trades["call_final"].join(
        trades["put_final"],
        on=["expiration", "date", "ticker", "baseClose"],
        how="inner",
        suffix="_put",
    )
    assert trades["call_final"].height == trades["put_final"].height == strangle_trades.height
    return strangle_trades.with_columns(
        strangle_profit=pl.col("call_profit") + pl.col("put_profit"),
        avgIV=(pl.col("IV_bid") + pl.col("IV_bid_put")) / 2,
    )


def default_output_path(settings: Settings, ticker: str) -> Path:
    return settings.strategy_strangle_trades_path(ticker)
