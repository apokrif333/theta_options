from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path

from .settings import PROJECT_ROOT


DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline.toml"


@dataclass(frozen=True)
class EodConfig:
    symbols: list[str]
    from_date: str | None
    end_date: str | None
    batch_mode: str
    max_dte: int | None
    strike_range: int | None


@dataclass(frozen=True)
class M1Config:
    symbols: list[str]
    from_date: str | None
    end_date: str | None
    batch_mode: str
    stock_concurrency: int
    option_concurrency: int
    option_expiration_mode: str


@dataclass(frozen=True)
class TickConfig:
    symbols: list[str]
    mode: str
    date: str | None
    start_ms: int
    end_ms: int
    interval: str
    expiration: str | None
    strike: float | None
    right: str | None


@dataclass(frozen=True)
class AnalysisConfig:
    account_id: str


@dataclass(frozen=True)
class PipelineConfig:
    eod: EodConfig
    m1: M1Config
    tick: TickConfig
    analysis: AnalysisConfig


def load_pipeline_config(path: str | Path | None = None) -> PipelineConfig:
    config_path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not config_path.exists():
        return PipelineConfig(
            eod=_default_eod_config(),
            m1=_default_m1_config(),
            tick=_default_tick_config(),
            analysis=_default_analysis_config()
        )

    raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    eod_raw = raw.get("eod", {})
    m1_raw = raw.get("m1", {})
    tick_raw = raw.get("tick", {})
    analysis_raw = raw.get("analysis", {})
    return PipelineConfig(
        eod=EodConfig(
            symbols=[str(symbol).upper() for symbol in eod_raw.get("symbols", ["SPY"])],
            from_date=_empty_to_none(eod_raw.get("from_date")),
            end_date=_empty_to_none(eod_raw.get("end_date")),
            batch_mode=str(eod_raw.get("batch_mode", "daily")),
            max_dte=_optional_int(eod_raw.get("max_dte")),
            strike_range=_optional_int(eod_raw.get("strike_range")),
        ),
        m1=M1Config(
            symbols=[str(symbol).upper() for symbol in m1_raw.get("symbols", eod_raw.get("symbols", ["SPY"]))],
            from_date=_empty_to_none(m1_raw.get("from_date")),
            end_date=_empty_to_none(m1_raw.get("end_date")),
            batch_mode=str(m1_raw.get("batch_mode", "weekly")),
            stock_concurrency=_optional_int(m1_raw.get("stock_concurrency")) or 1,
            option_concurrency=_optional_int(m1_raw.get("option_concurrency")) or 1,
            option_expiration_mode=str(m1_raw.get("option_expiration_mode", "same_day")),
        ),
        tick=TickConfig(
            symbols=[str(symbol).upper() for symbol in tick_raw.get("symbols", m1_raw.get("symbols", ["SPY"]))],
            mode=str(tick_raw.get("mode", "option_quote")),
            date=_empty_to_none(tick_raw.get("date")),
            start_ms=_optional_int(tick_raw.get("start_ms")) or 0,
            end_ms=_optional_int(tick_raw.get("end_ms")) or 0,
            interval=str(tick_raw.get("interval", "100ms")),
            expiration=_empty_to_none(tick_raw.get("expiration")),
            strike=_optional_float(tick_raw.get("strike")),
            right=_empty_to_none(tick_raw.get("right")),
        ),
        analysis=AnalysisConfig(
            account_id=str(analysis_raw.get("account_id", "")),
        ),
    )


def _default_eod_config() -> EodConfig:
    return EodConfig(
        symbols=["SPY"],
        from_date=None,
        end_date=None,
        batch_mode="daily",
        max_dte=None,
        strike_range=None,
    )


def _default_m1_config() -> M1Config:
    return M1Config(
        symbols=["SPY"],
        from_date=None,
        end_date=None,
        batch_mode="weekly",
        stock_concurrency=1,
        option_concurrency=1,
        option_expiration_mode="same_day",
    )


def _default_tick_config() -> TickConfig:
    return TickConfig(
        symbols=["SPY"],
        mode="option_quote",
        date=None,
        start_ms=0,
        end_ms=0,
        interval="100ms",
        expiration=None,
        strike=None,
        right=None,
    )


def _default_analysis_config() -> AnalysisConfig:
    return AnalysisConfig(
        account_id=""
    )


def _empty_to_none(value: object) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    return raw or None


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)
