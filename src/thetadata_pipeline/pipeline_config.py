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


@dataclass(frozen=True)
class PipelineConfig:
    eod: EodConfig
    m1: M1Config


def load_pipeline_config(path: str | Path | None = None) -> PipelineConfig:
    config_path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not config_path.exists():
        return PipelineConfig(eod=_default_eod_config(), m1=_default_m1_config())

    raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    eod_raw = raw.get("eod", {})
    m1_raw = raw.get("m1", {})
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
