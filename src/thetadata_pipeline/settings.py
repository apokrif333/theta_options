from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .constants import DEFAULT_SPY_EOD_FINAL_PATH, DEFAULT_SYMBOL

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    return default if raw is None else int(raw)


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    return default if raw is None else float(raw)


def _optional_path_env(name: str) -> Path | None:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    return _resolve_project_path(raw)


def _resolve_project_path(raw: str) -> Path:
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


_load_env_file(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    project_root: Path = PROJECT_ROOT
    theta_base_url: str = os.getenv("THETA_BASE_URL", "http://127.0.0.1:25503/v3").rstrip("/")
    data_dir: Path = _resolve_project_path(os.getenv("THETA_DATA_DIR", "data"))
    default_symbol: str = os.getenv("THETA_DEFAULT_SYMBOL", DEFAULT_SYMBOL)
    request_timeout_seconds: int = _int_env("THETA_REQUEST_TIMEOUT_SECONDS", 120)
    max_retries: int = _int_env("THETA_MAX_RETRIES", 12)
    retry_sleep_seconds: float = _float_env("THETA_RETRY_SLEEP_SECONDS", 5.0)
    risk_free_rates_path: Path = _resolve_project_path(
        os.getenv("THETA_RISK_FREE_RATES_PATH", "data/reference_rates/DGS1.parquet")
    )
    tiingo_usa_root: Path | None = _optional_path_env("THETA_TIINGO_USA_ROOT")
    m1_options_dir: Path = _resolve_project_path(os.getenv("THETA_M1_OPTIONS_DIR", "data/options/m1"))
    m1_with_greeks_dir: Path = _resolve_project_path(
        os.getenv("THETA_M1_WITH_GREEKS_DIR", "data/options/m1/with_greeks")
    )
    stock_m1_dir: Path = _resolve_project_path(os.getenv("THETA_STOCK_M1_DIR", "data/stocks/m1"))
    stock_eod_dir: Path = _resolve_project_path(os.getenv("THETA_STOCK_EOD_DIR", "data/stocks/eod"))
    stock_ticks_dir: Path = _resolve_project_path(os.getenv("THETA_STOCK_TICKS_DIR", "data/stocks/ticks"))
    ib_states_dir: Path = _resolve_project_path(os.getenv("IB_STATES_DIR", "data/ib"))
    strategies_dir: Path = _resolve_project_path(os.getenv("THETA_STRATEGIES_DIR", "data/strategies"))

    def eod_final_path(self, symbol: str | None = None) -> Path:
        resolved_symbol = (symbol or self.default_symbol).upper()
        override = os.getenv(f"THETA_{resolved_symbol}_EOD_FINAL_PATH")
        if override:
            return _resolve_project_path(override)

        if resolved_symbol == "SPY":
            return (self.data_dir / DEFAULT_SPY_EOD_FINAL_PATH).resolve()

        return (self.data_dir / "options" / "EOD" / "with_greeks" / f"{resolved_symbol}_etf_greeks.parquet").resolve()

    def strategy_strangle_trades_path(self, symbol: str | None = None) -> Path:
        resolved_symbol = (symbol or self.default_symbol).upper()
        override = os.getenv(f"THETA_{resolved_symbol}_STRANGLE_TRADES_PATH")
        if override:
            return _resolve_project_path(override)
        return (self.strategies_dir / f"{resolved_symbol}_strangle_trades.parquet").resolve()

    @property
    def staging_eod_dir(self) -> Path:
        return (self.data_dir / ".staging" / "eod").resolve()

    @property
    def staging_m1_dir(self) -> Path:
        return (self.data_dir / ".staging" / "m1").resolve()


def get_settings() -> Settings:
    return Settings()
