from pathlib import Path

DEFAULT_SYMBOL = "SPY"
THETA_DATE_FORMAT = "%Y%m%d"

DEFAULT_SPY_EOD_FINAL_PATH = Path("options") / "EOD" / "with_greeks" / "SPY_etf_greeks.parquet"

EOD_DEDUP_KEY = ["date", "expiration", "strike", "right", "ticker"]

EOD_FINAL_COLUMNS = [
    "expiration",
    "strike",
    "right",
    "ms_of_day2",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "count",
    "bid_size",
    "bid",
    "ask_size",
    "ask",
    "date",
    "ticker",
    "timeToExp",
    "splitFactor",
    "base_close",
    "IV_ask",
    "delta_ask",
    "gamma_ask",
    "theta_ask",
    "vega_ask",
    "rho_ask",
    "IV_bid",
    "delta_bid",
    "gamma_bid",
    "theta_bid",
    "vega_bid",
    "rho_bid",
    "probaOutOfM_ask",
    "probaOutOfM_bid",
]

EOD_FLOAT32_COLUMNS = [
    "timeToExp",
    "splitFactor",
    "base_close",
    "IV_ask",
    "delta_ask",
    "gamma_ask",
    "theta_ask",
    "vega_ask",
    "rho_ask",
    "IV_bid",
    "delta_bid",
    "gamma_bid",
    "theta_bid",
    "vega_bid",
    "rho_bid",
    "probaOutOfM_ask",
    "probaOutOfM_bid",
]

M1_FINAL_COLUMNS = [
    "ticker",
    "expiration",
    "strike",
    "right",
    "ms_of_day",
    "bid_size",
    "bid_exchange",
    "bid",
    "bid_condition",
    "ask_size",
    "ask_exchange",
    "ask",
    "ask_condition",
    "date",
    "dgs1",
    "baseClose",
    "timeToExp",
    "IV_ask",
    "delta_ask",
    "gamma_ask",
    "theta_ask",
    "vega_ask",
    "rho_ask",
    "IV_bid",
    "delta_bid",
    "gamma_bid",
    "theta_bid",
    "vega_bid",
    "rho_bid",
]

M1_INT16_COLUMNS = [
    "bid_exchange",
    "bid_condition",
    "ask_exchange",
    "ask_condition",
]

M1_INT32_COLUMNS = [
    "ms_of_day",
    "bid_size",
    "ask_size",
]

M1_FLOAT32_COLUMNS = [
    "strike",
    "bid",
    "ask",
    "dgs1",
    "baseClose",
    "timeToExp",
    "IV_ask",
    "delta_ask",
    "gamma_ask",
    "theta_ask",
    "vega_ask",
    "rho_ask",
    "IV_bid",
    "delta_bid",
    "gamma_bid",
    "theta_bid",
    "vega_bid",
    "rho_bid",
]
