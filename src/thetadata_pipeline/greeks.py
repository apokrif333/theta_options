from __future__ import annotations

import warnings

import numpy as np
import polars as pl
from scipy.stats import norm


def add_risk_free_rate(
    df: pl.DataFrame,
    risk_free_curve: pl.DataFrame,
) -> pl.DataFrame:
    if df.is_empty():
        return df.with_columns(pl.lit(None).cast(pl.Float64).alias("risk_free_rate"))

    rates = (
        risk_free_curve.select(
            pl.col("date").cast(pl.Date),
            pl.col("risk_free_rate").cast(pl.Float64),
        )
        .filter(pl.col("date").is_not_null())
        .filter(pl.col("risk_free_rate").is_not_null())
        .sort("date")
        .unique(subset=["date"], keep="last")
    )
    if rates.is_empty():
        raise ValueError("DGS1 risk-free curve is empty")

    result = (
        df.with_row_index("_theta_row_nr")
        .sort("date")
        .join_asof(rates, on="date", strategy="backward")
        .sort("_theta_row_nr")
        .drop("_theta_row_nr")
    )
    if result["risk_free_rate"].null_count() > 0:
        first_date = result.filter(pl.col("risk_free_rate").is_null())["date"].min()
        raise ValueError(f"DGS1 risk-free rate is missing on or before {first_date}")
    return result


def calculate_iv_greeks(df: pl.DataFrame, include_probability: bool = True) -> pl.DataFrame:
    if df.is_empty():
        return df

    result = df
    for price_col in ("ask", "bid"):
        result = _calculate_for_price(result, price_col)

    if include_probability:
        result = result.with_columns(
            _probability_out_of_money_expr("ask").alias("probaOutOfM_ask"),
            _probability_out_of_money_expr("bid").alias("probaOutOfM_bid"),
        )
    return result.drop("risk_free_rate")


def _calculate_for_price(df: pl.DataFrame, price_col: str) -> pl.DataFrame:
    import py_vollib_vectorized

    price = df[price_col].to_numpy()
    spot = df["base_close"].to_numpy()
    strike = df["strike"].to_numpy()
    tte = df["timeToExp"].to_numpy()
    rate = df["risk_free_rate"].to_numpy()
    flag = np.asarray(df["right"].to_list())
    q = df["dividend_yield"].to_numpy()

    valid = (
        (price > 0)
        & (spot > 0)
        & (strike > 0)
        & (tte > 0)
        & np.isfinite(price)
        & np.isfinite(spot)
        & np.isfinite(strike)
        & np.isfinite(tte)
        & np.isfinite(rate)
        & np.isfinite(q)
    )
    iv = np.full(len(df), np.nan, dtype=np.float64)

    if valid.any():
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            iv[valid] = py_vollib_vectorized.vectorized_implied_volatility(
                price=price[valid],
                S=spot[valid],
                K=strike[valid],
                t=tte[valid],
                r=rate[valid],
                flag=flag[valid],
                q=q[valid],
                on_error="ignore",
                model="black_scholes_merton",
                return_as="numpy",
            )
    iv[(~np.isfinite(iv)) | (iv <= 0)] = np.nan

    df = df.with_columns(pl.Series(f"IV_{price_col}", iv))

    finite_iv = valid & np.isfinite(iv)
    greek_values = {
        "delta": np.full(len(df), np.nan, dtype=np.float64),
        "gamma": np.full(len(df), np.nan, dtype=np.float64),
        "theta": np.full(len(df), np.nan, dtype=np.float64),
        "vega": np.full(len(df), np.nan, dtype=np.float64),
        "rho": np.full(len(df), np.nan, dtype=np.float64),
    }

    greek_functions = {
        "delta": py_vollib_vectorized.vectorized_delta,
        "gamma": py_vollib_vectorized.vectorized_gamma,
        "theta": py_vollib_vectorized.vectorized_theta,
        "vega": py_vollib_vectorized.vectorized_vega,
        "rho": py_vollib_vectorized.vectorized_rho,
    }

    if finite_iv.any():
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for greek, func in greek_functions.items():
                greek_values[greek][finite_iv] = func(
                    flag=flag[finite_iv],
                    S=spot[finite_iv],
                    K=strike[finite_iv],
                    t=tte[finite_iv],
                    r=rate[finite_iv],
                    sigma=iv[finite_iv],
                    q=q[finite_iv],
                    model="black_scholes_merton",
                    return_as="numpy",
                )

    return df.with_columns(
        pl.Series(f"delta_{price_col}", greek_values["delta"]),
        pl.Series(f"gamma_{price_col}", greek_values["gamma"]),
        pl.Series(f"theta_{price_col}", greek_values["theta"]),
        pl.Series(f"vega_{price_col}", greek_values["vega"]),
        pl.Series(f"rho_{price_col}", greek_values["rho"]),
    )


def _probability_out_of_money_expr(price_col: str) -> pl.Expr:
    iv_col = f"IV_{price_col}"
    d2 = (
        ((pl.col("base_close") / pl.col("strike")).log()
        + (pl.col("risk_free_rate") - pl.col("dividend_yield") - pl.col(iv_col) ** 2 / 2) * pl.col("timeToExp"))
        / (pl.col(iv_col) * pl.col("timeToExp").sqrt())
    )
    return (
        pl.when((pl.col(iv_col).is_not_null()) & (pl.col(iv_col) > 0) & (pl.col("right") == "p"))
        .then(d2.map_batches(lambda s: pl.Series(norm.cdf(s.to_numpy())), return_dtype=pl.Float64))
        .when((pl.col(iv_col).is_not_null()) & (pl.col(iv_col) > 0))
        .then(d2.map_batches(lambda s: pl.Series(1.0 - norm.cdf(s.to_numpy())), return_dtype=pl.Float64))
        .otherwise(None)
    )
