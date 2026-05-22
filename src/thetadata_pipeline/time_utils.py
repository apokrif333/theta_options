from __future__ import annotations

import datetime as dt

import pandas as pd
import polars as pl


IB_MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def time_to_ms_expr(column: str | pl.Expr = "time") -> pl.Expr:
    value = _as_expr(column)
    return (
        value.dt.hour().cast(pl.Int64) * 3_600_000
        + value.dt.minute().cast(pl.Int64) * 60_000
        + value.dt.second().cast(pl.Int64) * 1_000
        + (value.dt.microsecond().cast(pl.Int64) / 1_000).floor()
    ).cast(pl.Int32)


def datetime_to_ms_expr(column: str | pl.Expr) -> pl.Expr:
    return time_to_ms_expr(column)


def ms_of_day(value: pd.Timestamp | dt.datetime | dt.time) -> int:
    return value.hour * 3_600_000 + value.minute * 60_000 + value.second * 1_000 + value.microsecond // 1_000


def format_time_ms(ms: int) -> str:
    hours, remainder = divmod(int(ms), 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, milliseconds = divmod(remainder, 1_000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{milliseconds * 1000:06d}"


def to_date(value: object) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return pd.Timestamp(value).date()


def parse_ib_expiration(value: str) -> dt.date:
    raw = str(value).strip().upper()
    day = int(raw[:2])
    month = IB_MONTHS[raw[2:5]]
    year = 2000 + int(raw[5:])
    return dt.date(year, month, day)


def _as_expr(column: str | pl.Expr) -> pl.Expr:
    if isinstance(column, str):
        return pl.col(column)
    return column
