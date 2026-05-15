from __future__ import annotations

import datetime as dt
from collections.abc import Iterable

from .constants import THETA_DATE_FORMAT


def parse_date(value: str | dt.date | None) -> dt.date | None:
    if value is None or isinstance(value, dt.date):
        return value

    raw = value.strip()
    if "-" in raw:
        return dt.date.fromisoformat(raw)
    return dt.datetime.strptime(raw, THETA_DATE_FORMAT).date()


def format_theta_date(value: str | dt.date) -> str:
    parsed = parse_date(value)
    if parsed is None:
        raise ValueError("date value is required")
    return parsed.strftime(THETA_DATE_FORMAT)


def previous_business_day(today: dt.date | None = None) -> dt.date:
    day = today or dt.date.today()
    day -= dt.timedelta(days=1)
    while day.weekday() >= 5:
        day -= dt.timedelta(days=1)
    return day


def business_days(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    current = start
    while current <= end:
        if current.weekday() < 5:
            yield current
        current += dt.timedelta(days=1)
