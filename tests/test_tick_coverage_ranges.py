import datetime as dt

from src.thetadata_pipeline.loaders.tick import (
    OptionQuoteWindow,
    _coalesced_missing_ranges,
    _merge_option_quote_windows,
)


def test_coalesced_missing_ranges_redownloads_wide_window_over_point_coverage():
    covered = [(1000, 1000), (5300, 5300), (18400, 18400)]

    assert _coalesced_missing_ranges(1000, 61000, covered) == [(1000, 61000)]


def test_coalesced_missing_ranges_keeps_single_left_extension():
    assert _coalesced_missing_ranges(0, 120, [(100, 150)]) == [(0, 100)]


def test_coalesced_missing_ranges_keeps_single_right_extension():
    assert _coalesced_missing_ranges(120, 200, [(100, 150)]) == [(150, 200)]


def test_coalesced_missing_ranges_fills_between_separate_covered_ranges_once():
    covered = [(100, 150), (200, 300)]

    assert _coalesced_missing_ranges(120, 250, covered) == [(150, 200)]


def test_coalesced_missing_ranges_skips_fully_covered_window():
    assert _coalesced_missing_ranges(120, 140, [(100, 150)]) == []


def test_merge_option_quote_windows_by_contract():
    day = dt.date(2026, 5, 20)
    windows = [
        OptionQuoteWindow("SPY", day, day, 737.0, "C", 1000, 2000),
        OptionQuoteWindow("SPY", day, day, 737.0, "C", 1500, 3000),
        OptionQuoteWindow("SPY", day, day, 738.0, "C", 1500, 3000),
    ]

    merged = _merge_option_quote_windows(windows)

    assert merged == [
        OptionQuoteWindow("SPY", day, day, 737.0, "C", 1000, 3000),
        OptionQuoteWindow("SPY", day, day, 738.0, "C", 1500, 3000),
    ]
