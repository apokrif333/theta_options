import polars as pl
import pytest

from src.thetadata_pipeline.settings import Settings
from src.thetadata_pipeline.strategies.strategy import (
    filter_stock_trade_ticks,
    load_trade_conditions,
    normalize_stock_tick_source,
)


def test_normalize_stock_tick_source_accepts_aliases():
    assert normalize_stock_tick_source("quotes") == "quotes"
    assert normalize_stock_tick_source("stock_quote") == "quotes"
    assert normalize_stock_tick_source("trades") == "trades"
    assert normalize_stock_tick_source("stock_trade") == "trades"


def test_normalize_stock_tick_source_rejects_unknown_value():
    with pytest.raises(ValueError):
        normalize_stock_tick_source("m1")


def test_filter_stock_trade_ticks_keeps_only_allowed_primary_and_extended_conditions():
    frame = pl.DataFrame(
        {
            "condition": [0, 1, 0, 0],
            "ext_condition1": [255, 255, 1, 255],
            "ext_condition2": [255, 255, 255, 0],
            "ext_condition3": [255, 255, 255, 255],
            "ext_condition4": [255, 255, 255, 255],
            "price": [100.0, 101.0, 102.0, 103.0],
        }
    )

    filtered = filter_stock_trade_ticks(frame, {0})

    assert filtered["price"].to_list() == [100.0, 103.0]


def test_load_trade_conditions_reads_terminal_csv(tmp_path):
    terminal_dir = tmp_path / "ThetaTerminal"
    terminal_dir.mkdir()
    (terminal_dir / "TradeConditions.csv").write_text(
        "\n".join(
            [
                "Code,Name,Cancel,LateReport,AutoExecuted,OpenReport,Volume,High,Low,Last,Description",
                "0,REGULAR,false,false,false,false,true,true,true,true,Regular Trade",
                "1,FORM_T,false,false,false,false,true,false,false,false,Form T",
                "55,STOPPED_REGULAR,false,false,false,false,true,true,true,true,Stopped regular",
            ]
        ),
        encoding="utf-8",
    )
    settings = Settings(project_root=tmp_path)

    assert load_trade_conditions(settings) == {0, 55}
