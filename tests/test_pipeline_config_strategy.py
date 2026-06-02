from src.thetadata_pipeline.pipeline_config import load_pipeline_config


def test_load_pipeline_config_has_default_strategy_when_file_missing(tmp_path):
    config = load_pipeline_config(tmp_path / "missing.toml")

    assert config.strategy.ticker == "SPY"
    assert config.strategy.stock_tick_source == "quotes"
    assert config.strategy.trade_exit_option_window_ms == 60_000


def test_load_pipeline_config_reads_strategy_section(tmp_path):
    path = tmp_path / "pipeline.toml"
    path.write_text(
        """
[analysis]
account_id = "U1"

[strategy]
ticker = "qqq"
date_cut = "20160101"
need_time = 9.7
cut_delta_up = 0.41
cut_delta_down = 0.11
need_delta = 0.36
account_id = "U2"
latency_path = "data/custom_latency.csv"
latency_max_seconds = 60.0
option_concurrency = 4
tick_concurrency = 2
stop_window_ms = 180000
fallback_exit_option_price = 0.03
stock_tick_source = "trades"
trade_exit_option_window_ms = 45000
output = "data/strategies/custom.parquet"
random_seed = 123
""",
        encoding="utf-8",
    )

    config = load_pipeline_config(path)

    assert config.strategy.ticker == "QQQ"
    assert config.strategy.date_cut == "20160101"
    assert config.strategy.need_time == 9.7
    assert config.strategy.cut_delta_up == 0.41
    assert config.strategy.cut_delta_down == 0.11
    assert config.strategy.need_delta == 0.36
    assert config.strategy.account_id == "U2"
    assert config.strategy.latency_path == "data/custom_latency.csv"
    assert config.strategy.latency_max_seconds == 60.0
    assert config.strategy.option_concurrency == 4
    assert config.strategy.tick_concurrency == 2
    assert config.strategy.stop_window_ms == 180_000
    assert config.strategy.fallback_exit_option_price == 0.03
    assert config.strategy.stock_tick_source == "trades"
    assert config.strategy.trade_exit_option_window_ms == 45_000
    assert config.strategy.output == "data/strategies/custom.parquet"
    assert config.strategy.random_seed == 123
