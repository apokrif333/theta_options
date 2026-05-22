# Agent Notes

- Keep `ThetaTerminal/` as a local runtime dependency. Do not commit credentials or large data files.
- Use `docs/api/openapiv3.yaml` as the checked API reference; `ThetaTerminal/openapiv3.yaml` is the vendor copy.
- Current implementation scope is `SPY` EOD plus `SPY` M1 download/enrichment. Tick data should remain a separate loader under `src/thetadata_pipeline/loaders/`.
- The canonical EOD output is `data/options/EOD/with_greeks/SPY_etf_greeks.parquet`.
- M1 raw option files live in `data/options/m1/`; M1 enriched outputs live in `data/options/m1/with_greeks/`.
- M1 stock files live in `data/stocks/m1/` and are used for `baseClose` by `date + ms_of_day`.
- Avoid permanent raw-data copies for EOD. Use temporary staging files and merge into the canonical output.
