# ThetaData pipeline

Проект забирает данные из локального Theta Terminal v3 и готовит датасеты для тестирования стратегий. Текущий рабочий контур:

- `SPY` EOD: докачка недостающих дат, расчёт IV/греков, merge в финальный parquet.
- `SPY` M1: докачка stock OHLC 1m и option quote 1m, затем локальный расчёт IV/греков.
- Tick data будет отдельным этапом после стабилизации M1.

## Быстрый запуск

1. Запустить `ThetaTerminalv3.jar`.
2. Проверить `.env`: особенно `THETA_TIINGO_USA_ROOT`, `THETA_RISK_FREE_RATES_PATH`, M1-директории.
3. Настроить тикеры, даты и concurrency в `config/pipeline.toml`.

EOD dry-run:

```powershell
.\.venv\Scripts\python.exe main.py eod update --dry-run
```

EOD update:

```powershell
.\.venv\Scripts\python.exe main.py eod update
```

M1 update dry-run: скачать недостающие raw M1 и обогатить греками:

```powershell
.\.venv\Scripts\python.exe main.py m1 update --dry-run
```

M1 update:

```powershell
.\.venv\Scripts\python.exe main.py m1 update
```

Smoke-check на один день:

```powershell
.\.venv\Scripts\python.exe main.py m1 update --max-days 1 --overwrite-greeks
```

Только скачать raw M1 без расчёта греков:

```powershell
.\.venv\Scripts\python.exe main.py m1 download
```

Только пересчитать греки по уже локальным option M1 parquet:

```powershell
.\.venv\Scripts\python.exe main.py m1 enrich --overwrite
```

Ручное обновление локального DGS1:

```powershell
.\.venv\Scripts\python.exe main.py rates update
```

## Concurrency

Для M1 можно выбирать число concurrent requests: `1`, `2`, `4`, `8`.

```powershell
.\.venv\Scripts\python.exe main.py m1 update --stock-concurrency 2 --option-concurrency 4
```

Текущие значения в `config/pipeline.toml`:

```toml
[m1]
stock_concurrency = 2
option_concurrency = 4
option_expiration_mode = "same_day"
```

`same_day` означает 0DTE: `expiration = quote date`. Это соответствует текущим raw M1 файлам SPY. Режим `all` запрашивает `expiration=*` и может быть на порядки тяжелее.

## Python runners

Для запуска из IDE:

```powershell
.\.venv\Scripts\python.exe src\thetadata_pipeline\runners\eod.py --dry-run
.\.venv\Scripts\python.exe src\thetadata_pipeline\runners\eod.py
.\.venv\Scripts\python.exe src\thetadata_pipeline\runners\m1.py --dry-run
.\.venv\Scripts\python.exe src\thetadata_pipeline\runners\m1.py --max-days 1
.\.venv\Scripts\python.exe src\thetadata_pipeline\runners\m1.py --mode enrich --overwrite
```

## Данные

Канонический EOD файл:

```text
data/options/EOD/with_greeks/SPY_etf_greeks.parquet
```

Raw option M1 файлы:

```text
data/options/m1/*_SPY_m1_opts.parquet
```

Stock M1 файлы для базовой цены:

```text
data/stocks/m1/*SPY*.parquet
```

M1 with-greeks результат:

```text
data/options/m1/with_greeks/*_SPY_m1_greeks_opts.parquet
```

## Строгие справочники

Дивиденды берутся только из локальной Tiingo базы. Если Tiingo не покрывает даты, которые обрабатывает пайплайн, запуск падает. Автодокачки Tiingo в этом проекте нет.

DGS1 хранится в `data/reference_rates/DGS1.parquet`. Если для нужного диапазона нет локальной ставки и её не удалось обновить из FRED, запуск падает. Дефолтная risk-free ставка в расчётном пути не используется.

Для M1 `baseClose` берётся из локального stock M1 файла по ключу `date + ms_of_day`, а не из дневного close.
Если точной stock-минуты нет, M1 enrichment перекачивает stock M1 за этот день и повторяет exact join. Подстановка предыдущей минуты не используется.

## M1 схема

M1 enrichment пишет результат в компактных типах:

```text
ticker: String
expiration: Date
strike: Float32
right: String
ms_of_day: Int32
bid_size: Int32
bid_exchange: Int16
bid: Float32
bid_condition: Int16
ask_size: Int32
ask_exchange: Int16
ask: Float32
ask_condition: Int16
date: Date
dgs1: Float32
baseClose: Float32
timeToExp: Float32
IV_ask/delta_ask/gamma_ask/theta_ask/vega_ask/rho_ask: Float32
IV_bid/delta_bid/gamma_bid/theta_bid/vega_bid/rho_bid: Float32
```

`IV <= 0` считается нерешённым IV и записывается как `NaN`, чтобы нули не выглядели валидной волатильностью.
