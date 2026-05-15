# ThetaData pipeline

Проект забирает данные из локального Theta Terminal v3 и готовит датасеты для тестирования стратегий. Текущий рабочий контур:

- `SPY` EOD: докачка недостающих дат, расчёт IV/греков, merge в финальный parquet.
- `SPY` M1: обогащение уже скачанных локальных option M1 parquet греческими метриками.
- Live-download M1 через Theta будет добавлен отдельным шагом после покупки подписки.

## Быстрый запуск

1. Запустить `ThetaTerminalv3.jar`.
2. Проверить `.env`: особенно `THETA_TIINGO_USA_ROOT`, `THETA_RISK_FREE_RATES_PATH`, M1-директории.
3. Настроить тикеры и даты в `config/pipeline.toml`.

EOD dry-run:

```powershell
.\.venv\Scripts\python.exe main.py eod update --dry-run
```

EOD update:

```powershell
.\.venv\Scripts\python.exe main.py eod update
```

M1 dry-run по локальным option M1 файлам:

```powershell
.\.venv\Scripts\python.exe main.py m1 enrich --dry-run
```

M1 enrichment:

```powershell
.\.venv\Scripts\python.exe main.py m1 enrich
```

Принудительный пересчёт уже существующих M1 with-greeks файлов:

```powershell
.\.venv\Scripts\python.exe main.py m1 enrich --overwrite
```

Ограниченный smoke-check по одной дате:

```powershell
.\.venv\Scripts\python.exe main.py m1 enrich --from-date 20200103 --end-date 20200103 --overwrite
```

Ручное обновление локального DGS1:

```powershell
.\.venv\Scripts\python.exe main.py rates update
```

## Python runners

Для запуска из IDE:

```powershell
.\.venv\Scripts\python.exe src\thetadata_pipeline\runners\eod.py --dry-run
.\.venv\Scripts\python.exe src\thetadata_pipeline\runners\eod.py
.\.venv\Scripts\python.exe src\thetadata_pipeline\runners\m1.py --dry-run
.\.venv\Scripts\python.exe src\thetadata_pipeline\runners\m1.py --overwrite
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

Stock M1 файл для базовой цены:

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
