# Целевая структура проекта

```text
ThetaData/
  .env.example                 # пример локальных runtime-настроек
  .gitignore                   # исключает .venv, data, секреты и временные файлы
  AGENTS.md                    # правила для автоматических агентов
  README.md                    # быстрый запуск и текущее состояние
  requirements.txt             # Python-зависимости
  main.py                      # CLI entrypoint
  agents/                      # runbook-и и заметки для агентов
  config/
    pipeline.toml              # тикеры, диапазоны дат, concurrency, режимы пайплайнов
  docs/
    PROJECT_STRUCTURE.md       # этот файл
    api/openapiv3.yaml         # проверенная копия OpenAPI v3 Theta Terminal
  notebooks/
    legacy/                    # старые .ipynb как исторический источник логики
  src/
    thetadata_pipeline/
      constants.py             # имена колонок, финальные схемы, дефолтные относительные пути
      settings.py              # .env и runtime-настройки
      theta_client.py          # HTTP-клиент к локальному Theta Terminal v3
      tiingo.py                # чтение локальных Tiingo CSV и расчёт dividend TTM
      rates.py                 # локальная история DGS1 и обновление из FRED
      dates.py                 # парсинг дат и формат Theta YYYYMMDD
      greeks.py                # локальный расчёт IV и греков
      cli.py                   # команды CLI
      runners/
        eod.py                 # запуск EOD из Python-файла/IDE
        m1.py                  # запуск M1 update/download/enrich из Python-файла/IDE
      loaders/
        eod.py                 # EOD download + enrich + merge
        m1.py                  # M1 stock/option download + local option enrich
  tests/                       # unit/smoke тесты для новых модулей
  data/                        # локальные большие данные, не коммитятся
    options/EOD/with_greeks/   # финальные EOD parquet с греками
    options/m1/                # raw option quote M1 parquet
    options/m1/with_greeks/    # M1 parquet с IV/греками
    stocks/m1/                 # stock OHLC M1 parquet для baseClose
    reference_rates/           # DGS1.parquet
    .staging/                  # временные parquet во время сборки
  ThetaTerminal/               # локальный runtime Theta Terminal
```

## Правила данных

- `ThetaTerminal/` остаётся локальной runtime-зависимостью.
- `docs/api/openapiv3.yaml` является рабочей проверенной копией API; vendor copy лежит в `ThetaTerminal/openapiv3.yaml`.
- Канонический EOD SPY файл: `data/options/EOD/with_greeks/SPY_etf_greeks.parquet`.
- EOD не хранит постоянный raw-слой. Скачанный диапазон пишется во временный parquet и сразу вливается в финальный файл.
- EOD даты берутся из фактического Theta stock EOD ответа. Простой business-day календарь не используется для проверки Tiingo.
- M1 update скачивает недостающий stock OHLC 1m через `/stock/history/ohlc` и option quote 1m через `/option/history/quote`.
- Текущий M1 option режим по умолчанию: `same_day`, то есть `expiration = quote date` для 0DTE SPY.
- M1 concurrency задаётся отдельно для stock и option: `1`, `2`, `4` или `8` concurrent requests.
- M1 enrichment работает поверх локальных raw option M1 parquet и локального stock M1 parquet.
- M1 `baseClose` присоединяется только точным `date + ms_of_day`. Если stock-минута отсутствует, день stock M1 перекачивается; fallback на предыдущую минуту запрещён.
- Tiingo не обновляется этим проектом. Если локальный Tiingo CSV не покрывает нужные даты, запуск падает.
- DGS1 не подменяется дефолтной ставкой. Если локальный файл не покрывает нужный диапазон и обновление FRED не помогло, запуск падает.

## Текущий порядок развития

1. EOD SPY: рабочий контур готов, дальше только исправления по фактическим ошибкам.
2. M1 SPY: работает download raw stock/options M1 и enrichment с греками.
3. M1 multi-symbol: расширить после проверки SPY на полном диапазоне.
4. Tick loader: отдельный этап после стабилизации M1.
5. Backtest export/snapshot layer: отдельный слой поверх готовых parquet.
