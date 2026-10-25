# bklog — логгер стакана Binance Futures (Rust)

Пишет **публичные** потоки Binance USDⓈ-M Futures (diff depth 100 мс + aggTrade) и периодические REST-снимки стакана
в компактный бинарный формат. API-ключи и аккаунт Binance **не нужны** — на сервер ключи класть не надо.

## Сборка и запуск
```bash
curl https://sh.rustup.rs -sSf | sh        # если нет Rust
cargo build --release                       # бинарники: target/release/bklog, target/release/bkcat
./target/release/bklog --symbols btcusdt,ethusdt --out /data/bklog
```
Основные флаги (`--help`): `--symbols`, `--out`, `--snapshot-secs 600`, `--min-free-gb 5`, `--zstd-level 10`,
`--flush-secs 5`, `--depth-ms 100|500`, `--public-ws/--market-ws/--rest` (переопределение адресов).

Адреса по умолчанию — новая схема Binance: depth на `wss://fstream.binance.com/public/stream`,
aggTrade на `wss://fstream.binance.com/market/stream` (старые `/ws` и `/stream` Binance объявил выводимыми из эксплуатации;
дату в их документации сверьте).

## Что и как хранится
`<out>/<SYMBOL>/<YYYY-MM-DD>/<SYMBOL>_<YYYY-MM-DDTHH>.bklog.zst` — по файлу в час (UTC). Открытый файл имеет суффикс `.part`
и после закрытия переименовывается. Каждый час — самостоятельный файл (состояние кодека сбрасывается).

Внутри zstd-поток: заголовок + записи `[тип][длина][payload]`; числа — varint, время/id/цены — дельты, цены и объемы —
целые (по `pricePrecision`/`quantityPrecision` из exchangeInfo), без float. Типы записей:
- `DEPTH` — каждое diff-сообщение целиком (U, u, pu, все изменённые уровни) + время приема `recv_us` + время биржи (E, T);
- `TRADE` — aggTrade (id, цена, объем, сторона агрессора);
- `SNAPSHOT` — REST-снимок 1000 уровней (при старте, при каждом переподключении и разрыве цепочки, и раз в `--snapshot-secs`);
- `MARKER` — connect / disconnect / gap (разрыв pu≠prev u) / dropped (переполнение очереди) / snapshot_fail / shutdown / parse_err.

Потеря при `kill -9` / падении питания — не больше `--flush-secs` секунд, `.part`-файл читается до последнего сброса.

## Проверка и чтение
```bash
bkcat stats  data/BTCUSDT/*/*.zst        # записи, разрывы, задержка recv-E, МБ/сутки
bkcat dump   data/BTCUSDT/2026-10-26/BTCUSDT_2026-10-26T14.bklog.zst -n 20
bkcat book   data/BTCUSDT/2026-10-26/*.zst --every-ms 1000 > book.csv   # top-of-book, spread, OBI5
```
`bklog::reader::RecReader` + `bklog::book::Book` — готовые блоки для бэктестера (восстановление стакана по правилам Binance).

## Эксплуатация
- Время: нужен `chrony`/`systemd-timesyncd` (`timedatectl status` → synchronized). Для анализа используйте время биржи E/T;
  `recv_us` — локальные часы.
- Каждую минуту в логе: число сообщений, gaps, reconnects, dropped, задержка recv−E, сжатый объем, свободное место.
- Свободного места меньше `--min-free-gb` → файлы закрываются корректно, код выхода 2 (в unit-файле перезапуск при этом отключён).
- `bklog.service` — пример unit-файла (systemd).
- Тест без биржи: `python3 tests/mock_binance.py` и запуск логгера с `--public-ws ws://127.0.0.1:18081/public/stream --market-ws ws://127.0.0.1:18081/market/stream --rest http://127.0.0.1:18080`.

## Ограничения v1
Только Binance Futures (depth, aggTrade). Не пишет markPrice/funding, forceOrder, bookTicker (bookTicker восстанавливается из depth).
Суточный разрыв соединения Binance (24 ч) даёт короткую дыру ~1–2 с — она помечена маркерами и закрывается новым снимком.
