# depthcsv

Записывает публичные потоки Binance USDⓈ-M Futures без потерь, чтобы потом использовать их для бэктеста:
`<sym>@depth@100ms` (diff-стакан), `<sym>@aggTrade` и REST-снимки `/fapi/v1/depth?limit=1000`. API-ключи не нужны.
Включена проверка `depthcsv verify`: она собирает стакан из записанных данных и сверяет его со снимками.

## Формат
Строка файла: `<recv_ts_ns> <JSON>`. `recv_ts_ns` — локальное время приёма (wall clock, нс). JSON — сообщение
WebSocket целиком, как пришло (`{"stream":..,"data":{E,T,s,U,u,pu,b,a}}`), или тело REST-ответа
(`{"lastUpdateId",E,T,bids,asks}`). Цены и объёмы остаются строками биржи.

```
<OUT_DIR>/<SYM>/<SYM>_depth_YYYYMMDD_HHMMSS.jsonl.gz   diff depth + маркеры GAP
<OUT_DIR>/<SYM>/<SYM>_trade_YYYYMMDD_HHMMSS.jsonl.gz   aggTrade + маркеры GAP
<OUT_DIR>/<SYM>/<SYM>_snap_YYYYMMDD_HHMMSS.jsonl.gz    REST-снимки
```
Время в имени — время первой строки (UTC). Файлы режутся по часу (`ROTATE_MIN`). Пока файл пишется, он называется
`*.part` и сбрасывается на диск раз в 5 с. После закрытия появляется пустой `<file>.done`: файл готов, его можно забирать.
Автоматически рекордер ничего не удаляет, чистить диск нужно вручную (см. «Забор данных»).
Если после падения остались `.part`, при старте они закрываются: читаются до последнего сброса, `verify` пометит их как оборванные.
Чтение: `zcat file.jsonl.gz | head` или `gzip.open(...)` в Python.

### Маркеры GAP
`<ns> {"gap":{"s":"BTCUSDT","stream":"depth","reason":"...", ...}}`. Пишутся в тот же файл, поэтому порядок событий сохраняется.

| reason | когда |
|---|---|
| `start` / `shutdown` | запуск и остановка процесса |
| `disconnect` (+`detail`) | соединение оборвалось (в том числе суточный обрыв Binance), дальше переподключение |
| `pu_mismatch` (+`prev_u`,`pu`,`U`,`u`) | `pu` не равен `u` предыдущего события, запрашивается новый снимок |
| `queue_overflow` (+`lost`) | очередь записи переполнилась: сколько строк потеряно, запрашивается снимок |
| `trade_id_jump` (+`prev_a`,`a`) | пропуск в id aggTrade |
| `trade_watchdog` | по `WATCHDOG_SYMBOL` нет aggTrade `TRADE_WATCHDOG_SEC` с: WARN и переподключение |

Ничего не теряется молча: каждая потерянная строка входит в `lost` ближайшего маркера `queue_overflow`.

### Снимки и лимиты
Снимок запрашивается после первого события каждого depth-соединения (стрим к этому моменту уже пишется, это и есть
буфер из процедуры Binance), после каждого GAP и раз в `SNAPSHOT_SEC` (по умолчанию 600 с). Если `lastUpdateId` раньше
начала потока, запрос повторяется. Вес одного запроса 20 при лимите 2400 в минуту на IP. Ограничения в рекордере:
по одному символу не чаще раза в 5 с, между запросами 250 мс, при `X-MBX-USED-WEIGHT-1M > 1800` пауза до новой минуты,
при 429/418 ждём `Retry-After`.

### RPI-ордера
По документации Binance RPI-ордера (Retail Price Improvement) **исключены** и из `@depth`, и из `/fapi/v1/depth`. Они есть
только в `@rpiDepth@500ms` и `/fapi/v1/rpiDepth`. Источники здесь согласованы, отдельный учёт RPI в `verify` не нужен.
Смешивать их с rpi-источниками нельзя. Для бэктеста это значит, что ликвидность RPI в записанном стакане не видна.

## Проверка
```bash
depthcsv verify /var/lib/depthcsv                 # все символы в каталоге
depthcsv verify /mnt/copy BTCUSDT ETHUSDT          # любая копия каталога
```
Как собирается стакан: события с `u < L` отбрасываются; первым применяется событие с `pu ≤ L ≤ u`, это условие
`U ≤ L ≤ u` из документации плюс случай, когда L попадает между событиями; дальше проверяется `pu == u` предыдущего.
Контроль на каждом следующем снимке: верхние 50 уровней bid/ask собранного стакана после события, накрывающего его
`lastUpdateId`, сравниваются со стаканом «снимок + то же событие» (оба соответствуют моменту `u`).

Вывод по каждому символу: число контрольных снимков и несовпадений с долей, список разрывов с причиной и длительностью
до ресинхронизации, **необъяснённые** разрывы (`pu` не сходится, а маркера нет), покрытие (доля времени, когда стакан
синхронизирован), сводка aggTrade и таблица по дням UTC. Код выхода 0, если всё сошлось. 1, если есть несовпадения,
необъяснённые разрывы или не было ни одной проверки.

## Установка (Ubuntu)
```bash
curl https://sh.rustup.rs -sSf | sh && . ~/.cargo/env
sudo apt install -y build-essential
cargo build --release && cargo test
sudo useradd -r -s /usr/sbin/nologin depthcsv
sudo install -D target/release/depthcsv /opt/depthcsv/depthcsv
sudo install -m 644 depthcsv.env.example /etc/depthcsv.env      # поправить SYMBOLS при необходимости
sudo cp depthcsv.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now depthcsv     # таймер выгрузки НЕ включаем
journalctl -u depthcsv -f
```
Раз в минуту в журнал пишется строка `stats/мин: rows gaps reconnects parse_err dropped write_err queue_len free_gb`
(`free_gb` — свободное место на диске `OUT_DIR`; если меньше `MIN_FREE_GB`, по умолчанию 5, пишется WARN). Предупреждения
начинаются с `WARN`. При старте проверяется синхронизация часов: `timedatectl` и расхождение с `/fapi/v1/time`
больше 500 мс дают WARN. Нужен `chrony` или `systemd-timesyncd`.

### Забор данных на Windows (rclone sftp)
Данные забираются с ПК по SSH, на сервере ничего настраивать не нужно. Команда `copy` ничего не удаляет на сервере.
Фильтр `*.jsonl.gz` берёт только закрытые файлы: у открытых суффикс `.part`.
```powershell
winget install Rclone.Rclone
# один раз: remote "srv" по SSH-ключу (или pass=... через `rclone obscure`)
rclone config create srv sftp host=СЕРВЕР user=root key_file=$env:USERPROFILE\.ssh\id_ed25519

# забрать новые файлы (повторный запуск докачивает только новое; хэши сверяются через md5sum на сервере)
rclone copy srv:/var/lib/depthcsv D:\depthcsv --include "*.jsonl.gz" --checksum --transfers 4 -P
# убедиться, что всё на ПК совпадает с сервером
rclone check srv:/var/lib/depthcsv D:\depthcsv --include "*.jsonl.gz" --one-way
```
Диск на сервере чистить вручную и только после успешного `rclone check`, например файлы старше 7 дней:
`find /var/lib/depthcsv -name '*.jsonl.gz*' -mtime +7 -delete`. При 2 символах ожидается около 1 ГБ в сутки,
так что 60 ГБ хватит примерно на 4–6 недель.

### Выгрузка на сервере (по умолчанию выключена)
`depthcsv-upload.sh`, `.service` и `.timer` оставлены на случай, если понадобится выгрузка с сервера. Скрипт под `flock`
копирует (`rclone copy --checksum`, без удаления) файлы, у которых есть `.done`, в `RCLONE_REMOTE` из `/etc/depthcsv.env`.
Пока `RCLONE_REMOTE` не задан, скрипт только проверяет свободное место. Чтобы включить: задать `RCLONE_REMOTE`, настроить
`rclone config --config /etc/depthcsv/rclone.conf`, установить `/opt/depthcsv/depthcsv-upload.sh` и юниты, затем
`systemctl enable --now depthcsv-upload.timer`.

## Проверка готовности (2 часа, BTCUSDT + ETHUSDT)
```bash
sudo systemctl start depthcsv; sleep 2h; sudo systemctl stop depthcsv
/opt/depthcsv/depthcsv verify /var/lib/depthcsv      # ждём: несовпадений 0, необъяснённых 0, ИТОГ: OK
sudo systemctl start depthcsv
```
Без биржи можно проверить на mock-сервере: `python3 ../tests/mock_binance.py` (нужен `pip install websockets`), затем
`WS_DEPTH_BASE=ws://127.0.0.1:18081/public/stream WS_TRADE_BASE=ws://127.0.0.1:18081/market/stream REST_BASE=http://127.0.0.1:18080 OUT_DIR=./data SNAPSHOT_SEC=10 TRADE_WATCHDOG_SEC=0 ./target/release/depthcsv`.
Mock делает один разрыв `pu` на 8-й секунде и один обрыв depth-соединения на 14-й. `verify` должен показать оба как объяснённые.

## Ограничения
- Объём не замерялся. Оценка: 0,2–0,5 ГБ gzip в сутки на крупный символ (depth составляет основную часть).
- Между обрывом соединения и следующим снимком данных нет. Этот интервал помечен GAP и показан в `verify` с длительностью.
- `trade_id_jump` исходит из того, что id aggTrade идут подряд. Если на реальных данных такие маркеры появляются без
  обрывов, значит, Binance пропускает id (например, для ADL- или страховых сделок). Это не потеря данных.
