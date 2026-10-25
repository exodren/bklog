#!/bin/bash
# Копирует закрытые файлы (рядом есть <file>.done) в rclone-remote из RCLONE_REMOTE. По умолчанию выключено:
# без RCLONE_REMOTE только проверяет свободное место. Локально ничего не удаляет, .done остаются;
# --checksum: уже скопированные файлы с тем же хэшем повторно не передаются.
set -euo pipefail
DIR=${OUT_DIR:-/var/lib/depthcsv}
REMOTE=${RCLONE_REMOTE:-}
MIN_FREE_GB=${MIN_FREE_GB:-5}
export RCLONE_CONFIG=${RCLONE_CONFIG:-/etc/depthcsv/rclone.conf}

exec 9>"$DIR/.upload.lock"
flock -n 9 || { echo "предыдущая выгрузка ещё идёт"; exit 0; }

free=$(df --output=avail -B1G "$DIR" | tail -1 | tr -d ' ')
if (( free < MIN_FREE_GB )); then echo "<4>WARN: свободно ${free} ГБ (< ${MIN_FREE_GB} ГБ) в $DIR"; fi

[[ -n $REMOTE ]] || { echo "RCLONE_REMOTE не задан — выгрузка выключена"; exit 0; }

cd "$DIR"
list=$(mktemp)
trap 'rm -f "$list"' EXIT
while read -r m; do
  f=${m%.done}
  if [[ -f $f ]]; then echo "$f"; fi
done < <(find . -name '*.done' -printf '%P\n' | sort) > "$list"

n=$(wc -l < "$list")
(( n > 0 )) || exit 0
echo "копирую $n файлов в $REMOTE"
rclone copy "$DIR" "$REMOTE" --files-from-raw "$list" --checksum --no-traverse \
  --transfers 2 --retries 3 --low-level-retries 10 --log-level INFO
