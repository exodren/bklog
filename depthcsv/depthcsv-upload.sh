#!/bin/bash
# Выгружает закрытые файлы (рядом есть <file>.done) в rclone-remote и удаляет их локально.
# rclone move удаляет локальный файл только после успешной загрузки и сверки размера и хэша (у Google Drive — MD5);
# --checksum: уже лежащий в remote файл с тем же хэшем не перекачивается.
set -euo pipefail
DIR=${OUT_DIR:-/var/lib/depthcsv}
REMOTE=${RCLONE_REMOTE:-gdrive:depthcsv/}
MIN_FREE_GB=${MIN_FREE_GB:-5}
export RCLONE_CONFIG=${RCLONE_CONFIG:-/etc/depthcsv/rclone.conf}

exec 9>"$DIR/.upload.lock"
flock -n 9 || { echo "предыдущая выгрузка ещё идёт"; exit 0; }

free=$(df --output=avail -B1G "$DIR" | tail -1 | tr -d ' ')
if (( free < MIN_FREE_GB )); then echo "<4>WARN: свободно ${free} ГБ (< ${MIN_FREE_GB} ГБ) в $DIR"; fi

cd "$DIR"
list=$(mktemp)
trap 'rm -f "$list"' EXIT
while read -r m; do
  f=${m%.done}
  if [[ -f $f ]]; then echo "$f"; else rm -f -- "$m"; fi   # .done без файла — остался от прерванного запуска
done < <(find . -name '*.done' -printf '%P\n' | sort) > "$list"

n=$(wc -l < "$list")
(( n > 0 )) || exit 0
echo "выгружаю $n файлов в $REMOTE"
rc=0
rclone move "$DIR" "$REMOTE" --files-from-raw "$list" --checksum --no-traverse \
  --transfers 2 --retries 3 --low-level-retries 10 --log-level INFO || rc=$?

left=0
while read -r f; do
  if [[ -e $f ]]; then left=$((left + 1)); else rm -f -- "$f.done"; fi
done < "$list"
if (( left > 0 )); then echo "<4>WARN: не выгружено $left из $n (rclone rc=$rc), повтор при следующем запуске"; fi
exit "$rc"
