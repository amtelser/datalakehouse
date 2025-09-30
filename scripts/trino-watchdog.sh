#!/bin/bash
set -euo pipefail

THRESHOLD_MB=2048   # 2 GB

while true; do
  free_mb=$(awk '/MemAvailable:/ {print int($2/1024)}' /proc/meminfo)
  hour=$(date +%H)

  if [ "$free_mb" -lt "$THRESHOLD_MB" ]; then
    echo "$(date) - Memoria libre ${free_mb} MB < ${THRESHOLD_MB} MB, reiniciando Trino" >> /var/log/trino-watchdog.log
    docker compose restart trino
    sleep 300   # espera 5 min tras reinicio
  elif [ "$hour" -eq 9 ]; then
    echo "$(date) - Reinicio programado diario de Trino (9am UTC)" >> /var/log/trino-watchdog.log
    docker compose restart trino
    sleep 3600
  fi

  sleep 60
done