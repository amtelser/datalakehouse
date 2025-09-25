#!/usr/bin/env bash
set -euo pipefail

echo "=== [$(date)] Iniciando batch Flink + Trino ==="

# 1) Batch riesgo (Iceberg)
echo "[INFO] Ejecutando Batch Riesgo (Iceberg)..."
docker exec -i jobmanager bash -lc \
  "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/sink_risk_score_daily.sql"

# 2) Batch Cleanup RAW And DLQ
echo "[INFO] Ejecutando Cleanup RAW & DLQ..."
docker exec -e TRINO_PASSWORD='cleanup2025' -i iothub-stack-trino-1 bash -lc \
'trino \
  --server https://localhost:8080 \
  --insecure \
  --user cleanup \
  --password \
  --catalog nessie \
  --schema telematics \
  -f /opt/sql/cleanup.sql'

echo "=== [$(date)] Batch finalizado ==="