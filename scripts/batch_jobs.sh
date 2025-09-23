#!/usr/bin/env bash
set -euo pipefail

echo "=== [$(date)] Iniciando batch Flink + Trino ==="

# 1) Batch riesgo (Iceberg)
echo "[INFO] Ejecutando Batch Riesgo (Iceberg)..."
docker exec -i jobmanager bash -lc \
  "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/sink_risk_score_daily.sql"

# 2) Batch Cleanup RAW And DLQ
echo "[INFO] Ejecutando Cleanup RAW & DLQ..."
docker exec -i iothub-stack-trino-1 bash -lc \
'trino \
  --server https://localhost:8080 \
  --insecure \
  --user cleanup \
  --catalog nessie \
  --schema telematics \
  -f /opt/sql/cleanup_telematics_raw_dlq.sql'

# 3) Batch Cleanup telematics real time
echo "[INFO] Ejecutando Cleanup telematics real time..."
docker exec -i iothub-stack-trino-1 bash -lc \
'trino \
  --server https://localhost:8080 \
  --insecure \
  --user cleanup \
  --catalog nessie \
  --schema telematics \
  -f /opt/sql/cleanup_telematics_real_time.sql'

echo "=== [$(date)] Batch finalizado ==="