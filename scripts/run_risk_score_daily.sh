#!/usr/bin/env bash
set -euo pipefail

LOG_DIR="/var/log/datalakehouse"
mkdir -p "$LOG_DIR"

STAMP="$(date +'%Y-%m-%d_%H-%M-%S')"
LOG_FILE="${LOG_DIR}/risk_score_daily_${STAMP}.log"

# Evita solapamientos si se llegara a alargar
exec /usr/bin/flock -n /tmp/risk_score_daily.lock bash -lc "
  echo '=== START $(date -Is)'        | tee -a '$LOG_FILE' ;
  docker exec -i jobmanager bash -lc \
    \"bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/risk_score_daily.sql\" \
    2>&1 | tee -a '$LOG_FILE' ;
  STATUS=\${PIPESTATUS[0]} ;
  echo '=== END   $(date -Is) rc='\$STATUS | tee -a '$LOG_FILE' ;
  exit \$STATUS
"