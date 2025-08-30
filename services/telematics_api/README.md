# Telematics API (v4)

FastAPI + Trino API para exponer datos de tu Lakehouse (Iceberg/Nessie en Trino).

# levantar
docker compose up -d telematics_api

# health
curl -s http://localhost:9009/health

# gps_reports (últimos 100)
curl -s -H "Authorization: Bearer token1" \
  "http://localhost:9009/gps_reports?limit=100"

# gps_reports por device + rango
curl -s -H "Authorization: Bearer token1" \
  "http://localhost:9009/gps_reports?device_id=1520197325&from_ts=2025-08-31T00:00:00-06:00&to_ts=2025-08-31T23:59:59-06:00"

# latest_gps_by_device
curl -s -H "Authorization: Bearer token1" \
  "http://localhost:9009/latest_gps_by_device?device_id=1520197325"

# risk_score_daily por día
curl -s -H "Authorization: Bearer token1" \
  "http://localhost:9009/risk_score_daily?day=2025-08-31"

## Ejemplos
```bash
curl -H "Authorization: Bearer token1" "http://localhost:9009/health"
curl -H "Authorization: Bearer token1" "http://localhost:9009/latest/by-device?limit=5"
```
