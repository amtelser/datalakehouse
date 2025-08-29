# Telematics API (v4)

FastAPI + Trino API para exponer datos de tu Lakehouse (Iceberg/Nessie en Trino).

## Endpoints
- `GET /health`
- `GET /latest/by-device?device_ids=ID1,ID2&limit=100`
- `GET /gps/history?device_id=ID&date_str=YYYY-MM-DD&limit=1000`
- `GET /risk/score/daily?score_date=YYYY-MM-DD&limit=1000`

## Autenticación
Bearer tokens en la cabecera `Authorization: Bearer <token>`.
Configura tokens en `API_TOKENS` (separados por coma).

## Zona horaria
La API ejecuta `SET TIME ZONE '<TIME_ZONE>'` en cada conexión y convierte campos
`TIMESTAMP WITH TIME ZONE` a hora local con `AT TIME ZONE '<TIME_ZONE>'` + `CAST(... AS timestamp)`.

## Build & Run
```bash
docker build -t telematics-api:v4 .
cp .env.example .env   # edita tokens/host
docker run -d --name telematics_api -p 9009:9009 --env-file .env telematics-api:v4
```

## Ejemplos
```bash
curl -H "Authorization: Bearer token1" "http://localhost:9009/health"
curl -H "Authorization: Bearer token1" "http://localhost:9009/latest/by-device?limit=5"
```
