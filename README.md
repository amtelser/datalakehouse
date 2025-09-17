# 📊 Datalakehouse IoT Telematics

Pipeline analítico para telemetría GPS usando un enfoque **Lakehouse** (Iceberg + Nessie) con procesamiento **streaming** y **batch** sobre Flink y consultas interactivas en Trino.

## 🧱 Arquitectura (alto nivel)
| Flujo | Componente | Rol |
|-------|------------|-----|
| Ingesta | Confluent Kafka | Stream de eventos decodificados |
| Procesamiento streaming | Flink SQL | Inserción en `gps_reports` y mantenimiento `latest_gps_by_device` |
| Procesamiento streaming | Flink SQL | Inserción en `raw and dlq` |
| Batch diario | Flink SQL | Cálculo de score de riesgo (`risk_score_daily`) |
| Catálogo | Nessie | Versionado (branches, commits, snapshots) |
| Formato / Tablas | Apache Iceberg | Tablas ACID particionadas / evolución de esquema |
| Almacenamiento | S3 | Data Lake (archivos Parquet) |
| Metastore Nessie | Postgres | Persistencia de metadatos |
| SQL interactivo | Trino | Exploración / BI |
| API | FastAPI (telematics_api) | Exposición REST de datos (Trino) |

---

## 🚀 Quick Start
1. Requisitos: Docker + Docker Compose.
2. Levantar servicios base:
  ```bash
  docker compose up -d
  ```
3. Confirmar UI:
  - Flink: http://localhost:8081/
  - Trino: http://localhost:8080/
  - Nessie (UI): http://localhost:19120/content/main/telematics/gps_reports
4. Ejecutar SQL de creación (catálogo + tablas): ver sección siguiente.
5. Lanzar jobs streaming.
6. (Opcional) Ejecutar job batch de riesgo.
7. Consultar en Trino o vía API.

---

## 📂 Archivos SQL (carpeta `config/flink/`)
- `create.sql`: crea catálogo Nessie, DB `telematics`, tablas Iceberg y fuentes temporales (Kafka / JDBC Postgres).
- `gps_reports.sql`: job streaming → ingesta Kafka → Iceberg (`gps_reports`).
- `latest_gps_by_device.sql`: job streaming → tabla upsert con última posición por dispositivo.
- `telematics_raw_dlq.sql`: job streaming → ingesta Kafka → Iceberg (`raw and dlq`).
- `risk_score_daily.sql`: job batch → calcula score y escribe en tabla Iceberg `risk_score_daily`.
- `cleanup_raw_dlq.sql` (si se usa) / scripts auxiliares.

---

## ▶️ Ejecución de Jobs (desde contenedor Flink `jobmanager`)

### 1. Crear catálogo + tablas
```bash
docker exec -it jobmanager bash -lc "bin/sql-client.sh -f /opt/sql/create.sql"
```

### 2. Streaming → Ingesta `gps_reports`
```bash
docker exec -it jobmanager bash -lc "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/gps_reports.sql"
```

### 3. Streaming → Ingesta `raw and dlq`
```bash
docker exec -it jobmanager bash -lc "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/telematics_raw_dlq.sql"
```

### 4. Streaming → Tabla `latest_gps_by_device`
```bash
docker exec -it jobmanager bash -lc "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/latest_gps_by_device.sql"
```

### 5. Batch diario → Score de riesgo (elige destino)
Iceberg (tabla `telematics.risk_score_daily`):
```bash
docker exec -it jobmanager bash -lc "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/risk_score_daily.sql"
```

#### Ajustar rango de fechas
Ambos scripts definen un CTE `rango`:
```sql
WITH rango AS (
  SELECT DATE 'YYYY-MM-DD' AS d_ini, DATE 'YYYY-MM-DD' AS d_fin
)
```
Modifica `d_ini` y `d_fin` antes de lanzar el job para recalcular un intervalo histórico. (Podrías parametrizar en el futuro usando variables externas o plantillas.)

---

## 🔍 Consultas en Trino
Abrir CLI:
```bash
docker exec -it trino trino
```
Ejemplos:
```sql
SHOW TABLES IN nessie.telematics;

SELECT * FROM nessie.telematics.gps_reports ORDER BY received_epoch DESC LIMIT 10;

SELECT * FROM nessie.telematics.latest_gps_by_device LIMIT 20;

-- Ajusta la fecha (columna report_date)
SELECT *
FROM nessie.telematics.risk_score_daily
WHERE report_date = DATE '2025-08-31'
ORDER BY score DESC
LIMIT 20;
```

---

## 🧪 API REST (`telematics_api`)
Levantar sólo la API (si ya está el stack principal):
```bash
docker compose up -d telematics_api
```
Endpoints (requieren header `Authorization: Bearer token1` por defecto de ejemplo):
- `GET /health`
- `GET /gps_reports?limit=100`
- `GET /gps_reports?device_id=...&from_ts=...&to_ts=...`
- `GET /latest_gps_by_device?device_id=...`
- `GET /risk_score_daily?day=YYYY-MM-DD`

Ejemplos:
```bash
curl -H "Authorization: Bearer token1" http://localhost:9009/health
curl -H "Authorization: Bearer token1" "http://localhost:9009/gps_reports?limit=50"
```

---

## 🛠️ Mantenimiento / Utilidades
- `scripts/cleanup_gps_reports.sh`: ejemplo de limpieza / utilitario (ajustar antes de usar).
- Particiones Iceberg: revisar en S3 o vía `DESCRIBE TABLE` en Trino.
- Actualización de credenciales: externalizar en variables / `.env` (actualmente algunos valores están embebidos en `create.sql`).

---

## ⚠️ Observaciones Técnicas / TODO
1. `create.sql` define partición `'partitioning' = 'score_date, bucket(1024, device_id)'` para `risk_score_daily`, pero la columna se llama `report_date`. Verificar y alinear (renombrar a `score_date` o ajustar partición).
2. Externalizar secretos Kafka / Postgres (no versionar credenciales reales).
3. Parametrizar rango de fechas del job batch (ej: pasar como variables de entorno y hacer template).
4. Añadir tests ligeros para API (FastAPI + pytest) y un Makefile.
5. Monitoreo: integrar Flink metrics / Prometheus (opcional futuro).

---

## 📌 Notas importantes
- Jobs streaming se mantienen activos; al reiniciar el cluster se deben volver a lanzar si no hay savepoints configurados.
- El job batch puede re-ejecutarse múltiples veces para recalcular (upsert) el intervalo definido.
- Evitar `docker compose down -v` para no borrar volúmenes (Postgres + datos Iceberg).
- Usar ramas / commits Nessie para experimentos (ej: crear branch y comparar outputs de scoring).

---

## 🔎 Troubleshooting rápido
| Problema | Causa típica | Acción |
|----------|--------------|--------|
| Tabla vacía en Trino | Job streaming no iniciado | Revisar Jobs en UI Flink / relanzar |
| Error conector Kafka | Credenciales / offsets | Validar config en `create.sql` y conectividad | 
| No aparece commit Nessie | Job no escribió / falló | Logs del job en Flink / revisar excepciones |
| Duplicados en latest | Upsert no activo | Confirmar `write.upsert.enabled` y PK NOT ENFORCED |

---

## 📒 Referencia rápida (cheat sheet)
```bash
# Crear objetos base
docker exec -it jobmanager bash -lc "bin/sql-client.sh -f /opt/sql/create.sql"

# Jobs streaming
docker exec -it jobmanager bash -lc "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/gps_reports.sql"
docker exec -it jobmanager bash -lc "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/latest_gps_by_device.sql"

# Batch riesgo (Iceberg)
docker exec -it jobmanager bash -lc "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/risk_score_daily.sql"

# Trino CLI
docker exec -it trino trino
```

---

## ✅ Estado actual
- Ingesta y tablas Iceberg: OK
- Latest por dispositivo: OK (upsert habilitado)
- Score riesgo: scripts duales (Iceberg/Postgres) operativos
- API: disponible para consultas básicas

---

Contribuciones / mejoras bienvenidas. Mantener consistencia de estilo SQL y evitar credenciales hardcode en futuros cambios.