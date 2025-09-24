# 📊 Datalakehouse IoT Telematics

Pipeline analítico para telemetría GPS usando un enfoque **Lakehouse** (Iceberg + Nessie) con procesamiento **streaming** y **batch** sobre Flink y consultas interactivas en Trino.

## 🧱 Arquitectura (alto nivel)
| Flujo | Componente | Rol |
|-------|------------|-----|
| Ingesta | Confluent Kafka | Stream de eventos decodificados |
| Procesamiento streaming | Flink SQL | Inserción en `telematics_real_time` |
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
  - Nessie (UI): http://localhost:19120/content/main/telematics/telematics_real_time
4. Ejecutar SQL de creación (catálogo + tablas): ver sección siguiente.
5. Lanzar jobs streaming.
6. (Opcional) Ejecutar job batch de riesgo.
7. Consultar en Trino o vía API.

---

## 📂 Archivos SQL (carpeta `config/flink/`)
- `cleanup.sql` (cleanup) / scripts auxiliares.
- `create.sql`: crea catálogo Nessie, DB `telematics`, tablas Iceberg y fuentes temporales (Kafka / JDBC Postgres).
- `sink_risk_score_daily.sql`: job batch → calcula score y escribe en tabla Iceberg `sink_risk_score_daily`.
- `sink_telematics_real_time.sql`: job streaming → ingesta Kafka → Iceberg (`sink_telematics_real_time`).
- `sink_telematics_raw_dlq.sql`: job streaming → ingesta Kafka → Iceberg (`sink_telematics_raw_dlq`).

---

## ▶️ Ejecución de Jobs (desde contenedor Flink `jobmanager`)

### 1. Crear catálogo + tablas
```bash
docker exec -it jobmanager bash -lc "bin/sql-client.sh -f /opt/sql/create.sql"
```

### 2. Streaming → Ingesta `telematics_real_time`
```bash
docker exec -it jobmanager bash -lc "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/sink_telematics_real_time.sql"
```

### 3. Streaming → Ingesta `raw and dlq`
```bash
docker exec -it jobmanager bash -lc "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/sink_telematics_raw_dlq.sql"
```

### 4. Batch diario → Score de riesgo (elige destino)
Iceberg (tabla `telematics.risk_score_daily`):
```bash
docker exec -it jobmanager bash -lc "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/sink_risk_score_daily.sql"
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

SELECT * FROM nessie.telematics.telematics_real_time ORDER BY received_epoch DESC LIMIT 10;

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
- `GET /telematics_real_time?limit=100`
- `GET /telematics_real_time?device_id=...&from_ts=...&to_ts=...`
- `GET /risk_score_daily?day=YYYY-MM-DD`

Ejemplos:
```bash
curl -H "Authorization: Bearer token1" http://localhost:9009/health
curl -H "Authorization: Bearer token1" "http://localhost:9009/telematics_real_time?limit=50"
```

---

## 🛠️ Mantenimiento / Utilidades
- `scripts/cleanup.sh`: ejemplo de limpieza / utilitario (ajustar antes de usar).
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

---

## 📒 Referencia rápida (cheat sheet)
```bash
# Copiar el contenido al servidor: 
scp -r datalakehouse/* ubuntu@172.25.27.244:/opt/iothub-stack/
# Iniciar servicio
sudo systemctl start iothub-stack.service
# Iniciar FLINK SQL
docker compose exec jobmanager bin/sql-client.sh
```
---
```bash
# Crear objetos base
docker exec -it jobmanager bash -lc "bin/sql-client.sh -f /opt/sql/create.sql"

# Jobs streaming
docker exec -it jobmanager bash -lc "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/sink_telematics_real_time.sql"
docker exec -it jobmanager bash -lc "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/sink_telematics_raw_dlq.sql"

# Batch riesgo (Iceberg)
docker exec -it jobmanager bash -lc "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/sink_risk_score_daily.sql"

# Batch Cleanup
docker exec -it datalakehouse-trino-1 bash -lc \
'trino \
  --server https://localhost:8080 \
  --insecure \
  --user cleanup \
  --catalog nessie \
  --schema telematics \
  -f /opt/sql/cleanup.sql'

# Trino CLI
docker exec -it trino trino
```

---

## ✅ Estado actual
- Ingesta y tablas Iceberg: OK
- Latest por dispositivo: OK (upsert habilitado)
- Score riesgo: scripts duales (Iceberg/Postgres) operativos
- API: disponible para consultas básicas

## ✅ Cron
- chmod +x scripts/batch_jobs.sh
- crontab -e
- agregar para que se ejecute a las 1am (servidor en utc): 0 7 * * * /opt/iothub-stack/scripts/batch_jobs.sh >> /var/log/batch_jobs.log 2>&1