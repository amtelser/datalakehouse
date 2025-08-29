# 📊 Datalakehouse IoT Telematics

Este proyecto implementa un **Datalakehouse en contenedores Docker** para procesar y consultar en tiempo real datos de dispositivos GPS.  

Arquitectura:

- **Confluent Kafka** → fuente de datos (stream de telemetría).  
- **Flink SQL** → jobs streaming y batch (decodificación, latest por dispositivo, score de riesgo).  
- **Apache Iceberg + Nessie** → almacenamiento de tablas versionadas en MinIO.  
- **MinIO (S3 compatible)** → data lake.  
- **Postgres** → backend persistente de Nessie para no perder metadatos al reiniciar.  
- **Trino** → consultas analíticas SQL sobre Iceberg/Nessie.  

---

## 🏗️ Componentes

- **MinIO**: almacena los archivos Parquet del warehouse Iceberg.  http://localhost:9001/
- **Nessie**: catálogo Iceberg con control de versiones (branches, commits).   http://localhost:19120/content/main/telematics/gps_reports
- **Postgres**: base de datos para persistir la información de Nessie.  
- **Flink JobManager / TaskManager**: ejecuta jobs SQL de ingestión y batch.   http://localhost:8081/
- **Trino**: consultas analíticas SQL sobre Iceberg.  http://localhost:8080/
- **Kafka (Confluent Cloud)**: fuente de datos de telemetría GPS.  

---

## 📂 Estructura de SQL

- `create.sql`  
  Crea el catálogo Nessie, la base de datos `telematics`, las tablas Iceberg (`gps_reports`, `latest_gps_by_device`, `risk_score_daily`) y la fuente Kafka (`kafka_gps_reports`).  

- `gps_reports.sql`  
  Job **streaming**: inserta datos de Kafka en la tabla particionada `gps_reports`.  

- `latest_gps_by_device.sql`  
  Job **streaming**: mantiene la tabla `latest_gps_by_device` (1 fila por `device_id`) mediante **upsert**.  

- `risk_score_daily.sql` *(opcional)*  
  Job **batch diario**: calcula un **score de riesgo de conducción** por dispositivo y día.  

---

## ▶️ Ejecución de Jobs

Todos los comandos se corren desde el contenedor **JobManager** de Flink.

### 1. Crear catálogo, base de datos y tablas
```bash
docker exec -it jobmanager bash -lc   "bin/sql-client.sh -f /opt/sql/create.sql"
```

### 2. Job streaming → Ingesta a `gps_reports`
```bash
docker exec -it jobmanager bash -lc   "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/gps_reports.sql"
```

### 3. Job streaming → Tabla `latest_gps_by_device`
```bash
docker exec -it jobmanager bash -lc   "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/latest_gps_by_device.sql"
```

### 4. Job batch diario → Score de Riesgo
```bash
docker exec -it jobmanager bash -lc   "bin/sql-client.sh -i /opt/sql/create.sql -f /opt/sql/risk_score_daily.sql"
```

---

## 🔍 Consultas en Trino

Ejemplo para validar datos en Trino:

```bash
docker exec -it trino trino
```

Luego en el cliente:
```sql
-- Listar tablas
SHOW TABLES IN nessie.telematics;

-- Últimos 10 registros en gps_reports
SELECT * 
FROM nessie.telematics.gps_reports 
ORDER BY received_epoch DESC 
LIMIT 10;

-- Última posición por device
SELECT * 
FROM nessie.telematics.latest_gps_by_device 
LIMIT 20;

-- Score de riesgo del día anterior
SELECT * 
FROM nessie.telematics.risk_score_daily 
WHERE score_date = DATE '2025-08-26'
ORDER BY score DESC
LIMIT 20;
```

---

## 📌 Notas importantes

- Los jobs **streaming** (`gps_reports.sql` y `latest_gps_by_device.sql`) se mantienen activos indefinidamente en Flink.  
- El job **batch** (`risk_score_daily.sql`) se ejecuta una vez al día y termina. Puede re-ejecutarse para recalcular un día.  
- **Persistencia de Nessie**: la información del catálogo (commits, branches, snapshots) se guarda en **Postgres** para no perderse al hacer `docker compose down`.  
- Evitar usar `docker compose down -v`, ya que esto borraría los volúmenes de datos (incluyendo el de Postgres).  
