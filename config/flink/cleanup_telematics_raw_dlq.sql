-- ============================================================================
-- Mantenimiento Nessie / Iceberg desde Trino
-- Purga >90 días por created_day, compactación a ~128MiB, limpieza de metadata
-- Ejecutar con: trino --server http://<host>:8080 --catalog nessie --schema telematics -f mantenimiento_telematics.sql
-- ============================================================================

USE nessie.telematics;

-- =========================
-- 1) PURGA DE DATOS (>90d)
-- =========================
DELETE FROM telematics_maxtrack_raw      WHERE created_day < current_date - INTERVAL '5' day;
DELETE FROM telematics_queclink_raw      WHERE created_day < current_date - INTERVAL '90' day;
DELETE FROM telematics_suntech_raw       WHERE created_day < current_date - INTERVAL '90' day;
DELETE FROM telematics_maxtrack_raw_dlq  WHERE created_day < current_date - INTERVAL '90' day;
DELETE FROM telematics_queclink_raw_dlq  WHERE created_day < current_date - INTERVAL '90' day;
DELETE FROM telematics_suntech_raw_dlq   WHERE created_day < current_date - INTERVAL '90' day;

-- =======================================
-- 2) COMPACTACIÓN (~128 MiB por archivo)
--    Merge de pequeños data files
-- =======================================
ALTER TABLE telematics_maxtrack_raw      EXECUTE optimize(file_size_threshold => '128MB');
ALTER TABLE telematics_queclink_raw      EXECUTE optimize(file_size_threshold => '128MB');
ALTER TABLE telematics_suntech_raw       EXECUTE optimize(file_size_threshold => '128MB');
ALTER TABLE telematics_maxtrack_raw_dlq  EXECUTE optimize(file_size_threshold => '128MB');
ALTER TABLE telematics_queclink_raw_dlq  EXECUTE optimize(file_size_threshold => '128MB');
ALTER TABLE telematics_suntech_raw_dlq   EXECUTE optimize(file_size_threshold => '128MB');

-- ==========================
-- 3) ANALYZE (estadísticas)
--    Mejora el planner de Trino
-- ==========================
ANALYZE telematics_maxtrack_raw;
ANALYZE telematics_queclink_raw;
ANALYZE telematics_suntech_raw;
ANALYZE telematics_maxtrack_raw_dlq;
ANALYZE telematics_queclink_raw_dlq;
ANALYZE telematics_suntech_raw_dlq;

-- FIN