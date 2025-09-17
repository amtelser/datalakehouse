-- ============================================================
-- Housekeeping + Retention + Compaction (Flink SQL / Iceberg Nessie)
-- ============================================================

SET 'table.local-time-zone' = 'America/Mexico_City';
SET 'parallelism.default' = '1';
SET 'table.exec.resource.default-parallelism' = '1';
SET 'table.exec.sink.parallelism' = '1';
SET 'restart-strategy' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '10';
SET 'restart-strategy.fixed-delay.delay' = '5 s';
SET 'execution.checkpointing.interval' = '300 s';
SET 'execution.checkpointing.timeout'  = '10 min';
SET 'execution.checkpointing.min-pause' = '60 s';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';
SET 'execution.checkpointing.unaligned' = 'false';

USE CATALOG nessie;
USE telematics;

-- ============================================================
-- 1) MAXTRACK RAW  -> 5 días
-- ============================================================

-- 1.1) Borrado por partición (rápido por created_day)
DELETE FROM telematics.telematics_maxtrack_raw
WHERE created_day < CURRENT_DATE - INTERVAL '5' DAY;

-- 1.2) Expirar snapshots (buffer de seguridad)
CALL nessie.system.expire_snapshots(
  table => 'telematics.telematics_maxtrack_raw',
  older_than => (CURRENT_TIMESTAMP - INTERVAL '2' DAY),
  retain_last => 1
);

-- 1.3) Remover archivos huérfanos (1 día)
CALL nessie.system.remove_orphan_files(
  table => 'telematics.telematics_maxtrack_raw',
  older_than => (CURRENT_TIMESTAMP - INTERVAL '1' DAY)
);

-- 1.4) Compactación (opcional: dejar diario/quincenal)
CALL nessie.system.rewrite_data_files(
  table => 'telematics.telematics_maxtrack_raw',
  options => map[
    'min-input-files','5',
    'target-file-size-bytes','134217728'  -- 128 MiB
  ]
);
CALL nessie.system.rewrite_manifests(
  table => 'telematics.telematics_maxtrack_raw'
);

-- ============================================================
-- 2) OTRAS 5 TABLAS  -> 3 meses
--    - telematics_queclink_raw
--    - telematics_suntech_raw
--    - telematics_maxtrack_raw_dlq
--    - telematics_queclink_raw_dlq
--    - telematics_suntech_raw_dlq
-- ============================================================

-- 2.1) Borrado por partición (3 meses)
DELETE FROM telematics.telematics_queclink_raw
WHERE created_day < CURRENT_DATE - INTERVAL '3' MONTH;

DELETE FROM telematics.telematics_suntech_raw
WHERE created_day < CURRENT_DATE - INTERVAL '3' MONTH;

DELETE FROM telematics.telematics_maxtrack_raw_dlq
WHERE created_day < CURRENT_DATE - INTERVAL '3' MONTH;

DELETE FROM telematics.telematics_queclink_raw_dlq
WHERE created_day < CURRENT_DATE - INTERVAL '3' MONTH;

DELETE FROM telematics.telematics_suntech_raw_dlq
WHERE created_day < CURRENT_DATE - INTERVAL '3' MONTH;

-- 2.2) Expirar snapshots (buffer de 2 días)
CALL nessie.system.expire_snapshots(
  table => 'telematics.telematics_queclink_raw',
  older_than => (CURRENT_TIMESTAMP - INTERVAL '2' DAY),
  retain_last => 1
);
CALL nessie.system.expire_snapshots(
  table => 'telematics.telematics_suntech_raw',
  older_than => (CURRENT_TIMESTAMP - INTERVAL '2' DAY),
  retain_last => 1
);
CALL nessie.system.expire_snapshots(
  table => 'telematics.telematics_maxtrack_raw_dlq',
  older_than => (CURRENT_TIMESTAMP - INTERVAL '2' DAY),
  retain_last => 1
);
CALL nessie.system.expire_snapshots(
  table => 'telematics.telematics_queclink_raw_dlq',
  older_than => (CURRENT_TIMESTAMP - INTERVAL '2' DAY),
  retain_last => 1
);
CALL nessie.system.expire_snapshots(
  table => 'telematics.telematics_suntech_raw_dlq',
  older_than => (CURRENT_TIMESTAMP - INTERVAL '2' DAY),
  retain_last => 1
);

-- 2.3) Remover archivos huérfanos (1 día)
CALL nessie.system.remove_orphan_files(
  table => 'telematics.telematics_queclink_raw',
  older_than => (CURRENT_TIMESTAMP - INTERVAL '1' DAY)
);
CALL nessie.system.remove_orphan_files(
  table => 'telematics.telematics_suntech_raw',
  older_than => (CURRENT_TIMESTAMP - INTERVAL '1' DAY)
);
CALL nessie.system.remove_orphan_files(
  table => 'telematics.telematics_maxtrack_raw_dlq',
  older_than => (CURRENT_TIMESTAMP - INTERVAL '1' DAY)
);
CALL nessie.system.remove_orphan_files(
  table => 'telematics.telematics_queclink_raw_dlq',
  older_than => (CURRENT_TIMESTAMP - INTERVAL '1' DAY)
);
CALL nessie.system.remove_orphan_files(
  table => 'telematics.telematics_suntech_raw_dlq',
  older_than => (CURRENT_TIMESTAMP - INTERVAL '1' DAY)
);

-- 2.4) Compactación (mismo objetivo de 128 MiB)
CALL nessie.system.rewrite_data_files(
  table => 'telematics.telematics_queclink_raw',
  options => map['min-input-files','5','target-file-size-bytes','134217728']
);
CALL nessie.system.rewrite_manifests(
  table => 'telematics.telematics_queclink_raw'
);

CALL nessie.system.rewrite_data_files(
  table => 'telematics.telematics_suntech_raw',
  options => map['min-input-files','5','target-file-size-bytes','134217728']
);
CALL nessie.system.rewrite_manifests(
  table => 'telematics.telematics_suntech_raw'
);

CALL nessie.system.rewrite_data_files(
  table => 'telematics.telematics_maxtrack_raw_dlq',
  options => map['min-input-files','5','target-file-size-bytes','134217728']
);
CALL nessie.system.rewrite_manifests(
  table => 'telematics.telematics_maxtrack_raw_dlq'
);

CALL nessie.system.rewrite_data_files(
  table => 'telematics.telematics_queclink_raw_dlq',
  options => map['min-input-files','5','target-file-size-bytes','134217728']
);
CALL nessie.system.rewrite_manifests(
  table => 'telematics.telematics_queclink_raw_dlq'
);

CALL nessie.system.rewrite_data_files(
  table => 'telematics.telematics_suntech_raw_dlq',
  options => map['min-input-files','5','target-file-size-bytes','134217728']
);
CALL nessie.system.rewrite_manifests(
  table => 'telematics.telematics_suntech_raw_dlq'
);