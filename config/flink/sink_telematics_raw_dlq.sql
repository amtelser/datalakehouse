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

-- MAXTRACK RAW
INSERT INTO nessie.telematics.telematics_maxtrack_raw
SELECT
  device_id,
  raw_report,
  correlation_id,
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_DATE      AS created_day
FROM kafka_telematics_maxtrack_raw;

-- QUECLINK RAW
INSERT INTO nessie.telematics.telematics_queclink_raw
SELECT
  device_id,
  raw_report,
  correlation_id,
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_DATE      AS created_day
FROM kafka_telematics_queclink_raw;

-- SUNTECH RAW
INSERT INTO nessie.telematics.telematics_suntech_raw
SELECT
  device_id,
  raw_report,
  correlation_id,
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_DATE      AS created_day
FROM kafka_telematics_suntech_raw;

-- MAXTRACK DLQ
INSERT INTO nessie.telematics.telematics_maxtrack_raw_dlq
SELECT
  raw_report,
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_DATE      AS created_day
FROM kafka_telematics_maxtrack_raw_dlq;

-- QUECLINK DLQ
INSERT INTO nessie.telematics.telematics_queclink_raw_dlq
SELECT
  raw_report,
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_DATE      AS created_day
FROM kafka_telematics_queclink_raw_dlq;

-- SUNTECH DLQ
INSERT INTO nessie.telematics.telematics_suntech_raw_dlq
SELECT
  raw_report,
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_DATE      AS created_day
FROM kafka_telematics_suntech_raw_dlq;