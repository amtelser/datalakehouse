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

INSERT INTO nessie.telematics.telematics_real_time
SELECT
  report_type,
  tenant,
  provider,
  `model`,
  firmware,
  device_id,
  alert_type,
  CAST(latitude AS DOUBLE),
  CAST(longitude AS DOUBLE),
  CAST(gps_fixed AS BOOLEAN),
  CAST(TO_TIMESTAMP_LTZ(CAST(gps_epoch    AS BIGINT) * 1000, 3) AS TIMESTAMP_LTZ(3)) AS gps_epoch,
  satellites,
  CAST(speed_kmh AS DOUBLE) AS speed_kmh,
  heading,
  odometer_meters,
  engine_on,
  vehicle_battery_voltage,
  backup_battery_voltage,
  CAST(TO_TIMESTAMP_LTZ(CAST(received_epoch AS BIGINT) * 1000, 3) AS TIMESTAMP_LTZ(3)) AS received_epoch,
  CAST(TO_TIMESTAMP_LTZ(CAST(decoded_epoch  AS BIGINT) * 1000, 3) AS TIMESTAMP_LTZ(3)) AS decoded_epoch,
  correlation_id,
  CAST(MOD(ABS(HASH_CODE(device_id)), 1024) AS INT) AS device_id_bucket,
  CAST(TO_TIMESTAMP_LTZ(CAST(received_epoch AS BIGINT) * 1000, 3) AS DATE) AS received_day
FROM kafka_telematics_real_time
WHERE report_type IN ('STATUS','ALERT')
  AND gps_fixed = TRUE;