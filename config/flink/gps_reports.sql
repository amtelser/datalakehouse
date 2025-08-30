SET 'table.local-time-zone' = 'America/Mexico_City';
SET 'parallelism.default' = '16';
SET 'table.exec.resource.default-parallelism' = '1';
SET 'table.exec.sink.parallelism' = '16';
SET 'restart-strategy' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '10';
SET 'restart-strategy.fixed-delay.delay' = '5 s';

USE CATALOG nessie;
USE telematics;

INSERT INTO telematics.gps_reports
SELECT
  report_type,
  tenant,
  provider,
  `model`,
  firmware,
  device_id,
  alert_type,
  CAST(latitude AS DOUBLE)  AS latitude,
  CAST(longitude AS DOUBLE) AS longitude,
  CAST(gps_fixed AS BOOLEAN) AS gps_fixed,
  CAST(TO_TIMESTAMP_LTZ(CAST(gps_epoch AS BIGINT) * 1000, 3) AS TIMESTAMP_LTZ(6))      AS gps_epoch,
  satellites,
  CAST(speed_kmh AS DOUBLE)                                   AS speed_kmh,
  heading,
  odometer_meters,
  engine_on,
  vehicle_battery_voltage,
  backup_battery_voltage,
  CAST(TO_TIMESTAMP_LTZ(CAST(received_epoch AS BIGINT) * 1000, 3) AS TIMESTAMP_LTZ(6)) AS received_epoch,
  CAST(TO_TIMESTAMP_LTZ(CAST(decoded_epoch  AS BIGINT) * 1000, 3) AS TIMESTAMP_LTZ(6)) AS decoded_epoch,
  correlation_id,
  CAST(MOD(ABS(HASH_CODE(device_id)), 256) AS INT)            AS device_id_bucket,
  CAST(TO_TIMESTAMP_LTZ(CAST(received_epoch AS BIGINT) * 1000, 3) AS DATE)             AS received_day,
  CAST(EXTRACT(HOUR FROM TO_TIMESTAMP_LTZ(CAST(received_epoch AS BIGINT) * 1000, 3)) AS INT) AS received_hour,
  CAST(TO_TIMESTAMP_LTZ(CAST(gps_epoch AS BIGINT) * 1000, 3) AS DATE)                  AS gps_day
FROM kafka_gps_reports
WHERE device_id IS NOT NULL
  AND received_epoch IS NOT NULL
  AND gps_epoch IS NOT NULL
  AND report_type IS NOT NULL
  AND report_type NOT IN ('LORA', 'TRIP');