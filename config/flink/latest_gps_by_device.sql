SET 'table.local-time-zone' = 'America/Mexico_City';

USE CATALOG nessie;
USE telematics;

INSERT INTO telematics.latest_gps_by_device
SELECT
  device_id,
  report_type,
  CAST(latitude AS DOUBLE)                                 AS latitude,
  CAST(longitude AS DOUBLE)                                AS longitude,
  CAST(gps_fixed AS BOOLEAN)                               AS gps_fixed,
  TO_TIMESTAMP_LTZ(CAST(gps_epoch     AS BIGINT) * 1000, 3) AS gps_epoch,
  TO_TIMESTAMP_LTZ(CAST(received_epoch AS BIGINT) * 1000, 3) AS received_epoch,
  CAST(speed_kmh AS DOUBLE)                                AS speed_kmh,
  heading,
  odometer_meters,
  engine_on,
  vehicle_battery_voltage,
  backup_battery_voltage,
  correlation_id
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY device_id
      ORDER BY CAST(gps_epoch AS BIGINT) DESC,
               CAST(received_epoch AS BIGINT) DESC
    ) AS rn
  FROM kafka_gps_reports
  WHERE device_id IS NOT NULL
    AND received_epoch IS NOT NULL
    AND gps_epoch IS NOT NULL
    AND report_type IS NOT NULL
    AND report_type NOT IN ('LORA','TRIP')
)
WHERE rn = 1;