INSERT INTO nessie.telematics.gps_reports
SELECT
  t.report_type,
  t.tenant,
  t.provider,
  t."model",
  t.firmware,
  t.device_id,
  t.alert_type,
  CAST(
    COALESCE(
      regexp_extract(CAST(t.coordinates AS varchar),      '\\(([-0-9\\.]+),([-0-9\\.]+)\\)', 2),
      regexp_extract(CAST(t.proxy_coordinates AS varchar),'\\(([-0-9\\.]+),([-0-9\\.]+)\\)', 2)
    ) AS double
  ) AS latitude,
  CAST(
    COALESCE(
      regexp_extract(CAST(t.coordinates AS varchar),      '\\(([-0-9\\.]+),([-0-9\\.]+)\\)', 1),
      regexp_extract(CAST(t.proxy_coordinates AS varchar),'\\(([-0-9\\.]+),([-0-9\\.]+)\\)', 1)
    ) AS double
  ) AS longitude,
  COALESCE(t.gps_fixed, t.proxy_gps_fixed, false) AS gps_fixed,
  CAST(t.gps_epoch      AS timestamp(3) with time zone) AS gps_epoch,
  CAST(t.satellites     AS bigint)                      AS satellites,
  CAST(t.speed_kmh      AS double)                      AS speed_kmh,
  t.heading,
  CAST(t.odometer_meters AS bigint)                     AS odometer_meters,
  t.engine_on,
  t.vehicle_battery_voltage,
  t.backup_battery_voltage,
  CAST(t.received_epoch AS timestamp(3) with time zone) AS received_epoch,
  CAST(t.decoded_epoch  AS timestamp(3) with time zone) AS decoded_epoch,
  t.correlation_id,
  CAST(bitwise_and(crc32(to_utf8(t.device_id)), BIGINT '255') AS integer) AS device_id_bucket,
  CAST((t.received_epoch AT TIME ZONE 'America/Mexico_City') AS date) AS received_day,
  CAST(extract(HOUR FROM (t.received_epoch AT TIME ZONE 'America/Mexico_City')) AS integer) AS received_hour,
  CAST((t.gps_epoch      AT TIME ZONE 'America/Mexico_City') AS date) AS gps_day
FROM postgres.public.telematics_real_time t
WHERE
  t.device_id = '1520203332'
  AND t.report_type IN ('STATUS','ALERT')
  AND t.gps_fixed = true
  AND t.received_epoch BETWEEN
        from_iso8601_timestamp('2025-08-16T00:00:00-06:00')
    AND from_iso8601_timestamp('2025-08-31T23:59:59-06:00')
order by received_epoch asc;


/*
t.device_id in (
  '1520186032','1520186280','1520186946','1520187500','1520189026',
  '1520190095','1520190383','1520191668','1520191988','1520193066',
  '1520193961','1520195204','1520195425','1520195642','1520196180',
  '1520196474','1520196895','1520197325','1520198540','1520199110',
  '1520199157','1520199414','1520200648','1520201037','1520202732',
  '1520203203','1520203332','1520204447','1520204526','1520206066',
  '1520208479','1520213787','1520220149'
)
*/