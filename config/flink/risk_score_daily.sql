SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'America/Mexico_City';

USE CATALOG nessie;
USE telematics;

INSERT INTO telematics.risk_score_daily
WITH base AS (
  SELECT
    device_id,
    CAST(CAST(gps_epoch AS TIMESTAMP(3)) AS DATE) AS score_date_local,
    CAST(speed_kmh AS DOUBLE) AS speed_kmh,
    EXTRACT(HOUR FROM CAST(gps_epoch AS TIMESTAMP(3))) AS gps_hour_local
  FROM telematics.gps_reports
  WHERE `model` = 'ST310'
    AND CAST(CAST(gps_epoch AS TIMESTAMP(3)) AS DATE) = CURRENT_DATE - INTERVAL '1' DAY
),
agg AS (
  SELECT
    device_id,
    score_date_local AS score_date,
    COUNT(*) AS total_reports,
    SUM(CASE WHEN speed_kmh > 110 THEN 1 ELSE 0 END) AS speed_hi_reports,
    SUM(CASE WHEN (gps_hour_local >= 23 OR gps_hour_local < 4) THEN 1 ELSE 0 END) AS night_reports
  FROM base
  GROUP BY device_id, score_date_local
),
ratio AS (
  SELECT
    device_id, score_date, total_reports, speed_hi_reports, night_reports,
    CAST(speed_hi_reports AS DOUBLE) / CAST(total_reports AS DOUBLE) AS rs,
    CAST(night_reports   AS DOUBLE) / CAST(total_reports AS DOUBLE) AS rn
  FROM agg
),
transf AS (
  SELECT
    device_id, score_date, total_reports, speed_hi_reports, night_reports, rs, rn,
    POWER(rs, CAST(1.7 AS DOUBLE)) AS fs,
    POWER(rn, CAST(1.3 AS DOUBLE)) AS fn,
    (rs * rn) AS fint
  FROM ratio
),
scored AS (
  SELECT
    *,
    (0.55 * fs + 0.30 * fn + 0.15 * fint) AS risk_raw
  FROM transf
),
scored2 AS (
  SELECT
    *,
    100.0 / (1.0 + EXP(-12.0 * (risk_raw - CAST(0.1155 AS DOUBLE)))) AS score_raw
  FROM scored
)
SELECT
  device_id,
  score_date,
  total_reports,
  speed_hi_reports,
  night_reports,
  rs, rn, fs, fn, fint,
  risk_raw,
  CASE
    WHEN total_reports < 10 THEN NULL
    ELSE CAST(LEAST(GREATEST(ROUND(score_raw), 0), 100) AS DOUBLE)
  END AS score,
  CASE
    WHEN total_reports < 10 THEN 'Sin evidencia'
    WHEN CAST(LEAST(GREATEST(ROUND(score_raw), 0), 100) AS DOUBLE) <= 20 THEN 'Seguro'
    WHEN CAST(LEAST(GREATEST(ROUND(score_raw), 0), 100) AS DOUBLE) <= 60 THEN 'Menos seguro'
    ELSE 'Inseguro'
  END AS level,

  CURRENT_TIMESTAMP AS last_update
FROM scored2;