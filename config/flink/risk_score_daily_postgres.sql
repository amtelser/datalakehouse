SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'America/Mexico_City';
SET 'parallelism.default' = '1';
SET 'table.exec.resource.default-parallelism' = '1';
SET 'restart-strategy' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '5';
SET 'restart-strategy.fixed-delay.delay' = '5 s';

USE CATALOG nessie;
USE telematics;

INSERT INTO pg_driving_risk_score
WITH
rango AS (
  SELECT DATE '2025-09-04' AS d_ini, DATE '2025-09-04' AS d_fin
),
base AS (
  SELECT
    g.device_id,
    CAST(CAST(g.gps_epoch AS TIMESTAMP(3)) AS DATE)           AS report_date,
    CAST(g.speed_kmh AS DOUBLE)                                AS speed_kmh,
    EXTRACT(HOUR FROM CAST(g.gps_epoch AS TIMESTAMP(3)))       AS gps_hour
  FROM telematics.gps_reports g
  JOIN rango r
    ON CAST(CAST(g.gps_epoch AS TIMESTAMP(3)) AS DATE)
       BETWEEN r.d_ini AND r.d_fin
),
agg AS (
  SELECT
    device_id,
    report_date,
    COUNT(*) AS total_reports,
    SUM(CASE WHEN speed_kmh > 110 THEN 1 ELSE 0 END) AS overspeed_reports,
    SUM(CASE WHEN (gps_hour >= 23 OR gps_hour < 4) THEN 1 ELSE 0 END) AS night_reports
  FROM base
  GROUP BY device_id, report_date
),
ratio AS (
  SELECT
    device_id,
    report_date,
    total_reports,
    overspeed_reports,
    night_reports,
    CAST(overspeed_reports AS DOUBLE) / CAST(total_reports AS DOUBLE) AS rs,
    CAST(night_reports    AS DOUBLE) / CAST(total_reports AS DOUBLE) AS rn
  FROM agg
),
transf AS (
  SELECT
    device_id,
    report_date,
    total_reports,
    overspeed_reports,
    night_reports,
    rs,
    rn,
    POWER(rs, CAST(1.7 AS DOUBLE)) AS fs,
    POWER(rn, CAST(1.3 AS DOUBLE)) AS fn,
    (rs * rn) AS fint
  FROM ratio
),
scored AS (
  SELECT
    device_id,
    report_date,
    total_reports,
    overspeed_reports,
    night_reports,
    (0.55 * fs + 0.30 * fn + 0.15 * fint) AS risk_raw
  FROM transf
),
scored2 AS (
  SELECT
    device_id,
    report_date,
    total_reports,
    overspeed_reports,
    night_reports,
    100.0 / (1.0 + EXP(-12.0 * (risk_raw - CAST(0.1155 AS DOUBLE)))) AS score_raw
  FROM scored
),
final AS (
  SELECT
    device_id,
    report_date,
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
    total_reports,
    overspeed_reports,
    night_reports
  FROM scored2
)
SELECT * FROM final;