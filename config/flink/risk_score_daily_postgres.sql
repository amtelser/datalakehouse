SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'America/Mexico_City';
SET 'parallelism.default' = '16';
SET 'table.exec.resource.default-parallelism' = '16';
SET 'restart-strategy' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '5';
SET 'restart-strategy.fixed-delay.delay' = '5 s';

USE CATALOG nessie;
USE telematics;

INSERT INTO pg_driving_risk_score
WITH
rango AS (
  SELECT DATE '2025-08-01' AS d_ini, DATE '2025-08-31' AS d_fin
),
base AS (
  SELECT
    g.device_id,
    CAST(CAST(g.gps_epoch AS TIMESTAMP(3)) AS DATE)           AS score_date,
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
    score_date,
    COUNT(*) AS total_reports,
    SUM(CASE WHEN speed_kmh > 110 THEN 1 ELSE 0 END) AS speed_hi_reports,
    SUM(CASE WHEN (gps_hour >= 23 OR gps_hour < 4) THEN 1 ELSE 0 END) AS night_reports
  FROM base
  GROUP BY device_id, score_date
),
ratio AS (
  SELECT
    device_id,
    score_date,
    total_reports,
    speed_hi_reports,
    night_reports,
    CAST(speed_hi_reports AS DOUBLE) / CAST(total_reports AS DOUBLE) AS rs,
    CAST(night_reports    AS DOUBLE) / CAST(total_reports AS DOUBLE) AS rn
  FROM agg
),
transf AS (
  SELECT
    device_id,
    score_date,
    total_reports,
    speed_hi_reports,
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
    score_date,
    total_reports,
    speed_hi_reports,
    night_reports,
    (0.55 * fs + 0.30 * fn + 0.15 * fint) AS risk_raw
  FROM transf
),
scored2 AS (
  SELECT
    device_id,
    score_date,
    total_reports,
    speed_hi_reports,
    night_reports,
    100.0 / (1.0 + EXP(-12.0 * (risk_raw - CAST(0.1155 AS DOUBLE)))) AS score_raw
  FROM scored
),
final AS (
  SELECT
    device_id,
    score_date                                                AS report_date,
    CAST(
      CASE
        WHEN total_reports < 10 THEN NULL
        ELSE LEAST(GREATEST(ROUND(score_raw), 0), 100)
      END
      AS DECIMAL(5,2)
    )                                                         AS score,
    CASE
      WHEN total_reports < 10 THEN 'Sin evidencia'
      WHEN LEAST(GREATEST(ROUND(score_raw), 0), 100) <= 20 THEN 'Seguro'
      WHEN LEAST(GREATEST(ROUND(score_raw), 0), 100) <= 60 THEN 'Menos seguro'
      ELSE 'Inseguro'
    END                                                       AS `level`,
    CAST(total_reports     AS INT)                            AS total_reports,
    CAST(speed_hi_reports  AS INT)                            AS overspeed_reports,
    CAST(night_reports     AS INT)                            AS night_reports
  FROM scored2
)
SELECT * FROM final;