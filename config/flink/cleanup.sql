USE nessie.telematics;

-- RAW AND DLQ

DELETE FROM telematics_maxtrack_raw      WHERE created_day < current_date - INTERVAL '5' day;
DELETE FROM telematics_queclink_raw      WHERE created_day < current_date - INTERVAL '90' day;
DELETE FROM telematics_suntech_raw       WHERE created_day < current_date - INTERVAL '90' day;
DELETE FROM telematics_maxtrack_raw_dlq  WHERE created_day < current_date - INTERVAL '90' day;
DELETE FROM telematics_queclink_raw_dlq  WHERE created_day < current_date - INTERVAL '90' day;
DELETE FROM telematics_suntech_raw_dlq   WHERE created_day < current_date - INTERVAL '90' day;

ALTER TABLE telematics_maxtrack_raw      EXECUTE optimize(file_size_threshold => '128MB');
ALTER TABLE telematics_queclink_raw      EXECUTE optimize(file_size_threshold => '128MB');
ALTER TABLE telematics_suntech_raw       EXECUTE optimize(file_size_threshold => '128MB');
ALTER TABLE telematics_maxtrack_raw_dlq  EXECUTE optimize(file_size_threshold => '128MB');
ALTER TABLE telematics_queclink_raw_dlq  EXECUTE optimize(file_size_threshold => '128MB');
ALTER TABLE telematics_suntech_raw_dlq   EXECUTE optimize(file_size_threshold => '128MB');

ALTER TABLE "nessie"."telematics"."telematics_maxtrack_raw"     EXECUTE remove_orphan_files(retention_threshold => '2d');
ALTER TABLE "nessie"."telematics"."telematics_queclink_raw"     EXECUTE remove_orphan_files(retention_threshold => '2d');
ALTER TABLE "nessie"."telematics"."telematics_suntech_raw"      EXECUTE remove_orphan_files(retention_threshold => '2d');
ALTER TABLE "nessie"."telematics"."telematics_maxtrack_raw_dlq" EXECUTE remove_orphan_files(retention_threshold => '2d');
ALTER TABLE "nessie"."telematics"."telematics_queclink_raw_dlq" EXECUTE remove_orphan_files(retention_threshold => '2d');
ALTER TABLE "nessie"."telematics"."telematics_suntech_raw_dlq"  EXECUTE remove_orphan_files(retention_threshold => '2d');

ALTER TABLE "nessie"."telematics"."telematics_maxtrack_raw"     EXECUTE expire_snapshots(retention_threshold => '2d');
ALTER TABLE "nessie"."telematics"."telematics_queclink_raw"     EXECUTE expire_snapshots(retention_threshold => '2d');
ALTER TABLE "nessie"."telematics"."telematics_suntech_raw"      EXECUTE expire_snapshots(retention_threshold => '2d');
ALTER TABLE "nessie"."telematics"."telematics_maxtrack_raw_dlq" EXECUTE expire_snapshots(retention_threshold => '2d');
ALTER TABLE "nessie"."telematics"."telematics_queclink_raw_dlq" EXECUTE expire_snapshots(retention_threshold => '2d');
ALTER TABLE "nessie"."telematics"."telematics_suntech_raw_dlq"  EXECUTE expire_snapshots(retention_threshold => '2d');

ANALYZE telematics_maxtrack_raw;
ANALYZE telematics_queclink_raw;
ANALYZE telematics_suntech_raw;
ANALYZE telematics_maxtrack_raw_dlq;
ANALYZE telematics_queclink_raw_dlq;
ANALYZE telematics_suntech_raw_dlq;

-- RISK SCORE DAILY

ALTER TABLE nessie.telematics.risk_score_daily          EXECUTE optimize(file_size_threshold => '128MB') WHERE received_day = current_date - INTERVAL '1' day;

ALTER TABLE "nessie"."telematics"."risk_score_daily"    EXECUTE remove_orphan_files(retention_threshold => '2d');

ALTER TABLE "nessie"."telematics"."risk_score_daily"    EXECUTE expire_snapshots(retention_threshold => '2d');

ANALYZE nessie.telematics.risk_score_daily;

-- TELEMATICS REAL TIME

ALTER TABLE nessie.telematics.telematics_real_time          EXECUTE optimize(file_size_threshold => '256MB') WHERE received_day = current_date - INTERVAL '1' day;

ALTER TABLE "nessie"."telematics"."telematics_real_time"    EXECUTE remove_orphan_files(retention_threshold => '2d');

ALTER TABLE "nessie"."telematics"."telematics_real_time"    EXECUTE expire_snapshots(retention_threshold => '2d');

ANALYZE nessie.telematics.telematics_real_time;