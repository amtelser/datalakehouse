CREATE CATALOG nessie WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.nessie.NessieCatalog',
  'uri' = 'http://nessie:19120/api/v1',
  'ref' = 'main',
  'warehouse' = 's3://telematics-datalake/warehouse',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'https://s3.us-west-1.amazonaws.com',
  's3.access-key-id' = '',
  's3.secret-access-key' = '',
  's3.path-style-access' = 'true',
  's3.region' = 'us-west-1'
);

SET 'table.local-time-zone' = 'America/Mexico_City';

USE CATALOG nessie;
CREATE DATABASE IF NOT EXISTS telematics;
USE telematics;

CREATE TABLE IF NOT EXISTS nessie.telematics.gps_reports (
  report_type              STRING,
  tenant                   STRING,
  provider                 STRING,
  `model`                  STRING,
  firmware                 STRING,
  device_id                STRING,
  alert_type               STRING,
  latitude                 DOUBLE,
  longitude                DOUBLE,
  gps_fixed                BOOLEAN,
  gps_epoch                TIMESTAMP(3) WITH LOCAL TIME ZONE,
  satellites               BIGINT,
  speed_kmh                DOUBLE,
  heading                  STRING,
  odometer_meters          BIGINT,
  engine_on                BOOLEAN,
  vehicle_battery_voltage  DOUBLE,
  backup_battery_voltage   DOUBLE,
  received_epoch           TIMESTAMP(3) WITH LOCAL TIME ZONE,
  decoded_epoch            TIMESTAMP(3) WITH LOCAL TIME ZONE,
  correlation_id           STRING,
  device_id_bucket         INT,
  received_day             DATE
)
PARTITIONED BY (
  device_id_bucket,
  received_day
)
WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'ZSTD',
  'write.target-file-size-bytes' = '1073741824',
  'write.distribution-mode'      = 'hash',
  'write.metadata.metrics.default' = 'truncate(16)',
  'write.metadata.metrics.column.device_id'       = 'none',
  'write.metadata.metrics.column.gps_epoch'       = 'full',
  'write.metadata.metrics.column.received_epoch'  = 'full',
  'write.metadata.metrics.column.report_type'     = 'counts',
  'write.parquet.bloom-filter-enabled.column.device_id'      = 'true',
  'write.parquet.bloom-filter-enabled.column.report_type'    = 'false',
  'write.parquet.bloom-filter-enabled.column.correlation_id' = 'false',
  'write.parquet.bloom-filter-max-bytes'                     = '262144'
);

CREATE TEMPORARY TABLE kafka_gps_reports (
  report_type              STRING,
  tenant                   STRING,
  provider                 STRING,
  `model`                  STRING,
  firmware                 STRING,
  device_id                STRING,
  alert_type               STRING,
  latitude                 DOUBLE,
  longitude                DOUBLE,
  gps_fixed                BOOLEAN,
  gps_epoch                STRING,
  satellites               BIGINT,
  speed_kmh                STRING,
  heading                  STRING,
  odometer_meters          BIGINT,
  engine_on                BOOLEAN,
  vehicle_battery_voltage  DOUBLE,
  backup_battery_voltage   DOUBLE,
  received_epoch           STRING,
  decoded_epoch            STRING,
  correlation_id           STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'iot_decoder.report_decoded',
  'properties.bootstrap.servers' = 'pkc-rgm37.us-west-2.aws.confluent.cloud:9092',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="3AIPWKVF5JUEENXE" password="HpB8QzBAP8HjfIcvYfUnMM8JTNYJPjyA54iHbwUrE5yRfwU60B7dWvRPaDnyPwhs";',
  'properties.ssl.endpoint.identification.algorithm' = 'https',
  'properties.group.id' = 'production.datalake-telematics-gps',
  'scan.startup.mode'   = 'group-offsets',
  'properties.auto.offset.reset' = 'latest',
  -- ********* Tuning de consumo *********
  -- Tamaños de fetch moderados para evitar ráfagas gigantes:
  'properties.max.partition.fetch.bytes' = '1048576',   -- 1 MiB por partición
  'properties.fetch.max.bytes' = '5242880',             -- 5 MiB por request total
  'properties.fetch.min.bytes' = '65536',               -- 64 KiB mínimo antes de devolver
  'properties.fetch.max.wait.ms' = '200',               -- espera un poquito para batching

  -- Limita registros por poll para evitar picos en operadores downstream:
  'properties.max.poll.records' = '500',

  -- Mantén la sesión estable y tolerante a pausas (GC, I/O):
  'properties.session.timeout.ms' = '30000',
  'properties.heartbeat.interval.ms' = '10000',
  'properties.max.poll.interval.ms' = '600000',         -- 10 min

  -- Menos rebalances:
  'properties.partition.assignment.strategy' = 'org.apache.kafka.clients.consumer.CooperativeStickyAssignor',

  -- DNS / timeouts de red
  'properties.client.dns.lookup' = 'use_all_dns_ips',
  'properties.request.timeout.ms' = '120000',
  'properties.retry.backoff.ms'   = '1000',
  'properties.connections.max.idle.ms' = '300000',

  -- Descubrimiento de nuevas particiones (si aplica)
  'scan.topic-partition-discovery.interval' = '5 min',

  -- Formato
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- RISK SCORE DIARIO
CREATE TABLE IF NOT EXISTS telematics.risk_score_daily (
  device_id         STRING,
  report_date       DATE,
  score             DOUBLE,
  level             STRING,
  total_reports     INT,
  overspeed_reports INT,
  night_reports     INT,
  PRIMARY KEY (device_id, report_date) NOT ENFORCED
)
WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'parquet.compression' = 'zstd',
  'write.upsert.enabled' = 'true',
  'partitioning' = 'report_date, bucket(1024, device_id)'
);

-- MAXTRACK RAW
CREATE TABLE IF NOT EXISTS telematics.telematics_maxtrack_raw (
  device_id       STRING,
  raw_report      STRING,
  correlation_id  STRING,
  created_at      TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  created_day     DATE
)
PARTITIONED BY (created_day)
WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'parquet.compression' = 'zstd',
  -- Volumen bajo → archivos más pequeños (128 MiB)
  'write.target-file-size-bytes' = '134217728',
  'write.distribution-mode' = 'hash',
  -- Métricas mínimas (reduce metadatos)
  'write.metadata.metrics.default' = 'none'
);

CREATE TEMPORARY TABLE kafka_maxtrack_raw (
  device_id       STRING,
  raw_report      STRING,
  received_epoch  STRING,
  correlation_id  STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'maxtrack.iot_hub.telematics',
  'properties.bootstrap.servers' = 'pkc-rgm37.us-west-2.aws.confluent.cloud:9092',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="3AIPWKVF5JUEENXE" password="HpB8QzBAP8HjfIcvYfUnMM8JTNYJPjyA54iHbwUrE5yRfwU60B7dWvRPaDnyPwhs";',
  'properties.ssl.endpoint.identification.algorithm' = 'https',
  'properties.group.id' = 'production.datalake-telematics-maxtrack-raw',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'latest',
  -- Tuning liviano
  'properties.max.partition.fetch.bytes' = '524288',   -- 512 KiB
  'properties.fetch.max.bytes' = '2097152',            -- 2 MiB
  'properties.fetch.min.bytes' = '65536',              -- 64 KiB
  'properties.fetch.max.wait.ms' = '200',
  'properties.max.poll.records' = '200',
  'properties.session.timeout.ms' = '30000',
  'properties.heartbeat.interval.ms' = '10000',
  'properties.max.poll.interval.ms' = '600000',
  'properties.partition.assignment.strategy' = 'org.apache.kafka.clients.consumer.CooperativeStickyAssignor',
  'properties.client.dns.lookup' = 'use_all_dns_ips',
  'properties.request.timeout.ms' = '120000',
  'properties.retry.backoff.ms'   = '1000',
  'properties.connections.max.idle.ms' = '300000',
  'scan.topic-partition-discovery.interval' = '5 min',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- QUECLINK RAW
CREATE TABLE IF NOT EXISTS telematics.telematics_queclink_raw (
  device_id       STRING,
  raw_report      STRING,
  correlation_id  STRING,
  created_at      TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  created_day     DATE
)
PARTITIONED BY (created_day)
WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'parquet.compression' = 'zstd',
  'write.target-file-size-bytes' = '134217728',
  'write.distribution-mode' = 'hash',
  'write.metadata.metrics.default' = 'none'
);

CREATE TEMPORARY TABLE kafka_queclink_raw (
  device_id       STRING,
  raw_report      STRING,
  received_epoch  STRING,  -- NO se usa
  correlation_id  STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'queclink.iot_hub.telematics',
  'properties.bootstrap.servers' = 'pkc-rgm37.us-west-2.aws.confluent.cloud:9092',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="3AIPWKVF5JUEENXE" password="HpB8QzBAP8HjfIcvYfUnMM8JTNYJPjyA54iHbwUrE5yRfwU60B7dWvRPaDnyPwhs";',
  'properties.ssl.endpoint.identification.algorithm' = 'https',
  'properties.group.id' = 'production.datalake-telematics-queclink-raw',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'latest',
  -- Tuning liviano
  'properties.max.partition.fetch.bytes' = '524288',
  'properties.fetch.max.bytes' = '2097152',
  'properties.fetch.min.bytes' = '65536',
  'properties.fetch.max.wait.ms' = '200',
  'properties.max.poll.records' = '200',
  'properties.session.timeout.ms' = '30000',
  'properties.heartbeat.interval.ms' = '10000',
  'properties.max.poll.interval.ms' = '600000',
  'properties.partition.assignment.strategy' = 'org.apache.kafka.clients.consumer.CooperativeStickyAssignor',
  'properties.client.dns.lookup' = 'use_all_dns_ips',
  'properties.request.timeout.ms' = '120000',
  'properties.retry.backoff.ms'   = '1000',
  'properties.connections.max.idle.ms' = '300000',
  'scan.topic-partition-discovery.interval' = '5 min',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- SUNTECH RAW
CREATE TABLE IF NOT EXISTS telematics.telematics_suntech_raw (
  device_id       STRING,
  raw_report      STRING,
  correlation_id  STRING,
  created_at      TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  created_day     DATE
)
PARTITIONED BY (created_day)
WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'parquet.compression' = 'zstd',
  'write.target-file-size-bytes' = '134217728',
  'write.distribution-mode' = 'hash',
  'write.metadata.metrics.default' = 'none'
);

CREATE TEMPORARY TABLE kafka_suntech_raw (
  device_id       STRING,
  raw_report      STRING,
  received_epoch  STRING,  -- NO se usa
  correlation_id  STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'suntech.iot_hub.telematics',
  'properties.bootstrap.servers' = 'pkc-rgm37.us-west-2.aws.confluent.cloud:9092',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="3AIPWKVF5JUEENXE" password="HpB8QzBAP8HjfIcvYfUnMM8JTNYJPjyA54iHbwUrE5yRfwU60B7dWvRPaDnyPwhs";',
  'properties.ssl.endpoint.identification.algorithm' = 'https',
  'properties.group.id' = 'production.datalake-telematics-suntech-raw',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'latest',
  -- Tuning liviano
  'properties.max.partition.fetch.bytes' = '524288',
  'properties.fetch.max.bytes' = '2097152',
  'properties.fetch.min.bytes' = '65536',
  'properties.fetch.max.wait.ms' = '200',
  'properties.max.poll.records' = '200',
  'properties.session.timeout.ms' = '30000',
  'properties.heartbeat.interval.ms' = '10000',
  'properties.max.poll.interval.ms' = '600000',
  'properties.partition.assignment.strategy' = 'org.apache.kafka.clients.consumer.CooperativeStickyAssignor',
  'properties.client.dns.lookup' = 'use_all_dns_ips',
  'properties.request.timeout.ms' = '120000',
  'properties.retry.backoff.ms'   = '1000',
  'properties.connections.max.idle.ms' = '300000',
  'scan.topic-partition-discovery.interval' = '5 min',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- MAXTRACK DLQ
CREATE TABLE IF NOT EXISTS telematics.telematics_maxtrack_raw_dlq (
  raw_report      STRING,
  created_at      TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  created_day     DATE
)
PARTITIONED BY (created_day)
WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'parquet.compression' = 'zstd',
  'write.target-file-size-bytes' = '134217728',
  'write.distribution-mode' = 'hash',
  'write.metadata.metrics.default' = 'none'
);

CREATE TEMPORARY TABLE kafka_maxtrack_raw_dlq (
  device_id       STRING,  -- NO se usa
  raw_report      STRING,
  received_epoch  STRING,  -- NO se usa
  correlation_id  STRING.  -- NO se usa
) WITH (
  'connector' = 'kafka',
  'topic' = 'maxtrack.iot_decoder.dlq',
  'properties.bootstrap.servers' = 'pkc-rgm37.us-west-2.aws.confluent.cloud:9092',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="3AIPWKVF5JUEENXE" password="HpB8QzBAP8HjfIcvYfUnMM8JTNYJPjyA54iHbwUrE5yRfwU60B7dWvRPaDnyPwhs";',
  'properties.ssl.endpoint.identification.algorithm' = 'https',
  'properties.group.id' = 'production.datalake-telematics-maxtrack-raw-dlq',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'latest',
  -- Tuning liviano
  'properties.max.partition.fetch.bytes' = '524288',
  'properties.fetch.max.bytes' = '2097152',
  'properties.fetch.min.bytes' = '65536',
  'properties.fetch.max.wait.ms' = '200',
  'properties.max.poll.records' = '200',
  'properties.session.timeout.ms' = '30000',
  'properties.heartbeat.interval.ms' = '10000',
  'properties.max.poll.interval.ms' = '600000',
  'properties.partition.assignment.strategy' = 'org.apache.kafka.clients.consumer.CooperativeStickyAssignor',
  'properties.client.dns.lookup' = 'use_all_dns_ips',
  'properties.request.timeout.ms' = '120000',
  'properties.retry.backoff.ms'   = '1000',
  'properties.connections.max.idle.ms' = '300000',
  'scan.topic-partition-discovery.interval' = '5 min',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- QUECLINK DLQ
CREATE TABLE IF NOT EXISTS telematics.telematics_queclink_raw_dlq (
  raw_report      STRING,
  created_at      TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  created_day     DATE
)
PARTITIONED BY (created_day)
WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'parquet.compression' = 'zstd',
  'write.target-file-size-bytes' = '134217728',
  'write.distribution-mode' = 'hash',
  'write.metadata.metrics.default' = 'none'
);

CREATE TEMPORARY TABLE kafka_queclink_raw_dlq (
  device_id       STRING,  -- NO se usa
  raw_report      STRING,
  received_epoch  STRING,  -- NO se usa
  correlation_id  STRING   -- NO se usa  
) WITH (
  'connector' = 'kafka',
  'topic' = 'queclink.iot_decoder.dlq',
  'properties.bootstrap.servers' = 'pkc-rgm37.us-west-2.aws.confluent.cloud:9092',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="3AIPWKVF5JUEENXE" password="HpB8QzBAP8HjfIcvYfUnMM8JTNYJPjyA54iHbwUrE5yRfwU60B7dWvRPaDnyPwhs";',
  'properties.ssl.endpoint.identification.algorithm' = 'https',
  'properties.group.id' = 'production.datalake-telematics-queclink-raw-dlq',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'latest',
  -- Tuning liviano
  'properties.max.partition.fetch.bytes' = '524288',
  'properties.fetch.max.bytes' = '2097152',
  'properties.fetch.min.bytes' = '65536',
  'properties.fetch.max.wait.ms' = '200',
  'properties.max.poll.records' = '200',
  'properties.session.timeout.ms' = '30000',
  'properties.heartbeat.interval.ms' = '10000',
  'properties.max.poll.interval.ms' = '600000',
  'properties.partition.assignment.strategy' = 'org.apache.kafka.clients.consumer.CooperativeStickyAssignor',
  'properties.client.dns.lookup' = 'use_all_dns_ips',
  'properties.request.timeout.ms' = '120000',
  'properties.retry.backoff.ms'   = '1000',
  'properties.connections.max.idle.ms' = '300000',
  'scan.topic-partition-discovery.interval' = '5 min',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- SUNTECH DLQ
CREATE TABLE IF NOT EXISTS telematics.telematics_suntech_raw_dlq (
  raw_report      STRING,
  created_at      TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  created_day     DATE
)
PARTITIONED BY (created_day)
WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'parquet.compression' = 'zstd',
  'write.target-file-size-bytes' = '134217728',
  'write.distribution-mode' = 'hash',
  'write.metadata.metrics.default' = 'none'
);

CREATE TEMPORARY TABLE kafka_suntech_raw_dlq (
  device_id       STRING,  -- NO se usa
  raw_report      STRING,
  received_epoch  STRING,  -- NO se usa
  correlation_id  STRING   -- NO se usa
) WITH (
  'connector' = 'kafka',
  'topic' = 'suntech.iot_decoder.dlq',
  'properties.bootstrap.servers' = 'pkc-rgm37.us-west-2.aws.confluent.cloud:9092',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="3AIPWKVF5JUEENXE" password="HpB8QzBAP8HjfIcvYfUnMM8JTNYJPjyA54iHbwUrE5yRfwU60B7dWvRPaDnyPwhs";',
  'properties.ssl.endpoint.identification.algorithm' = 'https',
  'properties.group.id' = 'production.datalake-telematics-suntech-raw-dlq',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'latest',
  -- Tuning liviano
  'properties.max.partition.fetch.bytes' = '524288',
  'properties.fetch.max.bytes' = '2097152',
  'properties.fetch.min.bytes' = '65536',
  'properties.fetch.max.wait.ms' = '200',
  'properties.max.poll.records' = '200',
  'properties.session.timeout.ms' = '30000',
  'properties.heartbeat.interval.ms' = '10000',
  'properties.max.poll.interval.ms' = '600000',
  'properties.partition.assignment.strategy' = 'org.apache.kafka.clients.consumer.CooperativeStickyAssignor',
  'properties.client.dns.lookup' = 'use_all_dns_ips',
  'properties.request.timeout.ms' = '120000',
  'properties.retry.backoff.ms'   = '1000',
  'properties.connections.max.idle.ms' = '300000',
  'scan.topic-partition-discovery.interval' = '5 min',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);