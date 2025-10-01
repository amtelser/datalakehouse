import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

def java_hashcode(s: str) -> int:
    if s is None:
        return None
    h = 0
    for ch in s:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    if h & 0x80000000:
        h = -((~h + 1) & 0xFFFFFFFF)
    return int(h)

java_hash_udf = F.udf(java_hashcode, IntegerType())

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--pg-url", required=True)
    p.add_argument("--pg-user", required=True)
    p.add_argument("--pg-pass", required=True)
    p.add_argument("--pg-table", default="public.telematics_real_time")
    p.add_argument("--start-ts", required=True)
    p.add_argument("--end-ts", required=True)
    p.add_argument("--report-types", default="STATUS,ALERT")
    p.add_argument("--device-ids", required=True)
    p.add_argument("--nessie-uri", default="http://nessie:19120/api/v2")
    p.add_argument("--nessie-ref", default="main")
    p.add_argument("--warehouse", default="s3://iothub-telematics-data-stg/warehouse")
    p.add_argument("--s3-endpoint", default="https://s3.us-west-2.amazonaws.com")
    args = p.parse_args()

    report_types = [x.strip() for x in args.report_types.split(",") if x.strip()]
    device_ids = [x.strip() for x in args.device_ids.split(",") if x.strip()]

    def sql_str_list(vals):
        quoted = ["'" + v.replace("'", "''") + "'" for v in vals]
        return ",".join(quoted)

    spark = (
        SparkSession.builder
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", args.nessie_uri)
        .config("spark.sql.catalog.nessie.ref", args.nessie_ref)
        .config("spark.sql.catalog.nessie.warehouse", args.warehouse)
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.nessie.s3.endpoint", args.s3_endpoint)
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.sql.catalog.nessie.s3.region", "us-west-2")
        .config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )

    where = (
        f"gps_fixed IS TRUE "
        f"AND report_type IN ({sql_str_list(report_types)}) "
        f"AND device_id IN ({sql_str_list(device_ids)}) "
        f"AND received_epoch >= TIMESTAMP '{args.start_ts}' "
        f"AND received_epoch < TIMESTAMP '{args.end_ts}'"
    )

    df = (
        spark.read.format("jdbc")
        .option("url", args.pg_url)
        .option("dbtable", f"(SELECT * FROM {args.pg_table} WHERE {where}) as src")
        .option("user", args.pg_user)
        .option("password", args.pg_pass)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "5000")
        .load()
    )

    coords = F.split(F.regexp_replace(F.col("coordinates").cast("string"), r"[()]", ""), ",")

    out = (
        df.select(
            "report_type",
            "tenant",
            "provider",
            "model",
            "firmware",
            "device_id",
            "alert_type",
            coords.getItem(1).cast("double").alias("latitude"),
            coords.getItem(0).cast("double").alias("longitude"),
            "gps_fixed",
            F.col("gps_epoch").cast("timestamp").alias("gps_epoch"),
            F.col("satellites").cast("bigint").alias("satellites"),
            F.col("speed_kmh").cast("double").alias("speed_kmh"),
            "heading",
            F.col("odometer_meters").cast("bigint").alias("odometer_meters"),
            "engine_on",
            F.col("vehicle_battery_voltage").cast("double").alias("vehicle_battery_voltage"),
            F.col("backup_battery_voltage").cast("double").alias("backup_battery_voltage"),
            F.col("received_epoch").cast("timestamp").alias("received_epoch"),
            F.col("decoded_epoch").cast("timestamp").alias("decoded_epoch"),
            "correlation_id",
            (F.abs(java_hash_udf(F.col("device_id"))) % F.lit(32)).cast("int").alias("device_id_bucket"),
            F.to_date(F.col("received_epoch")).alias("received_day"),
        )
    )

    out.writeTo("nessie.telematics.telematics_real_time").append()
    spark.stop()

if __name__ == "__main__":
    main()