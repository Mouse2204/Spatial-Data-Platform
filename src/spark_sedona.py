import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from sedona.spark.SedonaContext import SedonaContext

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
deps_dir = os.path.join(base_dir, "deps")
if not os.path.exists(deps_dir):
    os.makedirs(deps_dir)

jar_files = [os.path.join(deps_dir, f) for f in os.listdir(deps_dir) if f.endswith(".jar")]
jar_config = ",".join(jar_files) if jar_files else ""

spark_builder = SedonaContext.builder() \
    .appName("BecaData-Spatial-Processor-3D") \
    .master("local[*]") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "becadata_admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "becadata_storage_secret") \
    .config("spark.hadoop.fs.s3a.path-style-access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

if jar_config:
    spark_builder.config("spark.jars", jar_config)

spark = SedonaContext.create(spark_builder.getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("feature_id", StringType()),
    StructField("feature_type", StringType()),
    StructField("wkt_data", StringType()),
    StructField("timestamp", StringType())
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092,localhost:9192,localhost:9292") \
    .option("subscribe", "spatial-events") \
    .load()

parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("parsed_timestamp", col("timestamp").cast("timestamp"))

parsed_df.createOrReplaceTempView("v_incoming_data")

spatial_df = spark.sql("""
    SELECT 
        feature_id, 
        feature_type,
        parsed_timestamp as event_timestamp,
        -- ST_Force3D đảm bảo geometry luôn lưu 3 trục X, Y, Z
        ST_AsBinary(ST_Force3D(ST_GeomFromText(wkt_data))) as geometry,
        ST_ZMax(ST_GeomFromText(wkt_data)) as altitude 
    FROM v_incoming_data
""")

spatial_df.createOrReplaceTempView("v_spatial_3d")

processed_df = spark.sql("""
    SELECT 
        *,
        CASE WHEN altitude > 300 THEN 'HIGH_ALTITUDE' ELSE 'NORMAL' END as flight_status
    FROM v_spatial_3d
""")

query = processed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://becadata-geo/checkpoints/geoparquet_3d_v3") \
    .option("path", "s3a://becadata-geo/tables/vehicle_3d_geoparquet") \
    .trigger(processingTime='15 seconds') \
    .start()

print("=== SPARK SEDONA 3D PROCESSOR IS RUNNING ===")
query.awaitTermination()