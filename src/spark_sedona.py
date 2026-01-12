import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
deps_dir = os.path.join(base_dir, "deps")
if not os.path.exists(deps_dir):
    os.makedirs(deps_dir)

jar_files = [os.path.join(deps_dir, f) for f in os.listdir(deps_dir) if f.endswith(".jar")]
jar_config = ",".join(jar_files) if jar_files else ""

spark_builder = SparkSession.builder \
    .appName("BecaData-Spatial-Processor") \
    .master("local[*]") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "becadata_admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "becadata_storage_secret") \
    .config("spark.hadoop.fs.s3a.path-style-access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .config("spark.hadoop.fs.s3a.retry.limit", "10") \
    .config("spark.hadoop.fs.s3a.retry.interval", "500ms")

if jar_config:
    spark_builder.config("spark.jars", jar_config)

spark = spark_builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")


from sedona.register import SedonaRegistrator
SedonaRegistrator.registerAll(spark)

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
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
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
        ST_AsText(ST_GeomFromText(wkt_data)) as geometry
    FROM v_incoming_data
""")

query = spatial_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://becadata-geo/checkpoints/complex_geospatial_v1") \
    .option("mergeSchema", "true") \
    .trigger(processingTime='15 seconds') \
    .start("s3a://becadata-geo/tables/vehicle_locations")

print("=== PIELINE ĐA DẠNG HÌNH HỌC ĐANG CHẠY ===")
query.awaitTermination()