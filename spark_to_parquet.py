from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("WriteToParquet") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🔬 BUSINESS USE CASE: ML Training Data Lake")
print("📊 Goal: Historical data for recommendation models")
print("👔 Stakeholder: Data Science Team")
print("🎯 Current Problem: 3-hour CSV exports from Postgres (nightly)")
print("💡 Solution: Real-time Parquet writes (columnar, compressed)")
print("⚡ Performance: 10x compression, 20x faster reads")
print("-" * 70)

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_price", DoubleType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("device", StringType(), True),
    StructField("country", StringType(), True),
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_clicks") \
    .option("startingOffsets", "latest") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as json_value")
df = df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")

df_with_date = df.withColumn("date", to_date(col("timestamp")))

query = df_with_date.writeStream \
    .format("parquet") \
    .option("path", "/tmp/datalake/events") \
    .option("checkpointLocation", "/tmp/datalake/checkpoints") \
    .partitionBy("date", "event_type") \
    .outputMode("append") \
    .trigger(processingTime='30 seconds') \
    .start()

print("\n✅ Streaming to Parquet Data Lake started")
print("📁 Storage location: /tmp/datalake/events/")
print("🗂️  Partitioning strategy:")
print("   - Level 1: date (e.g., date=2025-01-15/)")
print("   - Level 2: event_type (e.g., event_type=purchase/)")
print("\n💡 Query optimization:")
print("   'Show purchases on Jan 15' → Reads ONLY date=2025-01-15/event_type=purchase/")
print("   → 100x faster than scanning all files")
print("\n🎯 Data Science can now:")
print("   - Train models on 6+ months data in minutes (not hours)")
print("   - Query specific dates/event types efficiently")
print("   - No impact on Postgres (Finance unaffected)")
print("\n🚀 Writing Parquet files every 30 seconds...\n")

query.awaitTermination()
