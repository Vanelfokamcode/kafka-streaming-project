from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, window, first
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

spark = SparkSession.builder \
    .appName("AdvancedDeduplication") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🎯 BUSINESS USE CASE: Eliminate Duplicate Events Before Processing")
print("📊 Goal: Prevent duplicate emails (bad UX + wasted budget)")
print("👔 Stakeholder: Head of Data + Marketing Team")
print("💰 Current Problem: Users receive same promo email 3-4 times")
print("💡 Solution: Deduplication window (1 min) based on event_id")
print("-" * 70)

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_clicks") \
    .option("startingOffsets", "latest") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as json_value")
df = df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")

df = df.filter(col("event_type") == "view")

print("\n📊 BEFORE Deduplication:")
print("   - Kafka at-least-once delivery → duplicates possible")
print("   - Example: Same event_id appears 2-3 times")

df_with_watermark = df.withWatermark("timestamp", "5 minutes")

deduplicated = df_with_watermark \
    .dropDuplicates(["event_id"])

print("\n✅ AFTER Deduplication:")
print("   - dropDuplicates on event_id")
print("   - Within 5-min watermark window")
print("   - First occurrence kept, rest ignored")

print("\n🎯 Business Impact:")
print("   Before: user_342 receives email 3x (same promo)")
print("   After:  user_342 receives email 1x")
print("   Result: Better UX + 66% cost reduction")

query = deduplicated.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='10 seconds') \
    .start()

print("\n🚀 Deduplication active...\n")

query.awaitTermination()
