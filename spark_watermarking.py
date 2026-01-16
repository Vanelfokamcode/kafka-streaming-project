from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, sum as _sum, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("Watermarking") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("📱 BUSINESS USE CASE: Mobile App Analytics (Unstable Network)")
print("📊 Goal: Count all purchases even if events arrive late")
print("👔 Stakeholder: Product Team")
print("⏰ Challenge: Users in subway = delayed events")
print("💡 Solution: Watermark = Wait 15 min for late data")
print("-" * 70)

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_price", DoubleType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("device", StringType(), True),
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_clicks") \
    .option("startingOffsets", "latest") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp")
df = df.select(
    from_json(col("json_value"), schema).alias("data"),
    col("kafka_timestamp")
).select("data.*", "kafka_timestamp")

purchases = df.filter(col("event_type") == "purchase")

windowed_purchases = purchases \
    .withWatermark("timestamp", "15 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes")
    ) \
    .agg(
        count("*").alias("purchase_count"),
        _sum("product_price").alias("revenue")
    )

query = windowed_purchases.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='10 seconds') \
    .start()

print("\n✅ Watermarking enabled: 15 minutes")
print("💡 How it works:")
print("   - Event timestamped 10:30, arrives 10:35 → ✅ Counted")
print("   - Event timestamped 10:30, arrives 10:45 → ✅ Counted (within 15 min)")
print("   - Event timestamped 10:30, arrives 11:00 → ❌ Too late (>15 min)")
print("\n🎯 Business Decision:")
print("   15 min watermark = 99% coverage for subway users")
print("   Trade-off: Dashboard delayed by 15 min (acceptable for analytics)")
print("\n🚀 Monitoring...\n")

query.awaitTermination()
