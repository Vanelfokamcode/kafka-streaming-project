from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, sum as _sum, avg, min as _min, max as _max, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("SimpleSessions") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🎯 BUSINESS USE CASE: User Behavior Analysis (Simplified)")
print("📊 Goal: Understand user activity patterns per 30-min windows")
print("👔 Stakeholder: Product Manager")
print("-" * 70)

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_price", DoubleType(), True),
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

df = df.withWatermark("timestamp", "30 minutes")

user_sessions = df.groupBy(
    col("user_id"),
    window(col("timestamp"), "30 minutes", "30 minutes")
).agg(
    count("*").alias("total_events"),
    count(when(col("event_type") == "view", 1)).alias("views"),
    count(when(col("event_type") == "add_to_cart", 1)).alias("cart_adds"),
    count(when(col("event_type") == "purchase", 1)).alias("purchases"),
    _sum(when(col("event_type") == "purchase", col("product_price")).otherwise(0)).alias("revenue"),
    _min("timestamp").alias("session_start"),
    _max("timestamp").alias("session_end")
).select(
    col("user_id"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_events"),
    col("views"),
    col("cart_adds"),
    col("purchases"),
    col("revenue"),
    col("session_start"),
    col("session_end"),
    ((col("session_end").cast("long") - col("session_start").cast("long")) / 60).alias("duration_minutes"),
    when(col("purchases") > 0, "CONVERTED").otherwise("BROWSING").alias("outcome")
)

query = user_sessions.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='15 seconds') \
    .start()

print("\n✅ Session analysis started")
print("🪟 Window: 30-minute tumbling windows")
print("📊 Metrics per user per window:")
print("   - Total events (views + cart_adds + purchases)")
print("   - Session duration (first event → last event)")
print("   - Outcome (CONVERTED if purchase, else BROWSING)")
print("\n💡 Business Insights:")
print("   PM can see: 'Users who convert spend avg 12 min browsing'")
print("   PM can see: 'Browsers view 8 products, converters view 3'")
print("\n🚀 Analyzing user behavior...\n")

query.awaitTermination()
