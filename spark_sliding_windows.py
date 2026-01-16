from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("SlidingWindows") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🔒 BUSINESS USE CASE: Real-time Fraud Detection")
print("📊 Goal: Detect users with >10 purchases in 10 minutes")
print("👔 Stakeholder: Fraud Prevention Team")
print("💰 Impact: Prevent $100K+ daily fraud losses")
print("-" * 70)

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
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

purchases = df.filter(col("event_type") == "purchase")

fraud_check = purchases \
    .groupBy(
        col("user_id"),
        window(col("timestamp"), "10 minutes", "2 minutes")
    ) \
    .agg(
        count("*").alias("purchase_count"),
        _sum("product_price").alias("total_spent")
    ) \
    .filter(col("purchase_count") > 10)

query = fraud_check.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='5 seconds') \
    .start()

print("\n✅ Fraud detection started")
print("💡 Sliding Window: 10 minutes, slide every 2 minutes")
print("🚨 Alert triggers when: purchase_count > 10 in any window")
print("\n🎯 Business Logic:")
print("   Normal user: 2-3 purchases per 10 min")
print("   Stolen card: 15+ purchases per 10 min (bot behavior)")
print("\n🚀 Monitoring...\n")

query.awaitTermination()
