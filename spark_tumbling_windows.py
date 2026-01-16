from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, sum as _sum, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("TumblingWindows") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🏢 BUSINESS USE CASE: Black Friday Revenue Dashboard")
print("📊 Goal: Track revenue every 5 minutes")
print("👔 Stakeholder: Marketing Team")
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
    StructField("country", StringType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_clicks") \
    .option("startingOffsets", "latest") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as json_value")
df = df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")

purchases_only = df.filter(col("event_type") == "purchase")

windowed_revenue = purchases_only \
    .groupBy(window(col("timestamp"), "5 minutes")) \
    .agg(
        _sum("product_price").alias("total_revenue"),
        count("*").alias("num_purchases"),
        avg("product_price").alias("avg_order_value")
    )

query = windowed_revenue.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='10 seconds') \
    .start()

print("\n✅ Streaming started")
print("💡 Business Metrics Calculated:")
print("   - Total Revenue (per 5-min window)")
print("   - Number of Purchases")
print("   - Average Order Value")
print("\n🚀 Start producer to simulate Black Friday traffic...\n")

query.awaitTermination()
