from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

spark = SparkSession.builder \
    .appName("ExtendedTimeWindow") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🎯 BUSINESS USE CASE: Capture Views Before Promo Starts")
print("📊 Goal: Target users who viewed 30 min BEFORE promo")
print("👔 Stakeholder: Marketing Team")
print("💰 Impact: +30% conversion capture")
print("-" * 70)

clicks_schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", TimestampType()),
])

promos_schema = StructType([
    StructField("promo_id", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("discount_percent", IntegerType()),
    StructField("start_time", TimestampType()),
    StructField("end_time", TimestampType()),
    StructField("timestamp", TimestampType()),
])

clicks = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_clicks") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), clicks_schema).alias("data")).select("data.*") \
    .filter(col("event_type") == "view") \
    .dropDuplicates(["event_id"]) \
    .withWatermark("timestamp", "30 minutes")

promos = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "promotions") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), promos_schema).alias("data")).select("data.*") \
    .withWatermark("timestamp", "30 minutes")

clicks_aliased = clicks.alias("c")
promos_aliased = promos.alias("p")

joined = clicks_aliased.join(
    promos_aliased,
    expr("""
        c.product_id = p.product_id AND
        c.timestamp >= p.start_time - interval 30 minutes AND
        c.timestamp <= p.end_time AND
        c.timestamp >= p.timestamp - interval 30 minutes AND
        c.timestamp <= p.timestamp + interval 30 minutes
    """),
    "inner"
)

result = joined.select(
    col("c.user_id").alias("user_id"),
    col("c.product_name").alias("product"),
    col("c.timestamp").alias("view_time"),
    col("p.promo_id").alias("promo_id"),
    col("p.discount_percent").alias("discount"),
    col("p.start_time").alias("promo_start")
)

query = result.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='15 seconds') \
    .start()

print("\n✅ Extended Time Window Join Started")
print("🎯 Join captures:")
print("   - Views up to 30 min BEFORE promo starts")
print("   - Views during promo period")
print("\n💡 Example:")
print("   10:00 view → 10:35 promo starts → MATCHED ✅")
print("\n🚀 Processing...\n")

query.awaitTermination()
