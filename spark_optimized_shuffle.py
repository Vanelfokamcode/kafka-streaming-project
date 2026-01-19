from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("OptimizedShuffle") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("⚡ OPTIMIZATION: Reduce Shuffle Operations")
print("📊 Goal: Faster processing + Lower cost")
print("👔 Stakeholder: CTO + DevOps")
print("💰 Expected: 50% cost reduction via efficient shuffling")
print("-" * 70)

schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("product_id", StringType()),
    StructField("product_price", DoubleType()),
    StructField("event_type", StringType()),
    StructField("timestamp", TimestampType()),
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_clicks") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "10000") \
    .load()

df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

print("\n🔧 OPTIMIZATION APPLIED:")
print("   1. spark.sql.shuffle.partitions = 3 (default 200)")
print("      → 66x less shuffle tasks")
print("\n   2. maxOffsetsPerTrigger = 10000")
print("      → Process 10K events max per batch")
print("      → Prevent memory overflow")

result = df.groupBy("event_type") \
    .agg(
        count("*").alias("count"),
        _sum("product_price").alias("revenue")
    )

query = result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='5 seconds') \
    .start()

print("\n📊 Before Optimization:")
print("   - 200 shuffle partitions → high overhead")
print("   - Unlimited batch size → OOM risk")
print("\n📊 After Optimization:")
print("   - 3 shuffle partitions (matches Kafka partitions)")
print("   - 10K events/batch (predictable memory)")
print("\n🎯 Expected Improvement:")
print("   - Latency: 45s → 8s")
print("   - Cost: $8K → $3K/month")
print("\n🚀 Monitoring optimized pipeline...\n")

query.awaitTermination()
