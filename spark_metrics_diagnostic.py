from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import time

spark = SparkSession.builder \
    .appName("PerformanceDiagnostic") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🔍 PERFORMANCE DIAGNOSTIC")
print("📊 Goal: Identify bottlenecks in current pipeline")
print("👔 Stakeholder: CTO + DevOps Team")
print("💰 Problem: $8K/month cost, 45sec latency, 2M lag")
print("-" * 70)

schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("product_id", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", TimestampType()),
])

start_time = time.time()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_clicks") \
    .option("startingOffsets", "latest") \
    .load()

parse_start = time.time()
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")
parse_time = time.time() - parse_start

agg_start = time.time()
result = df.groupBy("event_type").count()
agg_time = time.time() - agg_start

print("\n⏱️  TIMING BREAKDOWN:")
print(f"   Read from Kafka: {parse_time:.3f}s")
print(f"   Aggregation: {agg_time:.3f}s")

query = result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='5 seconds') \
    .start()

print("\n📊 MONITORING (every 5 sec):")
print("   - Processing rate (events/sec)")
print("   - Batch duration")
print("   - Input rate vs Processing rate")
print("\n🎯 Bottleneck Indicators:")
print("   - Processing rate < Input rate → LAG INCREASING")
print("   - Batch duration > 5 sec → TUNING NEEDED")
print("\n🚀 Collecting metrics...\n")

query.awaitTermination()
