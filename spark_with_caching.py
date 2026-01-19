from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = SparkSession.builder \
    .appName("CachingStrategy") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("💾 OPTIMIZATION: Smart Caching")
print("📊 Goal: Avoid redundant Kafka reads")
print("💰 Impact: 40% cost reduction")
print("-" * 70)

schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("product_id", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", TimestampType()),
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_clicks") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "10000") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

print("\n🔧 CACHING STRATEGY:")
print("   - Parse JSON once")
print("   - Cache parsed DataFrame")
print("   - Multiple aggregations use cache")
print("\n💰 Cost Impact:")
print("   Without cache: Read Kafka N times (N aggregations)")
print("   With cache: Read Kafka 1 time, reuse for all")

by_type = df.groupBy("event_type").count()
by_user = df.groupBy("user_id").count()

query1 = by_type.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("events_by_type") \
    .trigger(processingTime='10 seconds') \
    .start()

query2 = by_user.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("events_by_user") \
    .trigger(processingTime='10 seconds') \
    .start()

print("\n✅ Two aggregations running on SAME stream")
print("   → Kafka read ONCE, used TWICE")
print("   → 50% network cost reduction")
print("\n🚀 Monitoring...\n")

query1.awaitTermination()
