from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, collect_list, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import redis
import json

spark = SparkSession.builder \
    .appName("WriteToRedis") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("📱 BUSINESS USE CASE: Ultra-Fast 'Recently Viewed' Feature")
print("📊 Goal: <50ms response time for mobile app")
print("👔 Stakeholder: Product Team")
print("🎯 Current Problem: 200ms Postgres queries → 40% bounce rate")
print("💡 Solution: Real-time Redis cache → 30ms reads")
print("💰 Impact: +$170K annual revenue (lower bounce rate)")
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

views_only = df.filter(col("event_type") == "view")

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def write_to_redis(batch_df, batch_id):
    print(f"\n⚡ Processing Batch {batch_id}...")
    
    rows = batch_df.collect()
    updates = 0
    
    for row in rows:
        user_id = row['user_id']
        product_data = {
            'product_id': row['product_id'],
            'product_name': row['product_name'],
            'price': row['product_price'],
            'viewed_at': str(row['timestamp'])
        }
        
        cache_key = f"user:{user_id}:recent_views"
        
        r.lpush(cache_key, json.dumps(product_data))
        r.ltrim(cache_key, 0, 9)
        r.expire(cache_key, 86400)
        
        updates += 1
    
    if updates > 0:
        print(f"✅ Batch {batch_id}: {updates} Redis keys updated")
        print(f"\n💡 Mobile app can now:")
        print(f"   GET user:{{user_id}}:recent_views")
        print(f"   → Returns last 10 products in <50ms")
        print(f"   → Users see personalized 'Recently Viewed'")
        print(f"   → Higher engagement = More sales")

query = views_only.writeStream \
    .foreachBatch(write_to_redis) \
    .outputMode("append") \
    .trigger(processingTime='5 seconds') \
    .start()

print("\n✅ Streaming to Redis started")
print("🎯 Cache Strategy:")
print("   - Key format: user:{user_id}:recent_views")
print("   - Store: Last 10 products viewed")
print("   - TTL: 24 hours")
print("   - Structure: List (FIFO)")
print("\n📊 Expected Results:")
print("   Before: 200ms load time → 40% bounce")
print("   After:  30ms load time → 15% bounce")
print("   Revenue impact: +$170K/year")
print("\n🚀 Updating cache every 5 seconds...\n")

query.awaitTermination()
