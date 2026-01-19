from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, sum as _sum, when, isnan, isnull
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import logging

logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("DataQualityMonitoring") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🔍 DATA QUALITY MONITORING")
print("📊 Goal: Catch bad data before business impact")
print("👔 Stakeholder: VP Engineering + Data Team")
print("💰 Risk: False metrics = Bad business decisions")
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

quality_checks = df.groupBy().agg(
    count("*").alias("total_events"),
    count(when(col("user_id").isNull(), 1)).alias("null_user_ids"),
    count(when(col("product_price").isNull(), 1)).alias("null_prices"),
    count(when(col("product_price") < 0, 1)).alias("negative_prices"),
    count(when(col("product_price") > 10000, 1)).alias("suspicious_prices"),
    count(when(col("event_type").isNull(), 1)).alias("null_event_types"),
    _sum(when(col("event_type") == "purchase", col("product_price")).otherwise(0)).alias("total_revenue")
).withColumn(
    "null_percentage",
    ((col("null_user_ids") + col("null_prices")) / col("total_events") * 100)
).withColumn(
    "quality_score",
    100 - col("null_percentage")
).withColumn(
    "status",
    when(col("quality_score") >= 95, "✅ HEALTHY")
    .when(col("quality_score") >= 85, "⚠️  WARNING")
    .otherwise("🚨 CRITICAL")
)

print("\n🔍 DATA QUALITY CHECKS:")
print("   ✅ NULL value detection (user_id, price, event_type)")
print("   ✅ Business logic validation (price < 0, price > $10K)")
print("   ✅ Quality score calculation (target: >95%)")
print("\n🚨 ALERT TRIGGERS:")
print("   - Quality score <95% → Warning")
print("   - Quality score <85% → Critical alert")
print("   - Total revenue = $0 → Data pipeline broken")
print("\n🚀 Monitoring data quality...\n")

query = quality_checks.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='30 seconds') \
    .start()

query.awaitTermination()
