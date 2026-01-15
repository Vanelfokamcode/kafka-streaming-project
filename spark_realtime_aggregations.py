from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("RealtimeAggregations") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("📊 Spark Streaming: Real-time Aggregations")
print("-" * 50)

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_category", StringType(), True),
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

df = df.selectExpr("CAST(value AS STRING) as json_value")
df = df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")

agg_df = df.groupBy("event_type").count()

query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='10 seconds') \
    .start()

print("✅ Aggregating events every 10 seconds...")
print("💡 Start producer to see real-time counts\n")

query.awaitTermination()
