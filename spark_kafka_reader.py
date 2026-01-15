from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("KafkaReader") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🎧 Spark Streaming: Reading from Kafka...")
print("-" * 50)

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_price", DoubleType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
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

query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

print("✅ Streaming started - Waiting for events from Kafka...")
print("💡 Start producer in another terminal to see events")
print("\nPress Ctrl+C to stop\n")

query.awaitTermination()
