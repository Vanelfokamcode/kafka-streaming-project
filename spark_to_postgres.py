from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as _sum, count, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("WriteToPostgres") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("💼 BUSINESS USE CASE: Financial Compliance & Audit Trail")
print("📊 Goal: Store ALL transactions in Postgres (permanent storage)")
print("👔 Stakeholder: CFO & Finance Team")
print("⚖️  Compliance: SOX, audit requirements")
print("💰 Risk: $500K+ fines if data lost")
print("-" * 70)

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_price", DoubleType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("country", StringType(), True),
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

hourly_sales = purchases \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window(col("timestamp"), "1 hour")) \
    .agg(
        _sum("product_price").alias("total_revenue"),
        count("*").alias("num_transactions"),
        avg("product_price").alias("avg_order_value")
    ) \
    .select(
        col("window.start").alias("hour_start"),
        col("window.end").alias("hour_end"),
        col("total_revenue"),
        col("num_transactions"),
        col("avg_order_value")
    )

def write_to_postgres(batch_df, batch_id):
    print(f"\n📝 Writing Batch {batch_id} to Postgres...")
    
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/ecommerce") \
        .option("dbtable", "hourly_sales") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
    count = batch_df.count()
    print(f"✅ Batch {batch_id}: {count} rows written to Postgres")
    
    if count > 0:
        print("\n💼 Finance Team can now:")
        print("   - Run SQL queries for audit")
        print("   - Generate monthly reports")
        print("   - Reconcile with bank statements")

query = hourly_sales.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .trigger(processingTime='30 seconds') \
    .start()

print("\n✅ Streaming to Postgres started")
print("💡 Data retention: FOREVER (not 7 days like Kafka)")
print("🎯 Business Value:")
print("   - Audit trail for compliance")
print("   - Historical analysis (multi-year)")
print("   - SQL queries for Finance team")
print("\n🚀 Writing data every 30 seconds...\n")

query.awaitTermination()
