from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestBatch") \
    .master("local[*]") \
    .getOrCreate()

print("🎉 Spark Session Created!")
print(f"   Version: {spark.version}")
print(f"   Master: {spark.sparkContext.master}")
print(f"   App Name: {spark.sparkContext.appName}")

data = [
    ("user_1", "product_A", 100),
    ("user_2", "product_B", 200),
    ("user_1", "product_C", 150),
    ("user_3", "product_A", 100),
    ("user_2", "product_A", 100),
]

df = spark.createDataFrame(data, ["user_id", "product_id", "amount"])

print("\n📊 Original Data:")
df.show()

print("\n📊 Total Revenue by User:")
df.groupBy("user_id").sum("amount").show()

print("\n📊 Total Revenue by Product:")
df.groupBy("product_id").sum("amount").show()

spark.stop()
