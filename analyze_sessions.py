from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, sum as _sum, when, col

# Simulation : lire depuis un fichier Parquet sauvegardé
# En prod, ce serait query directement le streaming state

print("📊 SESSION ANALYSIS REPORT")
print("=" * 70)

# Fake data pour simulation
data = [
    ("user_1", 12, 8, 3, 1, 999.0, 13.13, "CONVERTED"),
    ("user_2", 5, 4, 1, 0, 0.0, 6.97, "BROWSING"),
    ("user_3", 15, 10, 4, 1, 1199.0, 27.62, "CONVERTED"),
    ("user_4", 3, 3, 0, 0, 0.0, 2.15, "BROWSING"),
    ("user_5", 20, 15, 3, 2, 2398.0, 35.82, "CONVERTED"),
    ("user_6", 7, 6, 1, 0, 0.0, 8.45, "BROWSING"),
]

spark = SparkSession.builder.appName("Analysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.createDataFrame(data, [
    "user_id", "total_events", "views", "cart_adds", "purchases",
    "revenue", "duration_minutes", "outcome"
])

print("\n1️⃣  CONVERSION FUNNEL")
print("-" * 70)
funnel = df.groupBy("outcome").agg(
    count("*").alias("num_users"),
    avg("duration_minutes").alias("avg_duration"),
    avg("views").alias("avg_views"),
    avg("cart_adds").alias("avg_cart_adds")
)
funnel.show(truncate=False)

print("\n💡 Business Insight:")
converted = df.filter(col("outcome") == "CONVERTED")
browsing = df.filter(col("outcome") == "BROWSING")

conv_duration = converted.agg(avg("duration_minutes")).collect()[0][0]
brow_duration = browsing.agg(avg("duration_minutes")).collect()[0][0]

print(f"   - Converters browse {conv_duration:.1f} min on average")
print(f"   - Browsers browse {brow_duration:.1f} min on average")
print(f"   - Converters spend {conv_duration - brow_duration:.1f} min MORE")

print("\n2️⃣  REVENUE ANALYSIS")
print("-" * 70)
revenue_stats = converted.agg(
    _sum("revenue").alias("total_revenue"),
    avg("revenue").alias("avg_order_value"),
    count("*").alias("num_converters")
)
revenue_stats.show(truncate=False)

print("\n3️⃣  CONVERSION RATE")
print("-" * 70)
total_users = df.count()
converters = converted.count()
conversion_rate = (converters / total_users) * 100

print(f"   Total Users: {total_users}")
print(f"   Converters: {converters}")
print(f"   Conversion Rate: {conversion_rate:.1f}%")

print("\n🎯 ACTIONABLE RECOMMENDATIONS FOR PM:")
print("-" * 70)
print(f"   1. Optimize for {conv_duration:.0f}-minute sessions")
print(f"   2. Users who add to cart are {(converters/total_users)*100:.0f}% likely to convert")
print(f"   3. Target users inactive for >{brow_duration:.0f} min with re-engagement")

spark.stop()
