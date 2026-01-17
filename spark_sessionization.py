from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, ArrayType
from datetime import datetime, timedelta
import time

spark = SparkSession.builder \
    .appName("UserSessionization") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🎯 BUSINESS USE CASE: User Journey Analysis")
print("📊 Goal: Track user sessions from first view to purchase/abandon")
print("👔 Stakeholder: Product Manager")
print("💡 Business Question: How long do users browse before buying?")
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

class SessionState:
    def __init__(self):
        self.session_start = None
        self.session_end = None
        self.events = []
        self.num_views = 0
        self.num_cart_adds = 0
        self.num_purchases = 0
        self.total_value = 0.0
        self.products_viewed = set()
        self.session_active = True

def update_session_state(user_id, events, state):
    """
    Business Logic :
    - Accumule les events par user
    - Détecte timeout (30 min inactivité)
    - Finalise session si purchase ou timeout
    """
    
    if state is None:
        state = SessionState()
    
    for event in events:
        event_time = event.timestamp
        event_type = event.event_type
        
        if state.session_start is None:
            state.session_start = event_time
        
        state.session_end = event_time
        state.events.append({
            'type': event_type,
            'product': event.product_name,
            'time': event_time
        })
        
        if event_type == 'view':
            state.num_views += 1
            state.products_viewed.add(event.product_id)
        elif event_type == 'add_to_cart':
            state.num_cart_adds += 1
        elif event_type == 'purchase':
            state.num_purchases += 1
            state.total_value += event.product_price
            state.session_active = False
    
    if state.session_start and state.session_end:
        time_diff = (state.session_end - state.session_start).total_seconds()
        
        if time_diff > 1800:
            state.session_active = False
    
    if not state.session_active:
        session_duration_min = (state.session_end - state.session_start).total_seconds() / 60
        outcome = "PURCHASE" if state.num_purchases > 0 else "ABANDONED"
        
        result = {
            'user_id': user_id,
            'session_start': state.session_start,
            'session_end': state.session_end,
            'duration_minutes': round(session_duration_min, 2),
            'num_views': state.num_views,
            'num_cart_adds': state.num_cart_adds,
            'num_purchases': state.num_purchases,
            'unique_products': len(state.products_viewed),
            'total_value': state.total_value,
            'outcome': outcome,
            'conversion_rate': (state.num_purchases / state.num_views * 100) if state.num_views > 0 else 0
        }
        
        return (result, None)
    
    return (None, state)

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_clicks") \
    .option("startingOffsets", "latest") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as json_value")
df = df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")

df = df.withWatermark("timestamp", "30 minutes")

sessions = df.groupBy("user_id").applyInPandasWithState(
    update_session_state,
    StructType([
        StructField("user_id", StringType()),
        StructField("session_start", TimestampType()),
        StructField("session_end", TimestampType()),
        StructField("duration_minutes", DoubleType()),
        StructField("num_views", IntegerType()),
        StructField("num_cart_adds", IntegerType()),
        StructField("num_purchases", IntegerType()),
        StructField("unique_products", IntegerType()),
        StructField("total_value", DoubleType()),
        StructField("outcome", StringType()),
        StructField("conversion_rate", DoubleType())
    ]),
    StructType([
        StructField("session_start", TimestampType()),
        StructField("session_end", TimestampType()),
        StructField("events", ArrayType(StringType())),
        StructField("num_views", IntegerType()),
        StructField("num_cart_adds", IntegerType()),
        StructField("num_purchases", IntegerType()),
        StructField("total_value", DoubleType()),
        StructField("products_viewed", ArrayType(StringType())),
        StructField("session_active", StringType())
    ]),
    "append"
)

query = sessions.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='10 seconds') \
    .start()

print("\n✅ Sessionization started")
print("⏱️  Session timeout: 30 minutes of inactivity")
print("🎯 Session ends when:")
print("   1. User makes a purchase (outcome = PURCHASE)")
print("   2. 30 min inactive (outcome = ABANDONED)")
print("\n📊 Business Insights Generated:")
print("   - Average session duration")
print("   - Conversion rate (purchases/views)")
print("   - Products viewed per session")
print("   - Time to purchase")
print("\n🚀 Processing user journeys...\n")

query.awaitTermination()
