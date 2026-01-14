from kafka import KafkaConsumer
import json
import psycopg2
import redis

conn = psycopg2.connect(
    host="localhost",
    database="ecommerce",
    user="postgres",
    password="postgres"
)
cursor = conn.cursor()

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

consumer = KafkaConsumer(
    'user_clicks',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='orders_processor_v3',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("⚡ IDEMPOTENT Consumer with Redis Cache")
print("-" * 50)

processed_count = 0
duplicate_count = 0
cache_hits = 0

for message in consumer:
    event = message.value
    
    if event.get('event_type') == 'purchase':
        
        order_id = event.get('event_id', 'no_id')
        cache_key = f"order:{order_id}"
        
        if r.exists(cache_key):
            duplicate_count += 1
            cache_hits += 1
            print(f"⚡ CACHE HIT (duplicate): {order_id[:8]}... | User: {event['user_id']}")
        else:
            try:
                cursor.execute("""
                    INSERT INTO orders (order_id, user_id, product_id, amount)
                    VALUES (%s, %s, %s, %s)
                """, (
                    order_id,
                    event['user_id'],
                    event['product_id'],
                    event['product_price']
                ))
                conn.commit()
                
                r.setex(cache_key, 86400, "processed")
                
                processed_count += 1
                print(f"✅ Order processed: {order_id[:8]}... | User: {event['user_id']} | ${event['product_price']}")
                
            except Exception as e:
                print(f"❌ Error: {e}")
                conn.rollback()
        
        if processed_count == 3:
            print("\n💥 SIMULATING CRASH...")
            print("⚡ Redis cache persists across restarts")
            cursor.close()
            conn.close()
            exit(1)
        
        consumer.commit()
    
    if processed_count >= 10:
        break

print("\n" + "=" * 50)
print("📊 FINAL STATS")
print("=" * 50)
print(f"Orders Processed: {processed_count}")
print(f"Duplicates Blocked: {duplicate_count}")
print(f"Cache Hits: {cache_hits}")
print("=" * 50)

cursor.close()
conn.close()
consumer.close()
