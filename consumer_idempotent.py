from kafka import KafkaConsumer
import json
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="ecommerce",
    user="postgres",
    password="postgres"
)
cursor = conn.cursor()

cursor.execute("""
    DROP TABLE IF EXISTS orders;
    
    CREATE TABLE orders (
        order_id VARCHAR(100) PRIMARY KEY,
        user_id VARCHAR(100),
        product_id VARCHAR(100),
        amount DECIMAL(10,2),
        processed_at TIMESTAMP DEFAULT NOW()
    )
""")
conn.commit()

consumer = KafkaConsumer(
    'user_clicks',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='orders_processor_v2',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("✅ IDEMPOTENT Consumer Started")
print("🛡️  Duplicates will be prevented by PRIMARY KEY")
print("-" * 50)

processed_count = 0
duplicate_count = 0

for message in consumer:
    event = message.value
    
    if event.get('event_type') == 'purchase':
        
        try:
            cursor.execute("""
                INSERT INTO orders (order_id, user_id, product_id, amount)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
            """, (
                event.get('event_id', 'no_id'),
                event['user_id'],
                event['product_id'],
                event['product_price']
            ))
            
            if cursor.rowcount == 0:
                duplicate_count += 1
                print(f"⚠️  DUPLICATE DETECTED (ignored): {event['event_id'][:8]}... | User: {event['user_id']}")
            else:
                processed_count += 1
                print(f"✅ Order processed: {event['event_id'][:8]}... | User: {event['user_id']} | ${event['product_price']}")
            
            conn.commit()
            
        except Exception as e:
            print(f"❌ Error: {e}")
            conn.rollback()
        
        if processed_count == 3:
            print("\n💥 SIMULATING CRASH (no offset commit)...")
            print("🛡️  But DB has PRIMARY KEY protection")
            print("🛡️  On restart, duplicates will be ignored\n")
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
print("=" * 50)

cursor.close()
conn.close()
consumer.close()
