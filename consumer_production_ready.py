from kafka import KafkaConsumer
import json
import psycopg2
import redis
import time
from datetime import datetime

def get_db_connection(retries=3):
    for i in range(retries):
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="ecommerce",
                user="postgres",
                password="postgres",
                connect_timeout=5
            )
            return conn
        except Exception as e:
            print(f"⚠️  DB connection failed (attempt {i+1}/{retries}): {e}")
            if i < retries - 1:
                time.sleep(2 ** i)
            else:
                raise

def get_redis_connection():
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True, socket_connect_timeout=2)
        r.ping()
        return r
    except Exception as e:
        print(f"⚠️  Redis connection failed: {e}")
        return None

conn = get_db_connection()
cursor = conn.cursor()
r = get_redis_connection()

consumer = KafkaConsumer(
    'user_clicks',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='orders_processor_prod',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("🏭 PRODUCTION-READY Consumer")
print("✅ Retry logic")
print("✅ Fallback mechanisms")
print("✅ Error handling")
print("✅ Dead letter queue")
print("-" * 50)

metrics = {
    'processed': 0,
    'duplicates': 0,
    'errors': 0,
    'retries': 0,
    'dlq': 0
}

for message in consumer:
    
    try:
        event = message.value
        
        required_fields = ['event_id', 'user_id', 'product_id', 'product_price']
        if not all(field in event for field in required_fields):
            print(f"⚠️  MALFORMED EVENT (missing fields): {event}")
            metrics['errors'] += 1
            
            with open('dead_letter_queue.jsonl', 'a') as dlq:
                dlq.write(json.dumps({
                    'event': event,
                    'error': 'missing_fields',
                    'timestamp': datetime.now().isoformat()
                }) + '\n')
            metrics['dlq'] += 1
            
            consumer.commit()
            continue
        
        if event.get('event_type') != 'purchase':
            consumer.commit()
            continue
        
        order_id = event['event_id']
        
        is_duplicate = False
        if r:
            try:
                if r.exists(f"order:{order_id}"):
                    is_duplicate = True
                    metrics['duplicates'] += 1
                    print(f"⚡ CACHE HIT (duplicate): {order_id[:8]}...")
            except Exception as e:
                print(f"⚠️  Redis check failed, fallback to DB: {e}")
                
                try:
                    cursor.execute("SELECT 1 FROM orders WHERE order_id = %s", (order_id,))
                    if cursor.fetchone():
                        is_duplicate = True
                        metrics['duplicates'] += 1
                except Exception as db_e:
                    print(f"❌ DB fallback failed: {db_e}")
        
        if not is_duplicate:
            
            max_retries = 3
            for attempt in range(max_retries):
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
                    
                    if r:
                        try:
                            r.setex(f"order:{order_id}", 86400, "processed")
                        except:
                            pass
                    
                    metrics['processed'] += 1
                    print(f"✅ Order processed: {order_id[:8]}... | ${event['product_price']}")
                    break
                    
                except psycopg2.OperationalError as e:
                    metrics['retries'] += 1
                    print(f"⚠️  DB error (attempt {attempt+1}/{max_retries}): {e}")
                    
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        conn = get_db_connection()
                        cursor = conn.cursor()
                    else:
                        metrics['errors'] += 1
                        with open('dead_letter_queue.jsonl', 'a') as dlq:
                            dlq.write(json.dumps({
                                'event': event,
                                'error': str(e),
                                'timestamp': datetime.now().isoformat()
                            }) + '\n')
                        metrics['dlq'] += 1
                        
                except Exception as e:
                    metrics['errors'] += 1
                    print(f"❌ Unexpected error: {e}")
                    break
        
        consumer.commit()
        
        if metrics['processed'] % 10 == 0 and metrics['processed'] > 0:
            print("\n📊 Metrics:")
            print(f"   Processed: {metrics['processed']}")
            print(f"   Duplicates: {metrics['duplicates']}")
            print(f"   Errors: {metrics['errors']}")
            print(f"   Retries: {metrics['retries']}")
            print(f"   DLQ: {metrics['dlq']}\n")
        
        if metrics['processed'] >= 50:
            break
            
    except KeyboardInterrupt:
        print("\n⚠️  Shutting down gracefully...")
        break
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        metrics['errors'] += 1

print("\n" + "=" * 50)
print("📊 FINAL METRICS")
print("=" * 50)
for key, value in metrics.items():
    print(f"{key.capitalize():15}: {value}")
print("=" * 50)

cursor.close()
conn.close()
consumer.close()
