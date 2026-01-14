from kafka import KafkaProducer
from fake_data_generator import generate_user_click
import json
import time
from datetime import datetime

# 1. Config producer optimisé
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    compression_type='gzip',  # Compression pour performance
    batch_size=16384,  # Batch messages pour efficacité
    linger_ms=10  # Attendre 10ms pour batcher
)

# 2. Métriques
total_sent = 0
start_time = time.time()
errors = 0

print("🚀 High-Volume Producer Started")
print(f"📊 Target: 1000 events/sec")
print(f"⏰ Started at: {datetime.now().strftime('%H:%M:%S')}")
print("-" * 50)

try:
    # 3. Envoyer 10,000 events (10 secondes à 1000/sec)
    for i in range(10000):
        
        # Générer event
        event = generate_user_click()
        
        # Envoyer avec partition key = user_id
        producer.send(
            'user_clicks',
            key=event['user_id'].encode('utf-8'),
            value=event
        )
        
        total_sent += 1
        
        # Print progress toutes les 1000 events
        if total_sent % 1000 == 0:
            elapsed = time.time() - start_time
            rate = total_sent / elapsed
            print(f"✅ Sent {total_sent:,} events | Rate: {rate:.0f} events/sec")
        
        # Throttle pour atteindre ~1000/sec
        if i % 100 == 0:
            time.sleep(0.1)  # 100 events toutes les 0.1 sec = 1000/sec
    
    # 4. Flush pour être sûr que tout est envoyé
    producer.flush()
    
except KeyboardInterrupt:
    print("\n⚠️  Interrupted by user")
except Exception as e:
    print(f"❌ Error: {e}")
    errors += 1
finally:
    producer.close()
    
    # 5. Stats finales
    elapsed = time.time() - start_time
    rate = total_sent / elapsed if elapsed > 0 else 0
    
    print("\n" + "=" * 50)
    print("📊 FINAL STATS")
    print("=" * 50)
    print(f"Total Events Sent: {total_sent:,}")
    print(f"Duration: {elapsed:.2f} seconds")
    print(f"Average Rate: {rate:.0f} events/sec")
    print(f"Errors: {errors}")
    print("=" * 50)
