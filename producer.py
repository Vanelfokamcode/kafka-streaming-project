from kafka import KafkaProducer
import json
import time
from datetime import datetime

# 1. Créer le producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 2. Fonction pour générer un event
def generate_click_event(user_id, product_id):
    event = {
        'user_id': user_id,
        'product_id': product_id,
        'event_type': 'view',
        'timestamp': datetime.now().isoformat()
    }
    return event

# 3. Envoyer 10 events
print("🚀 Envoi de 10 events...")

for i in range(10):
    event = generate_click_event(
        user_id=f"user_{i % 3}",  # 3 users différents
        product_id=f"product_{i}"
    )
    
    # Envoyer au topic
    future = producer.send('user_clicks', value=event)
    
    # Attendre confirmation
    record_metadata = future.get(timeout=10)
    
    print(f"✅ Event {i+1} envoyé:")
    print(f"   Topic: {record_metadata.topic}")
    print(f"   Partition: {record_metadata.partition}")
    print(f"   Offset: {record_metadata.offset}")
    print(f"   User: {event['user_id']}, Product: {event['product_id']}")
    print()
    
    time.sleep(1)

producer.close()
print("✅ Producer fermé")
