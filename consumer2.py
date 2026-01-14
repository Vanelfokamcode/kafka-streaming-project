from kafka import KafkaConsumer
import json

# 1. Créer le consumer
consumer = KafkaConsumer(
    'user_clicks',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',  # Lire depuis le début
    enable_auto_commit=False,      # Manual commit (safe)
    group_id='recommendation_team',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("🎧 Consumer started - En attente de messages...")
print()

# 2. Lire les messages
message_count = 0

for message in consumer:
    message_count += 1
    
    # 3. Extraire les données
    event = message.value
    
    print(f"📩 Message #{message_count} reçu:")
    print(f"   Partition: {message.partition}")
    print(f"   Offset: {message.offset}")
    print(f"   User: {event['user_id']}")
    print(f"   Product: {event['product_id']}")
    print(f"   Timestamp: {event['timestamp']}")
    print()
    
    # 4. Commit manual après traitement
    consumer.commit()
    
    # Arrêter après 10 messages (pour le test)
    if message_count >= 10:
        break

consumer.close()
print("✅ Consumer fermé")
