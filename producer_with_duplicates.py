from kafka import KafkaProducer
import json
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🔁 DUPLICATE SIMULATOR")
print("📊 Sending events with intentional duplicates")
print("-" * 70)

event = {
    'event_id': 'evt_DUPLICATE_TEST_001',
    'user_id': 'user_999',
    'product_id': 'prod_001',
    'product_name': 'iPhone 15 Pro',
    'event_type': 'view',
    'timestamp': datetime.now().isoformat()
}

print("\n📤 Sending SAME event 3 times (simulating at-least-once retry):")

for i in range(3):
    producer.send('user_clicks', value=event)
    print(f"   Sent #{i+1}: event_id = {event['event_id']}")
    time.sleep(0.5)

print("\n⚠️  Without deduplication: Would process 3 times")
print("✅ With deduplication: Will process 1 time only")

producer.close()
