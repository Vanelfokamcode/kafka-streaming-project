from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime

# 1. Config consumer
consumer = KafkaConsumer(
    'user_clicks',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='analytics_v1',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# 2. Métriques en mémoire
metrics = {
    'total_events': 0,
    'events_by_type': defaultdict(int),
    'events_by_category': defaultdict(int),
    'events_by_device': defaultdict(int),
    'unique_users': set(),
    'total_revenue': 0.0
}

print("🎧 Analytics Consumer Started")
print("📊 Aggregating metrics in real-time...")
print("-" * 50)

start_time = datetime.now()

try:
    for message in consumer:
        event = message.value
        
        # 3. Update metrics
        metrics['total_events'] += 1
        metrics['events_by_type'][event['event_type']] += 1
        metrics['events_by_category'][event['product_category']] += 1
        metrics['events_by_device'][event['device']] += 1
        metrics['unique_users'].add(event['user_id'])
        
        # Track revenue from purchases
        if event['event_type'] == 'purchase':
            metrics['total_revenue'] += event['product_price']
        
        # 4. Commit every 100 messages
        if metrics['total_events'] % 100 == 0:
            consumer.commit()
        
        # 5. Print dashboard every 1000 events
        if metrics['total_events'] % 1000 == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = metrics['total_events'] / elapsed
            
            print(f"\n📊 DASHBOARD (after {metrics['total_events']:,} events)")
            print("-" * 50)
            print(f"Processing Rate: {rate:.0f} events/sec")
            print(f"Unique Users: {len(metrics['unique_users']):,}")
            print(f"Total Revenue: ${metrics['total_revenue']:,.2f}")
            print()
            print("Events by Type:")
            for event_type, count in metrics['events_by_type'].items():
                pct = (count / metrics['total_events']) * 100
                print(f"  {event_type:15} {count:6,} ({pct:5.1f}%)")
            print()
            print("Events by Category:")
            for category, count in metrics['events_by_category'].items():
                pct = (count / metrics['total_events']) * 100
                print(f"  {category:15} {count:6,} ({pct:5.1f}%)")
            print()
            print("Events by Device:")
            for device, count in metrics['events_by_device'].items():
                pct = (count / metrics['total_events']) * 100
                print(f"  {device:15} {count:6,} ({pct:5.1f}%)")
            print("-" * 50)
        
        # Stop after 10k events (for testing)
        if metrics['total_events'] >= 10000:
            break

except KeyboardInterrupt:
    print("\n⚠️  Interrupted by user")
finally:
    consumer.close()
    
    # Final report
    elapsed = (datetime.now() - start_time).total_seconds()
    print("\n" + "=" * 50)
    print("📊 FINAL ANALYTICS REPORT")
    print("=" * 50)
    print(f"Total Events Processed: {metrics['total_events']:,}")
    print(f"Duration: {elapsed:.2f} seconds")
    print(f"Unique Users: {len(metrics['unique_users']):,}")
    print(f"Total Revenue: ${metrics['total_revenue']:,.2f}")
    print(f"Conversion Rate: {(metrics['events_by_type']['purchase'] / metrics['total_events'] * 100):.2f}%")
    print("=" * 50)
