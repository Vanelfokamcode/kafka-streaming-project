import requests
import json
import time

print("📊 SPARK PERFORMANCE DASHBOARD")
print("=" * 70)

while True:
    try:
        response = requests.get("http://localhost:4040/api/v1/applications")
        
        if response.status_code == 200:
            apps = response.json()
            
            if apps:
                app_id = apps[0]['id']
                
                streaming_response = requests.get(
                    f"http://localhost:4040/api/v1/applications/{app_id}/streaming/statistics"
                )
                
                if streaming_response.status_code == 200:
                    stats = streaming_response.json()
                    
                    print(f"\n{'='*70}")
                    print(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"{'='*70}")
                    
                    if 'inputRate' in stats:
                        print(f"📥 Input Rate: {stats.get('inputRate', 0):.0f} events/sec")
                    
                    if 'processingRate' in stats:
                        print(f"⚡ Processing Rate: {stats.get('processingRate', 0):.0f} events/sec")
                    
                    if 'batchDuration' in stats:
                        print(f"⏱️  Batch Duration: {stats.get('batchDuration', 0):.2f} seconds")
                    
                    input_rate = stats.get('inputRate', 0)
                    processing_rate = stats.get('processingRate', 0)
                    
                    if processing_rate > 0:
                        if processing_rate >= input_rate:
                            print(f"✅ Status: HEALTHY (no lag)")
                        else:
                            lag_per_sec = input_rate - processing_rate
                            print(f"⚠️  Status: LAG GROWING (+{lag_per_sec:.0f} events/sec)")
                    
                    print(f"{'='*70}")
        
        time.sleep(5)
        
    except KeyboardInterrupt:
        print("\n\n👋 Monitoring stopped")
        break
    except Exception as e:
        print(f"⚠️  Error: {e}")
        time.sleep(5)
