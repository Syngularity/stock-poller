import redis
import json

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Fetch all keys for today's snapshot
keys = r.keys("scanner:*")

for key in sorted(keys):
    try:
        value = r.get(key)
        if value:
            decoded = json.loads(value)
            print(f"\n🔑 {key.decode('utf-8')}")
            for k, v in decoded.items():
                print(f"  {k}: {v}")
    except Exception as e:
        print(f"[ERROR] Reading key {key}: {e}")
        
print(f"Found {len(keys)} keys")