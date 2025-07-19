import redis
import json
import os

# Connect to Redis
redis_host = os.getenv("REDIS_HOST")
redis_port = os.getenv("REDIS_PORT")
r = redis.Redis(host=redis_host, port=redis_port, db=0)

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