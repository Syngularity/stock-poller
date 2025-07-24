import redis
import os

# Connect to Redis
redis_host = os.getenv("REDIS_HOST")
redis_port = os.getenv("REDIS_PORT")
redis_password = os.getenv("REDIS_PASSWORD")
r = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)

# Fetch all alert-related keys (adjust the pattern if your alert keys are different)
alert_keys = r.keys("alert:*")

deleted_count = 0
for key in alert_keys:
    try:
        r.delete(key)
        print(f"🗑️ Deleted key: {key.decode('utf-8')}")
        deleted_count += 1
    except Exception as e:
        print(f"[ERROR] Deleting key {key}: {e}")

print(f"\n✅ Deleted {deleted_count} alert keys total.")
