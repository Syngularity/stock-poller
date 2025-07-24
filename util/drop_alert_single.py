import redis
import os

# Connect to Redis
redis_host = os.getenv("REDIS_HOST")
redis_port = os.getenv("REDIS_PORT")
redis_password = os.getenv("REDIS_PASSWORD")
r = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)

# Ask user for ticker to delete
ticker = input("Enter ticker to delete alert keys for: ").strip().upper()

# Pattern matching (adjust if your keys have different formats)
pattern = f"momentum_alert:*{ticker}*"
alert_keys = r.keys(pattern)

if not alert_keys:
    print(f"⚠️ No alert keys found matching ticker '{ticker}'.")
else:
    deleted_count = 0
    for key in alert_keys:
        try:
            r.delete(key)
            print(f"🗑️ Deleted key: {key.decode('utf-8')}")
            deleted_count += 1
        except Exception as e:
            print(f"[ERROR] Deleting key {key}: {e}")

    print(f"\n✅ Deleted {deleted_count} alert key(s) for ticker '{ticker}'.")
