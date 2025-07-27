#!/usr/bin/env python3
"""
Utility script to load dummy Redis stock data for testing.
Creates sample stock data that meets the criteria with a 3-hour TTL.
"""

import asyncio
import json
import os
import random
from datetime import datetime, timedelta

try:
    import pytz
except ImportError:
    print("ERROR: pytz not installed. Install with: pip install pytz")
    exit(1)

try:
    import redis.asyncio as redis
except ImportError:
    print("ERROR: redis not installed. Install with: pip install redis[hiredis]")
    exit(1)


# Configuration
REDIS_HOST = "redis://" + os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
TTL_HOURS = 3
TTL_SECONDS = TTL_HOURS * 3600

# Sample tickers that meet criteria (price between $1-$20, reasonable float)
SAMPLE_TICKERS = [
    "AAPL", "TSLA", "NVDA", "LOOP", "MSFT", "GOOGL", "META", "AMZN", "NFLX", "ORCL",
    "CRM", "ADBE", "PYPL", "INTC", "CSCO", "UBER", "LYFT", "SNAP", "TWTR", "SPOT",
    "SQ", "SHOP", "ZM", "DOCU", "ROKU", "PINS", "DBX", "WORK", "TEAM", "OKTA"
]


def generate_dummy_stock_data(ticker: str, now: str) -> dict:
    """Generate realistic dummy stock data for a ticker."""
    # Generate realistic price data ($1-$20 range)
    current_price = round(random.uniform(1.0, 20.0), 2)
    old_price = current_price * random.uniform(0.85, 1.15)  # ±15% variation
    
    # Generate volume data (100k to 10M shares)
    volume = random.randint(100000, 10000000)
    
    # Generate 10-day moving average volume (usually lower than current)
    mav10 = volume * random.uniform(0.3, 0.9)
    
    # Generate float (shares outstanding, typically 1M to 20M for small caps)
    float_shares = random.randint(1000000, 20000000)
    
    # Calculate derived values
    delta = (current_price - old_price) / old_price
    multiplier = volume / mav10
    
    return {
        "ticker": ticker,
        "price": current_price,
        "prev_price": old_price,
        "volume": volume,
        "mav10": mav10,
        "float": float_shares,
        "delta": delta,
        "multiplier": multiplier,
        "timestamp": now,
        "first_seen": now
    }


async def load_dummy_data():
    """Load dummy stock data into Redis with 3-hour TTL."""
    print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
    
    r = await redis.from_url(REDIS_HOST, port=REDIS_PORT, db=0, password=REDIS_PASSWORD)
    
    try:
        # Test connection
        await r.ping()
        print("Connected to Redis successfully")
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        return
    
    # Get current timestamp
    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern).isoformat()
    date_str = datetime.now(eastern).date().isoformat()
    
    print(f"Loading data for {date_str} with {TTL_HOURS}h TTL")
    
    loaded_count = 0
    
    for ticker in SAMPLE_TICKERS:
        try:
            # Generate dummy data
            payload = generate_dummy_stock_data(ticker, now)
            
            # Create Redis keys
            historical_key = f"scanner:{date_str}:{ticker}"
            latest_key = f"scanner:latest:{ticker}"
            
            # Store in Redis with TTL
            await asyncio.gather(
                r.set(historical_key, json.dumps(payload)),  # Historical key (no TTL)
                r.set(latest_key, json.dumps(payload), ex=TTL_SECONDS)  # Latest key with TTL
            )
            
            loaded_count += 1
            
            # Print sample data for first few tickers
            if loaded_count <= 3:
                print(f"Stock {ticker}: ${payload['price']:.2f} (Delta {payload['delta']:+.2%}) "
                      f"Vol: {payload['volume']:,} (x{payload['multiplier']:.1f})")
            
        except Exception as e:
            print(f"Failed to load data for {ticker}: {e}")
    
    print(f"Successfully loaded {loaded_count} tickers into Redis")
    print(f"Data will expire in {TTL_HOURS} hours")
    
    # Verify data was loaded
    try:
        sample_key = f"scanner:latest:{SAMPLE_TICKERS[0]}"
        sample_data = await r.get(sample_key)
        if sample_data:
            print(f"Verification: Found data for {SAMPLE_TICKERS[0]}")
        else:
            print("Warning: Could not verify data was loaded")
    except Exception as e:
        print(f"Warning: Could not verify data: {e}")
    
    await r.close()


if __name__ == "__main__":
    print("Loading dummy Redis stock data...")
    asyncio.run(load_dummy_data())
    print("Done!")