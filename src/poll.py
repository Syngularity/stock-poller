from typing import Dict, Optional
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync, QueryApiAsync
import pandas as pd
import json
import logging
from datetime import datetime, timedelta
import pytz
import os
import requests
from influxdb_client.client.exceptions import InfluxDBError
from prometheus_client import start_http_server, Summary, Counter
import redis.asyncio as redis
import asyncio
import websockets
from aiorwlock import RWLock
import orjson


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

NAMESPACE = 'stock_poller'


def make_summary(name, documentation, **kwargs):
    return Summary(name, documentation, namespace=NAMESPACE, **kwargs)

def make_counter(name, documentation, **kwargs):
    return Counter(name, documentation, namespace=NAMESPACE, **kwargs)


POLL_DURATION = make_summary('poll_duration_seconds', 'Poll duration in seconds')
INFLUX_DURATION = make_summary('influx_duration_seconds', 'Influx duration in seconds')
INFLUX_QUERY_DURATION = make_summary('influx_query_duration_seconds', 'Influx duration in seconds', labelnames=["query"])
MERGE_DURATION = make_summary('merge_duration_seconds', 'Merge duration in seconds')
DELT_MULT_DURATION = make_summary('delt_mult_duration_seconds', 'delt mult duration in seconds')
REDIS_DURATION = make_summary('redis_duration_seconds', 'redis duration in seconds')
ALERT_DURATION = make_summary('alert_duration_seconds', 'alert duration in seconds')
WEBSOCKET_DISCONNECT = make_counter('websocket_disconnect_total', 'The amount of times the websocket disconnected')
TICKERS_PROCESSED = make_counter('tickers_processed_total', 'The amount of tickers processed')
TICKER_PROCESS_DURATION = make_summary('ticker_process_seconds', 'The amount of time it takes to process a ticker on average')

alert_url = os.getenv("ALERT_HOST")
url = os.getenv("INFLUX_URL")
token = os.getenv("INFLUX_TOKEN")
redis_host = "redis://" + os.getenv("REDIS_HOST")
redis_port = os.getenv("REDIS_PORT")
redis_password = os.getenv("REDIS_PASSWORD")
org = os.getenv("INFLUX_ORG")
bucket_stock = os.getenv("INFLUX_BUCKET_STOCK")
bucket_historical = os.getenv("INFLUX_BUCKET_HISTORICAL")
MULTIPLIER_THRESHOLD = float(os.getenv("MULTIPLIER_THRESHOLD", 1))
DELTA_THRESHOLD = float(os.getenv("DELTA_THRESHOLD", 8.0))

def get_minutes_since_open():
    now = datetime.now(pytz.timezone("US/Eastern"))
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    minutes_diff = int((now - market_open).total_seconds() / 60)
    return minutes_diff


VOLUME_PHASES = [
    {"name": "Early Premarket", "start": -120, "end": 0, "base_threshold": 0.03},
    {"name": "Early Market", "start": 0, "end": 15, "base_threshold": 0.05},
    {"name": "Market", "start": 15, "end": 60, "base_threshold": 0.08},
    {"name": "Late", "start": 60, "end": float("inf"), "base_threshold": 0.10},
]

MAV10_PHASES = [
    {"name": "Early Premarket", "start": -120, "end": 0, "base_threshold": 0.5},
    {"name": "Early Market", "start": 0, "end": 15, "base_threshold": 0.8},
    {"name": "1st Hour Market", "start": 15, "end": 60, "base_threshold": 1.2},
    {"name": "Late Market", "start": 60, "end": float("inf"), "base_threshold": 2.5},
]

ALERT_TIERS_THRESHOLDS = [
    ("HIGH", 10),
    ("MEDIUM", 3),
    ("LOW", 1),
]

TIER_PRIORITY = {
    "LOW": 1,
    "MEDIUM": 2,
    "HIGH": 3
}


def volume_threshold_by_time(minutes_since_open):
    for phase in VOLUME_PHASES:
        if phase["start"] <= minutes_since_open < phase["end"]:
            return phase["name"], phase["base_threshold"]
    return "NA", float("inf")

def mav10_threshold_by_time(minutes_since_open):
    for phase in MAV10_PHASES:
        if phase["start"] <= minutes_since_open < phase["end"]:
            return phase["name"], phase["base_threshold"]
    return "NA", float("inf")

def get_alert_tier(volume_ratio, base_threshold):
    for label, ratio in ALERT_TIERS_THRESHOLDS:
        if volume_ratio >= base_threshold * ratio:
            return label
    return None


# Calculate seconds left till midnight eastern
def seconds_until_midnight():
    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return int((midnight - now).total_seconds())


def should_send_alert(ticker: str, new_tier: str):
    today = datetime.now().strftime('%Y%m%d')
    key = f"momentum_alert:{ticker}:{today}"

    # Get existing alert tier from Redis
    current_tier = r.get(key)
    current_priority = TIER_PRIORITY.get(current_tier.decode(), 0) if current_tier else 0
    new_priority = TIER_PRIORITY[new_tier]

    # Only alert if it's higher than the current
    if new_priority > current_priority:
        ttl = seconds_until_midnight()
        r.setex(key, ttl, new_tier)
        return True

    return False


# Grab influx data and format to DF
async def fetch_and_format(flux_query, value_name, async_query_api: QueryApiAsync):
    with INFLUX_QUERY_DURATION.labels(query=value_name).time():
        try:
            df = await async_query_api.query_data_frame(org=org, query=flux_query)
            # If we have multiple df's merge them into one
            if isinstance(df, list):
                df = pd.concat(df, ignore_index=True)

            df = df[["ticker", value_name]]

            logger.info(f"✅ Successfully fetched {value_name} data from InfluxDB. Rows: {len(df)}, Columns: {df.columns.tolist()}")
            logger.debug(f"Sample data for {value_name}:\n{df.head().to_string()}")
            return df
        except Exception as e:
            logger.exception(f"Failed query for {value_name}")
            return pd.DataFrame()


# Alert Service Stub
async def send_to_alerts_service(ticker_data):

    payload = {
        "ticker": ticker_data['ticker'],
        "price": ticker_data['price'],
        "multiplier": ticker_data['multiplier'],
        "float_value": ticker_data['float'],
        "volume": ticker_data['volume'],
        "tier": ticker_data['tier'],
        "phase": ticker_data['phase'],
    }

    headers = {
        "Content-Type": "application/json"
    }

    print(f"Sending payload to {alert_url}:")
    print(json.dumps(payload, indent=2))

    try:
        response = requests.post(alert_url, headers=headers, json=payload)

        print("\n--- Response ---")
        print(f"Status Code: {response.status_code}")
        print(f"Response Body: {response.json()}")

        if response.status_code == 200:
            print("\nPayload sent successfully! Check your Discord channel.")
        else:
            print(f"\nFailed to send payload. Server responded with an error.")

    except requests.exceptions.ConnectionError:
        print(f"\nError: Could not connect to Flask server at {url}.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")


###################################
#   All the Queries
###################################

flux_10mav = '''
from(bucket: "10_mav")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "10mav" and r._field == "10_day_moving_avg")
  |> group(columns: ["sym"])
  |> last()
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> rename(columns: {sym: "ticker", "10_day_moving_avg": "10mav"})
  |> keep(columns: ["ticker", "10mav"])
'''

flux_old_price = '''
from(bucket: "stocks_5m")
  |> range(start: -3d)
  |> filter(fn: (r) => r._measurement == "t" and r._field == "c")
  |> group(columns: ["sym"])
  |> last()
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> rename(columns: {sym: "ticker", c: "old_price"})
  |> keep(columns: ["ticker", "old_price"])
'''

flux_float = '''
from(bucket: "default") |> range(start: -3d)
  |> filter(fn: (r) => r._measurement == "float" and r._field == "shares")
  |> filter(fn: (r) => r._value <= 20000000)
  |> group(columns: ["ticker"])
  |> last()
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> rename(columns: {shares: "float"})
  |> keep(columns: ["ticker", "float"])
'''

dataframes: Dict[str, Optional[pd.DataFrame]] = {
    "10mav": None,
    "old_price": None, 
    "float": None
}
dataframes_rwlock = RWLock()

async def run_influx_queries(async_query_api: QueryApiAsync):
    return await asyncio.gather(
        fetch_and_format(flux_10mav, "10mav", async_query_api ),
        fetch_and_format(flux_old_price, "old_price", async_query_api),
        fetch_and_format(flux_float, "float", async_query_api),
    )

async def set_ticker_index(df: pd.DataFrame):
    return df.set_index("ticker").to_dict("index")

async def process_ticker(r: redis.Redis, now, ticker, cp, op, v, mav, float, delta, multiplier):
    try:
        date_str = pd.to_datetime(now).date().isoformat()
        key = f"scanner:{date_str}:{ticker}"
        latest_key = f"scanner:latest:{ticker}"

        payload = {
            "ticker": ticker,
            "price": cp,
            "prev_price": op,
            "volume": v,
            "mav10": mav,
            "float": float,
            "delta": delta,
            "multiplier": multiplier,
            "timestamp": now
        }

        with REDIS_DURATION.time():

            key_from_redis = await r.get(key)

            # Check if historical key already exists
            first_seen = now
            if key_from_redis is not None:
                try:
                    existing = json.loads(key_from_redis)
                    first_seen = existing.get("first_seen", now)
                except Exception:
                    logger.warning(f"Couldn't parse existing Redis key for {ticker}, using current time. Key: {key} key from redis: {key_from_redis}")

            payload["first_seen"] = first_seen

            # Everything in the "latest" key should invalidate at midnight (throw in 10 min buffer)
            ttl_seconds = seconds_until_midnight() + 600
            await asyncio.gather(
                r.set(latest_key, json.dumps(payload), ex=ttl_seconds), 
                r.set(key, json.dumps(payload)))


        if payload['volume'] > 100000 and payload["multiplier"] > 1:

            minutes_open = get_minutes_since_open()

            phase, base_threshold = volume_threshold_by_time(minutes_since_open=get_minutes_since_open())
            _, mav10_threshold = mav10_threshold_by_time(minutes_open)

            vol_float_ratio = payload['volume']/payload['float']
        
            
            if payload["multiplier"] < mav10_threshold:
                logger.debug(f'Suppressed alert for {ticker}: volume/mav10 {payload["multiplier"]:.2f} < threshold {mav10_threshold:.2f}')

                return
            
            tier = get_alert_tier(vol_float_ratio, base_threshold)


            if tier:

                payload['tier'] = tier
                payload['phase'] = phase

                redis_alert_key = f"momentum_alert:{ticker}:{pd.to_datetime(now).date().isoformat()}"
                current = await r.get(redis_alert_key)
                current_rank = TIER_PRIORITY.get(current.decode(), 0) if current else 0
                new_rank = TIER_PRIORITY[tier]

                if new_rank > current_rank:
                    ttl = seconds_until_midnight() + 600
                    await r.set(redis_alert_key, tier, ex=ttl)

                    logger.info(f"{tier} alert triggered during {phase}: "
                        f"{vol_float_ratio:.2f} (base: {base_threshold}, ratio: {vol_float_ratio/base_threshold:.2f}x)")

                    await send_to_alerts_service(payload)
                else:
                    logger.debug(f"Suppressed {tier} alert for {ticker}: already sent {current.decode() if current else 'None'}")


    except Exception as e:
        logger.exception(
            f"Failed processing ticker '{ticker}': {type(e).__name__}: {str(e)}"
        )

def parse_ws_message(message) -> tuple:
    data = orjson.loads(message)
    logger.info(message)
    return (
        data.get('sym'),      # symbol
        data.get('c'),        # close  
        data.get('av'),       # volume
    )

async def lookup(table: Dict[str, Dict], key, ticker):
    if table is None:
        logger.info(f"No dataframe loaded when looking at ticker {ticker}")
        return None
    try:
        return table[ticker][key]
    except KeyError:
        return None


async def handle_message(r, message):
    try:
        with TICKER_PROCESS_DURATION.time():
            ticker, current_price, volume = parse_ws_message(message)

            if current_price < 1 or current_price > 20 or volume < 100_000:
                return

            # Need reader lock because these dataframes are updating in the bg
            async with dataframes_rwlock.reader:
                mav = await lookup(dataframes["10mav"], "10mav", ticker)
                old_price = await lookup(dataframes["old_price"], "old_price", ticker)
                float = await lookup(dataframes["float"], "float", ticker)

            if mav is None or old_price is None or float is None:
                # Do not process ticker if it's missing data
                # Could turn on debug logging here
                logger.info(f"coudn't process ticker {ticker} cuz empty mav=={mav} op=={old_price} float=={float}")
                return

            delta = ((current_price - old_price) / old_price)
            multiplier = volume / mav

            if multiplier < 1:
                return

            now = datetime.now(pytz.timezone("US/Eastern")).isoformat()

            await process_ticker(r, now, ticker, current_price, old_price, volume, mav, float, delta, multiplier)
            TICKERS_PROCESSED.inc()
    except Exception as e:
        logger.error(f"Unexpected error processing ticker: {e}\nFor: {message}")



async def listen_websocket():
    uri = "ws://ingestor-service.stock.svc.cluster.local/ws"
    r: redis.Redis = await redis.from_url(redis_host, port=redis_port, db=0, password=redis_password)

    while True:
        try:
            async with websockets.connect(uri, max_size=None, compression=None) as ws:
                async for message in ws:
                    await handle_message(r, message)
        except Exception as e:
            logger.error(f"Unexpected error: {e}. Reconnecting...")
            WEBSOCKET_DISCONNECT.inc()
            await asyncio.sleep(5)

async def load_data_from_influx(async_query_api):
    with INFLUX_DURATION.time():
        try:
            df_mav, df_old_price, df_float = await run_influx_queries(async_query_api)
            mav_dict = await set_ticker_index(df_mav)
            old_price_dict = await set_ticker_index(df_old_price)
            float_dict = await set_ticker_index(df_float)

            async with dataframes_rwlock.writer:
                dataframes["10mav"] = mav_dict
                dataframes["old_price"] = old_price_dict
                dataframes["float"] = float_dict

        except Exception as e:
            logger.error(f"Error updating dataframes: {e}")


async def poll_influx(async_query_api):
    while True:
        await load_data_from_influx(async_query_api)
        await asyncio.sleep(300)

async def main():
    async_client = InfluxDBClientAsync(url=url, token=token, org=org, enable_gzip=True)
    async_query_api = async_client.query_api()
    await load_data_from_influx(async_query_api) # Do it first to avoid race conditions
    logger.info("Loaded data from influx, starting tasks...")

    ws_task = asyncio.create_task(listen_websocket())
    influx_task = asyncio.create_task(poll_influx(async_query_api))
    await asyncio.gather(ws_task, influx_task)


if __name__ == "__main__":
    logger.info("Starting metric server")
    start_http_server(8000)
    logger.info("Starting poller loop")
    asyncio.run(main())
