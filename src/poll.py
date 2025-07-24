from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync, QueryApiAsync
import pandas as pd
import json
import logging
from datetime import datetime, timedelta
import pytz
import os
import requests
from influxdb_client.client.exceptions import InfluxDBError
from prometheus_client import start_http_server, Summary
import redis.asyncio as redis
import asyncio


# Eastern for the STONK MARKETS
eastern = pytz.timezone("US/Eastern")
now = datetime.now(eastern).isoformat()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

NAMESPACE = 'stock_poller'


def make_summary(name, documentation, **kwargs):
    return Summary(name, documentation, namespace=NAMESPACE, **kwargs)


POLL_DURATION = make_summary('poll_duration_seconds', 'Poll duration in seconds')
INFLUX_DURATION = make_summary('influx_duration_seconds', 'Influx duration in seconds')
INFLUX_QUERY_DURATION = make_summary('influx_query_duration_seconds', 'Influx duration in seconds', labelnames=["query"])
MERGE_DURATION = make_summary('merge_duration_seconds', 'Merge duration in seconds')
DELT_MULT_DURATION = make_summary('delt_mult_duration_seconds', 'delt mult duration in seconds')
REDIS_DURATION = make_summary('redis_duration_seconds', 'redis duration in seconds')
ALERT_DURATION = make_summary('alert_duration_seconds', 'alert duration in seconds')

alert_url = os.getenv("ALERT_URL")
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

def get_minutes_since_open(now=None):
    if not now:
        now = datetime.now()
    elif isinstance(now, str):
        now = datetime.fromisoformat(now)  # parse ISO string to datetime

    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    minutes_diff = int((now - market_open).total_seconds() / 60)
    return minutes_diff

# Return the alerting percentage thresholds for different day parts. IE A stock might alert earlier at lower moves if it's premarket

VOLUME_PHASES = [
    {"name": "Early Premarket", "start": -120, "end": 0, "base_threshold": 0.03},
    {"name": "Early Market", "start": 0, "end": 15, "base_threshold": 0.05},
    {"name": "Market", "start": 15, "end": 60, "base_threshold": 0.08},
    {"name": "Late", "start": 60, "end": float("inf"), "base_threshold": 0.10},
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
        ttl = get_seconds_until_midnight()
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
def send_to_alerts_service(ticker_data):

    payload = {
        "ticker": ticker_data['ticker'],
        "price": ticker_data['price'],
        "multiplier": ticker_data['multiplier'],
        "float_value": ticker_data['float'],
        "volume": ticker_data['volume'],
    }

    headers = {
        "Content-Type": "application/json"
    }

    print(f"Sending payload to {url}:")
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
        print("Please ensure your Flask app is running on http://127.0.0.1:5001.")
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

flux_last_volume = '''
from(bucket: "stocks_1s")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "t" and r._field == "av")
  |> group(columns: ["sym"])
  |> last()
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> rename(columns: {sym: "ticker", av: "volume"})
  |> keep(columns: ["ticker", "volume"])
'''

flux_current_price = '''
from(bucket: "stocks_1s")
  |> range(start: -2d)
  |> filter(fn: (r) => r._measurement == "t" and r._field == "c")
  |> group(columns: ["sym"])
  |> last()
  |> filter(fn: (r) => r._value >= 1.0 and r._value <= 20.0)
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> rename(columns: {sym: "ticker", c: "current_price"})
  |> keep(columns: ["ticker", "current_price"])
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
  |> filter(fn: (r) => r._value <= 2000000)
  |> group(columns: ["ticker"])
  |> last()
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> rename(columns: {shares: "float"})
  |> keep(columns: ["ticker", "float"])
'''

async def run_influx_queries(async_query_api: QueryApiAsync):
    return await asyncio.gather(
        fetch_and_format(flux_10mav, "10mav", async_query_api ),
        fetch_and_format(flux_last_volume, "volume", async_query_api),
        fetch_and_format(flux_old_price, "old_price", async_query_api),
        fetch_and_format(flux_current_price, "current_price", async_query_api),
        fetch_and_format(flux_float, "float", async_query_api),
    )

async def process_ticker(r: redis.Redis, row, now):
    try:
        ticker = row["ticker"]

        date_str = pd.to_datetime(now).date().isoformat()
        key = f"scanner:{date_str}:{ticker}"
        latest_key = f"scanner:latest:{ticker}"


        payload = {
            "ticker": ticker,
            "price": row["current_price"],
            "prev_price": row["old_price"],
            "volume": row["volume"],
            "mav10": row["10mav"],
            "float": row["float"],
            "delta": row["delta"],
            "multiplier": row["multiplier"],
            "timestamp": now
        }

        with REDIS_DURATION.time():

            key_from_redis = await asyncio.gather( r.get(key))

            # Check if historical key already exists
            first_seen = now
            if key_from_redis is not None:
                try:
                    existing = json.loads(key_from_redis)
                    first_seen = existing.get("first_seen", now)
                except Exception:
                    logger.warning(f"Couldn't parse existing Redis key for {ticker}, using current time.")

            payload["first_seen"] = first_seen

            # Everything in the "latest" key should invalidate at midnight (throw in 10 min buffer)
            ttl_seconds = seconds_until_midnight() + 600
            await asyncio.gather(
                r.set(latest_key, json.dumps(payload), ex=ttl_seconds), 
                r.set(key, json.dumps(payload)))


        if payload['volume'] > 100000:

            phase, base_threshold = volume_threshold_by_time(minutes_since_open=get_minutes_since_open(now))

            vol_float_ratio = payload['volume']/payload['float'] 
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

                    send_to_alerts_service(payload)
                else:
                    logger.debug(f"Suppressed {tier} alert for {ticker}: already sent {current.decode() if current else 'None'}")


    except Exception as e:
        logger.exception(
            f"Failed processing ticker '{row.get('ticker', 'UNKNOWN')}': {type(e).__name__}: {str(e)}"
        )


async def start_poll_loop():
    async_client = InfluxDBClientAsync(url=url, token=token, org=org, enable_gzip=True)
    async_query_api = async_client.query_api()

    try:
        health = await async_client.ping()
        if not health:
            logger.error("InfluxDB ping failed — server not reachable.")
            raise ConnectionError("InfluxDB is not responding to ping.")
        logger.info("✅ Connected to InfluxDB.")
    except InfluxDBError as e:
        logger.exception(f"InfluxDB client error: {type(e).__name__} - {e}")
        raise
    except Exception as e:
        logger.exception(f"Unhandled error checking InfluxDB connection: {type(e).__name__} - {e}")
        raise

    r: redis.Redis = await redis.from_url(redis_host, port=redis_port, db=0, password=redis_password)

    while True:
        try:
            with POLL_DURATION.time():
                try:
                    with INFLUX_DURATION.time():
                        df_mav, df_last_vol, df_old_price, df_curr_price, df_float = await run_influx_queries(async_query_api)

                    required_dfs = {
                        "df_mav": df_mav,
                        "df_last_vol": df_last_vol,
                        "df_old_price": df_old_price,
                        "df_curr_price": df_curr_price,
                        "df_float": df_float,
                    }

                    empty_dfs = [name for name, df in required_dfs.items() if df.empty]
                    if empty_dfs:
                        logger.error(f"❌  One or more required DataFrames are empty: {', '.join(empty_dfs)}. Retrying in 30s.")
                        await asyncio.sleep(30)
                        continue

                    if logger.isEnabledFor(logging.DEBUG):        
                        logger.debug(f"10mav tickers: {df_mav['ticker'].nunique()}, Sample: {df_mav['ticker'].unique()[:5]}")
                        logger.debug(f"volume tickers: {df_last_vol['ticker'].nunique()}, Sample: {df_last_vol['ticker'].unique()[:5]}")
                        logger.debug(f"old_price tickers: {df_old_price['ticker'].nunique()}, Sample: {df_old_price['ticker'].unique()[:5]}")
                        logger.debug(f"current_price tickers: {df_curr_price['ticker'].nunique()}, Sample: {df_curr_price['ticker'].unique()[:5]}")
                        logger.debug(f"float tickers: {df_float['ticker'].nunique()}, Sample: {df_float['ticker'].unique()[:5]}")

                    common_tickers = set(df_mav['ticker']) & set(df_last_vol['ticker']) & set(df_old_price['ticker']) & set(df_curr_price['ticker']) & set(df_float['ticker'])
                    logger.info(f"🧪 Common tickers across all datasets: {len(common_tickers)}")


                    with MERGE_DURATION.time():
                        df = df_mav.merge(df_last_vol, on="ticker") \
                            .merge(df_old_price, on="ticker") \
                            .merge(df_curr_price, on="ticker") \
                            .merge(df_float, on="ticker")

                except Exception:
                    logger.exception("Merge failed")
                    df = pd.DataFrame()

                # Calculate Delta
                with DELT_MULT_DURATION.time():
                    df["delta"] = ((df["current_price"] - df["old_price"]) / df["old_price"])

                    # Filter out incomplete or NaN data
                    # df = df.dropna(subset=["10mav", "volume", "old_price", "current_price", "float"])

                    df["multiplier"] = df["volume"] / df["10mav"]



                    
                    logger.info(f"✅ Merged DataFrame with {len(df)} rows after filtering.")


                with ALERT_DURATION.time():
                    if not df.empty:
                        tasks = [process_ticker(r, row, now) for _, row in df.iterrows()]
                        await asyncio.gather(*tasks)
        except Exception:
            logger.exception("Polling iteration failed")
        await asyncio.sleep(5)


if __name__ == "__main__":
    logger.info("Starting metric server")
    start_http_server(8000)
    logger.info("Starting poller loop")
    asyncio.run(start_poll_loop())