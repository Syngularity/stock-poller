from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
import json
import logging
import redis
from datetime import datetime, timedelta
import pytz
import os
import requests
import time
from influxdb_client.client.exceptions import InfluxDBError
from prometheus_client import start_http_server, Summary


# Eastern for the STONK MARKETS
eastern = pytz.timezone("US/Eastern")
now = datetime.now(eastern).isoformat()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


logger = logging.getLogger(__name__)

alert_url = os.getenv("ALERT_URL")
url = os.getenv("INFLUX_URL")
token = os.getenv("INFLUX_TOKEN")
redis_host = os.getenv("REDIS_HOST")
redis_port = os.getenv("REDIS_PORT")
redis_password = os.getenv("REDIS_PASSWORD")
org = os.getenv("INFLUX_ORG")
bucket_stock = os.getenv("INFLUX_BUCKET_STOCK")
bucket_historical = os.getenv("INFLUX_BUCKET_HISTORICAL")
MULTIPLIER_THRESHOLD = float(os.getenv("MULTIPLIER_THRESHOLD", 4.5))
DELTA_THRESHOLD = float(os.getenv("DELTA_THRESHOLD", 8.0))

client = InfluxDBClient(url=url, token=token, org=org)

try:
    # Try a cheap query to validate connection
    health = client.ping()
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


q = client.query_api()

# Calculate seconds left till midnight eastern
def seconds_until_midnight():
    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return int((midnight - now).total_seconds())

# Grab influx data and format to DF
def fetch_and_format(flux_query, value_name):
    try:
        result = q.query(org=org, query=flux_query)
        rows = []
        for table in result:
            for record in table.records:
                rows.append({
                    "ticker": record.values.get("ticker") or record.values.get("sym"),
                    value_name: record.get_value()
                })
        logger.info(f"✅ Successfully fetched {value_name} data from InfluxDB. {len(rows)} rows.")
        return pd.DataFrame(rows)
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
        response = requests.post(url, headers=headers, json=payload)

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
  |> group(columns: ["ticker"])
'''

flux_last_volume = '''
from(bucket: "stocks_1s")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "t" and r._field == "av")
  |> group(columns: ["sym"])
  |> last()
'''

flux_old_price = '''
from(bucket: "stocks_5m")
  |> range(start: -3d)
  |> filter(fn: (r) => r._measurement == "t" and r._field == "c")
  |> group(columns: ["sym"])
  |> last()
'''

flux_current_price = '''
from(bucket: "stocks_1s")
  |> range(start: -2d)
  |> filter(fn: (r) => r._measurement == "t" and r._field == "c")
  |> filter(fn: (r) => r._value >= 1.0 and r._value <= 20.0)
  |> group(columns: ["sym"])
  |> last()
'''

flux_float = ''' from(bucket: "default") |> range(start: -3d)
  |> filter(fn: (r) => r._measurement == "float" and r._field == "shares")
  |> filter(fn: (r) => r._value <= 10000000)
  |> group(columns: ["ticker"])
  |> last()
'''

NAMESPACE = 'stock_poller'


def make_summary(name, documentation):
    return Summary(name, documentation, namespace=NAMESPACE)


POLL_DURATION = make_summary('poll_duration_seconds', 'Poll duration in seconds')
INFLUX_DURATION = make_summary('influx_duration_seconds', 'Influx duration in seconds')
MERGE_DURATION = make_summary('merge_duration_seconds', 'Merge duration in seconds')
DELT_MULT_DURATION = make_summary('delt_mult_duration_seconds', 'delt mult duration in seconds')
REDIS_DURATION = make_summary('redis_duration_seconds', 'redis duration in seconds')
ALERT_DURATION = make_summary('alert_duration_seconds', 'alert duration in seconds')

if __name__ == "__main__":
    start_http_server(8000)
    logger.info("Starting poller loop")
    while True:
        try:
            with POLL_DURATION.time():
                try:
                    with INFLUX_DURATION.time():
                        df_mav = fetch_and_format(flux_10mav, "10mav")
                        df_last_vol = fetch_and_format(flux_last_volume, "volume")
                        df_old_price = fetch_and_format(flux_old_price, "old_price")
                        df_curr_price = fetch_and_format(flux_current_price, "current_price")
                        df_float = fetch_and_format(flux_float, "float")

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
                        time.sleep(30)
                        continue

                    logger.info(f"10mav tickers: {df_mav['ticker'].nunique()}, Sample: {df_mav['ticker'].unique()[:5]}")
                    logger.info(f"volume tickers: {df_last_vol['ticker'].nunique()}, Sample: {df_last_vol['ticker'].unique()[:5]}")
                    logger.info(f"old_price tickers: {df_old_price['ticker'].nunique()}, Sample: {df_old_price['ticker'].unique()[:5]}")
                    logger.info(f"current_price tickers: {df_curr_price['ticker'].nunique()}, Sample: {df_curr_price['ticker'].unique()[:5]}")
                    logger.info(f"float tickers: {df_float['ticker'].nunique()}, Sample: {df_float['ticker'].unique()[:5]}")

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

                with REDIS_DURATION.time():
                    r = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)

                with ALERT_DURATION.time():
                    if not df.empty:
                        for _, row in df.iterrows():
                            try:
                                ticker = row["ticker"]
                                logger.info(f"Adding ticker to redis: {ticker}")
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

                                # Everything in the "latest" key should invalidate at midnight (throw in 10 min buffer)
                                ttl_seconds = seconds_until_midnight() + 600
                                r.set(latest_key, json.dumps(payload), ex=ttl_seconds)

                                # Duplicate historical records so we can do some fast charting or whatever with it
                                r.set(key, json.dumps(payload))

                                # Alert trigger check
                                ticker_already_alerted_today = r.exists(latest_key) 
                                if payload["multiplier"] > MULTIPLIER_THRESHOLD and not ticker_already_alerted_today:
                                    send_to_alerts_service(payload)
                            except Exception as e:
                                logger.exception(
                                    f"Failed processing ticker '{row.get('ticker', 'UNKNOWN')}': {type(e).__name__}: {str(e)}"
                                )
        except Exception:
            logger.exception("Polling iteration failed")
        time.sleep(5)


