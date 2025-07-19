from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
import json
import logging
import redis
from datetime import datetime
import pytz
import os
import time

# Eastern for the STONK MARKETS
eastern = pytz.timezone("US/Eastern")
now = datetime.now(eastern).isoformat()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

url = os.getenv("INFLUX_URL")
token = os.getenv("INFLUX_TOKEN")
redis_host = os.getenv("REDIS_HOST")
redis_port = os.getenv("REDIS_PORT")
org = os.getenv("INFLUX_ORG")
bucket_stock = os.getenv("INFLUX_BUCKET_STOCK")
bucket_historical = os.getenv("INFLUX_BUCKET_HISTORICAL")
MULTIPLIER_THRESHOLD = float(os.getenv("MULTIPLIER_THRESHOLD", 4.5))
DELTA_THRESHOLD = float(os.getenv("DELTA_THRESHOLD", 8.0))

client = InfluxDBClient(url=url, token=token, org=org)

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 0)  # auto-detect width
pd.set_option("display.max_colwidth", None)

q = client.query_api()


def fetch_and_format(flux_query, value_name):
    try:
        result = q.query(org=org, query=flux_query)
        rows = []
        for table in result:
            for record in table.records:
                rows.append({
                    "ticker": record.values.get("ticker"),
                    value_name: record.get_value()
                })
        return pd.DataFrame(rows)
    except Exception as e:
        logger.exception(f"Failed query for {value_name}")
        return pd.DataFrame()

# Alert Service Stub
def send_to_alerts_service(ticker_data):
    print(f"[ALERT] {ticker_data['ticker']} triggered alert: Delta={ticker_data['delta']:.2f}%, Multiplier={ticker_data['multiplier']:.2f}")

flux_10mav = '''
from(bucket: "default")
  |> range(start: -2d)
  |> filter(fn: (r) => r._measurement == "10mav" and r._field == "10_day_moving_avg")
  |> group(columns: ["ticker"])
  |> last()
'''

flux_last_volume = '''
from(bucket: "default")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "current_volume" and r._field == "volume")
  |> group(columns: ["ticker"])
  |> last()
'''

flux_old_price = '''
from(bucket: "historical")
  |> range(start: -3d)
  |> filter(fn: (r) => r._measurement == "pricing" and r._field == "Close")
  |> group(columns: ["ticker"])
  |> last()
'''

flux_current_price = '''
from(bucket: "default")
  |> range(start: -2d)
  |> filter(fn: (r) => r._measurement == "current_price" and r._field == "price")
  |> filter(fn: (r) => r._value >= 1.0 and r._value <= 20.0)
  |> group(columns: ["ticker"])
  |> last()
'''

flux_float = '''
from(bucket: "default")
  |> range(start: -3d)
  |> filter(fn: (r) => r._measurement == "float" and r._field == "shares")
  |> filter(fn: (r) => r._value <= 10000000)
  |> group(columns: ["ticker"])
  |> last()
'''
if __name__ == "__main__":
    while True:
        try:
            df_mav = fetch_and_format(flux_10mav, "10mav")
            df_last_vol = fetch_and_format(flux_last_volume, "volume")
            df_old_price = fetch_and_format(flux_old_price, "old_price")
            df_curr_price = fetch_and_format(flux_current_price, "current_price")
            df_float = fetch_and_format(flux_float, "float")

            # Join all the things
            try:   
                df = df_mav.merge(df_last_vol, on="ticker") \
                            .merge(df_old_price, on="ticker") \
                            .merge(df_curr_price, on="ticker") \
                            .merge(df_float, on="ticker")
                logger.info(f"Merged DataFrame with {len(df)} records.")
            except Exception:
                logger.exception("Merge failed")
                df = pd.DataFrame()

            # Calculate Delta
            df["delta"] = ((df["current_price"] - df["old_price"]) / df["old_price"])

            # Filter out incomplete or NaN data
            df = df.dropna(subset=["10mav", "volume", "old_price", "current_price", "float"])

            df["multiplier"] = df["volume"] / df["10mav"]


            r = redis.Redis(host=redis_host, port=redis_port, db=0)

            if not df.empty:
                for _, row in df.iterrows():
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

                        r.set(key, json.dumps(payload))
                        r.set(latest_key, json.dumps(payload))

                        # Alert trigger check
                        if payload["multiplier"] > MULTIPLIER_THRESHOLD and abs(payload["delta"]) > DELTA_THRESHOLD:
                            send_to_alerts_service(payload)
                            
                    except Exception as e:
                        logger.exception(
                            f"Failed processing ticker '{row.get('ticker', 'UNKNOWN')}': {type(e).__name__}: {str(e)}"
                        )
        except Exception:
            logger.exception("Polling iteration failed")
        time.sleep(5) 


