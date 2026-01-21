import os
import pandas as pd
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


PARQUET_PATH = os.getenv("PARQUET_PATH", "./input/weather_daily.parquet")

INFLUX_URL="https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUX_ORG="itransition"
INFLUX_BUCKET="weather"
INFLUX_TOKEN="BYDGoxhWbKyC-sAmpHetPIyiui9b6myJmbhfojRXuMMLzIjCOimqlRUfM1L4h9BgNf-uL8myjFg5gwSV9AyY_w=="



MEASUREMENT = os.getenv("INFLUX_MEASUREMENT", "weather_daily")


def main():
    df = pd.read_parquet(PARQUET_PATH)

    # expected: dateid, date, location_id, ...
    df["date"] = pd.to_datetime(df["date"]).dt.date

    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    points = []
    for _, r in df.iterrows():
        dt = datetime.combine(r["date"], datetime.min.time()).replace(tzinfo=timezone.utc)

        p = (
            Point(MEASUREMENT)
            .tag("location_id", str(r.get("location_id", "UNKNOWN")))
            .tag("dominant_bucket", str(r.get("dominant_weather_bucket", "unknown")))
            .field("dateid", int(r["dateid"]))
            .field("avg_temp_c", float(r["avg_temp_c"]) if pd.notnull(r.get("avg_temp_c")) else None)
            .field("total_precip_mm", float(r["total_precip_mm"]) if pd.notnull(r.get("total_precip_mm")) else None)
            .field("rain_hours", int(r["rain_hours"]) if pd.notnull(r.get("rain_hours")) else 0)
            .field("rain_hours_pct", float(r["rain_hours_pct"]) if pd.notnull(r.get("rain_hours_pct")) else None)
            .time(dt, WritePrecision.S)
        )
        points.append(p)

    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
    client.close()

    print(f"[OK] Exported {len(points)} rows from parquet to InfluxDB")


if __name__ == "__main__":
    main()
