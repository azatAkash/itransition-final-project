import os
import json
import pandas as pd
from datetime import datetime

import great_expectations as gx

# parquet from Fabric export (Gold agg)
PARQUET_PATH = os.getenv("DQ_PARQUET_PATH", "./input/weather_daily.parquet")
REPORTS_DIR = os.getenv("DQ_REPORTS_DIR", "./dq_reports")


def run_validation():
    os.makedirs(REPORTS_DIR, exist_ok=True)

    df = pd.read_parquet(PARQUET_PATH)

    gdf = gx.from_pandas(df)

    # -----------------------
    # DQ checks (Expectations)
    # -----------------------
    gdf.expect_column_values_to_not_be_null("date")
    gdf.expect_column_values_to_not_be_null("dateid")
    gdf.expect_column_values_to_not_be_null("location_id")
    gdf.expect_column_values_to_not_be_null("avg_temp_c")

    # ranges
    gdf.expect_column_values_to_be_between("avg_temp_c", -60, 60)
    gdf.expect_column_values_to_be_between("total_precip_mm", 0, 500)
    gdf.expect_column_values_to_be_between("rain_hours", 0, 24)
    gdf.expect_column_values_to_be_between("rain_hours_pct", 0, 100)

    # format checks
    gdf.expect_column_values_to_match_regex("dateid", r"^\d{8}$")

    # allowed values
    gdf.expect_column_values_to_be_in_set(
        "dominant_weather_bucket",
        ["clear", "cloud", "fog", "rain", "snow", "storm", "other"],
    )

    # distribution-like check (mean)
    gdf.expect_column_mean_to_be_between("avg_temp_c", -30, 50)

    # -----------------------
    # Validate
    # -----------------------
    result = gdf.validate()
    result_dict = result.to_json_dict()

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    json_path = os.path.join(REPORTS_DIR, f"dq_result_{ts}.json")
    html_path = os.path.join(REPORTS_DIR, f"dq_report_{ts}.html")

    # Save JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result_dict, f, indent=2)

    # Simple HTML report
    failed = [r for r in result_dict["results"] if not r["success"]]
    passed = len(result_dict["results"]) - len(failed)

    html = f"""
    <html>
    <head><meta charset="utf-8"></head>
    <body>
      <h1>Great Expectations - Data Quality Report</h1>
      <p><b>Dataset:</b> {PARQUET_PATH}</p>
      <p><b>UTC time:</b> {ts}</p>

      <h2>Summary</h2>
      <ul>
        <li><b>Success:</b> {result_dict["success"]}</li>
        <li><b>Total expectations:</b> {len(result_dict["results"])}</li>
        <li><b>Passed:</b> {passed}</li>
        <li><b>Failed:</b> {len(failed)}</li>
      </ul>

      <h2>Failures</h2>
      <ol>
        {''.join([f"<li><pre>{r['expectation_config']['expectation_type']} {r['expectation_config']['kwargs']}</pre></li>" for r in failed])}
      </ol>
    </body>
    </html>
    """

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    return {
        "success": result_dict["success"],
        "total": len(result_dict["results"]),
        "passed": passed,
        "failed": len(failed),
        "html_path": html_path,
        "json_path": json_path,
    }


if __name__ == "__main__":
    res = run_validation()
    print(res)
