from fastapi import FastAPI, Query
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from google.oauth2 import service_account
import pandas as pd
import os
import json

app = FastAPI()

PROPERTY_ID = "279889272"
CREDENTIALS_FILE = "/etc/secrets/ga4-credentials.json"  # Secret File montado por Render

@app.get("/exportar")
def exportar_datos(start: str = Query(...), end: str = Query(...)):
    # ✅ Leer contenido del secret file como dict
    with open(CREDENTIALS_FILE, "r") as f:
        credentials_info = json.load(f)

    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    client = BetaAnalyticsDataClient(credentials=credentials)

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        dimensions=[
            Dimension(name="date"),
            Dimension(name="country"),
            Dimension(name="pagePath"),
        ],
        metrics=[
            Metric(name="users"),
            Metric(name="sessions"),
            Metric(name="screenPageViews")
        ],
        date_ranges=[DateRange(start_date=start, end_date=end)]
    )

    response = client.run_report(request)

    rows = [{
        "date": r.dimension_values[0].value,
        "country": r.dimension_values[1].value,
        "pagePath": r.dimension_values[2].value,
        "users": int(r.metric_values[0].value),
        "sessions": int(r.metric_values[1].value),
        "pageviews": int(r.metric_values[2].value)
    } for r in response.rows]

    return {"rows": rows}
