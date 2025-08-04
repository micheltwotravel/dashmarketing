from fastapi import FastAPI, Query
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from google.oauth2 import service_account
import pandas as pd

app = FastAPI()

PROPERTY_ID = "279889272"
KEY_PATH = "credentials.json"

@app.get("/exportar")
def exportar_datos(start: str = Query(...), end: str = Query(...)):
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
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
        "date": row.dimension_values[0].value,
        "country": row.dimension_values[1].value,
        "pagePath": row.dimension_values[2].value,
        "users": int(row.metric_values[0].value),
        "sessions": int(row.metric_values[1].value),
        "pageviews": int(row.metric_values[2].value)
    } for row in response.rows]

    df = pd.DataFrame(rows)
    output_file = f"export_{start}_to_{end}.csv"
    df.to_csv(output_file, index=False)
    return {"status": "success", "file": output_file}
