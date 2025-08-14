from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric, OrderBy
from google.oauth2 import service_account
import os, json, time, datetime as dt

app = FastAPI()

PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "279889272")
CREDENTIALS_FILE = os.getenv("GA4_CREDENTIALS_FILE", "/etc/secrets/ga4-credentials.json")
MIN_START = dt.date(2024, 1, 1)

def _ga4_client():
    with open(CREDENTIALS_FILE, "r") as f:
        info = json.load(f)
    creds = service_account.Credentials.from_service_account_info(info)
    return BetaAnalyticsDataClient(credentials=creds)

def _to_float(s: str):
    try: return float(s)
    except: return None

def _clamp_dates(start: str, end: str):
    s = max(dt.datetime.strptime(start, "%Y-%m-%d").date(), MIN_START)
    e_req = dt.datetime.strptime(end, "%Y-%m-%d").date()
    e = min(e_req, dt.date.today() - dt.timedelta(days=1))  # evita intradía
    if s > e:
        raise HTTPException(400, f"Rango inválido tras recorte: {s} > {e}")
    return s.isoformat(), e.isoformat()

@app.get("/exportar")
def exportar_datos(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str   = Query(..., description="YYYY-MM-DD"),
    page_size: int = Query(10000, ge=1, le=100000),
    max_pages: int = Query(200, ge=1, le=2000),
):
    try:
        start, end = _clamp_dates(start, end)
        client = _ga4_client()

        dims = [
            Dimension(name="date"), Dimension(name="country"), Dimension(name="city"),
            Dimension(name="deviceCategory"), Dimension(name="pagePath"),
            Dimension(name="sessionSource"), Dimension(name="sessionMedium"),
            Dimension(name="sessionCampaignName"),
        ]
        mets = [
            Metric(name="activeUsers"), Metric(name="newUsers"), Metric(name="sessions"),
            Metric(name="screenPageViews"), Metric(name="engagementRate"), Metric(name="bounceRate"),
            Metric(name="averageSessionDuration"), Metric(name="conversions"), Metric(name="totalRevenue"),
        ]

        req = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            date_ranges=[DateRange(start_date=start, end_date=end)],
            dimensions=dims, metrics=mets,
            order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))],
            limit=page_size, offset=0,
        )

        rows, pages, total = [], 0, None
        while True:
            resp = client.run_report(req)
            if total is None: total = resp.row_count
            if not resp.rows: break

            for r in resp.rows:
                item = {dims[i].name: r.dimension_values[i].value for i in range(len(dims))}
                item.update({mets[j].name: _to_float(r.metric_values[j].value) for j in range(len(mets))})
                rows.append(item)

            pages += 1
            if len(rows) >= total or pages >= max_pages: break
            req.offset += len(resp.rows)
            time.sleep(0.2)

        return JSONResponse(
            {"rows": rows, "rowCount": total or len(rows), "start": start, "end": end,
             "pages": pages, "truncated": (pages >= max_pages) or (total and len(rows) < total)},
            headers={"Cache-Control": "no-store"}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"GA4 export failed: {e}")
