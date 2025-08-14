from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, DateRange, Dimension, Metric, OrderBy
)
from google.oauth2 import service_account
import json
import os
import time

app = FastAPI()

# Config
PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "279889272")
CREDENTIALS_FILE = os.getenv("GA4_CREDENTIALS_FILE", "/etc/secrets/ga4-credentials.json")

# --- Utilidades ---
def _ga4_client() -> BetaAnalyticsDataClient:
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(f"GA4 credentials not found at {CREDENTIALS_FILE}")
    with open(CREDENTIALS_FILE, "r") as fh:
        info = json.load(fh)
    creds = service_account.Credentials.from_service_account_info(info)
    return BetaAnalyticsDataClient(credentials=creds)

def _to_float(s: str):
    # GA4 a veces retorna "" o "NaN"
    try:
        return float(s)
    except Exception:
        return None

# --- Endpoint con paginación ---
@app.get("/exportar")
def exportar_datos(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str   = Query(..., description="YYYY-MM-DD"),
    page_size: int = Query(10000, ge=1, le=100000, description="Filas por página (GA4 máx ~100k)"),
    max_pages: int = Query(200, ge=1, le=2000, description="Límite de páginas para evitar loops infinitos")
):
    """
    Exporta datos de GA4 con paginación por offset.
    Devuelve: {"rows":[...], "rowCount": <int>, "start":..., "end":..., "pages": <int>}
    """
    try:
        client = _ga4_client()

        dims = [
            Dimension(name="date"),
            Dimension(name="country"),
            Dimension(name="city"),
            Dimension(name="deviceCategory"),
            Dimension(name="pagePath"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium"),
            Dimension(name="sessionCampaignName"),
        ]
        mets = [
            Metric(name="activeUsers"),
            Metric(name="newUsers"),
            Metric(name="sessions"),
            Metric(name="screenPageViews"),
            Metric(name="engagementRate"),
            Metric(name="bounceRate"),
            Metric(name="averageSessionDuration"),
            Metric(name="conversions"),
            Metric(name="totalRevenue"),
        ]

        req = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            date_ranges=[DateRange(start_date=start, end_date=end)],
            dimensions=dims,
            metrics=mets,
            order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))],
            limit=page_size,
            offset=0,
        )

        out_rows = []
        pages = 0
        total = None

        while True:
            resp = client.run_report(req)

            if total is None:
                total = resp.row_count  # total esperado para todo el rango

            if not resp.rows:
                break

            for r in resp.rows:
                item = {}
                # dimensiones
                for i, d in enumerate(dims):
                    item[d.name] = r.dimension_values[i].value
                # métricas
                for j, m in enumerate(mets):
                    item[m.name] = _to_float(r.metric_values[j].value)
                out_rows.append(item)

            pages += 1
            if len(out_rows) >= total:
                break
            if pages >= max_pages:
                # Evita loops infinitos si la API reporta mal row_count
                break

            req.offset += len(resp.rows)
            # backoff suave por cuotas/latencias
            time.sleep(0.2)

        # Respuesta
        resp_body = {
            "rows": out_rows,
            "rowCount": total if total is not None else len(out_rows),
            "start": start,
            "end": end,
            "pages": pages,
            "truncated": (pages >= max_pages) or (total is not None and len(out_rows) < total)
        }
        return JSONResponse(resp_body, headers={"Cache-Control": "no-store"})
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        # Mensaje legible (útil para ver en Power Query cuando algo falla)
        raise HTTPException(status_code=500, detail=f"GA4 export failed: {e}")
