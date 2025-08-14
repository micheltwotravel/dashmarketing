# main.py
import os
import json
import time
import logging
import datetime as dt
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse

from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    DateRange,
    Dimension,
    Metric,
    OrderBy,
)

# -----------------------------------------------------------------------------
# Configuración básica
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("dashmarketing")

app = FastAPI(title="Dash Marketing API", version="1.0.0")

# CORS (ajusta origins si necesitas restringir)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Variables de entorno / Defaults
PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "279889272")
CREDENTIALS_FILE = os.getenv("GA4_CREDENTIALS_FILE", "/etc/secrets/ga4-credentials.json")
MIN_START_DATE = dt.date(2024, 1, 1)  # Límite inferior: 2024-01-01

# -----------------------------------------------------------------------------
# Utilidades
# -----------------------------------------------------------------------------
def _ga4_client() -> BetaAnalyticsDataClient:
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(f"GA4 credentials not found at {CREDENTIALS_FILE}")
    with open(CREDENTIALS_FILE, "r") as fh:
        info = json.load(fh)
    creds = service_account.Credentials.from_service_account_info(info)
    return BetaAnalyticsDataClient(credentials=creds)

def _parse_date(s: str) -> dt.date:
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {s}. Expected YYYY-MM-DD")

def _clamp_dates(start: str, end: str) -> (str, str):
    """Recorta el rango a [2024-01-01, hoy-1] y valida."""
    s = max(_parse_date(start), MIN_START_DATE)
    e_req = _parse_date(end)
    e = min(e_req, dt.date.today() - dt.timedelta(days=1))
    if s > e:
        raise HTTPException(status_code=400, detail=f"Invalid range after clamp: {s} > {e}")
    return s.isoformat(), e.isoformat()

def _to_float(s: str) -> Optional[float]:
    try:
        return float(s)
    except Exception:
        return None

def _dims() -> List[Dimension]:
    return [
        Dimension(name="date"),
        Dimension(name="country"),
        Dimension(name="city"),
        Dimension(name="deviceCategory"),
        Dimension(name="pagePath"),
        Dimension(name="sessionSource"),
        Dimension(name="sessionMedium"),
        Dimension(name="sessionCampaignName"),
    ]

def _mets() -> List[Metric]:
    return [
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

def _rows_from_response(resp, dims: List[Dimension], mets: List[Metric]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for r in resp.rows:
        item = {dims[i].name: r.dimension_values[i].value for i in range(len(dims))}
        for j in range(len(mets)):
            item[mets[j].name] = _to_float(r.metric_values[j].value)
        rows.append(item)
    return rows

def _month_range_iter(start: dt.date, end: dt.date) -> List[dt.date]:
    """Genera fechas del primer día de mes desde start hasta end."""
    cur = start.replace(day=1)
    out = []
    while cur <= end:
        out.append(cur)
        # sumar 1 mes de manera segura
        year = cur.year + (cur.month // 12)
        month = (cur.month % 12) + 1
        cur = dt.date(year, month, 1)
    return out

# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------
@app.get("/", response_class=PlainTextResponse)
def root() -> str:
    return "Dash Marketing API is up. See /docs for OpenAPI."

@app.get("/health", response_class=PlainTextResponse)
def health() -> str:
    return "ok"

@app.get("/version")
def version():
    return {"version": app.version}

@app.get("/exportar")
def exportar_datos(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    page_size: int = Query(10000, ge=1, le=100000, description="Rows per page (GA4 page size)"),
    max_pages: int = Query(200, ge=1, le=2000, description="Safety cap to avoid infinite loops"),
):
    """
    Exporta datos GA4 entre start y end (recortado a [2024-01-01, hoy-1]) con paginación por offset.
    Devuelve:
      {
        "rows": [...],
        "rowCount": <int>,
        "start": "YYYY-MM-DD",
        "end": "YYYY-MM-DD",
        "pages": <int>,
        "truncated": <bool>
      }
    """
    start, end = _clamp_dates(start, end)
    log.info(f"/exportar start={start} end={end} page_size={page_size} max_pages={max_pages}")

    try:
        client = _ga4_client()
        dims = _dims()
        mets = _mets()

        req = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            date_ranges=[DateRange(start_date=start, end_date=end)],
            dimensions=dims,
            metrics=mets,
            order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))],
            limit=page_size,
            offset=0,
        )

        out_rows: List[Dict[str, Any]] = []
        pages = 0
        total: Optional[int] = None

        while True:
            resp = client.run_report(req)
            if total is None:
                total = getattr(resp, "row_count", None)

            batch = _rows_from_response(resp, dims, mets)
            if not batch:
                break

            out_rows.extend(batch)
            pages += 1

            # corte natural
            if total is not None and len(out_rows) >= total:
                break
            if pages >= max_pages:
                log.warning("Reached max_pages cap; response may be truncated.")
                break

            # avanzar offset
            req.offset += len(batch)

            # backoff suave para no golpear cuotas
            time.sleep(0.15)

        body = {
            "rows": out_rows,
            "rowCount": total if total is not None else len(out_rows),
            "start": start,
            "end": end,
            "pages": pages,
            "truncated": (pages >= max_pages) or (total is not None and len(out_rows) < total),
        }
        return JSONResponse(body, headers={"Cache-Control": "no-store"})
    except FileNotFoundError as e:
        log.exception("Credentials file not found.")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        log.exception("GA4 export failed.")
        raise HTTPException(status_code=500, detail=f"GA4 export failed: {e}")

@app.get("/exportar_mensual")
def exportar_mensual(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    page_size: int = Query(25000, ge=1, le=100000),
    sleep_ms: int = Query(150, ge=0, le=2000, description="Backoff entre llamadas (ms)"),
):
    """
    Variante que parte el rango por meses (reduce tamaño de cada respuesta GA4).
    Retorna mismo esquema que /exportar pero con 'buckets' mensuales.
    """
    s_iso, e_iso = _clamp_dates(start, end)
    s = _parse_date(s_iso)
    e = _parse_date(e_iso)
    log.info(f"/exportar_mensual start={s_iso} end={e_iso} page_size={page_size}")

    try:
        client = _ga4_client()
        dims = _dims()
        mets = _mets()

        all_rows: List[Dict[str, Any]] = []
        pages_total = 0
        months = _month_range_iter(s, e)

        for m0 in months:
            m_start = m0
            # último día del mes
            next_month_year = m0.year + (m0.month // 12)
            next_month = (m0.month % 12) + 1
            m_end = (dt.date(next_month_year, next_month, 1) - dt.timedelta(days=1))

            # recorta a end global
            if m_end > e:
                m_end = e
            if m_start < s:
                m_start = s
            if m_start > m_end:
                continue

            req = RunReportRequest(
                property=f"properties/{PROPERTY_ID}",
                date_ranges=[DateRange(start_date=m_start.isoformat(), end_date=m_end.isoformat())],
                dimensions=dims,
                metrics=mets,
                order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))],
                limit=page_size,
                offset=0,
            )

            while True:
                resp = client.run_report(req)
                batch = _rows_from_response(resp, dims, mets)
                if not batch:
                    break
                all_rows.extend(batch)
                pages_total += 1

                # cortar si ya llegamos al total esperado de ese mes
                total_month = getattr(resp, "row_count", None)
                if total_month is not None and len(batch) < page_size and req.offset + len(batch) >= total_month:
                    break

                req.offset += len(batch)
                if sleep_ms:
                    time.sleep(sleep_ms / 1000.0)

        body = {
            "rows": all_rows,
            "rowCount": len(all_rows),
            "start": s_iso,
            "end": e_iso,
            "pages": pages_total,
            "truncated": False,
        }
        return JSONResponse(body, headers={"Cache-Control": "no-store"})
    except FileNotFoundError as e:
        log.exception("Credentials file not found.")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        log.exception("GA4 monthly export failed.")
        raise HTTPException(status_code=500, detail=f"GA4 monthly export failed: {e}")

# -----------------------------------------------------------------------------
# Error handlers
# -----------------------------------------------------------------------------
@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})

@app.exception_handler(Exception)
async def unhandled_exception_handler(_: Request, exc: Exception):
    log.exception("Unhandled error")
    return JSONResponse(status_code=500, content={"error": str(exc)})

# -----------------------------------------------------------------------------
# Entry point (local)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    log.info(f"Starting server on {host}:{port}")
    uvicorn.run("main:app", host=host, port=port, reload=os.getenv("RELOAD", "false").lower() == "true")
