from fastapi import FastAPI, Query
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import requests
import json
import os
from fastapi import FastAPI, HTTPException, Request
from google_auth_oauthlib.flow import Flow
import yaml
from google.ads.googleads.errors import GoogleAdsException
import traceback
import time
import requests
from fastapi import HTTPException

app = FastAPI()

PROPERTY_ID = "279889272"
CREDENTIALS_FILE = "/etc/secrets/ga4-credentials.json"  # Asegúrate que esté bien montado en Render

@app.get("/exportar")
def exportar_datos(start: str = Query(...), end: str = Query(...)):
    try:
        # Leer credenciales del archivo secreto
        with open(CREDENTIALS_FILE, "r") as f:
            credentials_info = json.load(f)

        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        client = BetaAnalyticsDataClient(credentials=credentials)

        # Solicitud a GA4
        request = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            dimensions=[
                Dimension(name="date"),
                Dimension(name="country"),
                Dimension(name="city"),
                Dimension(name="deviceCategory"),
                Dimension(name="pagePath"),
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
                Dimension(name="sessionCampaignName"),
            ],
            metrics=[
                Metric(name="activeUsers"),
                Metric(name="newUsers"),
                Metric(name="sessions"),
                Metric(name="screenPageViews"),
                Metric(name="engagementRate"),
                Metric(name="bounceRate"),
                Metric(name="averageSessionDuration"),
                Metric(name="conversions"),
                Metric(name="totalRevenue")
            ],
            date_ranges=[DateRange(start_date=start, end_date=end)]
        )

        # Procesar respuesta
        response = client.run_report(request)

        # Extraer filas como diccionario
        rows = []
        for row in response.rows:
            row_data = {}
            for i, dim in enumerate(request.dimensions):
                row_data[dim.name] = row.dimension_values[i].value
            for j, met in enumerate(request.metrics):
                row_data[met.name] = float(row.metric_values[j].value)
            rows.append(row_data)

        return {"rows": rows}
    
    except Exception as e:
        return {"error": str(e)}

# --- Google Ads con SDK oficial (lee /etc/secrets/google-ads.yaml) ---
from google.ads.googleads.client import GoogleAdsClient
import datetime as dt
from fastapi import Query, HTTPException

def _valid_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()

def _ads_client() -> GoogleAdsClient:
    return GoogleAdsClient.load_from_storage("/etc/secrets/google-ads.yaml")

def _customer_id_from_yaml(path: str = "/etc/secrets/google-ads.yaml") -> str:
    # Lee client_customer_id (o login_customer_id) desde el YAML de Render
    with open(path, "r") as fh:
        cfg = yaml.safe_load(fh) or {}
    cid = str(
        cfg.get("client_customer_id")
        or cfg.get("login_customer_id")
        or ""
    ).strip()
    if not cid:
        # Error legible si falta el ID en el yaml
        raise HTTPException(400, "No se encontró client_customer_id/login_customer_id en google-ads.yaml")
    return cid.replace("-", "")
    
@app.get("/ads")
def ads_report(start: str = Query(...), end: str = Query(...)):
    try:
        client = _ads_client()
        ga_service = client.get_service("GoogleAdsService")
        cid = _customer_id_from_yaml()

        # Consulta extremadamente simple (ID de campaña y nombre)
        query = f"""
          SELECT
            campaign.id,
            campaign.name
          FROM campaign
          WHERE segments.date BETWEEN '{start}' AND '{end}'
          ORDER BY segments.date, campaign.id
        """

        rows = []
        # Usar search en lugar de search_stream para probar la consulta
        response = ga_service.search(customer_id=cid, query=query)

        for row in response:
            rows.append({
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
            })

        return {"ok": True, "rows": rows}

    except GoogleAdsException as ex:
        return {
            "ok": False,
            "type": "GoogleAdsException",
            "request_id": ex.request_id,
            "errors": [
                {"code": e.error_code.__class__.__name__, "message": e.message}
                for e in ex.failure.errors
            ],
        }, 400
    except Exception as e:
        return {"ok": False, "type": type(e).__name__, "message": str(e)}, 500


def build_flow(state: str | None = None):
    client_id = os.environ["GOOGLE_OAUTH_CLIENT_ID"]
    client_secret = os.environ["GOOGLE_OAUTH_CLIENT_SECRET"]
    redirect_uri = os.environ["GOOGLE_OAUTH_REDIRECT_URI"]
    SCOPES = ["https://www.googleapis.com/auth/adwords"]

    cfg = {
        "web": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [redirect_uri],
        }
    }
    return Flow.from_client_config(cfg, scopes=SCOPES, state=state)

from fastapi.responses import RedirectResponse

@app.get("/auth_ads")
def auth_ads():
    flow = build_flow()
    flow.redirect_uri = os.environ["GOOGLE_OAUTH_REDIRECT_URI"].strip()
    auth_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    # redirige directo a Google (no JSON)
    return RedirectResponse(auth_url)

@app.get("/callback_ads")
def callback_ads(request: Request, code: str, state: str | None = None):
    try:
        flow = build_flow(state)
        flow.redirect_uri = os.environ["GOOGLE_OAUTH_REDIRECT_URI"].strip()  # <-- strip aquí también
        flow.fetch_token(code=code)
        creds = flow.credentials
        return {
            "message": "✅ Copia este refresh_token y pégalo en google-ads.yaml",
            "refresh_token": creds.refresh_token,
            "scopes": list(creds.scopes or []),
        }
    except Exception as e:
        raise HTTPException(400, f"No se pudo completar OAuth: {e}")

@app.get("/ads/health")
def ads_health():
    try:
        # Intentar cargar el cliente de Google Ads
        from google.ads.googleads.client import GoogleAdsClient
        client = GoogleAdsClient.load_from_storage("/etc/secrets/google-ads.yaml")
        
        # Verificar si se carga correctamente
        ga_service = client.get_service("GoogleAdsService")
        customer_id = client.login_customer_id  # Obtener el login_customer_id desde las credenciales

        # Si el cliente y el servicio están correctamente configurados, debería llegar hasta aquí sin problemas
        return {"ok": True, "customer_id": customer_id}
    
    except Exception as e:
        # Devolver el error si no se puede cargar el cliente
        return {"ok": False, "error": str(e)}


@app.get("/ads/ping")
def ads_ping():
    try:
        client = GoogleAdsClient.load_from_storage("/etc/secrets/google-ads.yaml")
        svc = client.get_service("CustomerService")
        res = svc.list_accessible_customers()
        return {"ok": True, "resource_names": list(res.resource_names)}
    except GoogleAdsException as ex:
        return {
            "ok": False,
            "type": "GoogleAdsException",
            "request_id": ex.request_id,
            "errors": [
                {"code": e.error_code.__class__.__name__, "message": e.message}
                for e in ex.failure.errors
            ],
        }, 400
    except Exception as e:
        return {
            "ok": False,
            "type": type(e).__name__,
            "message": str(e),
            "trace": traceback.format_exc(),
        }, 500

from google.ads.googleads.client import GoogleAdsClient

def test_google_ads_client():
    try:
        client = GoogleAdsClient.load_from_storage("/etc/secrets/google-ads.yaml")
        print("Google Ads client loaded successfully")
    except Exception as e:
        print(f"Error loading Google Ads client: {e}")

test_google_ads_client()
