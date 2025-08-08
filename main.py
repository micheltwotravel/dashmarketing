from fastapi import FastAPI, Query
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.ads.googleads.client import GoogleAdsClient

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
        
from fastapi import FastAPI, Query, HTTPException
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import traceback
import yaml
import os
from google.auth.transport.requests import Request
from google.auth.credentials import Credentials

app = FastAPI()

# Cargar cliente de Google Ads desde archivo yaml
def _ads_client() -> GoogleAdsClient:
    return GoogleAdsClient.load_from_storage("/etc/secrets/google-ads.yaml")

# Función para verificar que las credenciales de Google Ads funcionan correctamente
@app.get("/ads/health")
def ads_health():
    try:
        # Intentar cargar el cliente de Google Ads
        client = _ads_client()

        # Obtener el servicio de Google Ads
        ga_service = client.get_service("GoogleAdsService")
        customer_id = client.login_customer_id  # Obtener el login_customer_id desde las credenciales

        # Si el cliente y el servicio están correctamente configurados, debería llegar hasta aquí sin problemas
        return {"ok": True, "customer_id": customer_id}
    
    except Exception as e:
        # Devolver el error si no se puede cargar el cliente
        return {"ok": False, "error": str(e)}

# Endpoint para consultar campañas y métricas en un rango de fechas
@app.get("/ads")
def ads_report(start: str = Query(...), end: str = Query(...)):
    try:
        client = _ads_client()
        ga_service = client.get_service("GoogleAdsService")
        cid = client.login_customer_id  # Obtener el client_customer_id desde las credenciales

        # Consulta para obtener ID y nombre de las campañas
        query = f"""
        SELECT
            campaign.id,
            campaign.name
        FROM campaign
        WHERE segments.date BETWEEN '{start}' AND '{end}'
        ORDER BY campaign.id
        LIMIT 10
        """

        rows = []
        # Usar `search` en lugar de `search_stream`
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

# Endpoint para hacer ping y verificar la conexión con el servicio de Google Ads
@app.get("/ads/ping")
def ads_ping():
    try:
        # Intentar cargar el cliente de Google Ads
        client = _ads_client()
        svc = client.get_service("CustomerService")
        res = svc.list_accessible_customers()  # Listar clientes accesibles
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

# Endpoint para realizar la autenticación de OAuth
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

# Endpoint para redirigir a la página de Google OAuth
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
    # redirige directo a Google
    return RedirectResponse(auth_url)

# Endpoint para recibir el código de autorización y completar el flujo OAuth
@app.get("/callback_ads")
def callback_ads(request: Request, code: str, state: str | None = None):
    try:
        flow = build_flow(state)
        flow.redirect_uri = os.environ["GOOGLE_OAUTH_REDIRECT_URI"].strip()
        flow.fetch_token(code=code)
        creds = flow.credentials
        return {
            "message": "✅ Copia este refresh_token y pégalo en google-ads.yaml",
            "refresh_token": creds.refresh_token,
            "scopes": list(creds.scopes or []),
        }
    except Exception as e:
        raise HTTPException(400, f"No se pudo completar OAuth: {e}")

# Verificar las credenciales de Google Ads
def verify_credentials():
    try:
        # Cargar las credenciales desde el archivo google-ads.yaml
        client = GoogleAdsClient.load_from_storage("/etc/secrets/google-ads.yaml")
        ga_service = client.get_service("GoogleAdsService")
        
        # Realizar una consulta simple
        query = "SELECT campaign.id, campaign.name FROM campaign LIMIT 10"
        response = ga_service.search(customer_id="7603762609", query=query)  # Asegúrate de usar el client_customer_id correcto
        
        # Si la consulta se ejecuta correctamente, la conexión fue exitosa
        for row in response:
            print(f"Campaign ID: {row.campaign.id}, Campaign Name: {row.campaign.name}")
        print("Conexión exitosa a Google Ads")

    except Exception as e:
        print(f"Error al conectar: {e}")

# Verificar las credenciales
verify_credentials()
