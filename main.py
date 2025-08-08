from fastapi import FastAPI, Query
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.ads.googleads.client import GoogleAdsClient
from fastapi.responses import JSONResponse

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


# Ruta para cargar el cliente de Google Ads
def _ads_client() -> GoogleAdsClient:
    return GoogleAdsClient.load_from_storage("/etc/secrets/google-ads.yaml")

# Función para verificar las credenciales de Google Ads
@app.get("/ads/health")
def ads_health():
    try:
        # Intentar cargar el cliente de Google Ads
        client = _ads_client()

        # Obtener el servicio de Google Ads
        ga_service = client.get_service("GoogleAdsService")
        customer_id = client.login_customer_id  # Obtener el login_customer_id desde las credenciales

        # Si el cliente y el servicio están correctamente configurados, debería llegar hasta aquí sin problemas
        return JSONResponse(content={"ok": True, "customer_id": customer_id})
    
    except Exception as e:
        # Devolver el error si no se puede cargar el cliente
        return JSONResponse(content={"ok": False, "error": str(e)})

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

        return JSONResponse(content={"ok": True, "rows": rows})

    except Exception as e:
        return JSONResponse(content={"ok": False, "error": str(e)}, status_code=500)

# Endpoint para hacer ping y verificar la conexión con el servicio de Google Ads
@app.get("/ads/ping")
def ads_ping():
    try:
        # Intentar cargar el cliente de Google Ads
        client = _ads_client()
        svc = client.get_service("CustomerService")
        res = svc.list_accessible_customers()  # Listar clientes accesibles
        return JSONResponse(content={"ok": True, "resource_names": list(res.resource_names)})
    except Exception as e:
        return JSONResponse(content={"ok": False, "error": str(e)}, status_code=500)

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

def verify_refresh_token():
    try:
        credentials = Credentials.from_authorized_user_file('/etc/secrets/google-ads.yaml')
        
        if credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
            print("Acceso autorizado con nuevo access_token")
            return credentials.token

        else:
            print("Las credenciales son válidas, acceso permitido.")
            return credentials.token

    except Exception as e:
        print(f"Error al verificar las credenciales: {e}")
        return None


# Verificar el refresh_token
@app.get("/ads/verify_token")
def verify_token():
    token = verify_refresh_token()
    if token:
        return JSONResponse(content={"ok": True, "token": token})
    else:
        return JSONResponse(content={"ok": False, "message": "Las credenciales no son válidas."})

# Verificación de Google Ads Client al iniciar
def test_google_ads_client():
    try:
        client = _ads_client()
        print("Google Ads client loaded successfully")
    except Exception as e:
        print(f"Error loading Google Ads client: {e}")

test_google_ads_client()  # Prueba la carga del cliente


     for row in response:
    print(f"Campaign ID: {row.campaign.id}, Campaign Name: {row.campaign.name}")


# Usa el access token y client_id
get_campaigns("7603762609", access_token)
