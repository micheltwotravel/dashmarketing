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
# Cargar el cliente de Google Ads desde el archivo de configuración
def _ads_client() -> GoogleAdsClient:
    try:
        client = GoogleAdsClient.load_from_storage("/etc/secrets/google-ads.yaml")
        return client
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al cargar el cliente de Google Ads: {e}")

# Endpoint para verificar si las credenciales están funcionando
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

        # Consulta para obtener métricas de campañas
        query = f"""
        SELECT
            segments.date,
            campaign.id,
            campaign.name,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions,
            metrics.cost_micros
        FROM campaign
        WHERE segments.date BETWEEN '{start}' AND '{end}'
        ORDER BY segments.date, campaign.id
        """

        rows = []
        # Usar `search_stream` para obtener grandes volúmenes de datos
        response = ga_service.search_stream(customer_id=cid, query=query)

        for batch in response:
            for row in batch.results:
                rows.append({
                    "date": row.segments.date,
                    "campaign_id": row.campaign.id,
                    "campaign_name": row.campaign.name,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "conversions": row.metrics.conversions,
                    "cost": float(row.metrics.cost_micros) / 1_000_000.0,
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

# Endpoint para verificar la conexión con Google Ads
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

# Función de prueba para verificar que Google Ads Client se carga correctamente
def test_google_ads_client():
    try:
        client = _ads_client()
        print("Google Ads client loaded successfully")
    except Exception as e:
        print(f"Error loading Google Ads client: {e}")

test_google_ads_client()  # Prueba la carga del cliente
