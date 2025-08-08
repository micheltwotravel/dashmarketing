from fastapi import FastAPI, Query
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import requests
import json

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

@app.get("/ads")
def obtener_datos_ads():
    try:
        # Cargar el token generado
        with open("google_ads_token.json", "r") as f:
            token_data = json.load(f)

        credentials = Credentials.from_authorized_user_info(token_data)

        # Refrescar token si es necesario
        if credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())

        # Guardar el token refrescado
        with open("google_ads_token.json", "w") as f:
            f.write(credentials.to_json())

        access_token = credentials.token

        # Aquí haces la consulta al API de Google Ads (CAMBIA TU CUSTOMER_ID REAL)
        customer_id = "788685392081-lscsja3am8iqtrbvofd6e5lcucgml2lh.apps.googleusercontent.com"
        url = f"https://googleads.googleapis.com/v14/customers/{customer_id}/googleAds:search"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "developer-token": "bAkDdDbdaAGfhkMETHmHEA",
            "Content-Type": "application/json"
        }

        body = {
            "query": """
                SELECT
                  campaign.id,
                  campaign.name,
                  metrics.clicks,
                  metrics.impressions,
                  metrics.average_cpc,
                  metrics.cost_micros
                FROM campaign
                WHERE segments.date DURING LAST_30_DAYS
                LIMIT 20
            """
        }

        response = requests.post(url, headers=headers, json=body)
        return response.json()

    except Exception as e:
        return {"error": str(e)}

