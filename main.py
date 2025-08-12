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


