from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Metric, Dimension, RunReportRequest
from google.oauth2 import service_account
import pandas as pd

# 🔐 Ruta a tu JSON
KEY_PATH = "ruta/a/tu/archivo.json"  # <-- cámbiala por la ruta real
PROPERTY_ID = "279889272"

# 🗓️ Rango de fechas que quieres
start_date = "2024-07-01"
end_date = "2024-07-31"

# 🔑 Cargar credenciales
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = BetaAnalyticsDataClient(credentials=credentials)

# 📊 Solicitud de reporte
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
    date_ranges=[DateRange(start_date=start_date, end_date=end_date)]
)

response = client.run_report(request)

# 📄 Convertir a DataFrame
rows = []
for row in response.rows:
    rows.append({
        "date": row.dimension_values[0].value,
        "country": row.dimension_values[1].value,
        "pagePath": row.dimension_values[2].value,
        "users": int(row.metric_values[0].value),
        "sessions": int(row.metric_values[1].value),
        "pageviews": int(row.metric_values[2].value)
    })

df = pd.DataFrame(rows)

# 💾 Guardar CSV
df.to_csv("datos_marketing_julio2024.csv", index=False)
print("✅ ¡Datos guardados en datos_marketing_julio2024.csv!")
