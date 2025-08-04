from google_auth_oauthlib.flow import InstalledAppFlow
import json

# Carga tu archivo de OAuth descargado
with open("client_secret.json") as f:
    secrets = json.load(f)["web"]

# Scopes necesarios para Google Ads API
scopes = ["https://www.googleapis.com/auth/adwords"]

flow = InstalledAppFlow.from_client_config(
    {"installed": secrets},
    scopes=scopes
)

# Abre navegador para login
credentials = flow.run_local_server(port=8080)

# Guarda el token (lo necesitarás para hacer peticiones)
with open("google_ads_token.json", "w") as token_file:
    token_file.write(credentials.to_json())

print("✅ Refresh token generado y guardado.")
