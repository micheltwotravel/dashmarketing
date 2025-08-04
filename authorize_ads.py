from google_auth_oauthlib.flow import InstalledAppFlow
import json

# Carga tu archivo de OAuth
with open("client_secret.json") as f:
    secrets = json.load(f)["web"]

# Scopes necesarios para Google Ads API
scopes = ["https://www.googleapis.com/auth/adwords"]

flow = InstalledAppFlow.from_client_config(
    {"installed": secrets},
    scopes=scopes
)

# MOSTRARÁ UNA URL EN CONSOLA para que la copies y pegues
credentials = flow.run_console()

# Guarda el token generado
with open("google_ads_token.json", "w") as token_file:
    token_file.write(credentials.to_json())

print("✅ Refresh token generado y guardado.")
