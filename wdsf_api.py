
import requests
import json
from requests.auth import HTTPBasicAuth

WDSF_USER     = "ValeriIvanov1"
WDSF_PASSWORD = "sjJ@M9Va7I"
BASE_URL      = "https://services.worlddancesport.org/api/1"

session = requests.Session()
session.auth = HTTPBasicAuth(WDSF_USER, WDSF_PASSWORD)
session.headers.update({"Accept": "application/json"})

def test(endpoint):
    url = f"{BASE_URL}{endpoint}"
    r = session.get(url, timeout=30)
    print(f"\n{'='*50}")
    print(f"Endpoint: {endpoint}")
    print(f"Status: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(json.dumps(data if not isinstance(data,list) else data[:3], indent=2, ensure_ascii=False)[:1000])
    else:
        print(f"Error: {r.text[:300]}")

print("\n WDSF API - TEST DE CONEXION")
test("/country")
test("/age")
test("/ranking?discipline=Standard&agegroup=Adult")
test("/competition?status=Closed&from=2024-01-01&to=2024-12-31")
