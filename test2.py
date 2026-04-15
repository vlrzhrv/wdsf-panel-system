import requests, json
from requests.auth import HTTPBasicAuth

USER = "ValeriIvanov1"
PASS = "sjJ@M9Va7I"
BASE = "https://services.worlddancesport.org/api/1"

s = requests.Session()
s.auth = HTTPBasicAuth(USER, PASS)

def test(label, url, headers=None):
    h = {"Accept": "application/json"}
    if headers: h.update(headers)
    r = s.get(url, headers=h, timeout=30)
    print(f"\n{'='*60}")
    print(f"TEST: {label}")
    print(f"Status: {r.status_code}")
    if r.status_code == 200:
        d = r.json()
        print(json.dumps(d[:3] if isinstance(d, list) else d, indent=2, ensure_ascii=False)[:1500])
    else:
        print(f"Error: {r.text[:300]}")

# 1. Countries con header correcto
test("Countries (con header completo)",
     f"{BASE}/country",
     {"Accept": "application/vnd.worlddancesport.countries+json"})

# 2. Ranking con division=General
test("Ranking Standard Adult General",
     f"{BASE}/ranking?ageGroup=Adult&discipline=Standard&division=General")

# 3. Oficiales de una competicion real (Madrid 2024)
test("Oficiales competicion 60131 (Madrid Standard Adult)",
     f"{BASE}/official?competitionId=60131")

# 4. Persona especifica (primer oficial que aparezca)
test("Person endpoint",
     f"{BASE}/person/2000001")

# 5. Participantes competicion
test("Participantes competicion 60131",
     f"{BASE}/participant?competitionId=60131")

