import requests, json
from requests.auth import HTTPBasicAuth

s = requests.Session()
s.auth = HTTPBasicAuth("ValeriIvanov1", "sjJ@M9Va7I")
s.headers.update({"Accept": "application/json"})
BASE = "https://services.worlddancesport.org/api/1"

def test(label, url):
    print(f"\n{'='*60}\nTEST: {label}")
    r = s.get(url, timeout=30)
    print(f"Status: {r.status_code}")
    if r.status_code == 200 and r.text.strip():
        d = r.json()
        print(json.dumps(d[:5] if isinstance(d,list) else d, indent=2, ensure_ascii=False)[:3000])
    else:
        print(r.text[:300])

# Perfil de juez real (MIN de Rade Janjic)
test("Perfil juez Rade Janjic (MIN 10066914)",
     f"{BASE}/person/10066914")

# Todos los oficiales de Madrid 60131
test("Lista COMPLETA oficiales Madrid 60131",
     f"{BASE}/official?competitionId=60131")

# Ranking Latin Adult
test("Ranking Latin Adult General",
     f"{BASE}/ranking?ageGroup=Adult&discipline=Latin&division=General")

# Buscar campeonatos mundiales 2024-2025
test("Campeonatos importantes 2024-2025",
     f"{BASE}/competition?status=Closed&from=2024-01-01&to=2025-12-31&type=WORLD+CHAMPIONSHIP")

