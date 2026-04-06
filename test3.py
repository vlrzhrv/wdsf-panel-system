import requests, json
from requests.auth import HTTPBasicAuth

USER = "ValeriIvanov1"
PASS = "sjJ@M9Va7I"
BASE = "https://services.worlddancesport.org/api/1"

s = requests.Session()
s.auth = HTTPBasicAuth(USER, PASS)
s.headers.update({"Accept": "application/json"})

def test(label, url):
    print(f"\n{'='*60}")
    print(f"TEST: {label}")
    try:
        r = s.get(url, timeout=30)
        print(f"Status: {r.status_code}")
        print(f"Content-Type: {r.headers.get('Content-Type','?')}")
        print(f"Body length: {len(r.text)} chars")
        if r.status_code == 200 and r.text.strip():
            try:
                d = r.json()
                print(json.dumps(d[:3] if isinstance(d,list) else d, indent=2, ensure_ascii=False)[:2000])
            except:
                print(f"RAW (no JSON): {r.text[:500]}")
        else:
            print(f"Error/Empty: {r.text[:300]}")
    except Exception as e:
        print(f"EXCEPTION: {e}")

test("Countries",                  f"{BASE}/country")
test("Ranking Std Adult General",  f"{BASE}/ranking?ageGroup=Adult&discipline=Standard&division=General")
test("Oficiales Madrid 60131",     f"{BASE}/official?competitionId=60131")
test("Person MIN 2000001",         f"{BASE}/person/2000001")
test("Participantes 60131",        f"{BASE}/participant?competitionId=60131")
test("Competicion detalle 60131",  f"{BASE}/competition/60131")

