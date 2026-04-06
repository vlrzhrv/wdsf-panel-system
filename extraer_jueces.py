import requests, json, time, re
from requests.auth import HTTPBasicAuth

USER = "ValeriIvanov1"
PASS = "sjJ@M9Va7I"
BASE = "https://services.worlddancesport.org/api/1"

s = requests.Session()
s.auth = HTTPBasicAuth(USER, PASS)
s.headers.update({"Accept": "application/json"})

IMPORTANT_KEYWORDS = [
    "WORLD CHAMPIONSHIP", "GRAND SLAM", "EUROPEAN CHAMPIONSHIP",
    "CONTINENTAL CHAMPIONSHIP", "WORLD OPEN"
]

def get(url):
    time.sleep(0.4)
    try:
        r = s.get(url, timeout=30)
        if r.status_code == 200 and r.text.strip():
            return r.json()
    except Exception as e:
        print(f"  ERROR: {e}")
    return None

def parse_licenses(person_data):
    """Extrae licencias A/B/C por disciplina del perfil de persona"""
    result = {"level": None, "disciplines": [], "expires": None, "status": None}
    if not person_data or "licenses" not in person_data:
        return result
    for lic in person_data.get("licenses", []):
        if lic.get("type") == "Adjudicator" and lic.get("division") == "General":
            result["status"] = lic.get("status")
            result["expires"] = lic.get("expiresOn", "")[:10]
            levels_found = set()
            discs = []
            for d in lic.get("disciplines", []):
                match = re.search(r'^([\w\s]+)\s+\(([ABC])\)', d)
                if match:
                    disc_name = match.group(1).strip()
                    level = match.group(2)
                    levels_found.add(level)
                    if disc_name not in ["PD Latin","PD Standard","PD Ten Dance"]:
                        discs.append({"discipline": disc_name, "level": level})
            result["disciplines"] = discs
            if levels_found:
                result["level"] = sorted(levels_found)[0]  # A > B > C
    return result

def get_min_from_official(official):
    """Extrae el MIN del juez desde los links del oficial"""
    for link in official.get("link", []):
        if "person" in link.get("rel", ""):
            m = re.search(r'/person/(\d+)', link["href"])
            if m:
                return int(m.group(1))
    return None

# --- PASO 1: Obtener competiciones 2023-2025 ---
print("\n[1/4] Descargando lista de competiciones 2023-2025...")
competitions = []
for year in ["2023", "2024", "2025"]:
    url = f"{BASE}/competition?status=Closed&from={year}-01-01&to={year}-12-31"
    data = get(url)
    if data:
        competitions.extend(data)
        print(f"  {year}: {len(data)} competiciones")

print(f"  TOTAL: {len(competitions)} competiciones")

# --- PASO 2: Filtrar campeonatos importantes ---
print("\n[2/4] Filtrando campeonatos importantes...")
important = []
for c in competitions:
    name_upper = c.get("name", "").upper()
    if any(kw in name_upper for kw in IMPORTANT_KEYWORDS):
        important.append(c)

print(f"  Encontrados: {len(important)} campeonatos importantes")
for c in important[:10]:
    print(f"  - [{c['id']}] {c['name']}")
if len(important) > 10:
    print(f"  ... y {len(important)-10} más")

# --- PASO 3: Extraer jueces de cada competición ---
print("\n[3/4] Extrayendo jueces de cada competición...")
judges_db = {}  # min -> judge_data
comp_assignments = []  # lista de asignaciones

for i, comp in enumerate(important):
    comp_id = comp["id"]
    comp_name = comp["name"]
    print(f"  [{i+1}/{len(important)}] {comp_name[:60]}")

    officials = get(f"{BASE}/official?competitionId={comp_id}")
    if not officials:
        print(f"    Sin oficiales")
        continue

    adjudicators = [o for o in officials if "Adjudicator" in o.get("Name","")]
    print(f"    {len(adjudicators)} jueces encontrados")

    for off in adjudicators:
        min_id = get_min_from_official(off)
        if not min_id:
            continue

        comp_assignments.append({
            "min": min_id,
            "name": off.get("Name",""),
            "country": off.get("country",""),
            "competition_id": comp_id,
            "competition_name": comp_name
        })

        # Solo llamar a /person si no lo tenemos ya
        if min_id not in judges_db:
            person = get(f"{BASE}/person/{min_id}")
            if person:
                lic = parse_licenses(person)
                judges_db[min_id] = {
                    "min": min_id,
                    "first_name": person.get("name",""),
                    "last_name": person.get("surname",""),
                    "nationality": person.get("nationality",""),
                    "representing": person.get("country",""),
                    "year_of_birth": person.get("yearOfBirth"),
                    "license_level": lic["level"],
                    "license_status": lic["status"],
                    "license_expires": lic["expires"],
                    "disciplines": [d["discipline"] for d in lic["disciplines"]],
                    "competitions_judged": []
                }
            else:
                judges_db[min_id] = {
                    "min": min_id,
                    "first_name": off.get("Name",""),
                    "last_name": "",
                    "country": off.get("country",""),
                    "license_level": None,
                    "competitions_judged": []
                }

        judges_db[min_id]["competitions_judged"].append({
            "id": comp_id,
            "name": comp_name
        })

# --- PASO 4: Guardar resultados ---
print("\n[4/4] Guardando resultados...")
judges_list = list(judges_db.values())

# Estadísticas de licencias
lic_a = sum(1 for j in judges_list if j.get("license_level") == "A")
lic_b = sum(1 for j in judges_list if j.get("license_level") == "B")
lic_c = sum(1 for j in judges_list if j.get("license_level") == "C")
lic_none = sum(1 for j in judges_list if not j.get("license_level"))

output = {
    "extraction_date": "2026-03-05",
    "total_judges": len(judges_list),
    "license_A": lic_a,
    "license_B": lic_b,
    "license_C": lic_c,
    "license_unknown": lic_none,
    "total_competitions_analyzed": len(important),
    "judges": judges_list
}

with open("jueces_extraidos.json", "w", encoding="utf-8") as f:
    json.dump(output, f, indent=2, ensure_ascii=False)

print(f"\n{'='*50}")
print(f"EXTRACCION COMPLETADA")
print(f"  Jueces únicos encontrados: {len(judges_list)}")
print(f"  Licencia A: {lic_a}")
print(f"  Licencia B: {lic_b}")
print(f"  Licencia C: {lic_c}")
print(f"  Sin licencia detectada: {lic_none}")
print(f"  Competiciones analizadas: {len(important)}")
print(f"  Archivo guardado: jueces_extraidos.json")
