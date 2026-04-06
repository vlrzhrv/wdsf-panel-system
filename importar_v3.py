import json, requests, time, sys

SUPABASE_URL = "https://pdsonamtokeurxjezvrkk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBkc29uYW10b2tldXJ4amV6dnJrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI3MzI4NjksImV4cCI6MjA4ODMwODg2OX0.jED6LzBDZoUE_0Ilgi8QLI56vjjpbpbtz0f0kCRBagk"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal"
}

print("Probando conexion a Supabase...")
try:
    r = requests.get(f"{SUPABASE_URL}/rest/v1/judges?limit=1", headers=HEADERS, timeout=10)
    print(f"Conexion OK -> HTTP {r.status_code}")
    if r.status_code == 401:
        print("ERROR: Clave invalida.")
        sys.exit(1)
    elif r.status_code == 404:
        print("ERROR: Tabla 'judges' no existe. Ejecuta el SQL del Paso 1.")
        sys.exit(1)
except Exception as e:
    print(f"ERROR de conexion: {e}")
    sys.exit(1)

with open("jueces_extraidos.json", "r", encoding="utf-8") as f:
    data = json.load(f)

judges = data["judges"]
print(f"\nImportando {len(judges)} jueces...\n")

inserted = errors = 0

for j in judges:
    raw_discs = j.get("disciplines", [])
    mapped = list(set(
        "Standard" if "standard" in d.lower() else
        "Latin" if "latin" in d.lower() else
        "Combined" if "ten dance" in d.lower() else None
        for d in raw_discs
    ) - {None})

    comps = j.get("competitions_judged", [])
    wch = sum(1 for c in comps if "WORLD CHAMPIONSHIP" in c["name"].upper() and "OPEN" not in c["name"].upper())
    gs  = sum(1 for c in comps if "GRAND SLAM" in c["name"].upper())
    cch = sum(1 for c in comps if "CONTINENTAL" in c["name"].upper() or "EUROPEAN CHAMPIONSHIP" in c["name"].upper())

    record = {
        "wdsf_min":                      j.get("min"),
        "first_name":                    (j.get("first_name") or "")[:100],
        "last_name":                     (j.get("last_name") or "")[:100],
        "nationality":                   (j.get("nationality") or "")[:100],
        "representing":                  (j.get("representing") or "")[:100],
        "license_type":                  j.get("license_level"),
        "license_valid_until":           j.get("license_expires") or None,
        "disciplines":                   mapped if mapped else [],
        "judging_world_championships":   wch,
        "judging_grand_slams":           gs,
        "judging_continental_championships": cch,
        "active":                        j.get("license_status") == "Active"
    }

    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/judges",
            headers=HEADERS,
            json=record,
            timeout=15
        )
        if r.status_code in (200, 201):
            inserted += 1
        else:
            errors += 1
            print(f"  ERROR [{j.get('min')}] {j.get('first_name')} {j.get('last_name')}: {r.status_code} {r.text[:120]}")
        if (inserted + errors) % 50 == 0:
            print(f"  Procesados: {inserted + errors}/{len(judges)} — OK:{inserted} ERR:{errors}")
        time.sleep(0.05)
    except Exception as e:
        errors += 1
        print(f"  EXCEPCION [{j.get('min')}]: {e}")

print("\n" + "="*50)
print("IMPORTACION COMPLETADA")
print(f"  Importados : {inserted}")
print(f"  Errores    : {errors}")
print(f"\nAbre Supabase -> Table Editor -> judges")
print(f"Deberias ver {inserted} filas.")
