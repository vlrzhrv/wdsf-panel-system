import json, requests, time

SUPABASE_URL = "https://tvdrdmvnvnqfavpwtphk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InR2ZHJkbXZudm5xZmF2cHd0cGhrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDExODA4NzcsImV4cCI6MjA1Njc1Njg3N30.9tWGFR_OwmHqKLSwZ-k9PyGFuqkRcC9kHT6F2FEoiPQ"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal"
}

with open("jueces_extraidos.json", "r", encoding="utf-8") as f:
    data = json.load(f)

judges = data["judges"]
print(f"Importando {len(judges)} jueces a Supabase...\n")

inserted = 0
errors = 0

for j in judges:
    raw_discs = j.get("disciplines", [])
    mapped = []
    for d in raw_discs:
        dl = d.lower()
        if "standard" in dl:
            mapped.append("Standard")
        elif "latin" in dl:
            mapped.append("Latin")
        elif "ten dance" in dl:
            mapped.append("Combined")
    mapped = list(set(mapped))

    comps = j.get("competitions_judged", [])
    wch = sum(1 for c in comps if "WORLD CHAMPIONSHIP" in c["name"].upper() and "OPEN" not in c["name"].upper())
    gs  = sum(1 for c in comps if "GRAND SLAM" in c["name"].upper())
    cch = sum(1 for c in comps if "CONTINENTAL CHAMPIONSHIP" in c["name"].upper() or "EUROPEAN CHAMPIONSHIP" in c["name"].upper())

    record = {
        "wdsf_min":                          j.get("min"),
        "first_name":                        (j.get("first_name") or "")[:100],
        "last_name":                         (j.get("last_name") or "")[:100],
        "nationality":                       (j.get("nationality") or "")[:100],
        "representing":                      (j.get("representing") or "")[:100],
        "license_type":                      j.get("license_level"),
        "license_valid_until":               j.get("license_expires") or None,
        "disciplines":                       mapped if mapped else [],
        "judging_world_championships":       wch,
        "judging_grand_slams":               gs,
        "judging_continental_championships": cch,
        "active":                            j.get("license_status") == "Active"
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
            print(f"  ERROR [{j.get('min')}] {j.get('first_name')} {j.get('last_name')}: {r.status_code} {r.text[:100]}")
        if (inserted + errors) % 50 == 0:
            print(f"  Procesados: {inserted + errors}/{len(judges)} — OK:{inserted} ERR:{errors}")
        time.sleep(0.05)
    except Exception as e:
        errors += 1
        print(f"  EXCEPCION [{j.get('min')}]: {e}")

print(f"\n{'='*50}")
print(f"IMPORTACION COMPLETADA")
print(f"  Jueces importados : {inserted}")
print(f"  Errores           : {errors}")
print(f"\nAbre Supabase -> Table Editor -> judges")
print(f"Deberias ver {inserted} filas.")
