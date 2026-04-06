import json, sqlite3

DB = "/Users/valeriivanov/wdsf_app/wdsf_panel.db"
JSON = "/Users/valeriivanov/wdsf_app/jueces_extraidos.json"

conn = sqlite3.connect(DB)
c = conn.cursor()

# Crear tabla jueces
c.executescript("""
CREATE TABLE IF NOT EXISTS judges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wdsf_min INTEGER UNIQUE,
    first_name TEXT,
    last_name TEXT,
    nationality TEXT,
    representing TEXT,
    license_type TEXT,
    license_valid_until TEXT,
    disciplines TEXT,
    judging_world_championships INTEGER DEFAULT 0,
    judging_grand_slams INTEGER DEFAULT 0,
    judging_continental_championships INTEGER DEFAULT 0,
    active INTEGER DEFAULT 1,
    notes TEXT,
    career_level TEXT DEFAULT 'national',
    zone TEXT
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    date TEXT,
    location TEXT,
    country TEXT,
    discipline TEXT,
    age_group TEXT,
    division TEXT,
    event_type TEXT,
    is_ags INTEGER DEFAULT 0,
    coefficient REAL DEFAULT 1.0,
    status TEXT DEFAULT 'pending'
);

CREATE TABLE IF NOT EXISTS panel_assignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER,
    judge_id INTEGER,
    role TEXT DEFAULT 'adjudicator',
    position INTEGER,
    score REAL,
    status TEXT DEFAULT 'proposed',
    FOREIGN KEY(event_id) REFERENCES events(id),
    FOREIGN KEY(judge_id) REFERENCES judges(id)
);
""")

# Importar jueces
with open(JSON) as f:
    data = json.load(f)

judges = data["judges"]
inserted = errors = 0

for j in judges:
    raw_discs = j.get("disciplines", [])
    mapped = list(set(
        "Standard" if "standard" in d.lower() else
        "Latin" if "latin" in d.lower() else
        "Combined" if "ten dance" in d.lower() else "Other"
        for d in raw_discs
    ))
    mapped = [d for d in mapped if d != "Other" or not any(x in d.lower() for x in ["standard","latin","ten"])]

    comps = j.get("competitions_judged", [])
    wch = sum(1 for c in comps if "WORLD CHAMPIONSHIP" in c["name"].upper() and "OPEN" not in c["name"].upper())
    gs  = sum(1 for c in comps if "GRAND SLAM" in c["name"].upper())
    cch = sum(1 for c in comps if "CONTINENTAL" in c["name"].upper() or "EUROPEAN CHAMPIONSHIP" in c["name"].upper())

    try:
        c.execute("""
            INSERT OR REPLACE INTO judges
            (wdsf_min, first_name, last_name, nationality, representing,
             license_type, license_valid_until, disciplines,
             judging_world_championships, judging_grand_slams,
             judging_continental_championships, active)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            j.get("min"),
            (j.get("first_name") or "")[:100],
            (j.get("last_name") or "")[:100],
            (j.get("nationality") or "")[:100],
            (j.get("representing") or "")[:100],
            j.get("license_level"),
            j.get("license_expires"),
            ",".join(mapped) if mapped else "",
            wch, gs, cch,
            1 if j.get("license_status") == "Active" else 0
        ))
        inserted += 1
    except Exception as e:
        errors += 1
        print(f"ERROR {j.get('min')}: {e}")

conn.commit()
conn.close()

print(f"\n{'='*40}")
print(f"BD creada: {DB}")
print(f"Jueces importados : {inserted}")
print(f"Errores           : {errors}")
print(f"\nListo para arrancar el servidor.")
