#!/usr/bin/env python3
"""
importar_eventos.py
───────────────────
Descarga competiciones de la WDSF (API + scraping web) y las guarda en la BD.
Filtra: Standard, Latin, Ten Dance — próximos 4 meses por defecto.

Uso:
    python3 importar_eventos.py
    python3 importar_eventos.py --desde 2026-04-01 --hasta 2026-07-31
    python3 importar_eventos.py --solo-listar
"""

import sqlite3, os, sys, argparse, re
import requests
from requests.auth import HTTPBasicAuth
from datetime import date, datetime

try:
    from bs4 import BeautifulSoup
    BS4_OK = True
except ImportError:
    BS4_OK = False

# ─── Configuración ────────────────────────────────────────────────────────────

DB_PATH   = os.path.expanduser("~/wdsf_app/wdsf_panel.db")
API_BASE  = "https://services.worlddancesport.org/api/1"
API_USER  = "ValeriIvanov1"
API_PASS  = "sjJ@M9Va7I"
BASE_WEB  = "https://www.worlddancesport.org"

DISC_KEYWORDS = ["standard", "latin", "ten dance", "ballroom"]

EVENT_TYPE_MAP = {
    "World Championship":        "WORLD CHAMPIONSHIP",
    "European Championship":     "EUROPEAN CHAMPIONSHIP",
    "Continental Championship":  "CONTINENTAL CHAMPIONSHIP",
    "Grand Slam":                "GRAND SLAM",
    "World Cup":                 "WORLD CUP",
    "Grand Prix":                "GRAND PRIX",
    "Open Series":               "OPEN SERIES",
    "WCH": "WORLD CHAMPIONSHIP",
    "ECH": "EUROPEAN CHAMPIONSHIP",
    "CCH": "CONTINENTAL CHAMPIONSHIP",
    "GS":  "GRAND SLAM",
    "WC":  "WORLD CUP",
    "GP":  "GRAND PRIX",
    "OS":  "OPEN SERIES",
}

COEF_MAP = {
    "WORLD CHAMPIONSHIP":        1.0,
    "EUROPEAN CHAMPIONSHIP":     0.95,
    "CONTINENTAL CHAMPIONSHIP":  0.95,
    "GRAND SLAM":                0.9,
    "WORLD CUP":                 0.85,
    "GRAND PRIX":                0.8,
    "OPEN SERIES":               0.75,
    "AUTHORIZED COMPETITION":    0.7,
}

# ─── Base de datos ────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_columns(conn):
    cols = {r[1] for r in conn.execute("PRAGMA table_info(events)")}
    for col, typedef in [("wdsf_id", "TEXT"), ("url", "TEXT")]:
        if col not in cols:
            conn.execute(f"ALTER TABLE events ADD COLUMN {col} {typedef}")
            print(f"  ✓ Columna '{col}' añadida a la tabla events")
    conn.commit()

def event_exists(conn, wdsf_id, name, ev_date):
    if wdsf_id:
        if conn.execute("SELECT 1 FROM events WHERE wdsf_id=?", (str(wdsf_id),)).fetchone():
            return True
    if conn.execute(
        "SELECT 1 FROM events WHERE date=? AND LOWER(TRIM(name))=LOWER(TRIM(?))",
        (ev_date, name)
    ).fetchone():
        return True
    return False

# ─── Helpers ─────────────────────────────────────────────────────────────────

def is_relevant(disc):
    return any(k in disc.lower() for k in DISC_KEYWORDS)

def normalize_disc(disc):
    d = disc.lower()
    if "ten" in d:      return "Ten Dance"
    if "standard" in d: return "Standard"
    if "latin" in d:    return "Latin"
    return disc.title()

def normalize_event_type(raw):
    for key, val in EVENT_TYPE_MAP.items():
        if key.lower() in raw.lower():
            return val
    return "AUTHORIZED COMPETITION"

def parse_date(val):
    if not val:
        return None
    if isinstance(val, (int, float)):
        try:
            return datetime.fromtimestamp(val / 1000).strftime("%Y-%m-%d")
        except:
            return None
    s = str(val).strip()
    # Primero: si empieza por YYYY-MM-DD lo tomamos directamente
    if len(s) >= 10 and re.match(r'\d{4}-\d{2}-\d{2}', s):
        return s[:10]
    # Intentar otros formatos comunes
    for fmt in ("%d %B %Y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(s[:19], fmt).strftime("%Y-%m-%d")
        except:
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except:
                pass
    return None

def get_str(obj, *keys):
    for k in keys:
        v = obj.get(k)
        if isinstance(v, dict):
            v = v.get("name") or v.get("code") or v.get("value")
        if v:
            return str(v).strip()
    return ""

def age_group_from_name(name):
    for ag in ("Junior", "U21", "Youth", "Senior II", "Senior III", "Senior IV",
               "Senior I", "Senior", "Master", "Adult"):
        if ag.lower() in name.lower():
            return ag
    return "Adult"

# ─── Fuente 1: API WDSF ───────────────────────────────────────────────────────

def fetch_via_api(date_from, date_to):
    session = requests.Session()
    session.auth = HTTPBasicAuth(API_USER, API_PASS)
    session.headers.update({"Accept": "application/json"})
    events = []

    # El API WDSF usa "from"/"to" como parámetros de fecha (confirmado en wdsf_api.py)
    strategies = [
        ("competition", {"from": date_from, "to": date_to, "status": "Upcoming"}),
        ("competition", {"from": date_from, "to": date_to, "status": "upcoming"}),
        ("competition", {"from": date_from, "to": date_to, "status": "Approved"}),
        ("competition", {"from": date_from, "to": date_to}),
        ("competition", {"dateFrom": date_from, "dateTo": date_to, "status": "Upcoming"}),
        ("competition/granting", {"from": date_from, "to": date_to}),
    ]

    for endpoint, params in strategies:
        url = f"{API_BASE}/{endpoint}"
        param_str = "&".join(f"{k}={v}" for k, v in params.items())
        print(f"  → /{endpoint}?{param_str} …", end=" ", flush=True)
        try:
            r = session.get(url, params=params, timeout=20)
            if r.status_code == 200:
                body = r.text.strip()
                if not body:
                    print("respuesta vacía")
                    continue
                data = r.json()
                items = []
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict):
                    for key in ("data", "competitions", "events", "items", "results", "content"):
                        if key in data and isinstance(data[key], list):
                            items = data[key]
                            break
                if items:
                    print(f"{len(items)} registros ✓")
                    events = items
                    break
                else:
                    print(f"vacío (keys: {list(data.keys()) if isinstance(data,dict) else type(data).__name__})")
            else:
                print(f"HTTP {r.status_code}")
        except Exception as e:
            print(f"error: {e}")

    parsed = []
    for comp in events:
        disc     = get_str(comp, "discipline", "disciplineName", "sportDiscipline", "category")
        if not is_relevant(disc):
            continue
        ev_date  = parse_date(comp.get("startDate") or comp.get("date") or comp.get("dateFrom"))
        if not ev_date or not (date_from <= ev_date <= date_to):
            continue

        name     = get_str(comp, "name", "title", "competitionName", "eventName") or "Competición WDSF"
        country  = get_str(comp, "country", "hostCountry")
        if not country:
            venue = comp.get("venue") or {}
            if isinstance(venue, dict):
                country = venue.get("country", "")
        city     = get_str(comp, "city", "place")
        if not city:
            venue = comp.get("venue") or {}
            if isinstance(venue, dict):
                city = venue.get("city", "")
        ev_type  = normalize_event_type(get_str(comp, "type", "eventType", "competitionType", "level"))
        wdsf_id  = get_str(comp, "id", "competitionId", "wdsfId", "competitionGuid")
        url      = get_str(comp, "url", "link", "href")

        parsed.append({
            "wdsf_id":    wdsf_id,
            "name":       name,
            "date":       ev_date,
            "location":   city,
            "country":    country,
            "discipline": normalize_disc(disc),
            "age_group":  age_group_from_name(name),
            "event_type": ev_type,
            "is_ags":     1 if "grand slam" in ev_type.lower() else 0,
            "coefficient": COEF_MAP.get(ev_type, 0.7),
            "url":        url,
        })

    return parsed

# ─── Fuente 2: Web scraping worlddancesport.org ───────────────────────────────

def fetch_via_web(date_from, date_to):
    if not BS4_OK:
        print("  ⚠ beautifulsoup4 no instalado. Instala con: pip3 install beautifulsoup4")
        return []

    print("  → Scraping worlddancesport.org/Events/Granting …", end=" ", flush=True)
    try:
        r = requests.get(
            f"{BASE_WEB}/Events/Granting",
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                    "AppleWebKit/537.36 Chrome/124.0 Safari/537.36"}
        )
        r.raise_for_status()
    except Exception as e:
        print(f"error: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    rows = soup.select("table tr")[1:]
    print(f"{len(rows)} filas en tabla")

    events = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue

        # Intentar múltiples layouts de columna
        if len(cols) >= 5:
            comp_type  = cols[0].get_text(strip=True)
            age_group  = cols[1].get_text(strip=True)
            discipline = cols[2].get_text(strip=True)
            date_str   = cols[3].get_text(strip=True)
            loc_td     = cols[4]
        else:
            comp_type  = cols[0].get_text(strip=True)
            discipline = cols[1].get_text(strip=True)
            date_str   = cols[2].get_text(strip=True)
            loc_td     = cols[3]
            age_group  = "Adult"

        if not is_relevant(discipline):
            continue

        ev_date = parse_date(date_str)
        if not ev_date or not (date_from <= ev_date <= date_to):
            continue

        loc_text = loc_td.get_text(strip=True)
        parts    = [p.strip() for p in loc_text.split(",")]
        city     = parts[0] if parts else ""
        country  = parts[-1] if len(parts) > 1 else ""
        link     = loc_td.find("a")
        url      = (BASE_WEB + link["href"]) if link and link.get("href") else ""

        ev_type = normalize_event_type(comp_type)
        name    = f"{comp_type} - {discipline} {age_group}".strip(" -")

        events.append({
            "wdsf_id":     None,
            "name":        name,
            "date":        ev_date,
            "location":    city,
            "country":     country,
            "discipline":  normalize_disc(discipline),
            "age_group":   age_group_from_name(age_group + " " + name),
            "event_type":  ev_type,
            "is_ags":      1 if "grand slam" in ev_type.lower() else 0,
            "coefficient": COEF_MAP.get(ev_type, 0.7),
            "url":         url,
        })

    return events

# ─── Inserción ────────────────────────────────────────────────────────────────

def insert_events(conn, events, dry_run=False):
    inserted = skipped = 0
    for e in events:
        if event_exists(conn, e["wdsf_id"], e["name"], e["date"]):
            skipped += 1
            print(f"  ↷ Ya existe: {e['date']} · {e['name'][:55]}")
            continue

        tag = "[DRY]" if dry_run else "  +"
        disc_str = e['discipline'][:10].ljust(10)
        type_str = e['event_type'][:25].ljust(25)
        print(f"{tag} {e['date']} | {disc_str} | {type_str} | {e['name'][:48]} [{e['country']}]")

        if not dry_run:
            conn.execute("""
                INSERT INTO events
                  (wdsf_id, name, date, location, country, discipline,
                   age_group, division, event_type, is_ags, coefficient, url, status)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'pending')
            """, (
                e["wdsf_id"], e["name"], e["date"], e["location"], e["country"],
                e["discipline"], e["age_group"], "General", e["event_type"],
                e["is_ags"], e["coefficient"], e.get("url", "")
            ))
            inserted += 1

    if not dry_run:
        conn.commit()
    return inserted, skipped

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    today     = date.today()
    # Por defecto: desde hoy hasta 4 meses adelante
    default_from = today.strftime("%Y-%m-%d")
    default_to   = date(today.year + (1 if today.month > 8 else 0),
                        (today.month + 4 - 1) % 12 + 1, 28).strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(description="Importar eventos WDSF")
    parser.add_argument("--desde",       default=default_from, help="Fecha inicio YYYY-MM-DD")
    parser.add_argument("--hasta",       default=default_to,   help="Fecha fin YYYY-MM-DD")
    parser.add_argument("--solo-listar", action="store_true",  help="No insertar, solo listar")
    args = parser.parse_args()

    print("\n" + "═"*70)
    print("  WDSF Panel — Importador de Eventos")
    print(f"  Rango:  {args.desde}  →  {args.hasta}")
    print(f"  BD:     {DB_PATH}")
    if args.solo_listar:
        print("  MODO:   Solo listar (sin insertar)")
    print("═"*70 + "\n")

    if not os.path.exists(DB_PATH):
        print(f"✗ Base de datos no encontrada: {DB_PATH}")
        sys.exit(1)

    conn = get_db()
    if not args.solo_listar:
        ensure_columns(conn)

    # ── Intento 1: API WDSF ──────────────────────────────────────────────────
    print("[ Fuente 1: API WDSF ]")
    events = fetch_via_api(args.desde, args.hasta)

    # ── Intento 2: Web scraping (fallback) ───────────────────────────────────
    if not events:
        print("\n[ Fuente 2: Web scraping worlddancesport.org ]")
        events = fetch_via_web(args.desde, args.hasta)

    if not events:
        print("\n⚠  No se encontraron eventos.")
        print("   Comprueba tu conexión a internet y las credenciales WDSF.")
        conn.close()
        sys.exit(1)

    print(f"\n{len(events)} eventos relevantes encontrados. Procesando...\n")

    inserted, skipped = insert_events(conn, events, dry_run=args.solo_listar)

    print("\n" + "─"*70)
    print(f"  Importados:   {inserted}")
    print(f"  Ya existían:  {skipped}")
    print("─"*70)

    if inserted > 0:
        print(f"\n✓ {inserted} eventos nuevos guardados.")
        print("  Abre http://127.0.0.1:5001 → pestaña Eventos")
        print("  o ejecuta: python3 asignar_todos.py  para asignar paneles en lote")
    elif not args.solo_listar:
        print("\n  No hubo eventos nuevos para importar.")

    conn.close()


if __name__ == "__main__":
    main()
