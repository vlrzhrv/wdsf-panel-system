"""
sincronizar_nominados.py
Scrapes https://www.worlddancesport.org/NominatedOfficials every day,
finds Standard / Latin / Ten Dance events, extracts the adjudicator panels
and nominated officials, matches them to judges in the local DB, and
updates the official_nominations table.

Run manually:  python3 sincronizar_nominados.py
Run via cron:  0 7 * * * cd ~/wdsf_app && python3 sincronizar_nominados.py >> sync.log 2>&1
"""

import sqlite3, os, re, time, sys
from datetime import datetime, date
import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────
APP_DIR  = os.path.dirname(os.path.abspath(__file__))
BUNDLE   = os.path.join(APP_DIR, "wdsf_panel.db")
LOCAL    = os.path.expanduser("~/wdsf_app/wdsf_panel.db")
DB       = BUNDLE if os.path.exists(BUNDLE) else LOCAL

BASE_URL = "https://www.worlddancesport.org"
NOMINAT  = f"{BASE_URL}/NominatedOfficials"
NOW      = datetime.utcnow().isoformat()

# Disciplinas ballroom que nos interesan (case-insensitive match en nombre del evento)
BALLROOM_KW = ["standard", "latin", "ten dance", "ten-dance", "10dance"]

HEADERS  = {"User-Agent": "Mozilla/5.0 (compatible; WDSFPanelBot/1.0)"}
DELAY    = 0.8   # segundos entre peticiones

# ── Helpers ───────────────────────────────────────────────────────────────────

def get(url, retries=3):
    for attempt in range(retries):
        try:
            time.sleep(DELAY)
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200:
                return r.text
            print(f"  HTTP {r.status_code}: {url}")
        except Exception as e:
            print(f"  Error ({attempt+1}/{retries}): {e}")
    return None

def parse_date(text):
    """'3 April 2026' → '2026-04-03'"""
    try:
        return datetime.strptime(text.strip(), "%d %B %Y").strftime("%Y-%m-%d")
    except Exception:
        return text.strip()

def is_ballroom(event_name):
    name = event_name.lower()
    # Excluir hip hop, disco, stage, breaking, etc.
    for exc in ["hip hop", "disco", "stage", "breaking", "choreogr", "jazz",
                "contemporary", "rock", "caribbean", "show dance", "formation"]:
        if exc in name:
            return False
    for kw in BALLROOM_KW:
        if kw in name:
            return True
    return False

def detect_discipline(event_name, url):
    n = (event_name + " " + url).lower()
    if "ten dance" in n or "ten-dance" in n or "10dance" in n:
        return "Ten Dance"
    if "standard" in n:
        return "Standard"
    if "latin" in n:
        return "Latin"
    return None

def extract_comp_id(url):
    m = re.search(r"-(\d+)$", url)
    return int(m.group(1)) if m else None

def normalize_name(s):
    return " ".join(s.strip().split()).upper()

# ── DB helpers ────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_nominations_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS official_nominations (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            wdsf_comp_id    INTEGER NOT NULL,
            comp_name       TEXT,
            comp_date       TEXT,
            comp_discipline TEXT,
            comp_location   TEXT,
            comp_url        TEXT,
            judge_name      TEXT,
            judge_country   TEXT,
            judge_id        INTEGER REFERENCES judges(id),
            role            TEXT,
            status          TEXT,
            section         TEXT,
            position        TEXT,
            synced_at       TEXT,
            UNIQUE(wdsf_comp_id, judge_name, section)
        )
    """)
    conn.commit()

def find_judge(conn, full_name, country_code):
    """
    Intenta emparejar nombre + país con un juez de la BD.
    Estrategias: 'Apellido, Nombre', 'Nombre Apellido', solo apellido único.
    """
    parts = full_name.strip().split()
    if len(parts) < 2:
        return None

    country_map = {
        "FRA":"France","BEL":"Belgium","CZE":"Czechia","ESP":"Spain",
        "POR":"Portugal","ITA":"Italy","AUT":"Austria","LTU":"Lithuania",
        "ROU":"Romania","NOR":"Norway","GER":"Germany","POL":"Poland",
        "DEN":"Denmark","GBR":"United Kingdom","NED":"Netherlands","HUN":"Hungary",
        "SVK":"Slovakia","SLO":"Slovenia","CRO":"Croatia","BUL":"Bulgaria",
        "LAT":"Latvia","EST":"Estonia","UKR":"Ukraine","RUS":"Russia",
        "BLR":"Belarus","SWE":"Sweden","FIN":"Finland","SUI":"Switzerland",
        "TUR":"Türkiye","GRE":"Greece","SRB":"Serbia","MKD":"Macedonia",
        "MDA":"Moldova","GEO":"Georgia","AZE":"Azerbaijan","ARM":"Armenia",
        "CHN":"China","JPN":"Japan","KOR":"Korea","AUS":"Australia",
        "USA":"United States","CAN":"Canada","BRA":"Brazil","ARG":"Argentina",
        "MEX":"Mexico","ISR":"Israel",
    }
    country_full = country_map.get(country_code.upper(), country_code)

    last  = parts[-1]
    first = parts[0]

    # Probar primero apellido+nombre con el país
    for ln, fn in [(last, first), (first, last)]:
        rows = conn.execute(
            "SELECT id FROM judges WHERE UPPER(last_name)=? AND UPPER(first_name) LIKE ? "
            "AND (UPPER(representing)=? OR UPPER(nationality)=?)",
            (ln.upper(), fn.upper() + "%", country_full.upper(), country_full.upper())
        ).fetchall()
        if len(rows) == 1:
            return rows[0]["id"]

    # Solo apellido + país (si es único)
    for ln in [last, first]:
        rows = conn.execute(
            "SELECT id FROM judges WHERE UPPER(last_name)=? "
            "AND (UPPER(representing)=? OR UPPER(nationality)=?)",
            (ln.upper(), country_full.upper(), country_full.upper())
        ).fetchall()
        if len(rows) == 1:
            return rows[0]["id"]

    return None

def upsert_nomination(conn, comp_id, comp_name, comp_date, discipline,
                      location, comp_url, name, country, judge_id,
                      role, status, section, position):
    conn.execute("""
        INSERT INTO official_nominations
            (wdsf_comp_id, comp_name, comp_date, comp_discipline, comp_location, comp_url,
             judge_name, judge_country, judge_id, role, status, section, position, synced_at)
        VALUES (?,?,?,?,?,?, ?,?,?,?,?,?,?,?)
        ON CONFLICT(wdsf_comp_id, judge_name, section) DO UPDATE SET
            judge_id=excluded.judge_id,
            status=excluded.status,
            position=excluded.position,
            synced_at=excluded.synced_at
    """, (comp_id, comp_name, comp_date, discipline, location, comp_url,
          name, country, judge_id, role, status, section, position, NOW))

# ── Scrapers ──────────────────────────────────────────────────────────────────

def scrape_nominated_list():
    """
    Devuelve lista de dicts:
        {date, name, location, url, comp_id, discipline}
    Solo para eventos Standard / Latin / Ten Dance.
    """
    html = get(NOMINAT)
    if not html:
        print("ERROR: no se pudo descargar NominatedOfficials")
        return []

    soup = BeautifulSoup(html, "html.parser")
    results = []

    for row in soup.select("table tr"):
        cols = row.find_all(["td","th"])
        if len(cols) < 3:
            continue
        link = cols[1].find("a")
        if not link:
            continue
        event_name = link.get_text(strip=True)
        if not is_ballroom(event_name):
            continue
        href       = link.get("href","")
        comp_id    = extract_comp_id(href)
        discipline = detect_discipline(event_name, href)
        if not comp_id or not discipline:
            continue

        results.append({
            "date":       parse_date(cols[0].get_text(strip=True)),
            "name":       event_name,
            "location":   cols[2].get_text(strip=True),
            "url":        BASE_URL + href,
            "comp_id":    comp_id,
            "discipline": discipline,
        })

    return results


def scrape_officials_page(url):
    """
    Returns:
        adjudicators  — list of {name, country, position, status='confirmed'}
        nominated     — list of {name, country, role, status}
    """
    html = get(url)
    if not html:
        return [], []

    soup = BeautifulSoup(html, "html.parser")
    adjudicators = []
    nominated    = []

    # Find all <h2>/<h3> headings to locate sections
    current_section = None
    for el in soup.find_all(["h2","h3","h4","table"]):
        if el.name in ("h2","h3","h4"):
            heading = el.get_text(strip=True).lower()
            if "adjudicator" in heading:
                current_section = "adjudicators"
            elif "nominated" in heading:
                current_section = "nominated"
            elif "chairperson" in heading or "scrutin" in heading:
                current_section = None
            continue

        if el.name == "table" and current_section in ("adjudicators","nominated"):
            for row in el.find_all("tr"):
                cells = [c.get_text(strip=True) for c in row.find_all(["td","th"])]
                if len(cells) < 2 or cells[0].lower() in ("name",""):
                    continue
                name    = cells[0]
                country = cells[1] if len(cells) > 1 else ""

                if current_section == "adjudicators":
                    position = cells[2] if len(cells) > 2 else ""
                    adjudicators.append({
                        "name": name, "country": country,
                        "position": position, "status": "confirmed"
                    })
                else:
                    role   = cells[2] if len(cells) > 2 else "Adjudicator"
                    status = cells[3] if len(cells) > 3 else "Nominated"
                    nominated.append({
                        "name": name, "country": country,
                        "role": role, "status": status, "position": ""
                    })

    return adjudicators, nominated


# ── Main ──────────────────────────────────────────────────────────────────────

def sync():
    conn = get_db()
    ensure_nominations_table(conn)

    print(f"\n{'='*60}")
    print(f"  WDSF NominatedOfficials Sync — {NOW[:19]}")
    print(f"  DB: {DB}")
    print(f"{'='*60}")

    events = scrape_nominated_list()
    print(f"\n  Eventos ballroom encontrados: {len(events)}")

    total_adj = total_nom = total_matched = 0

    for ev in events:
        print(f"\n  [{ev['comp_id']}] {ev['name'][:55]}  ({ev['date']})")
        adj, nom = scrape_officials_page(ev["url"])
        print(f"    Adjudicadores confirmados: {len(adj)}  |  Nominados: {len(nom)}")

        for entry in adj:
            jid = find_judge(conn, entry["name"], entry["country"])
            if jid:
                total_matched += 1
            upsert_nomination(
                conn, ev["comp_id"], ev["name"], ev["date"], ev["discipline"],
                ev["location"], ev["url"],
                entry["name"], entry["country"], jid,
                "Adjudicator", entry["status"], "adjudicator", entry["position"]
            )
            total_adj += 1

        for entry in nom:
            jid = find_judge(conn, entry["name"], entry["country"])
            if jid:
                total_matched += 1
            upsert_nomination(
                conn, ev["comp_id"], ev["name"], ev["date"], ev["discipline"],
                ev["location"], ev["url"],
                entry["name"], entry["country"], jid,
                entry["role"], entry["status"], "nominated", entry["position"]
            )
            total_nom += 1

        conn.commit()

        # Si tenemos una competición en nuestra BD con el mismo nombre/fecha:
        our_ev = conn.execute(
            "SELECT id, status FROM events WHERE date=? AND discipline=?",
            (ev["date"], ev["discipline"])
        ).fetchone()
        if our_ev and our_ev["status"] not in ("officially_nominated","sent_for_review"):
            conn.execute(
                "UPDATE events SET status='officially_nominated' WHERE id=?",
                (our_ev["id"],)
            )
            conn.commit()
            print(f"    → Evento interno #{our_ev['id']} marcado como officially_nominated")

    print(f"\n  RESUMEN: {total_adj} adjudicadores, {total_nom} nominados, "
          f"{total_matched} emparejados con la BD")
    print(f"  Sincronización completada.\n")
    conn.close()
    return {"events": len(events), "adjudicators": total_adj,
            "nominated": total_nom, "matched": total_matched}


if __name__ == "__main__":
    sync()
