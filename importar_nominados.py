import requests, sqlite3, os, time
from bs4 import BeautifulSoup
from datetime import datetime

DB       = os.path.expanduser("~/wdsf_app/wdsf_panel.db")
BASE_WEB = "https://www.worlddancesport.org"
HEADERS  = {"User-Agent": "Mozilla/5.0"}

# Mapeo codigos WDSF (3 letras) -> nombre completo
COUNTRY_CODES = {
    "LAT":"Latvia","UKR":"Ukraine","GER":"Germany","BEL":"Belgium","CRO":"Croatia",
    "SRB":"Serbia","FRA":"France","AUT":"Austria","ROU":"Romania","POL":"Poland",
    "ITA":"Italy","ESP":"Spain","GBR":"United Kingdom","NED":"Netherlands",
    "HUN":"Hungary","CZE":"Czech Republic","SVK":"Slovakia","SVN":"Slovenia",
    "BUL":"Bulgaria","RUS":"Russia","BLR":"Belarus","SWE":"Sweden","NOR":"Norway",
    "DEN":"Denmark","FIN":"Finland","GRE":"Greece","TUR":"Turkey","POR":"Portugal",
    "SUI":"Switzerland","EST":"Estonia","LTU":"Lithuania","TPE":"Taiwan",
    "CHN":"China","JPN":"Japan","KOR":"South Korea","AUS":"Australia",
    "USA":"United States","CAN":"Canada","BRA":"Brazil","ISR":"Israel",
    "OIN":"Other","MDA":"Moldova","MKD":"North Macedonia","ALB":"Albania",
    "BIH":"Bosnia and Herzegovina","MNE":"Montenegro","KAZ":"Kazakhstan",
    "UZB":"Uzbekistan","PHI":"Philippines","MAS":"Malaysia","THA":"Thailand",
    "INA":"Indonesia","MEX":"Mexico","ARG":"Argentina","VIE":"Vietnam",
}

def get(url, delay=1):
    time.sleep(delay)
    r = requests.get(url, headers=HEADERS, timeout=30)
    return BeautifulSoup(r.text, "html.parser") if r.status_code == 200 else None

def find_judge_id(conn, full_name):
    parts = full_name.strip().split()
    if len(parts) < 2:
        return None
    first = parts[0]
    last  = " ".join(parts[1:])
    row = conn.execute(
        "SELECT id FROM judges WHERE lower(first_name)=lower(?) AND lower(last_name)=lower(?)",
        (first, last)
    ).fetchone()
    if row: return row[0]
    # Busqueda inversa (apellido primero)
    first2 = " ".join(parts[:-1])
    last2  = parts[-1]
    row = conn.execute(
        "SELECT id FROM judges WHERE lower(first_name)=lower(?) AND lower(last_name)=lower(?)",
        (first2, last2)
    ).fetchone()
    if row: return row[0]
    # Busqueda por apellido solo
    row = conn.execute(
        "SELECT id FROM judges WHERE lower(last_name)=lower(?)",
        (last,)
    ).fetchone()
    return row[0] if row else None

def insert_judge_if_missing(conn, full_name, country_code):
    judge_id = find_judge_id(conn, full_name)
    if judge_id: return judge_id, False
    parts = full_name.strip().split()
    first = parts[0] if parts else ""
    last  = " ".join(parts[1:]) if len(parts) > 1 else ""
    country = COUNTRY_CODES.get(country_code, country_code)
    conn.execute(
        "INSERT INTO judges (first_name,last_name,nationality,representing,active,license_type) VALUES (?,?,?,?,1,'A')",
        (first, last, country, country)
    )
    conn.commit()
    row = conn.execute("SELECT last_insert_rowid()").fetchone()
    return row[0], True

def find_or_create_event(conn, name, date_str, location, country_code):
    country = COUNTRY_CODES.get(country_code, country_code)
    # Buscar por fecha y ubicacion
    row = conn.execute(
        "SELECT id FROM events WHERE date=? AND location=?",
        (date_str, location)
    ).fetchone()
    if row: return row[0], False
    # Buscar por nombre parcial
    words = name.replace("-","").split()[:4]
    like  = "%" + "%".join(words[:3]) + "%"
    row = conn.execute("SELECT id FROM events WHERE name LIKE ?", (like,)).fetchone()
    if row: return row[0], False
    # Crear
    conn.execute(
        "INSERT INTO events (name,date,location,country,status) VALUES (?,?,?,?,'nominated')",
        (name, date_str, location, country)
    )
    conn.commit()
    row = conn.execute("SELECT last_insert_rowid()").fetchone()
    return row[0], True

def scrape_competition_panel(url, comp_name, comp_date, comp_location):
    soup = get(BASE_WEB + url)
    if not soup:
        print(f"  ERROR accediendo a {url}")
        return [], [], []

    adjudicators  = []
    chairpersons  = []
    nominated     = []

    tables = soup.find_all("table")
    h2s    = soup.find_all("h2")

    for i, h2 in enumerate(h2s):
        title = h2.get_text(strip=True).lower()
        table = h2.find_next("table")
        if not table: continue
        rows  = table.find_all("tr")[1:]

        if "adjudicator" in title:
            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 2: continue
                name_tag = cols[0].find("a")
                name     = name_tag.get_text(strip=True) if name_tag else cols[0].get_text(strip=True)
                country  = cols[1].get_text(strip=True)
                ident    = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                adjudicators.append({"name": name, "country": country, "identifier": ident})

        elif "chairperson" in title:
            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 2: continue
                name_tag = cols[0].find("a")
                name     = name_tag.get_text(strip=True) if name_tag else cols[0].get_text(strip=True)
                country  = cols[1].get_text(strip=True)
                chairpersons.append({"name": name, "country": country})

        elif "nominated" in title:
            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 3: continue
                name_tag = cols[0].find("a")
                name     = name_tag.get_text(strip=True) if name_tag else cols[0].get_text(strip=True)
                country  = cols[1].get_text(strip=True)
                task     = cols[2].get_text(strip=True)
                status   = cols[3].get_text(strip=True) if len(cols) > 3 else ""
                nominated.append({"name": name, "country": country, "task": task, "status": status})

    return adjudicators, chairpersons, nominated

def main():
    print("Descargando NominatedOfficials...")
    soup = get(BASE_WEB + "/NominatedOfficials", delay=0)
    if not soup:
        print("ERROR: no se pudo acceder a NominatedOfficials")
        return

    competitions = []
    for row in soup.select("table tr")[1:]:
        cols = row.find_all("td")
        if len(cols) < 3: continue
        date_str = cols[0].get_text(strip=True)
        name_tag = cols[1].find("a")
        if not name_tag: continue
        name     = name_tag.get_text(strip=True)
        url      = name_tag["href"]
        location_text = cols[2].get_text(strip=True)
        city     = location_text.split("-")[0].strip() if "-" in location_text else location_text
        country  = location_text.split("-")[1].strip() if "-" in location_text else ""
        try:
            date_obj = datetime.strptime(date_str, "%d %B %Y").strftime("%Y-%m-%d")
        except:
            date_obj = date_str
        competitions.append({
            "name": name, "date": date_obj, "location": city,
            "country": country, "url": url
        })

    print(f"Competiciones con paneles nominados: {len(competitions)}")

    conn = sqlite3.connect(DB)
    # Asegurar columna nominated_url
    try:
        conn.execute("ALTER TABLE panel_assignments ADD COLUMN competition_identifier TEXT")
        conn.commit()
    except: pass

    total_adj = total_new_judges = total_events_created = 0

    for comp in competitions:
        print(f"\n{'='*60}")
        print(f"  {comp['date']} | {comp['name']}")
        print(f"  {comp['location']}, {comp['country']}")

        adj, chairs, nomin = scrape_competition_panel(
            comp["url"], comp["name"], comp["date"], comp["location"]
        )

        event_id, created = find_or_create_event(
            conn, comp["name"], comp["date"], comp["location"], comp["country"]
        )
        if created:
            total_events_created += 1
            print(f"  Evento CREADO en BD (id={event_id})")
        else:
            print(f"  Evento encontrado en BD (id={event_id})")

        # Limpiar asignaciones previas de este evento
        conn.execute("DELETE FROM panel_assignments WHERE event_id=? AND status='nominated'", (event_id,))

        # Importar chairperson
        for ch in chairs:
            jid, new = insert_judge_if_missing(conn, ch["name"], ch["country"])
            if new: total_new_judges += 1
            conn.execute(
                "INSERT OR IGNORE INTO panel_assignments (event_id,judge_id,role,position,score,status,competition_identifier) VALUES (?,?,'chairperson',0,0,'confirmed',?)",
                (event_id, jid, "CHAIR")
            )
            print(f"  CHAIR   : {ch['name']} ({ch['country']})")

        # Importar adjudicadores confirmados
        for i, adj_j in enumerate(adj):
            jid, new = insert_judge_if_missing(conn, adj_j["name"], adj_j["country"])
            if new: total_new_judges += 1
            conn.execute(
                "INSERT OR IGNORE INTO panel_assignments (event_id,judge_id,role,position,score,status,competition_identifier) VALUES (?,?,'adjudicator',?,0,'confirmed',?)",
                (event_id, jid, i+1, adj_j["identifier"])
            )
            print(f"  ADJ [{adj_j['identifier']}]: {adj_j['name']} ({adj_j['country']}) ✅")
            total_adj += 1

        # Importar nominados (con su estado)
        for nom in nomin:
            jid, new = insert_judge_if_missing(conn, nom["name"], nom["country"])
            if new: total_new_judges += 1
            status = "declined" if "Declined" in nom["status"] else "nominated"
            conn.execute(
                "INSERT OR IGNORE INTO panel_assignments (event_id,judge_id,role,position,score,status,competition_identifier) VALUES (?,?,'adjudicator',99,0,?,?)",
                (event_id, jid, status, "NOM")
            )
            icon = "❌" if status == "declined" else "⏳"
            print(f"  NOM     : {nom['name']} ({nom['country']}) {icon} {nom['status']}")

        conn.execute("UPDATE events SET status='nominated' WHERE id=?", (event_id,))
        conn.commit()

    conn.close()

    print(f"\n{'='*60}")
    print("IMPORTACION COMPLETADA")
    print(f"  Competiciones procesadas : {len(competitions)}")
    print(f"  Adjudicadores importados : {total_adj}")
    print(f"  Jueces nuevos creados    : {total_new_judges}")
    print(f"  Eventos creados en BD    : {total_events_created}")
    print(f"\nAhora en tu app puedes ver los paneles nominados reales.")

main()