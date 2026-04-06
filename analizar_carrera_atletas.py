#!/usr/bin/env python3
"""
analizar_carrera_atletas.py
────────────────────────────────────────────────────────────────
Para cada juez activo de la base de datos, busca su perfil como
ATLETA en worlddancesport.org y extrae:

  • career_level  — mejor resultado en WCH (campeón, pódium, finalista…)
  • specialty     — disciplina predominante en sus últimos 5 años
                   de carrera (Standard / Latin / Both)

Uso:
    pip3 install requests beautifulsoup4 --break-system-packages
    python3 analizar_carrera_atletas.py           # actualiza todos
    python3 analizar_carrera_atletas.py --dry-run # solo muestra, no guarda
    python3 analizar_carrera_atletas.py --id 42   # solo el juez con id=42
"""

import sys
import time
import sqlite3
from datetime import datetime, date
from bs4 import BeautifulSoup
from collections import Counter

try:
    import cloudscraper
    _USE_CLOUDSCRAPER = True
except ImportError:
    import requests
    _USE_CLOUDSCRAPER = False
    print("⚠️  cloudscraper no instalado. Instala con:")
    print("    pip3 install cloudscraper --break-system-packages")
    print("   Continuando con requests (puede fallar con Cloudflare)...\n")

# ── Configuración ──────────────────────────────────────────────────────────
import os
DB_PATH    = os.path.expanduser("~/wdsf_app/wdsf_panel.db")
BASE_URL   = "https://www.worlddancesport.org"
SLEEP      = 1.2   # segundos entre peticiones (respetar servidor)
DRY_RUN    = "--dry-run" in sys.argv
DEBUG      = "--debug"   in sys.argv
ONLY_ID    = None
for i, a in enumerate(sys.argv):
    if a == "--id" and i + 1 < len(sys.argv):
        ONLY_ID = int(sys.argv[i + 1])

if _USE_CLOUDSCRAPER:
    # cloudscraper resuelve automáticamente los challenges de Cloudflare
    SESSION = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "mobile": False}
    )
else:
    SESSION = requests.Session()
    SESSION.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.worlddancesport.org",
        "Referer": "https://www.worlddancesport.org/Athletes",
    })

def init_session():
    """Verifica conexión con la WDSF."""
    try:
        r = SESSION.get(f"{BASE_URL}/Athletes", timeout=15)
        if r.status_code == 200 and len(r.text) > 50000:
            return True
        print(f"  ⚠️  Respuesta corta ({len(r.text)} bytes) — posible bloqueo Cloudflare")
        if not _USE_CLOUDSCRAPER:
            print("      → Instala cloudscraper: pip3 install cloudscraper --break-system-packages")
        return False
    except Exception as e:
        print(f"  ⚠️  Error de conexión: {e}")
    return False

# ── Mapeo país WDSF → código normalizado ──────────────────────────────────
# WDSF usa nombres completos; nuestra BD usa representaciones normalizadas
COUNTRY_MAP = {
    "Lithuania": "Lithuania", "Germany": "Germany", "Austria": "Austria",
    "United Kingdom": "United Kingdom", "England": "United Kingdom",
    "Russia": "Russia", "Ukraine": "Ukraine", "Poland": "Poland",
    "Italy": "Italy", "France": "France", "Spain": "Spain",
    "Netherlands": "Netherlands", "Switzerland": "Switzerland",
    "Czech Republic": "Czech Republic", "Czechia": "Czech Republic",
    "Hungary": "Hungary", "Romania": "Romania", "Bulgaria": "Bulgaria",
    "Denmark": "Denmark", "Sweden": "Sweden", "Norway": "Norway",
    "Finland": "Finland", "Estonia": "Estonia", "Latvia": "Latvia",
    "Croatia": "Croatia", "Serbia": "Serbia", "Slovakia": "Slovakia",
    "Slovenia": "Slovenia", "Belarus": "Belarus", "Kazakhstan": "Kazakhstan",
    "China": "China", "People's Republic of China": "China",
    "Japan": "Japan", "South Korea": "South Korea",
    "Australia": "Australia", "New Zealand": "New Zealand",
    "United States": "United States", "Canada": "Canada",
    "Brazil": "Brazil", "Argentina": "Argentina",
    "Chinese Taipei": "Chinese Taipei", "Taiwan": "Chinese Taipei",
    "Hong Kong, China": "Hong Kong", "Hong Kong": "Hong Kong",
    "Singapore": "Singapore", "Thailand": "Thailand",
    "Malaysia": "Malaysia", "Philippines": "Philippines",
    "Indonesia": "Indonesia", "India": "India",
    "Israel": "Israel", "Turkey": "Turkey", "Türkiye": "Turkey",
    "Georgia": "Georgia", "Armenia": "Armenia", "Azerbaijan": "Azerbaijan",
    "Moldova": "Moldova", "Portugal": "Portugal", "Belgium": "Belgium",
    "Ireland": "Ireland", "Iceland": "Iceland", "Luxembourg": "Luxembourg",
    "Greece": "Greece", "Cyprus": "Cyprus", "Malta": "Malta",
    "Mexico": "Mexico", "Colombia": "Colombia", "Peru": "Peru",
    "South Africa": "South Africa", "Morocco": "Morocco",
    "Egypt": "Egypt",
}

def normalize_country(c):
    if not c:
        return ""
    return COUNTRY_MAP.get(c.strip(), c.strip())

# ── Búsqueda de atleta por nombre ──────────────────────────────────────────
def search_athlete(last_name, first_name, representing):
    """
    Devuelve la URL del perfil WDSF del atleta o None si no se encuentra.
    Estrategia por niveles: primero nombre+apellido+país, luego sin país.
    """
    rep_norm = normalize_country(representing or "").lower()

    # Probar varias formas del nombre (algunos países usan apellido primero)
    queries = [
        f"{first_name} {last_name}",   # Vasile Constantin
        f"{last_name} {first_name}",   # Constantin Vasile (formato E. europeo)
        last_name,                      # Solo apellido
        first_name,                     # Solo nombre (si apellido es muy común)
    ]

    candidates = []   # acumular todos los candidatos de todas las búsquedas

    for query in queries:
        try:
            r = SESSION.post(
                f"{BASE_URL}/api/listitems/athletes",
                json={"name": query, "pageSize": 30, "pageIndex": 0},
                timeout=10
            )
            if DEBUG:
                try:
                    items = r.json().get("items", [])
                    print(f"\n    [search '{query}'] HTTP {r.status_code}, "
                          f"{len(items)} resultados: {[i.get('name') for i in items[:5]]}")
                except Exception as ex:
                    print(f"\n    [search '{query}'] HTTP {r.status_code}, "
                          f"respuesta no JSON: {r.text[:200]}")
            if r.status_code != 200:
                time.sleep(SLEEP)
                continue
            for it in r.json().get("items", []):
                it_name = (it.get("name") or "").lower()
                # Solo añadir si contiene ambas partes del nombre
                if last_name.lower() in it_name and first_name.lower() in it_name:
                    if it not in candidates:
                        candidates.append(it)
        except Exception as e:
            if DEBUG:
                print(f"\n    [search '{query}'] EXCEPCIÓN: {e}")
        time.sleep(SLEEP)

    if not candidates:
        return None

    # Paso 1: nombre + país exacto
    for it in candidates:
        it_country = normalize_country(it.get("country") or "").lower()
        if rep_norm and it_country == rep_norm:
            return it.get("url")

    # Paso 2: nombre sin importar país (puede haber cambiado de federación)
    return candidates[0].get("url")

# ── Parsear perfil de atleta ───────────────────────────────────────────────
def fetch_profile(url_path):
    """Devuelve el HTML del perfil."""
    try:
        r = SESSION.get(f"{BASE_URL}{url_path}", timeout=15)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    return None

def parse_competitions(html):
    """
    Extrae lista de competiciones del perfil de atleta.
    Devuelve lista de dicts con: rank, date, event, discipline, category.
    """
    soup = BeautifulSoup(html, "html.parser")
    comps = []

    # La tabla de competiciones tiene columnas:
    # Rank | Points | Date | Event | Discipline | Category | Location
    table = None
    for t in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in t.find_all("th")]
        if "discipline" in headers and "event" in headers:
            table = t
            break

    if not table:
        # Intentar con estructura de lista alternativa
        rows = soup.select("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 5:
                try:
                    rank_text = cells[0].get_text(strip=True)
                    rank = int(rank_text.rstrip(".")) if rank_text.rstrip(".").isdigit() else None
                    # Buscar fecha en formato "DD Month YYYY" o "Month YYYY"
                    date_text = ""
                    event = ""
                    discipline = ""
                    category = ""
                    for i, c in enumerate(cells):
                        t = c.get_text(strip=True)
                        if any(m in t for m in ["January","February","March","April","May","June",
                                                "July","August","September","October","November","December"]):
                            date_text = t
                            if i+1 < len(cells): event = cells[i+1].get_text(strip=True)
                            if i+2 < len(cells): discipline = cells[i+2].get_text(strip=True)
                            if i+3 < len(cells): category = cells[i+3].get_text(strip=True)
                            break
                    if date_text and rank is not None:
                        comps.append({
                            "rank": rank, "date": date_text,
                            "event": event, "discipline": discipline, "category": category
                        })
                except Exception:
                    pass
        return comps

    # Parsear tabla encontrada
    headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
    idx = {h: i for i, h in enumerate(headers)}
    for row in table.find_all("tr")[1:]:
        cells = row.find_all("td")
        if not cells:
            continue
        try:
            def get(col):
                i = idx.get(col, -1)
                return cells[i].get_text(strip=True) if 0 <= i < len(cells) else ""
            rank_s = get("rank").rstrip(".")
            rank = int(rank_s) if rank_s.isdigit() else None
            comps.append({
                "rank": rank,
                "date": get("date"),
                "event": get("event"),
                "discipline": get("discipline"),
                "category": get("category"),
            })
        except Exception:
            pass

    return comps

def parse_competitions_from_text(html):
    """
    Método alternativo: parsea directamente las filas de la tabla de competiciones
    usando BeautifulSoup de forma más robusta.
    """
    soup = BeautifulSoup(html, "html.parser")
    comps = []
    MONTHS = {"January","February","March","April","May","June",
              "July","August","September","October","November","December"}

    for row in soup.find_all("tr"):
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cells) < 4:
            continue
        # Primera celda: rango (ej. "1.", "2.", "12.")
        rank_s = cells[0].rstrip(".")
        if not rank_s.isdigit():
            continue
        rank = int(rank_s)
        # Buscar columna de fecha (contiene nombre de mes)
        date_idx = next((i for i, c in enumerate(cells) if any(m in c for m in MONTHS)), -1)
        if date_idx < 0:
            continue
        date_text   = cells[date_idx]
        event       = cells[date_idx + 1] if date_idx + 1 < len(cells) else ""
        discipline  = cells[date_idx + 2] if date_idx + 2 < len(cells) else ""
        category    = cells[date_idx + 3] if date_idx + 3 < len(cells) else ""
        comps.append({
            "rank": rank, "date": date_text,
            "event": event, "discipline": discipline, "category": category
        })
    return comps

def parse_retired_date(html):
    """Extrae la fecha de retirada del atleta."""
    soup = BeautifulSoup(html, "html.parser")
    # Buscar "Retired" en la tabla de pareja
    text = soup.get_text(separator=" ")
    import re
    m = re.search(r"Retired\s+(\d{2}/\d{2}/\d{4})", text)
    if m:
        try:
            return datetime.strptime(m.group(1), "%d/%m/%Y").date()
        except Exception:
            pass
    return None

# ── Determinar career_level ────────────────────────────────────────────────
def determine_career_level(comps):
    """
    Analiza resultados en WCH/ECH y devuelve el nivel de carrera.
    Orden de prioridad: world_champion > world_podium > world_finalist >
    world_participant > european_champion > european_podium > national
    """
    best = "national"
    PRIORITY = {
        "world_champion": 7, "world_podium": 6, "world_finalist": 5,
        "world_participant": 4, "european_champion": 3, "european_podium": 2,
        "national": 1
    }

    for c in comps:
        event = (c.get("event") or "").lower()
        rank  = c.get("rank")
        if rank is None:
            continue

        is_wch = "world championship" in event
        is_ech = "european championship" in event or "european cup" in event

        if is_wch:
            if rank == 1:
                level = "world_champion"
            elif rank <= 3:
                level = "world_podium"
            elif rank <= 6:
                level = "world_finalist"
            else:
                level = "world_participant"
        elif is_ech:
            if rank == 1:
                level = "european_champion"
            elif rank <= 3:
                level = "european_podium"
            else:
                continue
        else:
            continue

        if PRIORITY.get(level, 0) > PRIORITY.get(best, 0):
            best = level

    return best

# ── Determinar specialty ───────────────────────────────────────────────────
def normalize_discipline(raw):
    """
    Normaliza el nombre de disciplina de WDSF.
    Ejemplos: 'PD Latin' → 'Latin', 'PD Standard' → 'Standard',
              'Latin' → 'Latin', 'Standard' → 'Standard',
              'Ten Dance' → 'Ten Dance'
    """
    r = (raw or "").strip()
    # Quitar prefijos: "PD ", "Pro/Am ", "Amateur ", etc.
    for prefix in ("PD ", "Pro/Am ", "Amateur ", "WD ", "WDSF "):
        if r.startswith(prefix):
            r = r[len(prefix):]
    return r

def determine_specialty(comps, retired_date=None):
    """
    Analiza los últimos 5 años de carrera competitiva.
    Devuelve 'Standard', 'Latin' o 'Both'.
    """
    MONTHS = {"January":1,"February":2,"March":3,"April":4,"May":5,"June":6,
              "July":7,"August":8,"September":9,"October":10,"November":11,"December":12}

    def parse_date(s):
        parts = (s or "").split()
        try:
            if len(parts) == 3:  # "DD Month YYYY"
                return date(int(parts[2]), MONTHS.get(parts[1], 1), int(parts[0]))
            elif len(parts) == 2:  # "Month YYYY"
                return date(int(parts[1]), MONTHS.get(parts[0], 1), 1)
        except Exception:
            pass
        return None

    # Determinar fecha de fin de carrera
    parsed_dates = [parse_date(c["date"]) for c in comps if c.get("date")]
    parsed_dates = [d for d in parsed_dates if d]
    if not parsed_dates:
        return "Unknown"

    end_date     = retired_date or max(parsed_dates)
    start_cutoff = date(end_date.year - 5, end_date.month, end_date.day)

    # Contar disciplinas en ese período (normalizando nombres)
    disc_count = Counter()
    for c in comps:
        d = parse_date(c.get("date") or "")
        if d and d >= start_cutoff:
            disc = normalize_discipline(c.get("discipline") or "")
            if disc in ("Standard", "Latin"):
                disc_count[disc] += 1

    std = disc_count.get("Standard", 0)
    lat = disc_count.get("Latin", 0)
    total = std + lat
    if total == 0:
        return "Unknown"
    if std == 0:
        return "Latin"
    if lat == 0:
        return "Standard"
    ratio = std / total
    if ratio >= 0.75:
        return "Standard"
    elif ratio <= 0.25:
        return "Latin"
    else:
        return "Both"

# ── Main ───────────────────────────────────────────────────────────────────
def main():
    print("\n  Inicializando sesión con worlddancesport.org...", end=" ", flush=True)
    if not init_session():
        print("ERROR — sin conexión. Verifica internet y vuelve a intentar.")
        sys.exit(1)
    print("✓")
    time.sleep(1)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Asegurar columnas necesarias
    cols = [r[1] for r in conn.execute("PRAGMA table_info(judges)").fetchall()]
    for col in ["career_level", "specialty", "primary_discipline"]:
        if col not in cols:
            conn.execute(f"ALTER TABLE judges ADD COLUMN {col} TEXT")
    conn.commit()

    # Cargar jueces a procesar
    if ONLY_ID:
        judges = conn.execute("SELECT * FROM judges WHERE id=?", (ONLY_ID,)).fetchall()
    else:
        judges = conn.execute(
            "SELECT * FROM judges WHERE active=1 ORDER BY last_name, first_name"
        ).fetchall()

    total   = len(judges)
    updated = 0
    skipped = 0
    not_found = 0

    print(f"\n{'='*60}")
    print(f"  WDSF Athletic Career Analyzer")
    print(f"  Jueces a procesar: {total}")
    print(f"  Dry run: {DRY_RUN}")
    print(f"{'='*60}\n")

    for i, j in enumerate(judges):
        j          = dict(j)
        jid        = j["id"]
        first_name = (j.get("first_name") or "").strip()
        last_name  = (j.get("last_name") or "").strip()
        rep        = (j.get("representing") or j.get("nationality") or "").strip()
        current_cl = j.get("career_level") or ""
        current_sp = j.get("specialty") or ""

        progress = f"[{i+1}/{total}]"
        print(f"{progress} {last_name}, {first_name} ({rep}) ...", end=" ", flush=True)

        # Buscar atleta en WDSF
        profile_url = search_athlete(last_name, first_name, rep)
        time.sleep(SLEEP)

        if not profile_url:
            print(f"❌ No encontrado en WDSF")
            not_found += 1
            continue

        print(f"→ {profile_url.split('/')[-1][:40]} ...", end=" ", flush=True)

        # Obtener perfil
        html = fetch_profile(profile_url)
        time.sleep(SLEEP)
        if not html:
            print(f"⚠️  Error al descargar perfil")
            skipped += 1
            continue

        # Parsear competiciones (método tabla primero, luego texto)
        comps = parse_competitions(html)
        if DEBUG:
            print(f"\n  [DEBUG] parse_competitions → {len(comps)} entradas")
        if not comps:
            comps = parse_competitions_from_text(html)
            if DEBUG:
                print(f"  [DEBUG] parse_competitions_from_text → {len(comps)} entradas")

        if DEBUG and not comps:
            # Mostrar fragmento del HTML para diagnóstico
            idx_th = html.find('<th>Rank</th>')
            idx_tr = html.find('>Rank<')
            tables_count = html.count('<table')
            print(f"  [DEBUG] HTML len={len(html)}, tables={tables_count}, "
                  f"<th>Rank</th> at {idx_th}, >Rank< at {idx_tr}")
            if idx_th > 0:
                print(f"  [DEBUG] Table snippet: {repr(html[idx_th-100:idx_th+400])}")

        if not comps:
            print(f"⚠️  Sin datos de competición")
            skipped += 1
            continue

        # Calcular career_level y specialty
        retired_date = parse_retired_date(html)
        career_level = determine_career_level(comps)
        specialty    = determine_specialty(comps, retired_date)

        # Solo actualizar si hay mejora o si estaba vacío
        changed = (career_level != current_cl or specialty != current_sp)
        print(f"✓  {career_level} | {specialty} ({len(comps)} comps)", end="")
        if not changed:
            print(" [sin cambio]")
        else:
            print(f" [antes: {current_cl or '—'} | {current_sp or '—'}]")

        if not DRY_RUN and changed:
            conn.execute(
                "UPDATE judges SET career_level=?, specialty=? WHERE id=?",
                (career_level, specialty, jid)
            )
            # También actualizar primary_discipline si está vacío
            if not j.get("primary_discipline") and specialty in ("Standard", "Latin"):
                conn.execute(
                    "UPDATE judges SET primary_discipline=? WHERE id=?",
                    (specialty, jid)
                )
            conn.commit()
            updated += 1

    conn.close()
    print(f"\n{'='*60}")
    print(f"  Completado:")
    print(f"  ✓ Actualizados : {updated}")
    print(f"  ↔ Sin cambio   : {total - updated - not_found - skipped}")
    print(f"  ❌ No encontrado: {not_found}")
    print(f"  ⚠️ Errores      : {skipped}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
