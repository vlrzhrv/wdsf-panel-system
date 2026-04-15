#!/usr/bin/env python3
"""
analizar_especialidad.py
────────────────────────
Scraping de worlddancesport.org para detectar la especialidad de cada juez.
Analiza Grand Slams y Mundiales de Adulto Standard/Latin (últimos 3 años).
Actualiza la BD con: std_panels_count, lat_panels_count, specialty.

Uso:
    python3 analizar_especialidad.py
    python3 analizar_especialidad.py --desde 2022 --hasta 2024
    python3 analizar_especialidad.py --solo-mostrar
"""

import sqlite3, os, sys, time, re, argparse
import requests
from datetime import date
from collections import defaultdict

try:
    from bs4 import BeautifulSoup
    BS4 = True
except ImportError:
    BS4 = False

DB_PATH  = os.path.expanduser("~/wdsf_app/wdsf_panel.db")
BASE     = "https://www.worlddancesport.org"
HEADERS  = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/124.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"}

# ─── HTTP ──────────────────────────────────────────────────────────────────────

session = requests.Session()
session.headers.update(HEADERS)

def get(url, delay=0.5):
    time.sleep(delay)
    try:
        r = session.get(url, timeout=20)
        if r.status_code == 200:
            return r
        print(f"    HTTP {r.status_code}: {url}")
    except Exception as e:
        print(f"    Error: {e}")
    return None

def soup(url, delay=0.5):
    r = get(url, delay)
    if r and BS4:
        return BeautifulSoup(r.text, "html.parser")
    return None

# ─── Scraping ─────────────────────────────────────────────────────────────────

def get_events_for_month(year, month):
    """Devuelve lista de {href, text} de eventos del mes."""
    url = f"{BASE}/Calendar/Results?month={month}&year={year}"
    s = soup(url)
    if not s:
        return []
    events = []
    for a in s.select("a[href*='/Events/']"):
        href = a["href"]
        if "Granting" in href:
            continue
        # Get surrounding text
        parent = a.find_parent(["li","div","section"]) or a.parent
        text = parent.get_text(" ", strip=True) if parent else a.get_text(strip=True)
        events.append({"href": BASE + href if href.startswith("/") else href,
                       "text": text})
    return events

def get_competitions_for_event(event_url):
    """Devuelve lista de competiciones del evento con sus URLs de Officials."""
    s = soup(event_url)
    if not s:
        return []
    comps = []
    for a in s.select("a[href*='/Competitions/Ranking/']"):
        href = a["href"]
        text = a.get_text(strip=True)
        name = text.upper()
        # Solo Standard y Latin Adult GS y WCH
        if not (("STANDARD" in name or "LATIN" in name) and
                ("ADULT" in name or "GRANDSLA" in name)):
            continue
        comp_id = href.rstrip("/").split("-")[-1]
        slug    = href.replace("/Competitions/Ranking/","").rsplit("-",1)[0]
        officials_url = f"{BASE}/Competitions/Officials/{slug}-{comp_id}"
        disc = "Standard" if "STANDARD" in name or "STD" in name else "Latin"
        # Exclude non-adult
        if any(x in name for x in ("JUNIOR","YOUTH","SENIOR","U21","UNDER","RISING")):
            disc = None
        if disc:
            comps.append({"text": text, "disc": disc, "officials_url": officials_url})
    return comps

def get_adjudicators(officials_url):
    """Extrae lista de (name, country) de la página Officials."""
    s = soup(officials_url)
    if not s:
        return []
    adj = []
    # Find the adjudicators table
    for table in s.find_all("table"):
        header = table.get_text().lower()
        if "adjudicator" in header or "name" in header:
            for tr in table.find_all("tr")[1:]:
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    name    = tds[0].get_text(strip=True)
                    country = tds[1].get_text(strip=True)
                    if name and len(name) > 2 and country:
                        adj.append((name, country))
            if adj:
                break
    return adj

# ─── Base de datos ─────────────────────────────────────────────────────────────

def ensure_columns(conn):
    cols = {r[1] for r in conn.execute("PRAGMA table_info(judges)")}
    for col, typedef in [("std_panels_count","INTEGER DEFAULT 0"),
                         ("lat_panels_count", "INTEGER DEFAULT 0"),
                         ("specialty",        "TEXT DEFAULT 'Unknown'")]:
        if col not in cols:
            conn.execute(f"ALTER TABLE judges ADD COLUMN {col} {typedef}")
    conn.commit()

def find_judge(conn, name, country):
    """Busca juez por nombre completo, con fallbacks."""
    parts = name.strip().split()
    if len(parts) < 2:
        return None
    # Try "First Last" and "Last First"
    for first, last in [(parts[0], " ".join(parts[1:])),
                        (" ".join(parts[:-1]), parts[-1])]:
        row = conn.execute(
            "SELECT * FROM judges WHERE LOWER(TRIM(first_name))=LOWER(?) AND LOWER(TRIM(last_name))=LOWER(?)",
            (first.strip(), last.strip())
        ).fetchone()
        if row:
            return row
    # Try last name only if unique
    last = parts[-1]
    rows = conn.execute(
        "SELECT * FROM judges WHERE LOWER(TRIM(last_name))=LOWER(?)", (last,)
    ).fetchall()
    if len(rows) == 1:
        return rows[0]
    return None

# ─── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not BS4:
        print("✗ Instala beautifulsoup4:  pip3 install beautifulsoup4")
        sys.exit(1)

    year_now = date.today().year
    parser   = argparse.ArgumentParser()
    parser.add_argument("--desde",        type=int, default=year_now-3)
    parser.add_argument("--hasta",        type=int, default=year_now-1)
    parser.add_argument("--solo-mostrar", action="store_true")
    args = parser.parse_args()

    print("\n" + "═"*65)
    print("  WDSF — Análisis de Especialidad de Jueces")
    print(f"  Período: {args.desde} – {args.hasta}")
    print(f"  BD:      {DB_PATH}")
    print("═"*65 + "\n")

    # ── 1. Encontrar todos los eventos GS / WCH Adult ─────────────────────────
    print("[1/4] Buscando Grand Slams y Mundiales en el calendario WDSF...")
    target_events = []   # {href, text, year}

    for year in range(args.desde, args.hasta + 1):
        for month in range(1, 13):
            events = get_events_for_month(year, month)
            for ev in events:
                t = ev["text"].upper()
                if ("GRANDSLA" in t or "WORLD CHAMPIONSHIP" in t or "WORLDCHAMPIONSHIP" in t):
                    if ("STANDARD" in t or "LATIN" in t):
                        if not any(x in t for x in ("JUNIOR","YOUTH","SENIOR","U21","UNDER","RISING",
                                                     "SOLO","PD ","FORMATION","CHOREOGR")):
                            if ev not in target_events:
                                target_events.append({**ev, "year": year})

    # Deduplicar por href
    seen_hrefs = set()
    unique_events = []
    for ev in target_events:
        h = ev["href"].split("#")[0]
        if h not in seen_hrefs:
            seen_hrefs.add(h)
            unique_events.append(ev)

    print(f"  Encontrados: {len(unique_events)} eventos candidatos")
    for ev in unique_events:
        print(f"    {ev['year']}  {ev['text'][:80]}")

    # ── 2. Obtener lista de competiciones de cada evento ──────────────────────
    print(f"\n[2/4] Extrayendo competiciones de {len(unique_events)} eventos...")
    all_comps = []
    for i, ev in enumerate(unique_events):
        url = ev["href"].split("#")[0] + "#tab=competitions"
        print(f"  [{i+1}/{len(unique_events)}] {ev['text'][:60]}")
        comps = get_competitions_for_event(url)
        print(f"    → {len(comps)} competiciones Standard/Latin Adult")
        for c in comps:
            all_comps.append({**c, "event": ev["text"][:50]})

    print(f"\n  Total competiciones a analizar: {len(all_comps)}")

    # ── 3. Obtener adjudicadores de cada competición ──────────────────────────
    print(f"\n[3/4] Extrayendo paneles de jueces...")
    # judge_key -> {"std": N, "lat": N, "country": ""}
    judge_counts = defaultdict(lambda: {"std": 0, "lat": 0, "country": ""})

    for i, comp in enumerate(all_comps):
        print(f"  [{i+1}/{len(all_comps)}] {comp['disc']:8} {comp['event'][:50]}")
        adjs = get_adjudicators(comp["officials_url"])
        print(f"    → {len(adjs)} jueces")
        for name, country in adjs:
            key = name.strip().lower()
            judge_counts[key]["country"] = country
            if comp["disc"] == "Standard":
                judge_counts[key]["std"] += 1
            else:
                judge_counts[key]["lat"] += 1
            # Store original name
            if "name" not in judge_counts[key]:
                judge_counts[key]["name"] = name.strip()

    print(f"\n  Jueces únicos identificados: {len(judge_counts)}")

    # ── 4. Calcular especialidad y actualizar BD ──────────────────────────────
    print("\n[4/4] Calculando especialidad y actualizando BD...\n")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    if not args.solo_mostrar:
        ensure_columns(conn)

    updated = not_found = 0

    print(f"  {'Juez':<32} {'País':<18} {'STD':>4} {'LAT':>4}  Especialidad")
    print("  " + "─"*70)

    sorted_judges = sorted(judge_counts.items(),
                           key=lambda x: -(x[1]["std"] + x[1]["lat"]))

    for key, data in sorted_judges:
        std_n   = data["std"]
        lat_n   = data["lat"]
        total   = std_n + lat_n
        if total == 0:
            continue
        name    = data.get("name", key.title())
        country = data["country"]
        ratio   = std_n / total
        if ratio >= 0.70:   specialty = "Standard"
        elif ratio <= 0.30: specialty = "Latin"
        else:               specialty = "Both"

        tag = {"Standard":"🎩 Standard","Latin":"💃 Latin","Both":"⚡ Both"}[specialty]
        print(f"  {name:<32} {country:<18} {std_n:>4} {lat_n:>4}  {tag}")

        if args.solo_mostrar:
            continue

        row = find_judge(conn, name, country)
        if row:
            conn.execute(
                "UPDATE judges SET std_panels_count=?, lat_panels_count=?, specialty=? WHERE id=?",
                (std_n, lat_n, specialty, row["id"])
            )
            updated += 1
        else:
            not_found += 1

    if not args.solo_mostrar:
        conn.commit()

    conn.close()

    std_c = sum(1 for d in judge_counts.values() if d["std"]/(d["std"]+d["lat"])>=0.7 if d["std"]+d["lat"]>0)
    lat_c = sum(1 for d in judge_counts.values() if d["lat"]/(d["std"]+d["lat"])>=0.7 if d["std"]+d["lat"]>0)

    print("\n" + "─"*65)
    print(f"  🎩 Especialistas Standard: {std_c}")
    print(f"  💃 Especialistas Latin:    {lat_c}")
    print(f"  ⚡ Versátiles:             {len(judge_counts)-std_c-lat_c}")
    if not args.solo_mostrar:
        print(f"\n  ✓ Actualizados en BD:   {updated}")
        print(f"  ✗ No encontrados en BD: {not_found}")
        print("\n  Reinicia servidor.py para aplicar los cambios.")
    print("─"*65 + "\n")

if __name__ == "__main__":
    main()
