"""
sincronizar_jueces.py  –  Sincronización completa de jueces desde WDSF API
===========================================================================
Descarga TODOS los jueces que han participado en competiciones WDSF
(no sólo los campeonatos "importantes") e inserta/actualiza su registro
en la base de datos local.

Uso:
    python sincronizar_jueces.py [--years 2024 2025 2026] [--db /ruta/wdsf_panel.db]
                                 [--workers 10]

Variables de entorno que se usan si no se pasan credenciales en código:
    WDSF_USER, WDSF_PASS
"""

import os, sys, json, re, time, sqlite3, argparse, threading
import concurrent.futures
from datetime import date, timedelta
from requests.auth import HTTPBasicAuth
import requests

# ── Credenciales WDSF ─────────────────────────────────────────────────────────
WDSF_USER = os.environ.get("WDSF_USER", "ValeriIvanov1")
WDSF_PASS = os.environ.get("WDSF_PASS", "sjJ@M9Va7I")
WDSF_BASE = "https://services.worlddancesport.org/api/1"

# ── Número de workers paralelos ───────────────────────────────────────────────
MAX_WORKERS = 10   # 10 hilos → ~10x más rápido; sube a 15 si la API lo aguanta

# ── Base de datos (misma lógica que servidor.py) ──────────────────────────────
APP_DIR   = os.path.dirname(os.path.abspath(__file__))
VOLUME_DB = "/data/wdsf_panel.db"
BUNDLE_DB = os.path.join(APP_DIR, "wdsf_panel.db")
LOCAL_DB  = os.path.expanduser("~/wdsf_app/wdsf_panel.db")

def find_db():
    if os.path.isdir("/data") and os.path.exists(VOLUME_DB):
        return VOLUME_DB
    if os.path.exists(BUNDLE_DB):
        return BUNDLE_DB
    return LOCAL_DB

# ── Disciplinas válidas para jueces ballroom ──────────────────────────────────
BALLROOM_DISCS = {"Standard", "Latin", "Combined"}

# ── Mapeo código IOC → nombre país (para country code de oficiales) ───────────
COUNTRY_CODE_MAP = {
    "FRA":"France","BEL":"Belgium","CZE":"Czechia","ESP":"Spain",
    "POR":"Portugal","ITA":"Italy","AUT":"Austria","LTU":"Lithuania",
    "ROU":"Romania","NOR":"Norway","GER":"Germany","POL":"Poland",
    "DEN":"Denmark","GBR":"United Kingdom","NED":"Netherlands","HUN":"Hungary",
    "SVK":"Slovakia","SLO":"Slovenia","CRO":"Croatia","BUL":"Bulgaria",
    "LAT":"Latvia","EST":"Estonia","UKR":"Ukraine","RUS":"Russia",
    "BLR":"Belarus","SWE":"Sweden","FIN":"Finland","SUI":"Switzerland",
    "TUR":"Turkey","GRE":"Greece","SRB":"Serbia","MKD":"Macedonia",
    "MDA":"Moldova","GEO":"Georgia","AZE":"Azerbaijan","ARM":"Armenia",
    "CHN":"China, People's Republic of","JPN":"Japan","KOR":"Korea",
    "AUS":"Australia","USA":"United States","CAN":"Canada","BRA":"Brazil",
    "ARG":"Argentina","MEX":"Mexico","ISR":"Israel","VIE":"Vietnam",
    "ECU":"Ecuador","MLT":"Malta","PHI":"Philippines","MGL":"Mongolia",
    "OIN":"AIN","TPE":"Chinese Taipei","HKG":"Hong Kong","SGP":"Singapore",
    "NZL":"New Zealand","THA":"Thailand","MYS":"Malaysia","IDN":"Indonesia",
    "IND":"India","CMB":"Cambodia","KAZ":"Kazakhstan","UZB":"Uzbekistan",
    "ALB":"Albania","MNE":"Montenegro","BIH":"Bosnia and Herzegovina",
    "CYP":"Cyprus","AND":"Andorra","MCO":"Monaco","SMR":"San Marino",
    "LUX":"Luxembourg","ISL":"Iceland","IRL":"Ireland","SCO":"Scotland",
    "WLS":"Wales","ENG":"England","ZAF":"South Africa","EGY":"Egypt",
    "MAR":"Morocco","KEN":"Kenya","COL":"Colombia","CHL":"Chile",
    "CUB":"Cuba","VEN":"Venezuela","URY":"Uruguay",
}

# ── Thread-local HTTP sessions (una por hilo para evitar contención) ──────────
_tls = threading.local()

def _get_session():
    """Devuelve una sesión requests específica del hilo actual."""
    if not hasattr(_tls, "session"):
        s = requests.Session()
        s.auth = HTTPBasicAuth(WDSF_USER, WDSF_PASS)
        s.headers.update({"Accept": "application/json"})
        _tls.session = s
    return _tls.session

def wdsf_get(url, retries=2, delay=0.35):
    """GET a WDSF API URL usando sesión por hilo; devuelve JSON o None."""
    time.sleep(delay)
    session = _get_session()
    for attempt in range(retries + 1):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 200 and r.text.strip():
                return r.json()
            if r.status_code == 404:
                return None
            if attempt < retries:
                time.sleep(1)
        except Exception as e:
            if attempt < retries:
                time.sleep(1)
            else:
                print(f"  ERROR GET {url}: {e}", flush=True)
    return None

# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_licenses(person_data):
    """Extrae nivel de licencia, disciplinas y estado de un perfil de persona."""
    result = {"level": None, "disciplines": [], "expires": None, "status": None}
    if not person_data or "licenses" not in person_data:
        return result
    for lic in person_data.get("licenses", []):
        if lic.get("type") == "Adjudicator" and lic.get("division") == "General":
            result["status"]  = lic.get("status")
            result["expires"] = (lic.get("expiresOn") or "")[:10] or None
            levels_found = set()
            discs = []
            for d in lic.get("disciplines", []):
                m = re.search(r'^([\w\s]+)\s+\(([ABC])\)', d)
                if m:
                    disc_name = m.group(1).strip()
                    level = m.group(2)
                    levels_found.add(level)
                    if disc_name not in ("PD Latin","PD Standard","PD Ten Dance"):
                        discs.append(disc_name)
            result["disciplines"] = discs
            if levels_found:
                result["level"] = sorted(levels_found)[0]
    return result

def map_disciplines(raw_discs):
    """Map raw discipline strings to Standard / Latin / Combined."""
    mapped = set()
    for d in raw_discs:
        dl = d.lower()
        if "standard" in dl:
            mapped.add("Standard")
        elif "latin" in dl:
            mapped.add("Latin")
        elif "ten dance" in dl or "combined" in dl:
            mapped.add("Combined")
    return list(mapped)

def get_min_from_official(official):
    """Extract WDSF MIN integer from official's link list."""
    for link in official.get("link", []):
        if "person" in link.get("rel", ""):
            m = re.search(r'/person/(\d+)', link.get("href",""))
            if m:
                return int(m.group(1))
    return None

# ── Main sync logic ───────────────────────────────────────────────────────────

def run_sync(years=None, db_path=None, log=print, workers=MAX_WORKERS):
    """
    Full judge sync — versión paralelizada con ThreadPoolExecutor.

    Parameters
    ----------
    years    : list of int/str, e.g. [2025, 2026]. Defaults to current + previous year.
    db_path  : path to SQLite DB. Auto-detected if None.
    log      : callable for progress messages (default: print)
    workers  : número de hilos paralelos para llamadas a la API (default: MAX_WORKERS)

    Returns
    -------
    dict con claves: new_judges, updated_judges, skipped, errors, competitions_scanned,
                     unmatched_nominations_fixed
    """
    if years is None:
        today = date.today()
        years = [today.year - 1, today.year]
    years = [str(y) for y in years]

    if db_path is None:
        db_path = find_db()

    log(f"WDSF Judge Sync started. Years: {years}")

    # ── Conectar DB ──────────────────────────────────────────────────────────
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    existing_mins = set(
        r[0] for r in conn.execute(
            "SELECT wdsf_min FROM judges WHERE wdsf_min IS NOT NULL"
        ).fetchall()
    )
    log(f"Judges in DB: {len(existing_mins)}")

    # ── Descargar lista de competiciones ─────────────────────────────────────
    log(f"Fetching competition list for {', '.join(years)}...")
    competitions = []
    for year in years:
        for status in ("Closed", "Announced"):
            url = f"{WDSF_BASE}/competition?status={status}&from={year}-01-01&to={year}-12-31"
            data = wdsf_get(url, delay=0.2)
            if data:
                competitions.extend(data)
                log(f"  {year} {status}: {len(data)}")

    seen_ids = set()
    unique_comps = []
    for c in competitions:
        cid = c.get("id")
        if cid and cid not in seen_ids:
            seen_ids.add(cid)
            unique_comps.append(c)
    log(f"Competitions: {len(unique_comps)}")

    # ── PASO 2: Extraer oficiales EN PARALELO ─────────────────────────────────
    log(f"Scanning officials from {len(unique_comps)} competitions "
        f"using {workers} parallel workers...")

    all_seen_mins  = set()
    new_mins       = set()
    official_data  = {}   # min_id → {name, country}
    errors_comps   = 0
    _counter       = [0]
    _lock          = threading.Lock()

    def _fetch_officials(comp):
        """Descarga oficiales de una competición. Ejecutado en pool de hilos."""
        comp_id = comp.get("id")
        officials = wdsf_get(f"{WDSF_BASE}/official?competitionId={comp_id}", delay=0.35)

        local_mins   = set()
        local_offdata = {}

        if officials is None:
            return None  # señal de error

        for off in officials:
            if "Adjudicator" not in off.get("Name", ""):
                continue
            min_id = get_min_from_official(off)
            if not min_id:
                continue
            local_mins.add(min_id)
            if min_id not in existing_mins:
                local_offdata[min_id] = {
                    "name":    off.get("Name", ""),
                    "country": off.get("country", ""),
                }
        return local_mins, local_offdata

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(_fetch_officials, comp): comp
                      for comp in unique_comps}

        for future in concurrent.futures.as_completed(future_map):
            result = future.result()
            with _lock:
                _counter[0] += 1
                if _counter[0] % 500 == 0 or _counter[0] == len(unique_comps):
                    log(f"  [{_counter[0]}/{len(unique_comps)}] "
                        f"new_mins_so_far={len(new_mins)}", flush=True)
                if result is None:
                    errors_comps += 1
                else:
                    local_mins, local_offdata = result
                    all_seen_mins.update(local_mins)
                    for mid in local_offdata:
                        if mid not in existing_mins:
                            new_mins.add(mid)
                            official_data[mid] = local_offdata[mid]

    log(f"Unique MINs seen: {len(all_seen_mins)}")
    log(f"New judges to add: {len(new_mins)}")
    if errors_comps:
        log(f"Competition fetch errors: {errors_comps}")

    # ── PASO 3a: Descargar perfiles de jueces nuevos EN PARALELO ─────────────
    log(f"Downloading {len(new_mins)} person profiles in parallel ({workers} workers)...")

    person_cache = {}   # min_id → person_data (o None)
    _p_counter   = [0]

    def _fetch_person(min_id):
        return min_id, wdsf_get(f"{WDSF_BASE}/person/{min_id}", delay=0.35)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(_fetch_person, mid): mid
                      for mid in sorted(new_mins)}
        for future in concurrent.futures.as_completed(future_map):
            mid, person = future.result()
            with _lock:
                person_cache[mid] = person
                _p_counter[0] += 1
                if _p_counter[0] % 50 == 0:
                    log(f"  Profiles downloaded: {_p_counter[0]}/{len(new_mins)}")

    # ── PASO 3b: Insertar en BD (secuencial para SQLite) ─────────────────────
    log(f"Inserting into DB...")

    inserted = 0
    skipped  = 0
    errors   = 0
    today_str = date.today().isoformat()

    for idx, min_id in enumerate(sorted(new_mins)):
        person = person_cache.get(min_id)

        if person is None:
            off = official_data.get(min_id, {})
            name_parts = off.get("name", "").split()
            fn = name_parts[0] if name_parts else ""
            ln = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
            cc = off.get("country", "")
            country_name = COUNTRY_CODE_MAP.get(cc.upper(), cc)
            if not fn and not ln:
                skipped += 1
                continue
            record = {
                "wdsf_min": min_id, "first_name": fn[:100], "last_name": ln[:100],
                "nationality": country_name[:100], "representing": country_name[:100],
                "license_type": None, "license_valid_until": None,
                "disciplines": "", "active": False,
                "judging_world_championships": 0, "judging_grand_slams": 0,
                "judging_continental_championships": 0,
            }
        else:
            lic = parse_licenses(person)
            discs = map_disciplines(lic["disciplines"])
            is_active = (lic["status"] == "Active") and (
                not lic["expires"] or lic["expires"] >= today_str
            )
            record = {
                "wdsf_min":           min_id,
                "first_name":         (person.get("name")        or "")[:100],
                "last_name":          (person.get("surname")     or "")[:100],
                "nationality":        (person.get("nationality") or "")[:100],
                "representing":       (person.get("country")     or "")[:100],
                "license_type":       lic["level"],
                "license_valid_until": lic["expires"],
                "disciplines":        ",".join(discs),
                "active":             1 if is_active else 0,
                "judging_world_championships":       0,
                "judging_grand_slams":               0,
                "judging_continental_championships": 0,
            }

        try:
            conn.execute("""
                INSERT INTO judges
                    (wdsf_min, first_name, last_name, nationality, representing,
                     license_type, license_valid_until, disciplines, active,
                     judging_world_championships, judging_grand_slams,
                     judging_continental_championships)
                VALUES
                    (:wdsf_min, :first_name, :last_name, :nationality, :representing,
                     :license_type, :license_valid_until, :disciplines, :active,
                     :judging_world_championships, :judging_grand_slams,
                     :judging_continental_championships)
                ON CONFLICT(wdsf_min) DO NOTHING
            """, record)
            conn.commit()
            existing_mins.add(min_id)
            inserted += 1
        except Exception as e:
            errors += 1
            log(f"  ERROR inserting MIN {min_id}: {e}")

    log(f"Inserted: {inserted}, skipped: {skipped}, errors: {errors}")

    # ── PASO 3c: Actualizar licencias de jueces existentes ────────────────────
    log(f"Refreshing licenses for existing judges seen in recent competitions...")
    existing_to_update = all_seen_mins & existing_mins
    updated = 0

    needs_update = conn.execute(
        "SELECT id, wdsf_min FROM judges WHERE wdsf_min IS NOT NULL"
        " AND wdsf_min IN ({}) AND (license_type IS NULL OR license_valid_until < ?)".format(
            ",".join("?" * len(existing_to_update)),
        ),
        list(existing_to_update) + [today_str]
    ).fetchall() if existing_to_update else []

    log(f"  Licenses to refresh: {len(needs_update)}")

    # Descargar perfiles en paralelo, luego actualizar en serie
    refresh_cache = {}
    if needs_update:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            fut_map = {executor.submit(_fetch_person, row["wdsf_min"]): row
                       for row in needs_update}
            for future in concurrent.futures.as_completed(fut_map):
                mid, person = future.result()
                refresh_cache[mid] = person

    for row in needs_update:
        person = refresh_cache.get(row["wdsf_min"])
        if not person:
            continue
        lic = parse_licenses(person)
        discs = map_disciplines(lic["disciplines"])
        is_active = (lic["status"] == "Active") and (
            not lic["expires"] or lic["expires"] >= today_str
        )
        try:
            conn.execute("""
                UPDATE judges SET license_type=?, license_valid_until=?,
                    disciplines=?, active=?
                WHERE id=?
            """, (lic["level"], lic["expires"], ",".join(discs),
                  1 if is_active else 0, row["id"]))
            conn.commit()
            updated += 1
        except Exception as e:
            log(f"  ERROR updating id={row['id']}: {e}")

    log(f"  Licenses updated: {updated}")

    # ── PASO 4: Backfill nominations sin judge_id ─────────────────────────────
    log(f"Backfilling unmatched nominations...")

    unmatched_rows = conn.execute(
        "SELECT id, judge_name, judge_country FROM official_nominations WHERE judge_id IS NULL"
    ).fetchall()

    fixed_nominations = 0
    for row in unmatched_rows:
        nm = (row["judge_name"]    or "").strip()
        cc = (row["judge_country"] or "").strip().upper()
        cf = COUNTRY_CODE_MAP.get(cc, cc)
        parts = nm.split()
        if len(parts) < 2:
            continue
        jid = None
        last, first = parts[-1], parts[0]

        for ln, fn in [(last, first), (first, last)]:
            r = conn.execute(
                "SELECT id FROM judges WHERE UPPER(last_name)=? AND UPPER(first_name) LIKE ?"
                " AND (UPPER(representing)=? OR UPPER(nationality)=?)",
                (ln.upper(), fn.upper()+"%", cf.upper(), cf.upper())
            ).fetchall()
            if len(r) == 1:
                jid = r[0]["id"]; break

        if not jid and len(parts) >= 3:
            cln = " ".join(parts[1:])
            for ln, fn in [(cln, first), (first, cln)]:
                r = conn.execute(
                    "SELECT id FROM judges WHERE UPPER(last_name)=? AND UPPER(first_name) LIKE ?"
                    " AND (UPPER(representing)=? OR UPPER(nationality)=?)",
                    (ln.upper(), fn.upper()+"%", cf.upper(), cf.upper())
                ).fetchall()
                if len(r) == 1:
                    jid = r[0]["id"]; break

        if not jid:
            for ln in [last, first]:
                r = conn.execute(
                    "SELECT id FROM judges WHERE UPPER(last_name)=?"
                    " AND (UPPER(representing)=? OR UPPER(nationality)=?)",
                    (ln.upper(), cf.upper(), cf.upper())
                ).fetchall()
                if len(r) == 1:
                    jid = r[0]["id"]; break

        if jid:
            conn.execute(
                "UPDATE official_nominations SET judge_id=? WHERE id=?",
                (jid, row["id"])
            )
            fixed_nominations += 1

    if fixed_nominations:
        conn.commit()

    log(f"  Nominations fixed: {fixed_nominations}")

    conn.close()

    result = {
        "new_judges":                  inserted,
        "updated_judges":              updated,
        "skipped":                     skipped,
        "errors":                      errors,
        "competitions_scanned":        len(unique_comps),
        "unmatched_nominations_fixed": fixed_nominations,
        "years":                       years,
        "workers_used":                workers,
    }

    log(f"Done. New judges: {inserted}, Nominations fixed: {fixed_nominations}")
    return result


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sincronizar jueces con WDSF API")
    parser.add_argument("--years", nargs="+", type=int,
                        default=[date.today().year - 1, date.today().year],
                        help="Años a analizar (ej: 2024 2025 2026)")
    parser.add_argument("--db", type=str, default=None,
                        help="Ruta a la base de datos SQLite")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS,
                        help=f"Hilos paralelos para llamadas API (default: {MAX_WORKERS})")
    args = parser.parse_args()
    run_sync(years=args.years, db_path=args.db, workers=args.workers)
