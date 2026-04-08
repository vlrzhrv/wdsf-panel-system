from flask import Flask, jsonify, request, send_from_directory, Response
from flask_cors import CORS
import sqlite3, os, requests, shutil
from requests.auth import HTTPBasicAuth
from datetime import date

# ── Directorio raíz de la app (donde está este archivo) ──────────────────────
APP_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Base de datos ─────────────────────────────────────────────────────────────
# En Railway: volumen persistente montado en /data
# En local: ~/wdsf_app/wdsf_panel.db
VOLUME_DB = "/data/wdsf_panel.db"
BUNDLE_DB = os.path.join(APP_DIR, "wdsf_panel.db")
LOCAL_DB  = os.path.expanduser("~/wdsf_app/wdsf_panel.db")

if os.path.isdir("/data"):                         # Railway volume detectado
    if not os.path.exists(VOLUME_DB) and os.path.exists(BUNDLE_DB):
        shutil.copy(BUNDLE_DB, VOLUME_DB)          # primera vez: copiar DB inicial
    DB = VOLUME_DB
elif os.path.exists(BUNDLE_DB):
    DB = BUNDLE_DB                                  # directorio local de la app
else:
    DB = LOCAL_DB                                   # fallback desarrollo

# ── Contraseña de acceso (opcional) ──────────────────────────────────────────
# En Railway: añade la variable de entorno APP_PASSWORD en el panel de Railway.
# Si no está definida, el acceso es abierto.
APP_PASSWORD = os.environ.get("APP_PASSWORD", "")
APP_USER     = os.environ.get("APP_USER", "wdsf")

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "wdsf-panel-secret-2024")
CORS(app)

@app.before_request
def require_password():
    """Protección básica por contraseña si APP_PASSWORD está definida."""
    if not APP_PASSWORD:
        return None                                 # sin contraseña → acceso libre
    auth = request.authorization
    if not auth or auth.password != APP_PASSWORD:
        return Response(
            "Restricted access — enter username and password.",
            401,
            {"WWW-Authenticate": 'Basic realm="WDSF Panel System"'}
        )

# ── WDSF API credentials ──────────────────────────────────────────────────────
WDSF_USER = os.environ.get("WDSF_USER", "")
WDSF_PASS = os.environ.get("WDSF_PASS", "")
WDSF_BASE = "https://services.worlddancesport.org/api/1"

# Country name normalization (WDSF API names → standard names)
COUNTRY_NORMALIZE = {
    "Czechia": "Czech Republic",
    "Czech republic": "Czech Republic",
    "Türkiye": "Turkey",
    "Republic of Korea": "South Korea",
    "Korea, Republic of": "South Korea",
    "Korea": "South Korea",
    "China, People's Republic of": "China",
    "People's Republic of China": "China",
    "Chinese Taipei": "Taiwan",
    "Great Britain": "United Kingdom",
    "England": "United Kingdom",
    "Scotland": "United Kingdom",
    "Wales": "United Kingdom",
    "USA": "United States",
    "United States of America": "United States",
    "Brasil": "Brazil",
    "Slovak Republic": "Slovakia",
    "The Netherlands": "Netherlands",
    "Holland": "Netherlands",
    "Russian Federation": "Russia",
    "Byelorussia": "Belarus",
    "Belorussia": "Belarus",
    "Hong Kong, China": "Hong Kong",
    "Chinese Taipei": "Taiwan",
    "Taipei,China": "Taiwan",
    "Bosnia and Herzegovina": "Bosnia",
    "Bosnia & Herzegovina": "Bosnia",
    "Republic of Moldova": "Moldova",
}

def normalize_country(name):
    if not name:
        return ""
    return COUNTRY_NORMALIZE.get(name, name)

ZONES = {
    # West
    "Spain":"West","France":"West","Portugal":"West","Italy":"West","Switzerland":"West",
    "Monaco":"West","Andorra":"West","San Marino":"West",
    # Central
    "Germany":"Central","Austria":"Central","Netherlands":"Central","Belgium":"Central",
    "Poland":"Central","Czech Republic":"Central","Hungary":"Central","Slovakia":"Central",
    "Luxembourg":"Central",
    # East
    "Russia":"East","Ukraine":"East","Belarus":"East","Romania":"East","Bulgaria":"East",
    "Latvia":"East","Lithuania":"East","Estonia":"East","Moldova":"East","Georgia":"East",
    "Armenia":"East","Azerbaijan":"East","Kazakhstan":"East","Uzbekistan":"East",
    "Albania":"East","North Macedonia":"East",
    # North
    "United Kingdom":"North","Sweden":"North","Norway":"North","Denmark":"North",
    "Finland":"North","Iceland":"North","Ireland":"North",
    # South
    "Croatia":"South","Serbia":"South","Slovenia":"South","Greece":"South","Turkey":"South",
    "Bosnia":"South","Montenegro":"South","Cyprus":"South","Malta":"South",
    "Israel":"South",
    # Asia — dividida en sub-regiones
    # East Asia
    "China":"E.Asia","Japan":"E.Asia","South Korea":"E.Asia",
    "Taiwan":"E.Asia","Hong Kong":"E.Asia",
    # Southeast & South Asia
    "Thailand":"SE.Asia","Singapore":"SE.Asia","Malaysia":"SE.Asia",
    "Philippines":"SE.Asia","Indonesia":"SE.Asia","India":"SE.Asia",
    "Vietnam":"SE.Asia","Myanmar":"SE.Asia","Cambodia":"SE.Asia",
    # Oceania
    "Australia":"Oceania","New Zealand":"Oceania",
    # Americas
    "United States":"Americas","Canada":"Americas","Brazil":"Americas",
    "Argentina":"Americas","Mexico":"Americas","Colombia":"Americas","Chile":"Americas",
    "Cuba":"Americas","Venezuela":"Americas","Uruguay":"Americas",
    # Africa
    "South Africa":"Africa","Egypt":"Africa","Morocco":"Africa","Kenya":"Africa",
}

# Macro-zonas: agrupa las sub-regiones asiáticas bajo "Asia" para la regla del 50 %
MACRO_ZONE = {
    "E.Asia":  "Asia",
    "SE.Asia": "Asia",
    "Oceania": "Asia",
}
def macro_zone(zone):
    return MACRO_ZONE.get(zone, zone)

# Zona groups por continente
EUROPEAN_ZONES = {"West", "Central", "East", "North", "South"}
ASIAN_ZONES    = {"E.Asia", "SE.Asia", "Oceania"}

def get_event_region(event):
    """Detect the geographic region of a championship from name/type."""
    name  = (event.get("name")       or "").upper()
    etype = (event.get("event_type") or "").upper()
    if "WORLD CHAMPIONSHIP" in etype:
        return "World"
    if "WORLD" in name and "CHAMPIONSHIP" in name:
        return "World"
    if "ASIAN" in name or "ASIA" in name:
        return "Asia"
    if "EUROPEAN" in name or "EUROPEAN CHAMPIONSHIP" in etype:
        return "Europe"
    if "PAN AMERICAN" in name or "AMERICAN" in name:
        return "Americas"
    if "AFRICAN" in name or "AFRICA" in name:
        return "Africa"
    if "OCEANIA" in name:
        return "Oceania"
    return "World"   # Grand Slam, World Open, Open → global

def panel_zone_key(judge_zone, event_region):
    """
    For regional championships, collapse distant continents into one macro-zone
    so they don't over-contribute to panel diversity scoring.
    - Asian Championship  → all European sub-zones merge into "Europe"
    - European Championship → all Asian sub-zones merge into "Asia"
    - World               → keep all zones separate
    """
    if event_region == "Asia":
        if judge_zone in EUROPEAN_ZONES:
            return "Europe"   # all Europeans count as one zone
    elif event_region == "Europe":
        if judge_zone in ASIAN_ZONES:
            return "Asia"     # all Asians count as one zone
    return judge_zone         # keep original zone

# Rankings by discipline
RANKING_STD = ["Poland","Romania","Germany","Russia","Italy","Ukraine","France",
               "Czech Republic","Hungary","Austria","United Kingdom","Latvia",
               "Croatia","Bulgaria","Switzerland","Spain","Belarus","Netherlands",
               "Sweden","Slovenia"]
RANKING_LAT = ["France","Romania","Estonia","Italy","Germany","Russia","Poland",
               "Netherlands","United Kingdom","Czech Republic","Spain","Hungary",
               "Croatia","Latvia","Austria","Ukraine","Lithuania","Switzerland",
               "Portugal","Bulgaria"]
RANKING_TEN = ["Germany","Romania","Poland","Russia","Italy","United Kingdom","France",
               "Czech Republic","Austria","Hungary","Ukraine","Spain","Latvia",
               "Netherlands","Croatia","Switzerland","Bulgaria","Sweden","Belgium","Slovenia"]

# Regional rankings (for continental championships)
RANKING_STD_ASIA = ["China","South Korea","Japan","Hong Kong","Taiwan",
                    "Thailand","Singapore","Australia","Philippines","India","Malaysia"]
RANKING_LAT_ASIA = ["China","Thailand","Japan","South Korea","Singapore",
                    "Australia","Philippines","Malaysia","India","New Zealand"]
RANKING_STD_AMERICAS = ["United States","Canada","Brazil","Argentina","Mexico","Colombia","Chile"]
RANKING_LAT_AMERICAS = ["United States","Canada","Brazil","Argentina","Cuba","Mexico","Colombia"]
RANKING_STD_AFRICA   = ["South Africa","Egypt","Morocco","Kenya","Nigeria"]
RANKING_LAT_AFRICA   = ["South Africa","Egypt","Morocco","Kenya","Nigeria"]

def get_ranking_for_region(discipline, region):
    """Return ranking list filtered/adapted for the championship region.
    Prioriza datos de la tabla country_rankings en la BD si existen;
    si no, usa los rankings hardcodeados como fallback."""
    if "Standard" in discipline:
        disc_key = "Standard"
    elif "Latin" in discipline:
        disc_key = "Latin"
    else:
        disc_key = "Ten Dance"

    db_key = (disc_key, region)
    if _DB_RANKINGS and db_key in _DB_RANKINGS:
        return _DB_RANKINGS[db_key]

    if region == "Asia":
        return RANKING_STD_ASIA if "Standard" in discipline else RANKING_LAT_ASIA
    if region == "Americas":
        return RANKING_STD_AMERICAS if "Standard" in discipline else RANKING_LAT_AMERICAS
    if region == "Africa":
        return RANKING_STD_AFRICA  if "Standard" in discipline else RANKING_LAT_AFRICA
    if region == "Europe":
        base = RANKING_STD if "Standard" in discipline else (RANKING_LAT if "Latin" in discipline else RANKING_TEN)
        return [c for c in base if ZONES.get(c,"") in EUROPEAN_ZONES]
    if "Latin" in discipline:    return RANKING_LAT
    if "Standard" in discipline: return RANKING_STD
    return RANKING_TEN

def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Crea/migra TODAS las tablas. Seguro ejecutar en cada arranque.
    Garantiza que un deploy en Railway con volumen vacio arranca sin errores."""
    conn = get_db()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS judges (
            id                                INTEGER PRIMARY KEY AUTOINCREMENT,
            wdsf_min                          INTEGER UNIQUE,
            first_name                        TEXT,
            last_name                         TEXT,
            nationality                       TEXT,
            representing                      TEXT,
            license_type                      TEXT,
            license_valid_until               TEXT,
            disciplines                       TEXT,
            judging_world_championships       INTEGER DEFAULT 0,
            judging_grand_slams               INTEGER DEFAULT 0,
            judging_continental_championships INTEGER DEFAULT 0,
            active                            INTEGER DEFAULT 1,
            notes                             TEXT,
            career_level                      TEXT DEFAULT 'national',
            zone                              TEXT,
            std_panels_count                  INTEGER DEFAULT 0,
            lat_panels_count                  INTEGER DEFAULT 0,
            specialty                         TEXT,
            primary_discipline                TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT,
            date        TEXT,
            location    TEXT,
            country     TEXT,
            discipline  TEXT,
            age_group   TEXT,
            division    TEXT,
            event_type  TEXT,
            is_ags      INTEGER DEFAULT 0,
            coefficient REAL DEFAULT 1.0,
            status      TEXT DEFAULT 'pending',
            wdsf_id     TEXT,
            url         TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS panel_assignments (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id               INTEGER REFERENCES events(id),
            judge_id               INTEGER REFERENCES judges(id),
            role                   TEXT DEFAULT 'adjudicator',
            position               INTEGER,
            score                  REAL,
            status                 TEXT DEFAULT 'proposed',
            competition_identifier TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS official_nominations (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            wdsf_comp_id      INTEGER NOT NULL,
            comp_name         TEXT,
            comp_date         TEXT,
            comp_discipline   TEXT,
            comp_location     TEXT,
            comp_url          TEXT,
            judge_name        TEXT,
            judge_country     TEXT,
            judge_id          INTEGER REFERENCES judges(id),
            role              TEXT,
            status            TEXT,
            section           TEXT,
            position          TEXT,
            synced_at         TEXT,
            UNIQUE(wdsf_comp_id, judge_name, section)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS judge_marks_history (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            competition_slug TEXT NOT NULL,
            competition_name TEXT,
            competition_date TEXT,
            discipline       TEXT,
            round_num        INTEGER,
            judge_letter     TEXT,
            judge_name       TEXT,
            judge_country    TEXT,
            couple_num       TEXT,
            marks_count      INTEGER,
            scraped_at       TEXT,
            UNIQUE(competition_slug, round_num, judge_letter, couple_num)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS scraped_competitions (
            slug             TEXT PRIMARY KEY,
            competition_name TEXT,
            competition_date TEXT,
            discipline       TEXT,
            n_rounds         INTEGER,
            n_judges         INTEGER,
            n_couples        INTEGER,
            scraped_at       TEXT
        )
    """)

    existing_corr_cols = [r[1] for r in conn.execute(
        "PRAGMA table_info(judge_pair_correlations)"
    ).fetchall()]
    if existing_corr_cols and "discipline" not in existing_corr_cols:
        conn.execute("DROP TABLE judge_pair_correlations")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS judge_pair_correlations (
            judge_name_a      TEXT NOT NULL,
            judge_name_b      TEXT NOT NULL,
            discipline        TEXT NOT NULL DEFAULT 'Unknown',
            correlation       REAL,
            n_competitions    INTEGER,
            n_data_points     INTEGER,
            last_updated      TEXT,
            PRIMARY KEY (judge_name_a, judge_name_b, discipline)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS country_rankings (
            discipline  TEXT NOT NULL,
            region      TEXT NOT NULL,
            rank_order  INTEGER NOT NULL,
            country     TEXT NOT NULL,
            PRIMARY KEY (discipline, region, rank_order)
        )
    """)

    def _add_col(table, col, col_def):
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        if col not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}")

    _add_col("judges", "primary_discipline",  "TEXT")
    _add_col("judges", "std_panels_count",    "INTEGER DEFAULT 0")
    _add_col("judges", "lat_panels_count",    "INTEGER DEFAULT 0")
    _add_col("judges", "specialty",           "TEXT")
    _add_col("events", "wdsf_id",             "TEXT")
    _add_col("events", "url",                 "TEXT")
    _add_col("panel_assignments", "competition_identifier", "TEXT")

    conn.commit()
    conn.close()

init_db()


def _load_rankings_from_db():
    """Lee rankings de la BD. Si hay datos, los devuelve; si no, {} para usar hardcodeados."""
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT discipline, region, country FROM country_rankings ORDER BY discipline, region, rank_order"
        ).fetchall()
        conn.close()
        if not rows:
            return {}
        result = {}
        for r in rows:
            key = (r["discipline"], r["region"])
            result.setdefault(key, []).append(r["country"])
        return result
    except Exception:
        return {}

_DB_RANKINGS = _load_rankings_from_db()

# ── WDSF status sync state ──────────────────────────────────────────────
from concurrent.futures import ThreadPoolExecutor as _TPE, as_completed as _as_completed
import threading as _threading
_sync_state = {"running": False, "last_result": None}

def judge_dict(row):
    d = dict(row)
    raw_discs = d.get("disciplines","") or ""
    d["disciplines"] = [x.strip() for x in raw_discs.split(",") if x.strip()] if raw_discs else []
    # Solo disciplinas ballroom relevantes para mostrar
    d["ballroom_disciplines"] = [x for x in d["disciplines"]
                                  if x in ("Standard","Latin","Combined","Ten Dance")]
    country = normalize_country(d.get("representing") or d.get("nationality",""))
    d["representing_normalized"] = country
    d["zone"] = ZONES.get(country, "Other")
    d["std_panels_count"]    = d.get("std_panels_count") or 0
    d["lat_panels_count"]    = d.get("lat_panels_count") or 0
    d["specialty"]           = d.get("specialty") or "Unknown"
    d["primary_discipline"]  = d.get("primary_discipline") or None
    d["career_level"]        = d.get("career_level") or "national"
    return d

# Mapeo de disciplinas: nombre del evento → términos aceptados en el campo disciplines del juez
# "Combined" es el nombre WDSF para Ten Dance en la BD de jueces
DISC_ALIASES = {
    "Standard":  ["Standard"],
    "Latin":     ["Latin"],
    "Ten Dance": ["Combined", "Ten Dance"],
    "Combined":  ["Combined", "Ten Dance"],
}

# Disciplinas que admiten asignación de panel (excluye Hip Hop, Acrobatic, etc.)
BALLROOM_DISCIPLINES = {"Standard", "Latin", "Ten Dance", "Combined"}

def judge_has_discipline(judge, discipline):
    """
    Devuelve True si el juez tiene la disciplina requerida.
    - Jueces sin disciplinas definidas → SIEMPRE False (nunca asignables)
    - Ten Dance / Combined son equivalentes
    """
    discs = judge.get("disciplines", [])
    if not discs:
        return False
    accepted = DISC_ALIASES.get(discipline, [discipline])
    discs_set = {d.strip() for d in discs if d.strip()}
    return bool(discs_set & set(accepted))

def get_ranking(discipline):
    if "Latin" in discipline:
        return RANKING_LAT
    if "Standard" in discipline:
        return RANKING_STD
    return RANKING_TEN

def _judge_display_name(j):
    """Return 'LastName FirstName' for correlation lookup."""
    last  = (j.get("last_name") or "").strip()
    first = (j.get("first_name") or "").strip()
    return f"{last} {first}".strip() if last else first


def calc_score(j, event, assigned_zones, return_breakdown=False,
               assigned_panel_names=None, corr_map=None):
    score = 0
    discipline = (event.get("discipline") or "Standard")
    breakdown  = {}

    # A. Career as dancer (0-30)
    # Hierarchy: Adult World/Continental > Professional > Youth/Other > Open/GS > International
    career_pts = {
        # Adult World Championship
        "world_champion_adult":        30,
        "world_silver_adult":          28,
        "world_bronze_adult":          26,
        "world_finalist_adult":        23,
        "world_participant_adult":     18,
        # Adult Continental Championship
        "continental_champion_adult":  20,
        "continental_silver_adult":    18,
        "continental_bronze_adult":    16,
        "continental_finalist_adult":  13,
        # Professional World/Continental
        "pro_world_champion":          17,
        "pro_world_finalist":          14,
        "pro_continental_champion":    12,
        "pro_continental_finalist":    10,
        # Youth/Other World Championship
        "world_champion_youth":        12,
        "world_silver_youth":          10,
        "world_bronze_youth":           9,
        "world_finalist_youth":         8,
        # Youth/Other Continental
        "continental_champion_youth":   7,
        "continental_silver_youth":     6,
        "continental_bronze_youth":     5,
        "continental_finalist_youth":   4,
        # Open / Grand Slam
        "world_open_finalist":          6,
        "grand_slam_finalist":          5,
        # General
        "international":                3,
        "national":                     1,
        # Legacy labels (old data)
        "world_champion":              30,
        "world_finalist":              23,
        "continental_champion":        20,
        "continental_finalist":        13,
        "world_open_finalist_old":      6,
    }
    cl = j.get("career_level") or "national"
    a = career_pts.get(cl, 1)
    score += a
    breakdown["career"] = {"pts": a, "max": 30, "detail": cl}

    # B. Experience as judge (0-30)
    wch = j.get("judging_world_championships") or 0
    gs  = j.get("judging_grand_slams") or 0
    cch = j.get("judging_continental_championships") or 0
    b = min(30, wch * 6 + gs * 3 + cch * 2)
    score += b
    breakdown["experience"] = {"pts": b, "max": 30, "detail": f"WCH×{wch} + GS×{gs} + CCH×{cch}"}

    # Detect event region once (used in C and D)
    event_region = get_event_region(event)

    # C. Country ranking (0-20) — uses regional ranking for continental championships
    country = normalize_country(j.get("representing") or j.get("nationality",""))
    ranking = get_ranking_for_region(discipline, event_region)
    if country in ranking:
        pos = ranking.index(country)
        c = 20 if pos < 3 else 15 if pos < 6 else 10 if pos < 10 else 5 if pos < 20 else 2
        rank_label = f"#{pos+1} in {event_region} {discipline} ranking"
    else:
        c = 0
        rank_label = f"Not in {event_region} {discipline} ranking"
    score += c
    breakdown["ranking"] = {"pts": c, "max": 20, "detail": rank_label}

    # D. Geographic distribution (0-20)
    # For regional championships: collapse distant continents into macro-zones
    # so one continent can't fill all 5 "new zone" slots.
    # For World Championships: bonus if Asia not yet represented (at least 1 Asian judge).
    zone = ZONES.get(country, "Other")
    pzone        = panel_zone_key(zone, event_region)
    assigned_pzones = [panel_zone_key(ZONES.get(normalize_country(z), z), event_region)
                       for z in assigned_zones]

    if pzone not in assigned_pzones:
        d = 20
        zone_label = f"{pzone} — new zone"
    elif assigned_pzones.count(pzone) < 2:
        d = 10
        zone_label = f"{pzone} — 1 judge already"
    else:
        d = 0
        zone_label = f"{pzone} — zone already covered"

    # World Championship: guarantee at least 1 Asian judge
    if event_region == "World" and zone in ASIAN_ZONES:
        asia_in_panel = any(ZONES.get(normalize_country(z),"") in ASIAN_ZONES
                            for z in assigned_zones)
        if not asia_in_panel:
            d = max(d, 18)   # strong bonus to bring in first Asian judge
            zone_label += " ⭐ Asia guarantee"

    score += d
    breakdown["zone"] = {"pts": d, "max": 20, "detail": zone_label}

    # E. Specialty bonus/penalty (from dancer career — independent of panel history)
    specialty = j.get("specialty") or "Unknown"
    e = 0
    if specialty in ("Standard", "Latin", "10-Dance"):
        if discipline == "Standard" and specialty == "Standard":
            e = 10; spec_label = "Specialist in Standard ✓"
        elif discipline == "Latin" and specialty == "Latin":
            e = 10; spec_label = "Specialist in Latin ✓"
        elif discipline in ("Standard", "Latin") and specialty == "10-Dance":
            e = 5;  spec_label = "10-Dance dancer (versatile)"
        elif discipline == "Standard" and specialty == "Latin":
            e = -5; spec_label = "Latin specialist — mismatch"
        elif discipline == "Latin" and specialty == "Standard":
            e = -5; spec_label = "Standard specialist — mismatch"
        else:
            e = 0;  spec_label = f"Specialty: {specialty}"
    else:
        spec_label = "No specialty data (N/A)"
    score += e
    breakdown["specialty"] = {"pts": e, "max": 10, "detail": spec_label}

    # F. Panel independence — based on historical correlation (0-15)
    # 0 = perfectly correlated with panel, 15 = perfectly independent
    f = 0
    ind_label = "No historical data yet"
    if assigned_panel_names and corr_map:
        j_name = _judge_display_name(j)
        corrs = []
        for pname in assigned_panel_names:
            c = corr_map.get((j_name, pname)) or corr_map.get((pname, j_name))
            if c is not None:
                corrs.append(c)
        if corrs:
            avg_corr = round(sum(corrs) / len(corrs), 3)
            # Map [-1, 1] → [15, 0]: score = 7.5 * (1 - avg_corr)
            f = round(max(0, min(15, 7.5 * (1 - avg_corr))), 1)
            ind_label = f"Avg r={avg_corr:+.2f} with {len(corrs)} panel members → {f}/15 independence"
        else:
            ind_label = "No shared competitions with current panel"
    score += f
    breakdown["independence"] = {"pts": f, "max": 15, "detail": ind_label}

    score = round(score, 2)
    if return_breakdown:
        return score, breakdown
    return score

# ─── WDSF API proxy ───────────────────────────────────────────────────────────

@app.route("/api/wdsf/<path:endpoint>")
def wdsf_proxy(endpoint):
    """Proxy WDSF API. Para /competition aplica filtrado server-side porque
    la API v1 ignora status/discipline/take."""
    try:
        params = dict(request.args)

        if endpoint.lower() in ("competition", "competition/"):
            today = date.today().isoformat()
            req_status     = params.pop("status",     None)
            req_discipline = params.pop("discipline", None)
            req_take       = params.pop("take",       None)

            if req_status == "Upcoming":
                params.setdefault("from", today)
            elif req_status == "Closed":
                params.setdefault("to", today)
                params["status"] = "Closed"

            url  = f"{WDSF_BASE}/competition"
            resp = requests.get(url, auth=HTTPBasicAuth(WDSF_USER, WDSF_PASS),
                                params=params, timeout=20,
                                headers={"Accept": "application/json"})
            try:
                data = resp.json()
            except Exception:
                return jsonify({"status": resp.status_code, "raw": resp.text[:2000]}), 200

            if not isinstance(data, list):
                return jsonify(data), resp.status_code

            if req_status == "Upcoming":
                def _comp_date(c):
                    parts = c.get("name","").rsplit(" - ", 1)
                    if len(parts) == 2:
                        return parts[1].strip().replace("/", "-")
                    return ""
                data = [c for c in data if _comp_date(c) >= today]

            if req_discipline:
                disc_upper = req_discipline.upper()
                disc_keywords = {
                    "LAT": ["LATIN"], "STD": ["STANDARD"],
                    "TEN": ["TEN DANCE", "10 DANCE"],
                }.get(disc_upper, [disc_upper])
                data = [c for c in data
                        if any(kw in c.get("name","").upper() for kw in disc_keywords)]

            if req_take:
                try: data = data[:int(req_take)]
                except (ValueError, TypeError): pass

            return jsonify(data), resp.status_code

        url  = f"{WDSF_BASE}/{endpoint}"
        resp = requests.get(url, auth=HTTPBasicAuth(WDSF_USER, WDSF_PASS),
                            params=params, timeout=15,
                            headers={"Accept": "application/json"})
        try:
            return jsonify(resp.json()), resp.status_code
        except Exception:
            return jsonify({"status": resp.status_code, "raw": resp.text[:2000]}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

_AGE_GROUP_SLUG = {
    "ADULT":        "Adult",
    "JUNIOR I":     "Junior-I",
    "JUNIOR II":    "Junior-II",
    "JUNIOR":       "Junior",
    "YOUTH":        "Youth",
    "UNDER 21":     "Under-21",
    "SENIOR I":     "Senior-I",
    "SENIOR II":    "Senior-II",
    "SENIOR III":   "Senior-III",
    "SENIOR IV":    "Senior-IV",
    "SENIOR V":     "Senior-V",
    "JUVENILE I":   "Juvenile-I",
    "JUVENILE II":  "Juvenile-II",
    "JUVENILE":     "Juvenile",
    "RISING STARS": "Rising-Stars",
    "RISING STAR":  "Rising-Stars",
    "MIX":          "Mix",
}

# Age groups to EXCLUDE from correlation scraping (team / non-standard judging)
_AGE_SKIP = {"(TEAM)", "FORMATION", "SHOW DANCE", "PD ", "SOLO",
             "SYNCHRO", "CHOREOGR", "TEN DANCE", "8 DANCE", "6 DANCE"}

def _slug_from_api_comp(comp):
    """Construct WDSF website slug from API competition name + id.
    Handles International Open and regular Open across all age groups.
    Name format: "{TYPE} {DISCIPLINE}  {AGE_GROUP} - {City} - {Country} - YYYY/MM/DD"
    Slug format:  {Type}-{City}-{AgeGroup}-{Discipline}-{id}
    Returns None if pattern doesn't match or competition type not supported.
    """
    name = comp.get("name", "")
    comp_id = comp.get("id")
    if not name or not comp_id:
        return None

    name_upper = name.upper()

    # Skip non-couple disciplines (solo, formation, show dance, etc.)
    if any(skip in name_upper for skip in _AGE_SKIP):
        return None

    # Split on " - " to get: [type+disc+age, city, country, date]
    parts = [p.strip() for p in name.split(" - ")]
    if len(parts) < 4:
        return None
    header = parts[0].upper()
    city   = parts[1].strip()

    # Determine competition type
    if header.startswith("INTERNATIONAL OPEN"):
        type_slug = "International-Open"
        rest = header[len("INTERNATIONAL OPEN"):].strip()
    elif header.startswith("OPEN"):
        type_slug = "Open"
        rest = header[len("OPEN"):].strip()
    else:
        return None  # Not a standard open competition

    # Determine discipline (comes before age group in header)
    if "STANDARD" in rest:
        discipline_slug = "Standard"
        rest = rest.replace("STANDARD", "").strip()
    elif "LATIN" in rest:
        discipline_slug = "Latin"
        rest = rest.replace("LATIN", "").strip()
    else:
        return None  # Not Standard or Latin

    # Remaining text is the age group
    age_raw = rest.strip()
    age_slug = _AGE_GROUP_SLUG.get(age_raw)
    if not age_slug:
        return None  # Unknown age group

    city_slug = (city.replace(" ", "-").replace("'", "")
                     .replace(".", "").replace(",", "").replace("–", "-"))
    return f"{type_slug}-{city_slug}-{age_slug}-{discipline_slug}-{comp_id}"


@app.route("/api/list-wdsf-competitions", methods=["POST"])
def list_wdsf_competitions():
    """Query WDSF API for ALL Open Standard/Latin competitions (any age group).
    Fetches by quarter to avoid timeouts. Constructs slugs from name+id (no secondary calls).
    Body (optional): {year: 2026}
    """
    data = request.json or {}
    year = int(data.get("year", 2026))

    api_session = requests.Session()
    api_session.auth = HTTPBasicAuth(WDSF_USER, WDSF_PASS)
    api_session.headers.update({"Accept": "application/json"})

    conn = get_db()
    already_scraped = {r["slug"] for r in
                       conn.execute("SELECT slug FROM scraped_competitions").fetchall()}
    conn.close()

    from datetime import date as _date
    today_str = _date.today().isoformat()
    quarters = [
        (f"{year}-01-01", f"{year}-03-31"),
        (f"{year}-04-01", f"{year}-06-30"),
        (f"{year}-07-01", f"{year}-09-30"),
        (f"{year}-10-01", f"{year}-12-31"),
    ]

    all_results = []
    errors = []
    for q_from, q_to in quarters:
        # Use status=Closed only for past quarters; omit for future/current quarters
        if q_to < today_str:
            url = f"{WDSF_BASE}/competition?status=Closed&from={q_from}&to={q_to}"
        else:
            url = f"{WDSF_BASE}/competition?from={q_from}&to={q_to}"
        try:
            r = api_session.get(url, timeout=30)
            r.raise_for_status()
            comps = r.json()
            if not isinstance(comps, list):
                errors.append(f"{q_from}: unexpected response")
                continue
            for c in comps:
                slug = _slug_from_api_comp(c)
                if not slug:
                    # Only log failures for things that look like opens
                    name_upper = c.get("name", "").upper()
                    if ("OPEN" in name_upper and
                            ("STANDARD" in name_upper or "LATIN" in name_upper)):
                        errors.append(f"slug_fail: {c.get('name')}")
                    continue
                all_results.append({
                    "id":              c.get("id"),
                    "name":            c.get("name"),
                    "slug":            slug,
                    "already_scraped": slug in already_scraped,
                })
        except requests.exceptions.HTTPError as e:
            body = ""
            try: body = e.response.text[:300]
            except: pass
            errors.append(f"{q_from}: {e} | body={body}")
        except Exception as e:
            errors.append(f"{q_from}: {e}")

    new_slugs = [r for r in all_results if not r["already_scraped"]]
    return jsonify({
        "ok":      True,
        "year":    year,
        "total":   len(all_results),
        "new":     len(new_slugs),
        "results": all_results,
        "errors":  errors,
    })


_SLUG_SKIP_TERMS = {
    "solo", "formation", "show-dance", "show-latin", "show-standard",
    "choreogr", "ten-dance", "8-dance", "6-dance", "pd-", "synchro",
    "solo-standard", "solo-latin", "solo-waltz", "solo-tango",
    "solo-samba", "solo-cha", "solo-rumba", "solo-paso", "solo-jive",
}

def _slugs_from_event_page_all_ages(event_href):
    """Like _slugs_from_event_page but accepts ALL age groups (not just Adult).
    Filters: Open/International-Open, Standard or Latin, no Solo/Formation/Show/etc.
    Returns list of {slug, discipline, age_group, event_type}.
    """
    HEADERS = {"User-Agent": "Mozilla/5.0 WDSF-PanelSystem/1.0"}
    results = []
    seen = set()
    event_url = "https://www.worlddancesport.org" + event_href.split("#")[0]
    try:
        er = requests.get(event_url, timeout=15, headers=HEADERS)
        er.raise_for_status()
        from bs4 import BeautifulSoup
        esoup = BeautifulSoup(er.text, "html.parser")
        for a in esoup.find_all("a", href=True):
            href = a.get("href", "")
            if ("/Competitions/Results/" not in href
                    and "/Competitions/Marks/" not in href
                    and "/Competitions/Ranking/" not in href
                    and "/Competitions/Detail/" not in href):
                continue
            slug = href.rstrip("/").split("/")[-1]
            if not slug or slug in seen:
                continue
            seen.add(slug)
            sl = slug.lower()
            # Must be an Open (not Championship, GrandSlam, etc.)
            if not (sl.startswith("open-") or sl.startswith("international-open-")):
                continue
            # Must be Standard or Latin
            if "standard" not in sl and "latin" not in sl:
                continue
            # Skip unwanted types
            if any(skip in sl for skip in _SLUG_SKIP_TERMS):
                continue
            discipline = "Standard" if "standard" in sl else "Latin"
            results.append({"slug": slug, "discipline": discipline})
    except Exception:
        pass
    return results


@app.route("/api/scrape-calendar-year", methods=["POST"])
def scrape_calendar_year():
    """Scrape WDSF Calendar/Results page month by month for a given year.
    Finds all Open Standard/Latin competitions (all age groups).
    Body (optional): {year: 2026}
    """
    from bs4 import BeautifulSoup
    data = request.json or {}
    year = int(data.get("year", 2026))

    HEADERS = {"User-Agent": "Mozilla/5.0 WDSF-PanelSystem/1.0",
               "Accept": "text/html,application/xhtml+xml"}

    conn = get_db()
    already_scraped = {r["slug"] for r in
                       conn.execute("SELECT slug FROM scraped_competitions").fetchall()}
    conn.close()

    all_event_hrefs = set()
    errors = []

    from datetime import date as _date
    current_month = _date.today().month
    current_year  = _date.today().year

    for month in range(1, 13):
        # Skip future months beyond current (no results yet)
        if year > current_year or (year == current_year and month > current_month):
            continue
        url = f"https://www.worlddancesport.org/Calendar/Results?Month={month}&Year={year}"
        try:
            r = requests.get(url, timeout=30, headers=HEADERS)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                h = a["href"]
                if "/Events/" in h and "/Events/Granting" not in h:
                    all_event_hrefs.add(h.split("#")[0])
        except Exception as e:
            errors.append(f"Month {month}: {e}")

    all_results = []
    for ev_href in sorted(all_event_hrefs):
        try:
            comps = _slugs_from_event_page_all_ages(ev_href)
            for c in comps:
                c["already_scraped"] = c["slug"] in already_scraped
                c["event_href"] = ev_href
            all_results.extend(comps)
        except Exception as e:
            errors.append(f"{ev_href}: {e}")

    new_slugs = [c for c in all_results if not c["already_scraped"]]
    return jsonify({
        "ok":     True,
        "year":   year,
        "events": len(all_event_hrefs),
        "total":  len(all_results),
        "new":    len(new_slugs),
        "results": all_results,
        "errors": errors[:20],
    })


@app.route("/api/scan-intopen-2025", methods=["POST"])
def scan_intopen_2025():
    """Scan WDSF Calendar/Results page for 2025 World Ranking events,
    then call _slugs_from_event_page for each to collect International Open
    Adult Standard/Latin slugs not yet scraped.
    """
    from bs4 import BeautifulSoup

    HEADERS = {"User-Agent": "Mozilla/5.0 WDSF-PanelSystem/1.0",
               "Accept": "text/html,application/xhtml+xml"}

    conn = get_db()
    already_scraped = {r["slug"] for r in
                       conn.execute("SELECT slug FROM scraped_competitions").fetchall()}
    conn.close()

    # Fetch the Calendar/Results page with World Ranking filter for 2025
    found_events = set()
    errors = []

    try:
        r = requests.post(
            "https://www.worlddancesport.org/Calendar/Results",
            data={"CalendarTypeName": "World Ranking",
                  "DateFrom": "2025-01-01", "DateTo": "2025-12-31"},
            headers=HEADERS, timeout=30
        )
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            h = a["href"]
            if "/Events/" in h and "/Events/Granting" not in h:
                found_events.add(h.split("#")[0])
    except Exception as e:
        errors.append(f"Calendar fetch: {e}")

    # Also try GET with query params
    if not found_events:
        try:
            r = requests.get(
                "https://www.worlddancesport.org/Calendar/Results",
                params={"CalendarTypeName": "World Ranking",
                        "DateFrom": "2025-01-01", "DateTo": "2025-12-31"},
                headers=HEADERS, timeout=30
            )
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                h = a["href"]
                if "/Events/" in h and "/Events/Granting" not in h:
                    found_events.add(h.split("#")[0])
        except Exception as e:
            errors.append(f"Calendar GET: {e}")

    # For each event page, get competition slugs
    all_slugs = []
    scanned_events = 0
    for ev_href in sorted(found_events):
        try:
            comps = _slugs_from_event_page(ev_href)
            for c in comps:
                c["already_scraped"] = c["slug"] in already_scraped
                c["event_href"] = ev_href
            all_slugs.extend(comps)
            scanned_events += 1
        except Exception as e:
            errors.append(f"{ev_href}: {e}")

    # Filter to International Opens only (event_type = "Unknown" = not GS/WO/WCH)
    intopen = [c for c in all_slugs if c["event_type"] == "Unknown"]
    new_slugs = [c for c in intopen if not c["already_scraped"]]

    return jsonify({
        "ok": True,
        "events_found": len(found_events),
        "events_scanned": scanned_events,
        "intopen_total": len(intopen),
        "intopen_new": len(new_slugs),
        "new_slugs": new_slugs,
        "errors": errors[:10],
    })

# ─── Local API ────────────────────────────────────────────────────────────────

@app.route("/api/stats")
def stats():
    conn = get_db()
    total  = conn.execute("SELECT COUNT(*) FROM judges").fetchone()[0]
    active = conn.execute("SELECT COUNT(*) FROM judges WHERE active=1").fetchone()[0]
    lic_a  = conn.execute("SELECT COUNT(*) FROM judges WHERE license_type='A'").fetchone()[0]
    events_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    assigned = conn.execute("SELECT COUNT(*) FROM events WHERE status='assigned'").fetchone()[0]
    conn.close()
    return jsonify({
        "total_judges": total, "active_judges": active,
        "license_a": lic_a, "events": events_count, "assigned_events": assigned
    })

@app.route("/api/judges")
def judges():
    conn = get_db()
    q, params = "SELECT * FROM judges WHERE 1=1", []
    if request.args.get("active"):
        q += " AND active=1"
    if request.args.get("license"):
        q += " AND license_type=?"; params.append(request.args["license"])
    if request.args.get("discipline"):
        q += " AND disciplines LIKE ?"; params.append(f"%{request.args['discipline']}%")
    if request.args.get("search"):
        s = f"%{request.args['search']}%"
        q += " AND (first_name LIKE ? OR last_name LIKE ? OR nationality LIKE ? OR representing LIKE ?)"
        params += [s, s, s, s]
    rows = conn.execute(q + " ORDER BY last_name, first_name", params).fetchall()
    conn.close()
    return jsonify([judge_dict(r) for r in rows])

@app.route("/api/judges/<int:jid>", methods=["GET","PUT"])
def judge(jid):
    conn = get_db()
    if request.method == "PUT":
        data = request.json or {}
        EDITABLE = ["career_level", "zone", "notes", "active", "license_type",
                    "license_valid_until", "primary_discipline"]
        for field in EDITABLE:
            if field in data:
                conn.execute(f"UPDATE judges SET {field}=? WHERE id=?", (data[field], jid))
        conn.commit()
    row = conn.execute("SELECT * FROM judges WHERE id=?", (jid,)).fetchone()
    conn.close()
    return jsonify(judge_dict(row)) if row else (jsonify({"error":"Not found"}), 404)




# ── WDSF status sync ────────────────────────────────────────────────────────────────────────

def _run_wdsf_sync():
    """Background thread: checks WDSF API for each judge and updates active status."""
    _sync_state["running"] = True
    try:
        conn = get_db()
        judges = conn.execute(
            "SELECT id, wdsf_min, first_name, last_name, active FROM judges WHERE wdsf_min IS NOT NULL"
        ).fetchall()
        conn.close()

        changed, errors, checked = [], [], 0
        today = date.today().isoformat()

        def _check_one(j):
            try:
                resp = requests.get(
                    f"{WDSF_BASE}/adjudicator/{j[\'wdsf_min\']}",
                    auth=HTTPBasicAuth(WDSF_USER, WDSF_PASS),
                    timeout=10, headers={"Accept": "application/json"}
                )
                if resp.status_code != 200:
                    return None, f"{j[\'first_name\']} {j[\'last_name\']}: HTTP {resp.status_code}"
                data = resp.json()
                lic_type  = (data.get("licenseType")  or "").strip()
                lic_valid = (data.get("licenseValidUntil") or "").strip()[:10]
                should_active = bool(lic_type) and (not lic_valid or lic_valid >= today)
                return {"j": j, "should_active": should_active,
                        "lic_type": lic_type, "lic_valid": lic_valid}, None
            except Exception as ex:
                return None, f"{j[\'first_name\']} {j[\'last_name\']}: {str(ex)[:60]}"

        with _TPE(max_workers=8) as pool:
            futures = {pool.submit(_check_one, j): j for j in judges}
            for future in _as_completed(futures):
                result, err = future.result()
                if err:
                    errors.append(err)
                    continue
                if result is None:
                    continue
                checked += 1
                j, should_active = result["j"], result["should_active"]
                lic_type, lic_valid = result["lic_type"], result["lic_valid"]
                was_active = bool(j["active"])
                if was_active != should_active:
                    reason = "" if should_active else (
                        f"WDSF sync {today}: lic={lic_type or \'none\'}, valid_until={lic_valid or \'n/a\'}"
                    )
                    c2 = get_db()
                    c2.execute("UPDATE judges SET active=?, notes=? WHERE id=?",
                               (1 if should_active else 0, reason if not should_active else None, j["id"]))
                    c2.commit(); c2.close()
                    changed.append({"name": f"{j[\'first_name\']} {j[\'last_name\']}",
                                    "was": "Active" if was_active else "Inactive",
                                    "now": "Active" if should_active else "Inactive"})

        _sync_state["last_result"] = {"checked": checked, "changed": changed,
                                      "errors": errors[:20], "timestamp": today}
    except Exception as ex:
        _sync_state["last_result"] = {"error": str(ex), "checked": 0, "changed": [], "errors": []}
    finally:
        _sync_state["running"] = False


@app.route("/api/judges/sync-wdsf-status", methods=["POST"])
def post_sync_wdsf():
    if _sync_state["running"]:
        return jsonify({"status": "already_running"}), 202
    _threading.Thread(target=_run_wdsf_sync, daemon=True).start()
    return jsonify({"status": "started"}), 202

@app.route("/api/judges/sync-wdsf-status", methods=["GET"])
def get_sync_wdsf():
    return jsonify({"running": _sync_state["running"], "last_result": _sync_state["last_result"]})

def _fetch_discipline_from_min(wdsf_min):
    """Given a WDSF MIN, fetch the athlete profile and return discipline + career level.
    Returns dict: {discipline, career_level} or None on failure.
      discipline:   'Standard' | 'Latin' | '10-Dance' | None
      career_level: 'world_champion' | 'world_finalist' | 'continental_champion' |
                    'world_open_finalist' | 'grand_slam_finalist' | 'international' | None
    """
    from bs4 import BeautifulSoup
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.worlddancesport.org/",
        "Origin": "https://www.worlddancesport.org",
    }
    try:
        # 1. Find athlete profile URL by searching with their MIN
        r = requests.post(
            "https://www.worlddancesport.org/api/listitems/athletes",
            json={"name": str(wdsf_min), "page": 1, "pageSize": 5},
            headers=HEADERS, timeout=15
        )
        r.raise_for_status()
        items = r.json().get("items", [])
        if not items:
            return None
        athlete_url = "https://www.worlddancesport.org" + items[0]["url"]

        # 2. Fetch athlete profile page (SSR — competitions embedded in HTML)
        rp = requests.get(athlete_url, headers=HEADERS, timeout=15)
        rp.raise_for_status()
        soup = BeautifulSoup(rp.text, "html.parser")

        # 3. Parse competition rows from the page
        # Each row has: rank, date, event_type, discipline, category, location
        rows = []
        for tr in soup.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if len(cells) >= 4:
                rows.append(cells)

        # Only count disciplines from actual competition rows (not full page text)
        # Require at least 2 competition rows to make a determination
        comp_rows = [r for r in rows if any(
            kw in " ".join(r).lower()
            for kw in ["standard", "latin", "open", "championship", "grand prix", "grand slam"]
        )]

        if len(comp_rows) < 2:
            # Not enough competition data — can't determine discipline
            std_count = 0
            lat_count = 0
        else:
            all_text = " ".join(" ".join(row) for row in comp_rows).lower()
            std_count = all_text.count("standard")
            lat_count  = all_text.count("latin")

        # 4. Determine discipline — require meaningful signal (at least 3 occurrences total)
        discipline = None
        total = std_count + lat_count
        if total >= 3:
            if lat_count == 0 or std_count / total >= 0.80:
                discipline = "Standard"
            elif std_count == 0 or lat_count / total >= 0.80:
                discipline = "Latin"
            else:
                discipline = "10-Dance"

        # 5. Determine career level — hierarchy:
        #    Adult World/Continental > Professional World/Continental
        #    > Youth/Other World/Continental > World Open > Grand Slam
        #    Within each: Gold(1) > Silver(2) > Bronze(3) > Finalist(top6)
        best_level_score = 0
        LEVEL_MAP = [
            # (score, label)
            # ── Adult World Championship ──────────────────────────────
            (1000, "world_champion_adult"),       # 🥇 rank 1
            ( 950, "world_silver_adult"),          # 🥈 rank 2
            ( 900, "world_bronze_adult"),          # 🥉 rank 3
            ( 800, "world_finalist_adult"),        # 🎖 top 6
            ( 750, "world_participant_adult"),     # top 12
            # ── Adult Continental Championship ────────────────────────
            ( 700, "continental_champion_adult"),  # 🥇 rank 1
            ( 680, "continental_silver_adult"),    # 🥈 rank 2
            ( 660, "continental_bronze_adult"),    # 🥉 rank 3
            ( 600, "continental_finalist_adult"),  # 🎖 top 6
            # ── Professional World Championship ───────────────────────
            ( 550, "pro_world_champion"),          # 🥇 rank 1
            ( 510, "pro_world_finalist"),          # 🎖 top 6
            # ── Professional Continental Championship ─────────────────
            ( 460, "pro_continental_champion"),    # 🥇 rank 1
            ( 420, "pro_continental_finalist"),    # 🎖 top 6
            # ── Youth/Junior/Senior/Other World Championship ──────────
            ( 370, "world_champion_youth"),        # 🥇 rank 1
            ( 340, "world_silver_youth"),          # 🥈 rank 2
            ( 320, "world_bronze_youth"),          # 🥉 rank 3
            ( 290, "world_finalist_youth"),        # 🎖 top 6
            # ── Youth/Other Continental Championship ─────────────────
            ( 250, "continental_champion_youth"),  # 🥇 rank 1
            ( 230, "continental_silver_youth"),    # 🥈 rank 2
            ( 210, "continental_bronze_youth"),    # 🥉 rank 3
            ( 190, "continental_finalist_youth"),  # 🎖 top 6
            # ── Open competitions ─────────────────────────────────────
            ( 150, "world_open_finalist"),         # top 8 at World Open
            ( 100, "grand_slam_finalist"),         # top 8 at Grand Slam
            (  10, "international"),               # competed internationally
        ]
        SCORE_MAP = {lbl: sc for sc, lbl in LEVEL_MAP}

        for row in comp_rows:
            row_text = " ".join(row).lower()
            rank_str = row[0].strip().rstrip(".").strip()
            try:
                rank = int(rank_str)
            except ValueError:
                rank = 99

            # Category: Adult / Professional / Youth-Other
            cat = row[5].lower() if len(row) > 5 else ''
            is_adult = "adult" in cat
            is_pro   = "professional" in cat or "pro" in cat or "professional" in row_text

            is_world_champ  = "world championship" in row_text
            is_continental  = ("continental championship" in row_text or
                               "european championship" in row_text or
                               "pan american" in row_text or
                               "asian championship" in row_text or
                               "african championship" in row_text or
                               "oceania championship" in row_text)
            is_world_open   = "world open" in row_text
            is_grand_slam   = "grand slam" in row_text or "grandslam" in row_text

            if is_world_champ:
                if is_adult:
                    if   rank == 1: best_level_score = max(best_level_score, SCORE_MAP["world_champion_adult"])
                    elif rank == 2: best_level_score = max(best_level_score, SCORE_MAP["world_silver_adult"])
                    elif rank == 3: best_level_score = max(best_level_score, SCORE_MAP["world_bronze_adult"])
                    elif rank <= 6: best_level_score = max(best_level_score, SCORE_MAP["world_finalist_adult"])
                    elif rank <=12: best_level_score = max(best_level_score, SCORE_MAP["world_participant_adult"])
                elif is_pro:
                    if   rank == 1: best_level_score = max(best_level_score, SCORE_MAP["pro_world_champion"])
                    elif rank <= 6: best_level_score = max(best_level_score, SCORE_MAP["pro_world_finalist"])
                else:  # youth / senior / juvenile / other
                    if   rank == 1: best_level_score = max(best_level_score, SCORE_MAP["world_champion_youth"])
                    elif rank == 2: best_level_score = max(best_level_score, SCORE_MAP["world_silver_youth"])
                    elif rank == 3: best_level_score = max(best_level_score, SCORE_MAP["world_bronze_youth"])
                    elif rank <= 6: best_level_score = max(best_level_score, SCORE_MAP["world_finalist_youth"])
            elif is_continental:
                if is_adult:
                    if   rank == 1: best_level_score = max(best_level_score, SCORE_MAP["continental_champion_adult"])
                    elif rank == 2: best_level_score = max(best_level_score, SCORE_MAP["continental_silver_adult"])
                    elif rank == 3: best_level_score = max(best_level_score, SCORE_MAP["continental_bronze_adult"])
                    elif rank <= 6: best_level_score = max(best_level_score, SCORE_MAP["continental_finalist_adult"])
                elif is_pro:
                    if   rank == 1: best_level_score = max(best_level_score, SCORE_MAP["pro_continental_champion"])
                    elif rank <= 6: best_level_score = max(best_level_score, SCORE_MAP["pro_continental_finalist"])
                else:
                    if   rank == 1: best_level_score = max(best_level_score, SCORE_MAP["continental_champion_youth"])
                    elif rank == 2: best_level_score = max(best_level_score, SCORE_MAP["continental_silver_youth"])
                    elif rank == 3: best_level_score = max(best_level_score, SCORE_MAP["continental_bronze_youth"])
                    elif rank <= 6: best_level_score = max(best_level_score, SCORE_MAP["continental_finalist_youth"])
            elif is_world_open and rank <= 8:
                best_level_score = max(best_level_score, SCORE_MAP["world_open_finalist"])
            elif is_grand_slam and rank <= 8:
                best_level_score = max(best_level_score, SCORE_MAP["grand_slam_finalist"])
            elif best_level_score == 0:
                best_level_score = SCORE_MAP["international"]  # competed at least once internationally

        # Map best score back to label
        career_level = "international"
        for sc, lbl in LEVEL_MAP:
            if best_level_score >= sc:
                career_level = lbl
                break

        return {"discipline": discipline, "career_level": career_level}
    except Exception as e:
        import traceback
        print(f"[enrich] MIN={wdsf_min} error: {e}\n{traceback.format_exc()}")
        return {"discipline": None, "career_level": None, "_error": str(e)}


# ── Background enrichment task ────────────────────────────────────────────────
_enrich_status = {"running": False, "processed": 0, "total": 0, "updated": 0,
                  "not_found": 0, "errors": [], "done": False}

def _run_enrich_background(judge_ids):
    import time
    global _enrich_status
    _enrich_status.update({"running": True, "processed": 0, "total": len(judge_ids),
                           "updated": 0, "not_found": 0, "errors": [], "done": False})
    for jid in judge_ids:
        conn = get_db()
        row = conn.execute("SELECT id, first_name, last_name, wdsf_min FROM judges WHERE id=?", (jid,)).fetchone()
        conn.close()
        if not row:
            continue
        disc = _fetch_discipline_from_min(row["wdsf_min"])
        conn = get_db()
        try:
            if disc and not disc.get("_error"):
                conn.execute("UPDATE judges SET specialty=?, career_level=? WHERE id=?",
                             (disc["discipline"] or "Unknown", disc["career_level"] or "international", jid))
                conn.commit()
                _enrich_status["updated"] += 1
            elif disc and disc.get("_error"):
                conn.execute("UPDATE judges SET specialty='N/A' WHERE id=?", (jid,))
                conn.commit()
                _enrich_status["errors"].append(f"{row['first_name']} {row['last_name']}: {disc['_error']}")
                _enrich_status["not_found"] += 1
            else:
                conn.execute("UPDATE judges SET specialty='N/A' WHERE id=?", (jid,))
                conn.commit()
                _enrich_status["not_found"] += 1
        except Exception as e:
            _enrich_status["errors"].append(f"{row['first_name']} {row['last_name']}: {e}")
        finally:
            conn.close()
        _enrich_status["processed"] += 1
        time.sleep(0.2)  # small delay to avoid rate limiting
    _enrich_status["running"] = False
    _enrich_status["done"] = True


@app.route("/api/judges/test-min", methods=["POST"])
def test_min():
    """Debug: test _fetch_discipline_from_min for a single MIN. Body: {min: 12345678}"""
    import traceback
    data = request.json or {}
    wdsf_min = data.get("min")
    # Step 1: raw API search
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.worlddancesport.org/",
        "Origin": "https://www.worlddancesport.org",
    }
    try:
        r = requests.post(
            "https://www.worlddancesport.org/api/listitems/athletes",
            json={"name": str(wdsf_min), "page": 1, "pageSize": 5},
            headers=HEADERS, timeout=15
        )
        api_status = r.status_code
        api_body = r.text[:500]
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()})
    result = _fetch_discipline_from_min(wdsf_min)
    return jsonify({"min": wdsf_min, "api_status": api_status, "api_body": api_body, "result": result})


@app.route("/api/judges/enrich-disciplines", methods=["POST"])
def enrich_judge_disciplines():
    """Start background enrichment of all judges. Body: {force: bool}"""
    import threading
    global _enrich_status
    if _enrich_status["running"]:
        return jsonify({"ok": False, "error": "Already running", "status": _enrich_status})
    data = request.json or {}
    force = bool(data.get("force", False))
    conn = get_db()
    if force:
        rows = conn.execute(
            "SELECT id FROM judges WHERE wdsf_min IS NOT NULL"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id FROM judges WHERE wdsf_min IS NOT NULL "
            "AND (specialty IS NULL OR specialty = '' OR specialty = 'Unknown')"
        ).fetchall()
    conn.close()
    judge_ids = [r["id"] for r in rows]
    t = threading.Thread(target=_run_enrich_background, args=(judge_ids,), daemon=True)
    t.start()
    return jsonify({"ok": True, "started": True, "total": len(judge_ids)})


@app.route("/api/judges/enrich-status", methods=["GET"])
def enrich_disciplines_status():
    """Poll background enrichment progress."""
    return jsonify(_enrich_status)

@app.route("/api/events", methods=["GET","POST"])
def events():
    conn = get_db()
    if request.method == "POST":
        d = request.json
        cur = conn.execute(
            "INSERT INTO events (name,date,location,country,discipline,age_group,division,event_type,is_ags,coefficient,status) VALUES (?,?,?,?,?,?,?,?,?,?,'pending')",
            (d["name"], d["date"], d.get("location",""), d.get("country",""),
             d.get("discipline","Standard"), d.get("age_group","Adult"),
             d.get("division","General"), d.get("event_type","WORLD CHAMPIONSHIP"),
             1 if d.get("is_ags") else 0, float(d.get("coefficient", 1.0))))
        conn.commit()
        row = conn.execute("SELECT * FROM events WHERE id=?", (cur.lastrowid,)).fetchone()
        conn.close()
        return jsonify(dict(row)), 201
    rows = conn.execute("SELECT * FROM events ORDER BY date DESC").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/events/<int:eid>", methods=["GET","DELETE"])
def event_detail(eid):
    conn = get_db()
    if request.method == "DELETE":
        conn.execute("DELETE FROM panel_assignments WHERE event_id=?", (eid,))
        conn.execute("DELETE FROM events WHERE id=?", (eid,))
        conn.commit()
        conn.close()
        return jsonify({"ok": True})
    row = conn.execute("SELECT * FROM events WHERE id=?", (eid,)).fetchone()
    conn.close()
    return jsonify(dict(row)) if row else (jsonify({"error":"Not found"}), 404)

def get_committed_judge_ids(conn, exclude_event_id=None):
    """
    Devuelve el conjunto de judge_id ya comprometidos:
    1. Paneles internos confirmados / en revisión
    2. Adjudicadores confirmados en la web oficial WDSF (official_nominations)
    Se excluye el propio evento para no bloquearse al reasignar.
    """
    # 1. Paneles internos
    q = """
        SELECT DISTINCT pa.judge_id
        FROM panel_assignments pa
        JOIN events e ON pa.event_id = e.id
        WHERE e.status IN ('confirmed', 'sent_for_review', 'officially_nominated')
          AND pa.role != 'reserve'
    """
    params = []
    if exclude_event_id:
        q += " AND pa.event_id != ?"
        params.append(exclude_event_id)
    ids = {r[0] for r in conn.execute(q, params).fetchall()}

    # 2. Adjudicadores confirmados en WDSF oficial (si la tabla existe)
    try:
        wdsf_ids = {r[0] for r in conn.execute("""
            SELECT DISTINCT judge_id FROM official_nominations
            WHERE section='adjudicator' AND judge_id IS NOT NULL
        """).fetchall()}
        ids |= wdsf_ids
    except Exception:
        pass

    return ids


@app.route("/api/events/<int:eid>/confirm", methods=["POST"])
def confirm_event(eid):
    """Confirms the event panel: status → 'confirmed'. Judges become committed."""
    conn = get_db()
    ev = conn.execute("SELECT * FROM events WHERE id=?", (eid,)).fetchone()
    if not ev:
        conn.close()
        return jsonify({"error": "Event not found"}), 404
    if dict(ev).get("status") not in ("assigned",):
        conn.close()
        return jsonify({"error": "The event must be in 'assigned' status to confirm it"}), 400

    conn.execute("UPDATE events SET status='confirmed' WHERE id=?", (eid,))
    # Mark assignments as confirmed
    conn.execute(
        "UPDATE panel_assignments SET status='confirmed' WHERE event_id=? AND role != 'reserve'",
        (eid,)
    )
    conn.commit()

    # Return summary with committed judges
    panel = conn.execute("""
        SELECT pa.judge_id, j.first_name, j.last_name, j.representing, j.nationality, pa.role, pa.position
        FROM panel_assignments pa JOIN judges j ON pa.judge_id=j.id
        WHERE pa.event_id=? AND pa.role != 'reserve'
        ORDER BY pa.position
    """, (eid,)).fetchall()
    conn.close()

    return jsonify({
        "ok": True,
        "event_id": eid,
        "status": "confirmed",
        "committed_judges": [
            {"id": r["judge_id"],
             "name": f"{r['last_name']}, {r['first_name']}",
             "country": normalize_country(r["representing"] or r["nationality"] or ""),
             "role": r["role"], "position": r["position"]}
            for r in panel
        ]
    })


@app.route("/api/events/<int:eid>/send_review", methods=["POST"])
def send_review(eid):
    """Marks the panel as 'sent for review'."""
    conn = get_db()
    ev = conn.execute("SELECT * FROM events WHERE id=?", (eid,)).fetchone()
    if not ev:
        conn.close()
        return jsonify({"error": "Event not found"}), 404
    conn.execute("UPDATE events SET status='sent_for_review' WHERE id=?", (eid,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "event_id": eid, "status": "sent_for_review"})


@app.route("/api/events/<int:eid>/reopen", methods=["POST"])
def reopen_panel(eid):
    """Devuelve el panel a estado 'assigned' para poder editarlo de nuevo."""
    conn = get_db()
    ev = conn.execute("SELECT * FROM events WHERE id=?", (eid,)).fetchone()
    if not ev:
        conn.close()
        return jsonify({"error": "Event not found"}), 404
    allowed = ("confirmed", "sent_for_review", "officially_nominated")
    if dict(ev).get("status") not in allowed:
        conn.close()
        return jsonify({"error": "Solo se puede reabrir un panel confirmado o en revisión"}), 400
    conn.execute("UPDATE events SET status='assigned' WHERE id=?", (eid,))
    conn.execute("UPDATE panel_assignments SET status='assigned' WHERE event_id=?", (eid,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "event_id": eid, "status": "assigned"})


@app.route("/api/committed_judges")
def committed_judges():
    """Lista todos los jueces ya comprometidos en paneles confirmados."""
    conn = get_db()
    rows = conn.execute("""
        SELECT DISTINCT j.id, j.first_name, j.last_name, j.representing, j.nationality,
               e.id as event_id, e.name as event_name, e.date as event_date, e.discipline,
               pa.role, pa.position
        FROM panel_assignments pa
        JOIN judges j ON pa.judge_id = j.id
        JOIN events e ON pa.event_id = e.id
        WHERE e.status IN ('confirmed','sent_for_review') AND pa.role != 'reserve'
        ORDER BY e.date, pa.position
    """).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        d["country"] = normalize_country(d.get("representing") or d.get("nationality") or "")
        result.append(d)
    return jsonify(result)


@app.route("/api/events/<int:eid>/assign", methods=["POST"])
def assign(eid):
    conn = get_db()
    ev = conn.execute("SELECT * FROM events WHERE id=?", (eid,)).fetchone()
    if not ev:
        conn.close()
        return jsonify({"error": "Event not found"}), 404

    event      = dict(ev)

    # Validar que el evento tiene fecha y es futura (o hoy)
    event_date = (event.get("date") or "").strip()
    if not event_date:
        conn.close()
        return jsonify({"error": "Este evento no tiene fecha. Añade una fecha antes de asignar el panel."}), 400
    if event_date < date.today().isoformat():
        conn.close()
        return jsonify({"error": f"La fecha del evento ({event_date}) ya ha pasado. Solo se pueden asignar paneles a eventos futuros."}), 400

    body       = request.get_json(silent=True) or {}
    default_ps = 12 if event.get("is_ags") else 9
    try:
        panel_size = max(3, min(20, int(body.get("panel_size", default_ps))))
    except (TypeError, ValueError):
        panel_size = default_ps
    discipline  = (event.get("discipline") or "").strip()
    host        = normalize_country(event.get("country",""))
    today       = date.today().isoformat()

    # Optional list of judge IDs pre-invited by the organizer
    raw_invited = body.get("invited_judge_ids") or []
    invited_ids = []
    for x in raw_invited:
        try:
            invited_ids.append(int(x))
        except (TypeError, ValueError):
            pass

    # Rechazar eventos sin disciplina válida de ballroom
    if discipline not in BALLROOM_DISCIPLINES:
        conn.close()
        return jsonify({
            "error": f"Disciplina '{discipline or 'no definida'}' no es una disciplina de ballroom asignable. "
                     f"Solo se admiten: {', '.join(sorted(BALLROOM_DISCIPLINES))}. "
                     f"Edita el evento y establece la disciplina correcta primero."
        }), 400

    # Load eligible judges (active + License A)
    all_j = [judge_dict(r) for r in conn.execute(
        "SELECT * FROM judges WHERE active=1 AND license_type='A'").fetchall()]

    # Excluir jueces ya comprometidos en paneles confirmados (de otros eventos)
    committed_ids = get_committed_judge_ids(conn, exclude_event_id=eid)

    # Filtrar ESTRICTAMENTE por disciplina:
    # - Jueces sin disciplinas definidas → excluidos siempre
    # - Ten Dance requiere "Combined" o "Ten Dance" en la lista del juez
    # - Standard requiere "Standard", Latin requiere "Latin"
    # - Jueces comprometidos en paneles confirmados → excluidos
    cands = [j for j in all_j
             if judge_has_discipline(j, discipline)
             and j["id"] not in committed_ids]

    # Filter by valid license
    valid = [j for j in cands if not j.get("license_valid_until") or j["license_valid_until"] >= today]

    # Pre-load judge correlation map for criterion F — filtered by discipline
    corr_map = {}
    try:
        corr_rows = conn.execute(
            "SELECT judge_name_a, judge_name_b, correlation FROM judge_pair_correlations WHERE discipline = ?",
            (discipline,)
        ).fetchall()
        for cr in corr_rows:
            corr_map[(cr["judge_name_a"], cr["judge_name_b"])] = cr["correlation"]
    except Exception:
        pass  # Table may not exist yet; criterion F will show "no data"

    panel, used_countries, used_nationalities, used_zones, exclusions = [], [], [], [], []

    # ¿El evento se celebra en Asia? → regla del 50 %
    host_zone       = ZONES.get(host, "Other")
    event_in_asia   = macro_zone(host_zone) == "Asia"
    import math
    asia_minimum    = math.ceil(panel_size / 2) if event_in_asia else 0

    def panel_asia_count():
        return sum(1 for j in panel if macro_zone(j["zone"]) == "Asia")

    # 1. Host country judge (mandatory, position #1)
    host_pool = sorted(
        [j for j in valid if normalize_country(j.get("representing") or j.get("nationality","")) == host],
        key=lambda x: (x.get("judging_world_championships") or 0) * 6 +
                      (x.get("judging_grand_slams") or 0) * 3 +
                      (x.get("judging_continental_championships") or 0) * 2,
        reverse=True
    )
    non_host = [j for j in valid if normalize_country(j.get("representing") or j.get("nationality","")) != host]

    def panel_names():
        """Current list of judge display names in the panel (for criterion F)."""
        return [_judge_display_name(p) for p in panel]

    if host_pool:
        hj = host_pool[0]
        hj["score"], hj["score_breakdown"] = calc_score(
            hj, event, [], return_breakdown=True,
            assigned_panel_names=[], corr_map=corr_map)
        hj["selection_reason"] = "Mandatory: judge from the host country of the event."
        hj["role"] = "host_country"
        panel.append(hj)
        used_countries.append(host)
        used_nationalities.append(normalize_country(hj.get("nationality","")))
        used_zones.append(hj["zone"])
    else:
        exclusions.append(f"No Licencia A judge from host country: {host}")

    def rescore_pool(pool):
        """Rescore all judges in pool using current panel state."""
        pnames = panel_names()
        for j in pool:
            j["score"], j["score_breakdown"] = calc_score(
                j, event, used_zones, return_breakdown=True,
                assigned_panel_names=pnames, corr_map=corr_map)
        pool.sort(key=lambda x: x["score"], reverse=True)

    # Score non-host judges (initial scoring, no panel context yet)
    rescore_pool(non_host)

    # 2. Top-3 ranking countries (mandatory) — use REGIONAL ranking for continental championships
    event_region = get_event_region(event)
    ranking = get_ranking_for_region(discipline, event_region)
    for idx_top, top_c in enumerate(ranking[:3]):
        if top_c in used_countries:
            continue
        pool = [j for j in non_host if normalize_country(j.get("representing") or j.get("nationality","")) == top_c]
        if pool and len(panel) < panel_size:
            bj = pool[0]
            bj["role"] = "top3_required"
            bj["selection_reason"] = f"Mandatory: {top_c} is #{idx_top+1} in the {event_region} {discipline} ranking — a spot is reserved for top-3 regional countries."
            panel.append(bj)
            used_countries.append(top_c)
            used_nationalities.append(normalize_country(bj.get("nationality","")))
            used_zones.append(bj["zone"])

    # 2b. Cuota Asia — garantizar al menos la mitad de jueces de Asia si el evento es en Asia
    #     Intentamos distribuir entre las sub-regiones (E.Asia, SE.Asia, Oceania)
    if event_in_asia and panel_asia_count() < asia_minimum:
        # Ordenar candidatos asiáticos por score y sub-región ya usada
        asia_pool = [j for j in non_host
                     if macro_zone(j["zone"]) == "Asia"
                     and j not in panel
                     and normalize_country(j.get("representing") or j.get("nationality","")) not in used_countries]
        asia_subzones_used = [j["zone"] for j in panel if macro_zone(j["zone"]) == "Asia"]

        # Puntuar de nuevo priorizando sub-región no representada aún
        def asia_priority(j):
            sub_bonus = 20 if j["zone"] not in asia_subzones_used else 0
            return j["score"] + sub_bonus

        asia_pool.sort(key=asia_priority, reverse=True)

        for j in asia_pool:
            if panel_asia_count() >= asia_minimum or len(panel) >= panel_size:
                break
            country = normalize_country(j.get("representing") or j.get("nationality",""))
            nat     = normalize_country(j.get("nationality",""))
            if country in used_countries:
                continue
            if used_nationalities.count(nat) >= 2:
                continue
            j["role"] = "asia_quota"
            j["selection_reason"] = f"Asia quota: event held in Asia — at least {asia_minimum} of {panel_size} judges must come from Asia. Selected from {j['zone']} sub-region."
            panel.append(j)
            used_countries.append(country)
            used_nationalities.append(nat)
            used_zones.append(j["zone"])
            asia_subzones_used.append(j["zone"])

        if panel_asia_count() < asia_minimum:
            exclusions.append(
                f"Cuota Asia no alcanzada: {panel_asia_count()}/{asia_minimum} jueces de Asia disponibles"
            )

    # 2c. Invited judges (organiser's pre-invited list) — prefer them if eligible
    invited_summary = []   # [{judge, included, reason}]
    if invited_ids:
        # Load ALL judges (not just eligible) to be able to report why some can't be included
        all_for_invited = {r["id"]: dict(r) for r in conn.execute("SELECT * FROM judges").fetchall()}
        for inv_id in invited_ids:
            if inv_id not in all_for_invited:
                invited_summary.append({"id": inv_id, "name": f"ID {inv_id}", "included": False,
                                        "reason": "Judge not found in the database."})
                continue
            inv_raw = all_for_invited[inv_id]
            inv_name = f"{inv_raw.get('first_name','')} {inv_raw.get('last_name','')}".strip()
            # Find in valid pool (already filtered by discipline + license + active + not committed)
            inv_j = next((j for j in valid if j["id"] == inv_id), None)
            if inv_j is None:
                # Diagnose why not eligible
                if not inv_raw.get("active"):
                    reason = "Inactive judge."
                elif inv_raw.get("license_type") != "A":
                    reason = f"License type is '{inv_raw.get('license_type','?')}' (License A required)."
                elif inv_id in committed_ids:
                    reason = "Already committed to another confirmed panel."
                elif not judge_has_discipline(judge_dict(inv_raw), discipline):
                    reason = f"Not qualified for {discipline} (check judge's discipline list)."
                else:
                    license_exp = inv_raw.get("license_valid_until","")
                    if license_exp and license_exp < today:
                        reason = f"License expired on {license_exp}."
                    else:
                        reason = "Not in the eligible pool (unknown reason)."
                invited_summary.append({"id": inv_id, "name": inv_name, "included": False, "reason": reason})
                continue

            # Judge is eligible — try to include them
            if inv_j in panel:
                invited_summary.append({"id": inv_id, "name": inv_name, "included": True,
                                        "reason": "Already added as a mandatory spot."})
                inv_j["invited"] = True
                continue

            country = normalize_country(inv_j.get("representing") or inv_j.get("nationality",""))
            nat     = normalize_country(inv_j.get("nationality",""))

            if country in used_countries:
                # Find which judge from the same country is already in the panel
                conflict = next((j for j in panel
                                 if normalize_country(j.get("representing") or j.get("nationality","")) == country), None)
                conflict_name = f"{conflict.get('first_name','')} {conflict.get('last_name','')}".strip() if conflict else "?"
                invited_summary.append({"id": inv_id, "name": inv_name, "included": False,
                                        "reason": f"Country {country} already represented by {conflict_name}."})
            elif used_nationalities.count(nat) >= 2:
                invited_summary.append({"id": inv_id, "name": inv_name, "included": False,
                                        "reason": f"Nationality {nat} already has 2 judges in the panel (max 2)."})
            elif len(panel) >= panel_size:
                invited_summary.append({"id": inv_id, "name": inv_name, "included": False,
                                        "reason": "Panel is already full."})
            else:
                # Add with priority
                inv_j["score"], inv_j["score_breakdown"] = calc_score(
                    inv_j, event, used_zones, return_breakdown=True,
                    assigned_panel_names=panel_names(), corr_map=corr_map)
                inv_j["role"] = "invited"
                inv_j["invited"] = True
                inv_j["selection_reason"] = f"✈️ Invited by the organiser — eligible for {discipline} and meets all panel requirements."
                panel.append(inv_j)
                used_countries.append(country)
                used_nationalities.append(nat)
                used_zones.append(inv_j["zone"])
                invited_summary.append({"id": inv_id, "name": inv_name, "included": True,
                                        "reason": "Included as an organiser-invited judge."})

    # 3. Fill rest by score (global) — rescore after each addition so criterion F is dynamic
    while len(panel) < panel_size:
        rescore_pool(non_host)
        added = False
        for j in non_host:
            if j in panel:
                continue
            country = normalize_country(j.get("representing") or j.get("nationality",""))
            nat     = normalize_country(j.get("nationality",""))
            if country in used_countries:
                continue
            if used_nationalities.count(nat) >= 2:
                continue
            j["role"] = "selected"
            j["selection_reason"] = f"Selected by score: highest score among eligible candidates from {country} ({j['zone']} zone), not yet represented in the panel."
            panel.append(j)
            used_countries.append(country)
            used_nationalities.append(nat)
            used_zones.append(j["zone"])
            added = True
            break
        if not added:
            break

    # 4. 2 reserves
    reserves = []
    for j in non_host:
        if len(reserves) >= 2:
            break
        country = normalize_country(j.get("representing") or j.get("nationality",""))
        if j not in panel and country not in used_countries:
            j["role"] = "reserve"
            j["selection_reason"] = f"Reserve: next highest-scoring eligible judge from {country} not already in the panel."
            reserves.append(j)

    # Save to DB
    conn.execute("DELETE FROM panel_assignments WHERE event_id=?", (eid,))
    for i, j in enumerate(panel):
        conn.execute(
            "INSERT INTO panel_assignments (event_id,judge_id,role,position,score,status) VALUES (?,?,?,?,?,?)",
            (eid, j["id"], j["role"], i + 1, j["score"], "proposed"))
    for j in reserves:
        conn.execute(
            "INSERT INTO panel_assignments (event_id,judge_id,role,position,score,status) VALUES (?,?,?,?,?,?)",
            (eid, j["id"], "reserve", 99, j["score"], "reserve"))
    conn.execute("UPDATE events SET status='assigned' WHERE id=?", (eid,))
    conn.commit()
    conn.close()

    ranking_used = get_ranking_for_region(discipline, get_event_region(event))
    panel_logic = {
        "discipline": discipline,
        "host_country": host,
        "host_zone": host_zone,
        "panel_size": panel_size,
        "event_in_asia": event_in_asia,
        "asia_minimum": asia_minimum if event_in_asia else 0,
        "mandatory_countries": [host] + [c for c in ranking_used[:3] if c != host],
        "top3_ranking": [{"pos": i+1, "country": c} for i, c in enumerate(ranking_used[:10])],
        "score_criteria": [
            {"label": "A. Dancing career",       "max": 30, "description": "World Champion=30, World Podium=25, World Finalist=20, WCH Participant=15, European Champion=12, European Podium=8, National=5"},
            {"label": "B. Judging experience",   "max": 30, "description": "WCH×6 + GS×3 + CCH×2 (capped at 30)"},
            {"label": "C. Country ranking",       "max": 20, "description": "Top-3=20, Top 4-6=15, Top 7-10=10, Top 11-20=5, Not ranked=2"},
            {"label": "D. Zone distribution",     "max": 20, "description": "New zone=20, Zone with 1 judge=10, Already covered=0"},
            {"label": "E. Discipline specialty",  "max": 10, "description": "Specialist in discipline=+10, Versatile=+5, Specialist in opposite=−5"},
            {"label": "F. Panel independence",    "max": 15, "description": "Based on historical Spearman correlation. r=−1 (opposite views)=15pts, r=0 (independent)=7.5pts, r=+1 (identical voting)=0pts. Requires scraped competition data."},
        ],
        "has_correlation_data": bool(corr_map),
        "exclusion_rules": [
            "Only active judges with License A",
            "One judge per country (representing)",
            "Max 2 judges of same nationality",
            "Judges already committed to confirmed panels are excluded",
            "Judges without any disciplines defined are excluded",
        ]
    }

    return jsonify({
        "event": event,
        "panel": panel,
        "reserves": reserves,
        "panel_size": panel_size,
        "zones": sorted(set(used_zones)),
        "exclusions": exclusions,
        "panel_logic": panel_logic,
        "invited_summary": invited_summary,
        "stats": {
            "candidates": len(valid),
            "assigned": len(panel),
            "reserves": len(reserves)
        }
    })

@app.route("/api/events/<int:eid>/panel")
def panel_detail(eid):
    conn = get_db()
    rows = conn.execute("""
        SELECT pa.*, j.first_name, j.last_name, j.nationality, j.representing,
               j.license_type, j.disciplines, j.judging_world_championships,
               j.judging_grand_slams, j.judging_continental_championships, j.career_level,
               j.specialty, j.std_panels_count, j.lat_panels_count, j.id as judge_id
        FROM panel_assignments pa
        JOIN judges j ON pa.judge_id = j.id
        WHERE pa.event_id = ?
        ORDER BY pa.position
    """, (eid,)).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        d["disciplines"] = d.get("disciplines","").split(",") if d.get("disciplines") else []
        country = normalize_country(d.get("representing") or d.get("nationality",""))
        d["zone"] = ZONES.get(country, "Other")
        d["representing_normalized"] = country
        d["std_panels_count"] = d.get("std_panels_count") or 0
        d["lat_panels_count"] = d.get("lat_panels_count") or 0
        d["specialty"]        = d.get("specialty") or "Unknown"
        result.append(d)
    return jsonify(result)

@app.route("/api/events/<int:eid>/alternatives/<int:judge_id>")
def alternatives(eid, judge_id):
    """
    Devuelve candidatos alternativos para sustituir a judge_id en el panel del evento eid.
    Filtra por disciplina, excluye los ya en el panel y sus países, y ordena por puntuación.
    """
    conn = get_db()
    ev = conn.execute("SELECT * FROM events WHERE id=?", (eid,)).fetchone()
    if not ev:
        conn.close()
        return jsonify({"error": "Event not found"}), 404

    event      = dict(ev)
    discipline = (event.get("discipline") or "").strip()
    today      = date.today().isoformat()

    # Jueces actuales en el panel (todos, incluido el que se quiere cambiar)
    current_rows = conn.execute(
        "SELECT judge_id, role, position FROM panel_assignments WHERE event_id=?", (eid,)
    ).fetchall()
    current_ids  = {r["judge_id"] for r in current_rows}

    # País y ZONA del juez que se va a reemplazar
    replacing_row = conn.execute(
        "SELECT j.representing, j.nationality FROM panel_assignments pa "
        "JOIN judges j ON pa.judge_id=j.id "
        "WHERE pa.event_id=? AND pa.judge_id=?", (eid, judge_id)
    ).fetchone()
    replacing_country = ""
    replacing_zone    = "Other"
    if replacing_row:
        replacing_country = normalize_country(
            replacing_row["representing"] or replacing_row["nationality"] or ""
        )
        replacing_zone = ZONES.get(replacing_country, "Other")

    # Países ocupados (sin contar el que se reemplaza)
    occupied_countries = set()
    for r in current_rows:
        if r["judge_id"] == judge_id:
            continue
        jrow = conn.execute("SELECT representing, nationality FROM judges WHERE id=?",
                            (r["judge_id"],)).fetchone()
        if jrow:
            occupied_countries.add(
                normalize_country(jrow["representing"] or jrow["nationality"] or "")
            )

    # Zonas del panel sin el juez reemplazado
    panel_zones = []
    for r in current_rows:
        if r["judge_id"] == judge_id:
            continue
        jrow = conn.execute("SELECT representing, nationality FROM judges WHERE id=?",
                            (r["judge_id"],)).fetchone()
        if jrow:
            c = normalize_country(jrow["representing"] or jrow["nationality"] or "")
            panel_zones.append(ZONES.get(c, "Other"))

    # Todos los jueces elegibles para la disciplina
    all_j = [judge_dict(r) for r in conn.execute(
        "SELECT * FROM judges WHERE active=1 AND license_type='A'"
    ).fetchall()]

    # Excluir jueces comprometidos en paneles confirmados (de otros eventos)
    committed_ids = get_committed_judge_ids(conn, exclude_event_id=eid)
    conn.close()

    # Zona solicitada (parámetro opcional): "all" = sin filtro, cualquier otra = filtrar por zona
    zone_filter = request.args.get("zone", "same")  # "same" = misma zona por defecto

    def base_eligible(zone_req):
        return [
            j for j in all_j
            if judge_has_discipline(j, discipline)
            and (not j.get("license_valid_until") or j["license_valid_until"] >= today)
            and j["id"] not in current_ids
            and j["id"] not in committed_ids
            and normalize_country(j.get("representing") or j.get("nationality",""))
                not in occupied_countries
            and (zone_req == "all" or j["zone"] == zone_req)
        ]

    if zone_filter == "same":
        eligible = base_eligible(replacing_zone)
        # Fallback a todas las zonas si hay pocos candidatos en la misma zona
        if len(eligible) < 3:
            eligible = base_eligible("all")
    else:
        eligible = base_eligible(zone_filter)

    # Puntuar y ordenar
    for j in eligible:
        j["score"] = calc_score(j, event, panel_zones)
    eligible.sort(key=lambda x: x["score"], reverse=True)

    # Devolver los 15 mejores con indicación de si son de la misma zona
    result = []
    for j in eligible[:15]:
        country = normalize_country(j.get("representing") or j.get("nationality",""))
        result.append({
            "id":           j["id"],
            "first_name":   j["first_name"],
            "last_name":    j["last_name"],
            "country":      country,
            "zone":         j["zone"],
            "same_zone":    j["zone"] == replacing_zone,   # True = misma zona
            "replacing_zone": replacing_zone,
            "score":        j["score"],
            "judging_world_championships":      j.get("judging_world_championships") or 0,
            "judging_grand_slams":              j.get("judging_grand_slams") or 0,
            "judging_continental_championships":j.get("judging_continental_championships") or 0,
            "career_level": j.get("career_level") or "national",
            "license_type": j.get("license_type",""),
        })
    return jsonify(result)


@app.route("/api/events/<int:eid>/swap", methods=["POST"])
def swap_judge(eid):
    """
    Intercambia old_judge_id por new_judge_id en el panel del evento.
    Mantiene posición y rol del juez original.
    Body: { old_judge_id: int, new_judge_id: int }
    """
    conn = get_db()
    ev = conn.execute("SELECT * FROM events WHERE id=?", (eid,)).fetchone()
    if not ev:
        conn.close()
        return jsonify({"error": "Event not found"}), 404

    data         = request.json or {}
    old_judge_id = data.get("old_judge_id")
    new_judge_id = data.get("new_judge_id")
    if not old_judge_id or not new_judge_id:
        conn.close()
        return jsonify({"error": "Faltan old_judge_id o new_judge_id"}), 400

    event      = dict(ev)
    discipline = (event.get("discipline") or "").strip()
    today      = date.today().isoformat()

    # Verificar que el nuevo juez es elegible
    new_j_row = conn.execute(
        "SELECT * FROM judges WHERE id=? AND active=1 AND license_type='A'", (new_judge_id,)
    ).fetchone()
    if not new_j_row:
        conn.close()
        return jsonify({"error": "El juez seleccionado no es elegible"}), 400

    new_j = judge_dict(new_j_row)
    if not judge_has_discipline(new_j, discipline):
        conn.close()
        return jsonify({"error": f"El juez no tiene la disciplina {discipline}"}), 400

    # Obtener posición y rol del juez antiguo
    old_pa = conn.execute(
        "SELECT * FROM panel_assignments WHERE event_id=? AND judge_id=?",
        (eid, old_judge_id)
    ).fetchone()
    if not old_pa:
        conn.close()
        return jsonify({"error": "El juez no está en este panel"}), 404

    old_position = old_pa["position"]
    old_role     = old_pa["role"]

    # Calcular score del nuevo juez en contexto del panel actual
    other_zones = []
    for r in conn.execute(
        "SELECT j.representing, j.nationality FROM panel_assignments pa "
        "JOIN judges j ON pa.judge_id=j.id "
        "WHERE pa.event_id=? AND pa.judge_id!=?", (eid, old_judge_id)
    ).fetchall():
        c = normalize_country(r["representing"] or r["nationality"] or "")
        other_zones.append(ZONES.get(c, "Other"))

    new_score = calc_score(new_j, event, other_zones)

    # Hacer el intercambio
    conn.execute(
        "UPDATE panel_assignments SET judge_id=?, score=? WHERE event_id=? AND judge_id=?",
        (new_judge_id, new_score, eid, old_judge_id)
    )
    conn.commit()

    # Devolver el panel actualizado
    rows = conn.execute("""
        SELECT pa.*, j.first_name, j.last_name, j.nationality, j.representing,
               j.license_type, j.disciplines, j.judging_world_championships,
               j.judging_grand_slams, j.judging_continental_championships, j.career_level
        FROM panel_assignments pa
        JOIN judges j ON pa.judge_id = j.id
        WHERE pa.event_id = ?
        ORDER BY pa.position
    """, (eid,)).fetchall()
    conn.close()

    result = []
    for r in rows:
        d = dict(r)
        d["disciplines"] = d.get("disciplines","").split(",") if d.get("disciplines") else []
        country = normalize_country(d.get("representing") or d.get("nationality",""))
        d["zone"] = ZONES.get(country, "Other")
        d["representing_normalized"] = country
        result.append(d)

    return jsonify({
        "ok": True,
        "swapped": {"old": old_judge_id, "new": new_judge_id,
                    "position": old_position, "role": old_role},
        "panel": result
    })


@app.route("/api/sync_nominations", methods=["POST","GET"])
def sync_nominations():
    """Lanza la sincronización con NominatedOfficials de WDSF en segundo plano."""
    import subprocess, sys
    script = os.path.join(APP_DIR, "sincronizar_nominados.py")
    try:
        result = subprocess.run(
            [sys.executable, script],
            capture_output=True, text=True, timeout=300
        )
        output = result.stdout + result.stderr
        return jsonify({"ok": True, "output": output[-3000:]})
    except subprocess.TimeoutExpired:
        return jsonify({"ok": False, "error": "Timeout (>5 min)"}), 500
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/ranking/<discipline>")
def ranking_for_discipline(discipline):
    """Return the country ranking used for a given discipline."""
    ranking = get_ranking(discipline)
    conn = get_db()
    # For each ranked country, check how many eligible License A judges exist
    result = []
    for i, country in enumerate(ranking):
        count = conn.execute(
            "SELECT COUNT(*) FROM judges WHERE active=1 AND license_type='A' AND (representing=? OR nationality=?)",
            (country, country)
        ).fetchone()[0]
        result.append({"pos": i+1, "country": country, "judges_available": count})
    conn.close()
    return jsonify(result)


@app.route("/api/nominations")
def nominations():
    """Lista todas las nominaciones oficiales WDSF guardadas en la BD."""
    conn = get_db()
    # Asegurar que la tabla existe
    conn.execute("""
        CREATE TABLE IF NOT EXISTS official_nominations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wdsf_comp_id INTEGER, comp_name TEXT, comp_date TEXT,
            comp_discipline TEXT, comp_location TEXT, comp_url TEXT,
            judge_name TEXT, judge_country TEXT, judge_id INTEGER,
            role TEXT, status TEXT, section TEXT, position TEXT, synced_at TEXT,
            UNIQUE(wdsf_comp_id, judge_name, section)
        )
    """)
    rows = conn.execute("""
        SELECT n.*, j.first_name, j.last_name, j.representing, j.specialty,
               j.judging_world_championships, j.judging_grand_slams
        FROM official_nominations n
        LEFT JOIN judges j ON n.judge_id = j.id
        ORDER BY n.comp_date, n.comp_name, n.section, n.position
    """).fetchall()
    conn.close()

    # Agrupar por competición
    comps = {}
    for r in rows:
        d = dict(r)
        cid = d["wdsf_comp_id"]
        if cid not in comps:
            comps[cid] = {
                "wdsf_comp_id": cid,
                "name":         d["comp_name"],
                "date":         d["comp_date"],
                "discipline":   d["comp_discipline"],
                "location":     d["comp_location"],
                "url":          d["comp_url"],
                "adjudicators": [],
                "nominated":    [],
                "synced_at":    d["synced_at"],
            }
        entry = {
            "judge_name":    d["judge_name"],
            "judge_country": d["judge_country"],
            "judge_id":      d["judge_id"],
            "position":      d["position"],
            "status":        d["status"],
            "matched":       d["judge_id"] is not None,
            "first_name":    d.get("first_name"),
            "last_name":     d.get("last_name"),
        }
        if d["section"] == "adjudicator":
            comps[cid]["adjudicators"].append(entry)
        else:
            comps[cid]["nominated"].append(entry)

    return jsonify(sorted(comps.values(), key=lambda x: x["date"] or ""))


@app.route("/api/nominations/committed_ids")
def nominations_committed():
    """IDs de jueces en paneles confirmados EN WDSF (adjudicadores ya asignados)."""
    conn = get_db()
    rows = conn.execute("""
        SELECT DISTINCT judge_id FROM official_nominations
        WHERE section='adjudicator' AND judge_id IS NOT NULL
    """).fetchall()
    conn.close()
    return jsonify([r[0] for r in rows])


# ─── Judge Correlation Analysis ───────────────────────────────────────────────

# (tabla de correlaciones gestionada en init_db())


def _rank_with_ties(lst):
    """Convert list of values to average ranks, handling ties."""
    n = len(lst)
    sorted_vals = sorted(lst)
    ranks = []
    for v in lst:
        positions = [i + 1 for i, x in enumerate(sorted_vals) if x == v]
        ranks.append(sum(positions) / len(positions))
    return ranks


def _spearman_corr(x, y):
    """Pearson correlation of ranks = Spearman r. Returns None if not enough data."""
    n = len(x)
    if n < 4:
        return None
    rx = _rank_with_ties(x)
    ry = _rank_with_ties(y)
    mean_rx = sum(rx) / n
    mean_ry = sum(ry) / n
    num   = sum((a - mean_rx) * (b - mean_ry) for a, b in zip(rx, ry))
    denom = (sum((a - mean_rx) ** 2 for a in rx) * sum((b - mean_ry) ** 2 for b in ry)) ** 0.5
    if denom == 0:
        return None
    return round(num / denom, 4)


def _scrape_officials_page(slug):
    """Fetch judge letter→name+country from /Competitions/Officials/{slug}.
    Returns dict: {letter: {name, country}}
    """
    url = f"https://www.worlddancesport.org/Competitions/Officials/{slug}"
    r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0 WDSF-PanelSystem/1.0"})
    r.raise_for_status()

    officials = {}
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, "html.parser")
        for row in soup.select("table tr"):
            cells = row.find_all("td")
            if len(cells) >= 3:
                name       = cells[0].get_text(strip=True)
                country    = cells[1].get_text(strip=True)
                identifier = cells[2].get_text(strip=True)
                if identifier and 1 <= len(identifier) <= 4 and identifier.isalpha() and identifier.isupper():
                    officials[identifier] = {"name": name, "country": country}
    except Exception:
        # Regex fallback: "Name CZE A view"
        import re
        for m in re.finditer(
            r'<td[^>]*>([^<]+)</td>\s*<td[^>]*>([A-Z]{2,3})</td>\s*<td[^>]*>([A-Z]{1,4})</td>',
            r.text
        ):
            officials[m.group(3)] = {"name": m.group(1).strip(), "country": m.group(2).strip()}
    return officials


def _scrape_results_page(slug):
    """Fetch per-judge mark counts from /Competitions/Results/{slug}.
    Returns list of {round_num, couple, judge_letter, marks_count}
    """
    url = f"https://www.worlddancesport.org/Competitions/Results/{slug}"
    r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0 WDSF-PanelSystem/1.0"})
    r.raise_for_status()

    marks = []
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if not table:
            return marks

        rows = table.find_all("tr")
        if not rows:
            return marks

        # Count rounds: each round contributes 12 columns (A-K plus =)
        # Header row cells tell us the structure
        header_cells = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
        n_rounds = sum(1 for c in header_cells if c == "A")
        if n_rounds == 0:
            # Try second header row
            if len(rows) > 1:
                header_cells = [th.get_text(strip=True) for th in rows[1].find_all(["th", "td"])]
                n_rounds = sum(1 for c in header_cells if c == "A")

        JUDGE_LETTERS = list("ABCDEFGHIJK")

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if len(cells) < 14:
                continue
            # First column: rank (may be "1", "2-3", etc.)
            # Second column: couple number
            couple = cells[1] if len(cells) > 1 else None
            if not couple:
                continue
            # Keep only numeric couple numbers
            if not couple.replace(" ", "").isdigit():
                continue

            for round_idx in range(n_rounds):
                start = 2 + round_idx * 12
                if start + 11 > len(cells):
                    break
                for j_idx, letter in enumerate(JUDGE_LETTERS):
                    val_str = cells[start + j_idx] if start + j_idx < len(cells) else ""
                    if val_str and val_str.isdigit():
                        marks.append({
                            "round_num":    round_idx + 1,
                            "couple":       couple,
                            "judge_letter": letter,
                            "marks_count":  int(val_str),
                        })
    except Exception as e:
        pass

    return marks


def _scrape_marks_page(slug):
    """Fetch per-judge mark counts from /Competitions/Marks/{slug}.
    This handles the AJS raw-marks format where each cell is *, + or empty.
    Judge codes may be multi-character (AD, AI, AO, AQ, E, J, …).
    Returns list of {round_num, couple, judge_letter, marks_count}.
    """
    url = f"https://www.worlddancesport.org/Competitions/Marks/{slug}"
    try:
        r = requests.get(url, timeout=20,
                         headers={"User-Agent": "Mozilla/5.0 WDSF-PanelSystem/1.0"})
        r.raise_for_status()
    except Exception:
        return []

    marks = []
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if not table:
            return marks

        rows = table.find_all("tr")
        if len(rows) < 2:
            return marks

        # ── 1. Find header row with judge identifiers ──────────────────────
        # Judge codes are uppercase alpha 1-4 chars (A-K or AD/AI/AO/AQ/…)
        # The target row has ≥10 such cells (one per judge × repeated per dance)
        judge_header_idx = None
        judge_col_entries = []   # list of (col_idx, judge_id) for ALL occurrences

        for ri, row in enumerate(rows[:5]):
            cells = [th.get_text(strip=True) for th in row.find_all(["th", "td"])]
            entries = [
                (i, c) for i, c in enumerate(cells)
                if c and c.isalpha() and c.isupper() and 1 <= len(c) <= 4
                and c not in ("=",)
            ]
            if len(entries) >= 10:
                judge_header_idx = ri
                judge_col_entries = entries
                break

        if judge_header_idx is None:
            return marks

        # ── 2. Build per-judge dance-column lists ──────────────────────────
        # Same code appears once per dance (usually 5 dances)
        judge_dance_cols = {}   # judge_id -> [col_idx_d1, col_idx_d2, …]
        for ci, jid in judge_col_entries:
            judge_dance_cols.setdefault(jid, []).append(ci)

        # ── 3. Locate couple/round prefix columns ─────────────────────────
        # Typical layout: col0=Rank, col1=Couple, col2=Round, then judges
        first_judge_col = judge_col_entries[0][0]
        couple_col = max(0, first_judge_col - 2)
        round_col  = max(0, first_judge_col - 1)

        # ── 4. Parse data rows ─────────────────────────────────────────────
        current_couple = None

        for row in rows[judge_header_idx + 1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if len(cells) < first_judge_col:
                continue

            # Couple column: carry forward when empty (rowspan-style layout)
            couple_cell = cells[couple_col] if couple_col < len(cells) else ""
            if couple_cell and couple_cell.isdigit():
                current_couple = couple_cell

            if not current_couple:
                continue

            # Round number
            round_cell = cells[round_col] if round_col < len(cells) else ""
            try:
                round_num = int(round_cell)
            except (ValueError, TypeError):
                continue

            # Sum marks per judge across all dances (* and + both count as 1)
            for jid, dance_cols in judge_dance_cols.items():
                total = sum(
                    1 for ci in dance_cols
                    if ci < len(cells) and cells[ci] in ("*", "+")
                )
                marks.append({
                    "round_num":    round_num,
                    "couple":       current_couple,
                    "judge_letter": jid,
                    "marks_count":  total,
                })
    except Exception:
        pass

    return marks


def _parse_scores_table(table):
    """Parse a single WDSF Scores-page table.
    Returns {(couple_str, judge_letter): float_score} or {} if not a per-judge table.
    """
    rows = table.find_all("tr")
    if len(rows) < 2:
        return {}

    # Find the header row that contains judge identifiers.
    # Judges can be single-letter (A-K) OR multi-char codes (AD, AI, AO, AQ, L, O, P, Q, Y, …)
    # Exclude known criteria labels and dividers.
    SKIP_LABELS = {"=", "TQ", "PS", "MM", "CP", "SUM", "TOTAL"}
    judge_cols = {}     # letter/code -> [col_idx, ...]
    header_row_idx = None

    for ri, row in enumerate(rows[:4]):
        cells = row.find_all(["th", "td"])
        col_idx = 0
        temp = {}
        for cell in cells:
            text  = cell.get_text(strip=True)
            span  = int(cell.get("colspan", "1") or "1")
            # Judge identifier: uppercase alpha, 1-4 chars, not a criteria/divider label
            if (text and text.isalpha() and text.isupper()
                    and 1 <= len(text) <= 4 and text not in SKIP_LABELS):
                temp[text] = list(range(col_idx, col_idx + span))
            col_idx += span
        if len(temp) >= 3:
            judge_cols       = temp
            header_row_idx   = ri
            break

    if not judge_cols:
        return {}   # Not a per-judge scoring table (e.g., recall / skating table)

    result = {}
    current_couple = None

    for row in rows[header_row_idx + 1:]:
        cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
        if not cells or (len(cells) == 1 and not cells[0]):
            continue  # empty separator row

        # Couple number always in first cell; carry forward if blank
        first = cells[0] if cells else ""
        if first and first.isdigit():
            current_couple = first

        if not current_couple:
            continue

        # Skip subheader rows (criteria labels like "TQ & PS")
        if not first.isdigit() and current_couple and first not in ("", ):
            # Only skip if this looks like a real sub-header (non-numeric, non-empty)
            # but keep going – we still accumulate values for the current couple
            pass

        for letter, cols in judge_cols.items():
            total = 0.0
            for ci in cols:
                if ci < len(cells) and cells[ci]:
                    val = cells[ci].rstrip("+").strip()
                    try:
                        total += float(val)
                    except ValueError:
                        pass
            if total > 0:
                key = (current_couple, letter)
                result[key] = result.get(key, 0.0) + total

    return result


def _parse_scores_table_worldopen(table, name_to_letter):
    """Parse World Open (new series) Scores-page table format.
    In this format judges appear as full-name row values, not as column headers.

    Structure:
      Header row : ["Couple", "Waltz", "Tango", ..., "Total", "Place"]
      Per couple :
        Couple row : [couple_num, dance1_total, ..., grand_total, place]
        Per dance (repeated n_dances times):
          ["Component score", ...] optional aggregate row
          [judge_full_name, TQ, (blank), PS, (blank), dance_total]  × n_judges

    Returns {(couple_str, judge_letter): float_total_score_across_dances}
    """
    rows = table.find_all("tr")
    if len(rows) < 3:
        return {}

    # Detect: first header cell must be "Couple"
    header_cells = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
    if not header_cells or header_cells[0].strip().lower() != "couple":
        return {}

    result = {}
    current_couple = None

    # Build lookup indices for robust name matching:
    #   exact         → {name: letter}
    #   lowercase     → {name.lower(): letter}
    #   reversed-lower → {"last first".lower(): letter}  (handles "WOTA Robert" ↔ "Robert Wota")
    #   token-set     → {frozenset(tokens): letter}
    name_lower = {}
    name_reversed = {}
    name_tokens = {}
    for name, letter in name_to_letter.items():
        nl = name.lower().strip()
        name_lower[nl] = letter
        parts = nl.split()
        if len(parts) >= 2:
            name_reversed[" ".join(reversed(parts))] = letter
            name_tokens[frozenset(parts)] = letter

    def _resolve_name(cell_text):
        """Try several normalisation strategies to map a cell to a judge letter."""
        t = cell_text.strip()
        if not t:
            return None
        lw = t.lower()
        # 1. exact
        if t in name_to_letter:
            return name_to_letter[t]
        # 2. lowercase
        if lw in name_lower:
            return name_lower[lw]
        # 3. reversed token order
        if lw in name_reversed:
            return name_reversed[lw]
        # 4. token-set match (order-independent)
        tokens = frozenset(lw.split())
        if tokens in name_tokens:
            return name_tokens[tokens]
        return None

    for row in rows[1:]:
        cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
        if not cells:
            continue
        first = cells[0].strip()

        # Couple-number row: first cell is purely numeric (couple bib)
        if first.isdigit():
            current_couple = first
            continue

        if not current_couple:
            continue

        # Skip aggregate/component rows and blank rows
        if not first or first.lower().startswith("component"):
            continue

        # Try to resolve judge name to letter
        letter = _resolve_name(first)
        if not letter:
            continue

        # The last non-empty numeric value in the row is the judge's dance total
        dance_score = 0.0
        for val in reversed(cells[1:]):
            v = val.strip().rstrip("+")
            if v:
                try:
                    dance_score = float(v)
                    break
                except ValueError:
                    continue

        if dance_score > 0:
            key = (current_couple, letter)
            result[key] = result.get(key, 0.0) + dance_score

    return result


FINAL_SKIP = {"=", "TQ", "PS", "MM", "CP", "SUM", "TOTAL"}


def _scrape_final_page(slug):
    """Fetch skating positions from /Competitions/Final/{slug}.
    Used for International Open format where the final uses the skating system.
    Each per-dance table has columns: Couple | <judge_letters> | 1.|1. ... | Place
    Returns list of {round_num: 99, couple, judge_letter, marks_count}.
    marks_count = sum of positions (1-6) across all dances × 100.
    """
    url = f"https://www.worlddancesport.org/Competitions/Final/{slug}"
    try:
        r = requests.get(url, timeout=20,
                         headers={"User-Agent": "Mozilla/5.0 WDSF-PanelSystem/1.0"})
        r.raise_for_status()
    except Exception:
        return []

    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, "html.parser")
        body = (soup.find("div", {"id": "content"})
                or soup.find("main")
                or soup.body)
        if not body:
            return []

        totals = {}  # (couple_str, judge_letter) → cumulative position sum

        for table in body.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue
            header_cells = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
            if not header_cells or header_cells[0].strip().lower() != "couple":
                continue

            # Identify judge-letter columns: uppercase alpha, no "|", not in skip set
            judge_cols = {}  # col_index → letter
            for ci, h in enumerate(header_cells[1:], start=1):
                h = h.strip()
                if (h and h.isalpha() and h.isupper()
                        and 1 <= len(h) <= 4 and h not in FINAL_SKIP):
                    judge_cols[ci] = h

            if not judge_cols:
                continue  # not a per-dance skating table

            for row in rows[1:]:
                cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                if not cells:
                    continue
                first = cells[0].strip()
                if not first.isdigit():
                    continue  # skip non-couple rows
                couple = first
                for ci, letter in judge_cols.items():
                    if ci >= len(cells):
                        continue
                    val = cells[ci].strip().rstrip(".")
                    try:
                        pos = float(val)
                        if 1.0 <= pos <= 9.0:  # sanity: valid skating position
                            key = (couple, letter)
                            totals[key] = totals.get(key, 0.0) + pos
                    except ValueError:
                        pass

        return [
            {"round_num": 99, "couple": couple, "judge_letter": letter,
             "marks_count": int(round(total * 100))}
            for (couple, letter), total in totals.items()
        ]

    except Exception:
        return []


def _scrape_scores_page(slug, officials=None):
    """Fetch per-judge numerical scores from /Competitions/Scores/{slug}.
    Parses the Final (round 99) and each numbered round (4, 5, …).
    Returns list of {round_num, couple, judge_letter, marks_count}.
    marks_count = total score × 100, stored as integer so the schema is happy.

    officials: dict {letter: {name, country}} from _scrape_officials_page.
    Required to resolve full judge names in the World Open (new series) format.
    """
    url = f"https://www.worlddancesport.org/Competitions/Scores/{slug}"
    try:
        r = requests.get(url, timeout=20,
                         headers={"User-Agent": "Mozilla/5.0 WDSF-PanelSystem/1.0"})
        r.raise_for_status()
    except Exception:
        return []

    # Build name → letter reverse map for World Open format
    name_to_letter = {}
    if officials:
        for letter, info in officials.items():
            name = info.get("name", "")
            if name:
                name_to_letter[name] = letter

    marks = []
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, "html.parser")

        # Walk elements in document order; H2 headings set the current round
        # "Final" → 99   |   "N. Round" → N
        current_round = None
        # Accumulate scores per round: {round_num: {(couple, judge_letter): float}}
        round_scores = {}

        # Find main content area (skip nav/sidebar)
        body = (soup.find("div", {"id": "content"})
                or soup.find("main")
                or soup.body)
        if not body:
            return marks

        for elem in body.find_all(["h2", "table"]):
            if elem.name == "h2":
                text = elem.get_text(strip=True)
                if text == "Final":
                    current_round = 99
                elif ". Round" in text:
                    try:
                        current_round = int(text.split(".")[0].strip())
                    except ValueError:
                        current_round = None
                else:
                    current_round = None  # Rule X sub-headings are H3, so this is unlikely
                continue

            if elem.name == "table" and current_round is not None:
                # Try GrandSlam format first (judge letters in column headers)
                parsed = _parse_scores_table(elem)
                # Fall back to World Open format (judge full names in row cells)
                if not parsed and name_to_letter:
                    parsed = _parse_scores_table_worldopen(elem, name_to_letter)
                if parsed:
                    bucket = round_scores.setdefault(current_round, {})
                    for (couple, letter), score in parsed.items():
                        bucket[(couple, letter)] = bucket.get((couple, letter), 0.0) + score

        # Convert accumulated scores to marks list
        for round_num, bucket in round_scores.items():
            for (couple, letter), total in bucket.items():
                marks.append({
                    "round_num":    round_num,
                    "couple":       couple,
                    "judge_letter": letter,
                    "marks_count":  int(round(total * 100)),
                })

    except Exception:
        pass

    return marks


def _compute_all_correlations():
    """Recompute Spearman correlations for all judge pairs, separately per discipline.
    Only uses the FINAL round of each competition (highest round_num per slug+discipline),
    as finals reflect the most important judging decisions on the top couples.
    """
    conn = get_db()
    rows = conn.execute("""
        SELECT competition_slug, round_num, discipline, judge_name, couple_num, marks_count
        FROM judge_marks_history
        ORDER BY competition_slug, round_num, judge_name
    """).fetchall()

    from collections import defaultdict

    # Identify the final round (max round_num) per (slug, discipline)
    max_round = {}
    for r in rows:
        disc = r["discipline"] or "Unknown"
        key  = (r["competition_slug"], disc)
        if key not in max_round or r["round_num"] > max_round[key]:
            max_round[key] = r["round_num"]

    # Group by discipline, only for final rounds → (slug, round) → judge → couple
    data_by_disc = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    for r in rows:
        disc = r["discipline"] or "Unknown"
        key  = (r["competition_slug"], disc)
        if r["round_num"] != max_round[key]:
            continue   # skip non-final rounds
        round_key = (r["competition_slug"], r["round_num"])
        data_by_disc[disc][round_key][r["judge_name"]][r["couple_num"]] = r["marks_count"]

    # Accumulate per-discipline correlation data
    pair_data_by_disc = defaultdict(lambda: defaultdict(list))
    # pair_data_by_disc[disc][(nameA, nameB)] → [(corr, n_points), …]

    for disc, data in data_by_disc.items():
        for (slug, rnd), judge_map in data.items():
            judge_names = sorted(judge_map.keys())
            for i in range(len(judge_names)):
                for j in range(i + 1, len(judge_names)):
                    name_a  = judge_names[i]
                    name_b  = judge_names[j]
                    if not name_a or not name_a.strip() or not name_b or not name_b.strip():
                        continue
                    # Excluir identificadores anónimos tipo "Judge_A", "Judge_B", etc.
                    import re as _re
                    if _re.match(r'^Judge_[A-Z]$', name_a) or _re.match(r'^Judge_[A-Z]$', name_b):
                        continue
                    couples_a = judge_map[name_a]
                    couples_b = judge_map[name_b]
                    common  = sorted(set(couples_a.keys()) & set(couples_b.keys()))
                    if len(common) < 4:
                        continue
                    x = [couples_a[c] for c in common]
                    y = [couples_b[c] for c in common]
                    if len(set(x)) < 2 or len(set(y)) < 2:
                        continue
                    corr = _spearman_corr(x, y)
                    if corr is not None:
                        pair_data_by_disc[disc][(name_a, name_b)].append((corr, len(common)))

    # Store per-discipline weighted-average correlations
    from datetime import datetime
    now = datetime.utcnow().isoformat()
    conn.execute("DELETE FROM judge_pair_correlations")
    total = 0
    for disc, pair_data in pair_data_by_disc.items():
        for (name_a, name_b), entries in pair_data.items():
            if not name_a or not name_a.strip() or not name_b or not name_b.strip():
                continue
            if len(entries) < 3:          # ignorar pares con menos de 3 competiciones
                continue
            total_pts = sum(n for _, n in entries)
            if total_pts == 0:
                continue
            weighted_corr = sum(c * n for c, n in entries) / total_pts
            conn.execute("""
                INSERT OR REPLACE INTO judge_pair_correlations
                (judge_name_a, judge_name_b, discipline, correlation, n_competitions, n_data_points, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (name_a, name_b, disc, round(weighted_corr, 4), len(entries), total_pts, now))
            total += 1

    conn.commit()
    conn.close()
    return total


@app.route("/api/correlations/scrape", methods=["POST"])
def scrape_competition_marks():
    """Scrape Officials + Results pages for a WDSF competition and store marks."""
    data = request.json or {}
    slug = data.get("slug", "").strip()
    if not slug:
        return jsonify({"error": "slug required"}), 400

    try:
        # 1. Fetch officials (letter → name mapping)
        officials = _scrape_officials_page(slug)
        if not officials:
            return jsonify({"error": "Could not parse officials page", "slug": slug}), 422

        # 2. Fetch preliminary marks (* / + format from Marks page or Results page)
        marks = _scrape_results_page(slug)
        if not marks:
            marks = _scrape_marks_page(slug)
        if not marks:
            return jsonify({"error": "Could not parse results or marks page", "slug": slug}), 422

        # 2b. Fetch final/semifinal numerical scores from Scores page (round 99 = Final)
        scores_marks = _scrape_scores_page(slug, officials)

        # 2c. For Int. Open (skating final): try /Final page if Scores page has no round-99
        final_marks = []
        if not any(m["round_num"] == 99 for m in scores_marks):
            final_marks = _scrape_final_page(slug)

        # Merge: use scores_marks + final_marks for rounds that appear there
        special_rounds = ({m["round_num"] for m in scores_marks}
                          | {m["round_num"] for m in final_marks})
        all_marks = ([m for m in marks if m["round_num"] not in special_rounds]
                     + scores_marks + final_marks)

        # 3. Store in DB
        from datetime import datetime
        now = datetime.utcnow().isoformat()

        # Extract meta from slug (best effort)
        parts = slug.split("-")
        comp_name = slug
        comp_date = None
        discipline = None
        for p in parts:
            if p in ("Standard", "Latin", "Combined"):
                discipline = p
            if len(p) == 8 and p.isdigit():
                comp_date = p[:4] + "-" + p[4:6] + "-" + p[6:]

        conn = get_db()

        # Count unique judges, couples, rounds
        judge_letters_seen = {m["judge_letter"] for m in all_marks}
        couples_seen = {m["couple"] for m in all_marks}
        rounds_seen = {m["round_num"] for m in all_marks}

        # Store scraped_competitions record
        conn.execute("""
            INSERT OR REPLACE INTO scraped_competitions
            (slug, competition_name, competition_date, discipline, n_rounds, n_judges, n_couples, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (slug, comp_name, comp_date, discipline,
              len(rounds_seen), len(judge_letters_seen), len(couples_seen), now))

        # Delete existing marks for this slug before re-inserting
        conn.execute("DELETE FROM judge_marks_history WHERE competition_slug = ?", (slug,))

        # Store per-judge marks, joining with officials for name
        inserted = 0
        for m in all_marks:
            letter = m["judge_letter"]
            official = officials.get(letter, {})
            judge_name    = official.get("name", f"Judge_{letter}")
            judge_country = official.get("country", "")
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO judge_marks_history
                    (competition_slug, competition_name, competition_date, discipline,
                     round_num, judge_letter, judge_name, judge_country,
                     couple_num, marks_count, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (slug, comp_name, comp_date, discipline,
                      m["round_num"], letter, judge_name, judge_country,
                      m["couple"], m["marks_count"], now))
                inserted += 1
            except Exception:
                pass

        conn.commit()
        conn.close()

        return jsonify({
            "ok": True,
            "slug": slug,
            "officials": officials,
            "n_marks_stored": inserted,
            "n_rounds": len(rounds_seen),
            "n_judges": len(judge_letters_seen),
            "n_couples": len(couples_seen),
            "has_final_scores": bool(scores_marks or final_marks),
            "n_final_marks": len(scores_marks) + len(final_marks),
            "final_source": "scores" if scores_marks else ("skating" if final_marks else "none"),
        })

    except Exception as e:
        return jsonify({"error": str(e), "slug": slug}), 500


@app.route("/api/correlations/scrape-bulk", methods=["POST"])
def scrape_bulk():
    """Scrape a list of competition slugs and store their marks.
    Body: {slugs: ["slug1", "slug2", ...]}
    Skips slugs already in scraped_competitions unless force=true.
    Returns {done, ok, skipped, errors, withFinal}.
    """
    from datetime import datetime as _dt
    data = request.json or {}
    slugs = data.get("slugs", [])
    force = data.get("force", False)

    conn = get_db()
    already = {r["slug"] for r in conn.execute("SELECT slug FROM scraped_competitions").fetchall()}
    conn.close()

    done = ok_count = skipped = with_final = 0
    errors = []

    for slug in slugs:
        if slug in already and not force:
            skipped += 1
            done += 1
            continue
        try:
            officials = _scrape_officials_page(slug)
            if not officials:
                errors.append({"slug": slug, "error": "no officials"})
                done += 1
                continue

            marks = _scrape_results_page(slug)
            if not marks:
                marks = _scrape_marks_page(slug)
            if not marks:
                errors.append({"slug": slug, "error": "no marks"})
                done += 1
                continue

            scores_marks = _scrape_scores_page(slug, officials)
            final_marks = []
            if not any(m["round_num"] == 99 for m in scores_marks):
                final_marks = _scrape_final_page(slug)
            special_rounds = ({m["round_num"] for m in scores_marks}
                              | {m["round_num"] for m in final_marks})
            all_marks = ([m for m in marks if m["round_num"] not in special_rounds]
                         + scores_marks + final_marks)

            now = _dt.utcnow().isoformat()
            parts = slug.split("-")
            discipline = next((p for p in parts if p in ("Standard", "Latin", "Combined")), None)
            comp_date = None
            for p in parts:
                if len(p) == 8 and p.isdigit():
                    comp_date = p[:4] + "-" + p[4:6] + "-" + p[6:]

            judge_letters_seen = {m["judge_letter"] for m in all_marks}
            couples_seen = {m["couple"] for m in all_marks}
            rounds_seen  = {m["round_num"] for m in all_marks}

            c = get_db()
            c.execute("""
                INSERT OR REPLACE INTO scraped_competitions
                (slug, competition_name, competition_date, discipline,
                 n_rounds, n_judges, n_couples, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (slug, slug, comp_date, discipline,
                  len(rounds_seen), len(judge_letters_seen), len(couples_seen), now))
            c.execute("DELETE FROM judge_marks_history WHERE competition_slug = ?", (slug,))
            for m in all_marks:
                letter = m["judge_letter"]
                official = officials.get(letter, {})
                judge_name    = official.get("name", f"Judge_{letter}")
                judge_country = official.get("country", "")
                try:
                    c.execute("""
                        INSERT OR REPLACE INTO judge_marks_history
                        (competition_slug, competition_name, competition_date, discipline,
                         round_num, judge_letter, judge_name, judge_country,
                         couple_num, marks_count, scraped_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (slug, slug, comp_date, discipline,
                          m["round_num"], letter, judge_name, judge_country,
                          m["couple"], m["marks_count"], now))
                except Exception:
                    pass
            c.commit()
            c.close()

            if scores_marks or final_marks:
                with_final += 1
            ok_count += 1
        except Exception as exc:
            errors.append({"slug": slug, "error": str(exc)})
        done += 1

    # Recompute correlations after bulk scrape
    n_pairs = 0
    try:
        n_pairs = _compute_all_correlations()
    except Exception:
        pass

    return jsonify({
        "finished":  True,
        "done":      done,
        "ok":        ok_count,
        "skipped":   skipped,
        "withFinal": with_final,
        "pairs":     n_pairs,
        "errors":    errors[:20],
    })


@app.route("/api/correlations/rescrape-all", methods=["POST"])
def rescrape_all():
    """Re-scrape all already-scraped competitions using the latest parsing logic.
    Useful after server code updates to pick up new Scores-page parsers.
    Returns progress JSON: {done, total, withFinal, errors, finished}.
    """
    conn = get_db()
    slugs = [r["slug"] for r in conn.execute("SELECT slug FROM scraped_competitions ORDER BY slug").fetchall()]
    conn.close()

    done = 0
    with_final = 0
    errors = []

    from datetime import datetime as _dt

    for slug in slugs:
        try:
            officials = _scrape_officials_page(slug)
            if not officials:
                errors.append({"slug": slug, "error": "no officials"})
                done += 1
                continue

            marks = _scrape_results_page(slug)
            if not marks:
                marks = _scrape_marks_page(slug)
            if not marks:
                errors.append({"slug": slug, "error": "no marks"})
                done += 1
                continue

            scores_marks = _scrape_scores_page(slug, officials)
            final_marks = []
            if not any(m["round_num"] == 99 for m in scores_marks):
                final_marks = _scrape_final_page(slug)
            special_rounds = ({m["round_num"] for m in scores_marks}
                              | {m["round_num"] for m in final_marks})
            all_marks = ([m for m in marks if m["round_num"] not in special_rounds]
                         + scores_marks + final_marks)

            now = _dt.utcnow().isoformat()
            parts = slug.split("-")
            discipline = next((p for p in parts if p in ("Standard", "Latin", "Combined")), None)
            comp_date = None
            for p in parts:
                if len(p) == 8 and p.isdigit():
                    comp_date = p[:4] + "-" + p[4:6] + "-" + p[6:]

            judge_letters_seen = {m["judge_letter"] for m in all_marks}
            couples_seen = {m["couple"] for m in all_marks}
            rounds_seen  = {m["round_num"] for m in all_marks}

            c = get_db()
            c.execute("""
                INSERT OR REPLACE INTO scraped_competitions
                (slug, competition_name, competition_date, discipline,
                 n_rounds, n_judges, n_couples, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (slug, slug, comp_date, discipline,
                  len(rounds_seen), len(judge_letters_seen), len(couples_seen), now))

            c.execute("DELETE FROM judge_marks_history WHERE competition_slug = ?", (slug,))

            for m in all_marks:
                letter   = m["judge_letter"]
                official = officials.get(letter, {})
                judge_name    = official.get("name", f"Judge_{letter}")
                judge_country = official.get("country", "")
                try:
                    c.execute("""
                        INSERT OR REPLACE INTO judge_marks_history
                        (competition_slug, competition_name, competition_date, discipline,
                         round_num, judge_letter, judge_name, judge_country,
                         couple_num, marks_count, scraped_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (slug, slug, comp_date, discipline,
                          m["round_num"], letter, judge_name, judge_country,
                          m["couple"], m["marks_count"], now))
                except Exception:
                    pass
            c.commit()
            c.close()

            if scores_marks or final_marks:
                with_final += 1
        except Exception as exc:
            errors.append({"slug": slug, "error": str(exc)})

        done += 1

    # Recompute correlations
    n_pairs = 0
    try:
        n_pairs = _compute_all_correlations()
    except Exception:
        pass

    return jsonify({
        "finished":  True,
        "done":      done,
        "total":     len(slugs),
        "withFinal": with_final,
        "pairs":     n_pairs,
        "errors":    errors,
    })


@app.route("/api/panel-diversity", methods=["POST"])
def panel_diversity():
    """Compute diversity score for a proposed panel of judges.
    Body: {judges: ["Name1", "Name2", ...], discipline: "Standard"|"Latin"}
    Returns diversity score (0-100), avg correlation, pair matrix, worst/best pairs.
    Diversity = (1 - avg_corr) / 2 * 100:
      100 = judges always disagree (maximum diversity)
       50 = judges are independent (ideal)
        0 = judges always agree (minimum diversity / problematic)
    """
    data = request.json or {}
    judges = [j.strip() for j in data.get("judges", []) if j.strip()]
    discipline = data.get("discipline", "").strip()

    if len(judges) < 2:
        return jsonify({"error": "Need at least 2 judges"}), 400

    conn = get_db()
    try:
        if discipline:
            rows = conn.execute("""
                SELECT judge_name_a, judge_name_b, correlation, n_competitions, n_data_points
                FROM judge_pair_correlations WHERE discipline = ?
            """, (discipline,)).fetchall()
        else:
            rows = conn.execute("""
                SELECT judge_name_a, judge_name_b, AVG(correlation) as correlation,
                       SUM(n_competitions) as n_competitions, SUM(n_data_points) as n_data_points
                FROM judge_pair_correlations
                GROUP BY judge_name_a, judge_name_b
            """).fetchall()
    finally:
        conn.close()

    # Build lookup: (nameA, nameB) → {corr, n_comp, n_pts}  (both orderings)
    corr_map = {}
    for r in rows:
        key1 = (r["judge_name_a"], r["judge_name_b"])
        key2 = (r["judge_name_b"], r["judge_name_a"])
        entry = {"corr": r["correlation"], "n_comp": r["n_competitions"], "n_pts": r["n_data_points"]}
        corr_map[key1] = entry
        corr_map[key2] = entry

    # Build matrix of all pairs
    pairs = []
    known_corrs = []
    for i in range(len(judges)):
        for j in range(i + 1, len(judges)):
            a, b = judges[i], judges[j]
            entry = corr_map.get((a, b)) or corr_map.get((b, a))
            pair = {
                "judge_a": a,
                "judge_b": b,
                "correlation": round(entry["corr"], 4) if entry else None,
                "n_competitions": entry["n_comp"] if entry else 0,
                "known": entry is not None,
            }
            pairs.append(pair)
            if entry:
                known_corrs.append(entry["corr"])

    # Compute diversity metrics
    n_total = len(pairs)
    n_known = len(known_corrs)
    avg_corr = sum(known_corrs) / n_known if known_corrs else None
    diversity_score = round((1 - avg_corr) / 2 * 100, 1) if avg_corr is not None else None

    known_pairs = [p for p in pairs if p["known"]]
    worst_pair = max(known_pairs, key=lambda p: p["correlation"]) if known_pairs else None
    best_pair  = min(known_pairs, key=lambda p: p["correlation"]) if known_pairs else None

    # Color rating
    if diversity_score is None:
        rating = "unknown"
    elif diversity_score >= 60:
        rating = "excellent"
    elif diversity_score >= 50:
        rating = "good"
    elif diversity_score >= 40:
        rating = "caution"
    else:
        rating = "poor"

    return jsonify({
        "ok": True,
        "judges": judges,
        "discipline": discipline,
        "n_judges": len(judges),
        "n_pairs_total": n_total,
        "n_pairs_known": n_known,
        "avg_correlation": round(avg_corr, 4) if avg_corr is not None else None,
        "diversity_score": diversity_score,
        "rating": rating,
        "worst_pair": worst_pair,
        "best_pair": best_pair,
        "pairs": sorted(pairs, key=lambda p: (-(p["correlation"] or -99) if p["known"] else -99)),
    })


@app.route("/api/correlations/compute", methods=["POST"])
def compute_correlations():
    """Recompute all Spearman correlations from stored mark history."""
    try:
        n_pairs = _compute_all_correlations()
        return jsonify({"ok": True, "pairs_computed": n_pairs})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/correlations")
def get_correlations():
    """Return correlation matrix for all judge pairs. Optional ?discipline=Standard|Latin filter."""
    disc_filter = request.args.get("discipline", "").strip()
    conn = get_db()
    try:
        if disc_filter:
            rows = conn.execute("""
                SELECT judge_name_a, judge_name_b, discipline, correlation, n_competitions, n_data_points
                FROM judge_pair_correlations
                WHERE discipline = ?
                ORDER BY correlation DESC
            """, (disc_filter,)).fetchall()
        else:
            rows = conn.execute("""
                SELECT judge_name_a, judge_name_b, discipline, correlation, n_competitions, n_data_points
                FROM judge_pair_correlations
                ORDER BY correlation DESC
            """).fetchall()
    except Exception:
        rows = []

    # Also get list of scraped competitions
    try:
        comps = conn.execute("""
            SELECT slug, competition_name, competition_date, discipline,
                   n_rounds, n_judges, n_couples, scraped_at
            FROM scraped_competitions
            ORDER BY competition_date DESC
        """).fetchall()
    except Exception:
        comps = []

    conn.close()
    return jsonify({
        "correlations": [dict(r) for r in rows],
        "scraped_competitions": [dict(c) for c in comps],
    })


@app.route("/api/correlations/panel-score", methods=["POST"])
def panel_independence_score():
    """Given a list of judge names and a discipline, return pairwise correlations.
    Body: {names: [...], discipline: "Standard"|"Latin"}
    """
    data = request.json or {}
    names      = data.get("names", [])
    discipline = data.get("discipline", "")
    if len(names) < 2:
        return jsonify({"avg_correlation": None, "pairs": []})

    conn = get_db()
    try:
        placeholders = ",".join("?" * len(names))
        if discipline:
            rows = conn.execute(f"""
                SELECT judge_name_a, judge_name_b, discipline, correlation,
                       n_competitions, n_data_points
                FROM judge_pair_correlations
                WHERE discipline = ?
                  AND judge_name_a IN ({placeholders})
                  AND judge_name_b IN ({placeholders})
            """, [discipline] + names + names).fetchall()
        else:
            rows = conn.execute(f"""
                SELECT judge_name_a, judge_name_b, discipline, correlation,
                       n_competitions, n_data_points
                FROM judge_pair_correlations
                WHERE judge_name_a IN ({placeholders})
                  AND judge_name_b IN ({placeholders})
            """, names + names).fetchall()
    except Exception:
        rows = []
    conn.close()

    pairs = [{"judge_a": r["judge_name_a"], "judge_b": r["judge_name_b"],
              "discipline": r["discipline"], "correlation": r["correlation"],
              "n_competitions": r["n_competitions"] or 0,
              "n_data_points": r["n_data_points"] or 0}
             for r in rows]
    high_risk = [p for p in pairs if p["correlation"] > 0.7]
    avg = sum(p["correlation"] for p in pairs) / len(pairs) if pairs else None
    return jsonify({"avg_correlation": avg, "pairs": pairs, "high_risk_pairs": high_risk})


@app.route("/api/correlations/pair-detail")
def pair_detail():
    """Return per-competition evidence for a judge pair.
    Query params: judge_a, judge_b, discipline (optional)
    Returns list of competitions with couple-by-couple marks for both judges.
    """
    name_a     = request.args.get("judge_a", "").strip()
    name_b     = request.args.get("judge_b", "").strip()
    discipline = request.args.get("discipline", "").strip()
    finals_only = request.args.get("finals_only", "1").strip() != "0"
    if not name_a or not name_b:
        return jsonify({"error": "judge_a and judge_b required"}), 400

    conn = get_db()
    try:
        q = """
            SELECT competition_slug, round_num, discipline, judge_name, couple_num, marks_count
            FROM judge_marks_history
            WHERE judge_name IN (?, ?)
        """
        params = [name_a, name_b]
        if discipline:
            q += " AND discipline = ?"
            params.append(discipline)
        q += " ORDER BY competition_slug, round_num, couple_num"
        rows = conn.execute(q, params).fetchall()
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()

    from collections import defaultdict

    # Find final round per (slug, discipline) if finals_only
    if finals_only:
        max_rnd = {}
        for r in rows:
            disc = r["discipline"] or "Unknown"
            k = (r["competition_slug"], disc)
            if k not in max_rnd or r["round_num"] > max_rnd[k]:
                max_rnd[k] = r["round_num"]

    # Group by (slug, round)
    rounds = defaultdict(lambda: {"a": {}, "b": {}})
    disc_map = {}
    for r in rows:
        disc = r["discipline"] or "Unknown"
        if finals_only and r["round_num"] != max_rnd.get((r["competition_slug"], disc)):
            continue
        key = (r["competition_slug"], r["round_num"])
        disc_map[key] = disc
        if r["judge_name"] == name_a:
            rounds[key]["a"][r["couple_num"]] = r["marks_count"]
        else:
            rounds[key]["b"][r["couple_num"]] = r["marks_count"]

    competitions = []
    for (slug, rnd), data in sorted(rounds.items()):
        marks_a = data["a"]
        marks_b = data["b"]
        common  = sorted(set(marks_a.keys()) & set(marks_b.keys()))
        if len(common) < 4:
            continue
        x = [marks_a[c] for c in common]
        y = [marks_b[c] for c in common]
        corr = _spearman_corr(x, y)

        couples = [{"couple": c, "marks_a": marks_a[c], "marks_b": marks_b[c]} for c in common]
        competitions.append({
            "slug":        slug,
            "round":       rnd,
            "discipline":  disc_map[(slug, rnd)],
            "correlation": round(corr, 4) if corr is not None else None,
            "n_couples":   len(common),
            "couples":     couples,
        })

    competitions.sort(key=lambda c: (c["correlation"] or 0), reverse=True)
    return jsonify({
        "judge_a":      name_a,
        "judge_b":      name_b,
        "competitions": competitions,
        "n_competitions": len(competitions),
    })


# ─── End Judge Correlation Analysis ───────────────────────────────────────────


def _slugs_from_event_page(event_href):
    """Fetch a WDSF /Events/... page and return Adult Standard/Latin competition slugs.
    Works because event pages are static HTML (not SPA).
    Returns list of {slug, discipline, event_type}.
    """
    HEADERS = {"User-Agent": "Mozilla/5.0 WDSF-PanelSystem/1.0"}
    results = []
    event_url = "https://www.worlddancesport.org" + event_href.split("#")[0]
    try:
        er = requests.get(event_url, timeout=15, headers=HEADERS)
        er.raise_for_status()
        from bs4 import BeautifulSoup
        esoup = BeautifulSoup(er.text, "html.parser")
        for a in esoup.find_all("a", href=True):
            href = a.get("href", "")
            # Event pages link to /Competitions/Ranking/, Results pages to /Results/ or /Marks/
            if ("/Competitions/Results/" not in href and "/Competitions/Marks/" not in href
                    and "/Competitions/Ranking/" not in href):
                continue
            slug = href.rstrip("/").split("/")[-1]
            if not slug:
                continue
            slug_lower = slug.lower()
            if "adult" not in slug_lower:
                continue
            if "standard" not in slug_lower and "latin" not in slug_lower:
                continue
            ev_type = "Unknown"
            sl_nohyphen = slug_lower.replace("-", "")
            if "grandslam" in sl_nohyphen:
                ev_type = "GrandSlam"
            elif "worldopen" in sl_nohyphen:
                ev_type = "World-Open"
            elif "worldchampionship" in sl_nohyphen or "worldchampionship" in sl_nohyphen:
                ev_type = "World-Championship"
            discipline = "Standard" if "standard" in slug_lower else "Latin"
            results.append({"slug": slug, "discipline": discipline, "event_type": ev_type})
    except Exception:
        pass
    return results


def _scan_calendar_month(month, year):
    """Scan one WDSF calendar month and return Adult Standard/Latin GS/WO/WCH slugs.
    NOTE: The WDSF calendar is a SPA — raw HTML has no competition data.
    This function is a no-op when called server-side; use /api/scan-event-page instead.
    Browser-side scanning should call /api/scan-event-page for each event href found
    in the Chrome-rendered calendar page.
    Returns empty list (calendar must be scanned via browser JS + /api/scan-event-page).
    """
    return []


@app.route("/api/scan-event-page", methods=["POST"])
def scan_event_page():
    """Given a WDSF /Events/... href, return Adult Standard/Latin competition slugs.
    Body: {event_href: "/Events/Brno-Czechia-19102025-8372"}
    """
    data = request.json or {}
    event_href = data.get("event_href", "").strip()
    if not event_href:
        return jsonify({"error": "event_href required"}), 400
    conn = get_db()
    scraped = {r["slug"] for r in conn.execute("SELECT slug FROM scraped_competitions").fetchall()}
    conn.close()
    slugs = _slugs_from_event_page(event_href)
    for s in slugs:
        s["already_scraped"] = s["slug"] in scraped
    return jsonify({"ok": True, "event_href": event_href, "competitions": slugs})


@app.route("/api/scan-calendar", methods=["POST"])
def scan_calendar():
    """Scan WDSF calendar for Adult Standard/Latin GS/World-Open/WCH competitions.
    Body: {months: [{month, year}]}  or  {from_date: "YYYY-MM-DD", to_date: "YYYY-MM-DD"}
    Returns list of slugs found (with already-scraped flag).
    """
    data = request.json or {}

    # Build list of (month, year) to scan
    month_list = []
    if "months" in data:
        for m in data["months"]:
            month_list.append((int(m["month"]), int(m["year"])))
    else:
        # Default: last 2 weeks (previous week + current) → last 1-2 months
        from datetime import datetime, timedelta
        end   = datetime.utcnow()
        start = end - timedelta(days=int(data.get("days_back", 14)))
        cur = start.replace(day=1)
        while cur <= end:
            month_list.append((cur.month, cur.year))
            # Advance one month
            if cur.month == 12:
                cur = cur.replace(year=cur.year + 1, month=1)
            else:
                cur = cur.replace(month=cur.month + 1)

    month_list = list(dict.fromkeys(month_list))

    # Get already-scraped slugs
    conn = get_db()
    scraped = {r["slug"] for r in conn.execute("SELECT slug FROM scraped_competitions").fetchall()}
    conn.close()

    found = []
    for month, year in month_list:
        comps = _scan_calendar_month(month, year)
        for c in comps:
            c["already_scraped"] = c["slug"] in scraped
            found.append(c)

    # Deduplicate by slug
    seen = set()
    unique = []
    for c in found:
        if c["slug"] not in seen:
            seen.add(c["slug"])
            unique.append(c)

    return jsonify({"ok": True, "competitions": unique, "months_scanned": len(month_list)})


@app.route("/api/auto-scrape", methods=["POST"])
def auto_scrape():
    """Scan calendar for the previous week, scrape any new competitions, recompute correlations.
    Designed to be called by a weekly cron/scheduled task.
    Body (optional): {days_back: 14}
    """
    import traceback
    data = request.json or {}
    days_back = int(data.get("days_back", 14))

    # 1. Scan calendar
    from datetime import datetime, timedelta
    end   = datetime.utcnow()
    start = end - timedelta(days=days_back)
    month_list = []
    cur = start.replace(day=1)
    while cur <= end:
        month_list.append((cur.month, cur.year))
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)

    conn = get_db()
    scraped = {r["slug"] for r in conn.execute("SELECT slug FROM scraped_competitions").fetchall()}
    conn.close()

    new_slugs = []
    for month, year in set(month_list):
        for c in _scan_calendar_month(month, year):
            if c["slug"] not in scraped and c["slug"] not in new_slugs:
                new_slugs.append(c["slug"])

    # 2. Scrape each new competition
    results = []
    for slug in new_slugs:
        try:
            officials = _scrape_officials_page(slug)
            if not officials:
                results.append({"slug": slug, "ok": False, "error": "no officials"})
                continue
            marks = _scrape_results_page(slug)
            if not marks:
                marks = _scrape_marks_page(slug)
            if not marks:
                results.append({"slug": slug, "ok": False, "error": "no marks"})
                continue

            # Merge in final/semifinal numerical scores from Scores page
            scores_marks = _scrape_scores_page(slug, officials)
            final_marks = []
            if not any(m["round_num"] == 99 for m in scores_marks):
                final_marks = _scrape_final_page(slug)
            special_rounds = ({m["round_num"] for m in scores_marks}
                              | {m["round_num"] for m in final_marks})
            all_marks = ([m for m in marks if m["round_num"] not in special_rounds]
                         + scores_marks + final_marks)

            from datetime import datetime as _dt
            now = _dt.utcnow().isoformat()
            parts = slug.split("-")
            discipline = next((p for p in parts if p in ("Standard", "Latin", "Combined")), None)
            comp_date = None
            for p in parts:
                if len(p) == 8 and p.isdigit():
                    comp_date = p[:4] + "-" + p[4:6] + "-" + p[6:]

            judge_letters_seen = {m["judge_letter"] for m in all_marks}
            couples_seen = {m["couple"] for m in all_marks}
            rounds_seen  = {m["round_num"] for m in all_marks}

            conn = get_db()
            conn.execute("""
                INSERT OR REPLACE INTO scraped_competitions
                (slug, competition_name, competition_date, discipline,
                 n_rounds, n_judges, n_couples, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (slug, slug, comp_date, discipline,
                  len(rounds_seen), len(judge_letters_seen), len(couples_seen), now))

            conn.execute("DELETE FROM judge_marks_history WHERE competition_slug = ?", (slug,))

            inserted = 0
            for m in all_marks:
                letter   = m["judge_letter"]
                official = officials.get(letter, {})
                judge_name    = official.get("name", f"Judge_{letter}")
                judge_country = official.get("country", "")
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO judge_marks_history
                        (competition_slug, competition_name, competition_date, discipline,
                         round_num, judge_letter, judge_name, judge_country,
                         couple_num, marks_count, scraped_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (slug, slug, comp_date, discipline,
                          m["round_num"], letter, judge_name, judge_country,
                          m["couple"], m["marks_count"], now))
                    inserted += 1
                except Exception:
                    pass
            conn.commit()
            conn.close()
            results.append({"slug": slug, "ok": True, "n_marks": inserted,
                             "has_final_scores": bool(scores_marks or final_marks),
                             "final_source": "scores" if scores_marks else ("skating" if final_marks else "none")})
        except Exception as exc:
            results.append({"slug": slug, "ok": False, "error": str(exc)})

    # 3. Recompute correlations if anything new was scraped
    n_pairs = 0
    if any(r["ok"] for r in results):
        try:
            n_pairs = _compute_all_correlations()
        except Exception:
            pass

    return jsonify({
        "ok": True,
        "new_competitions_found": len(new_slugs),
        "scraped": results,
        "pairs_recomputed": n_pairs,
        "days_back": days_back,
    })


# ── Judge license checker ──────────────────────────────────────────────────────
def _check_judge_license(wdsf_min):
    """Fetch WDSF athlete profile and return license status.
    Returns dict: {status, division, expires, wdsf_active}
    """
    from bs4 import BeautifulSoup
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,*/*",
    }
    try:
        r = requests.post(
            "https://www.worlddancesport.org/api/listitems/athletes",
            json={"name": str(wdsf_min), "page": 1, "pageSize": 3},
            headers=HEADERS, timeout=15
        )
        items = r.json().get("items", [])
        if not items:
            return None
        url = "https://www.worlddancesport.org" + items[0]["url"]
        rp = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(rp.text, "html.parser")
        # Find license section — look for "Status" near "Licenses"
        text = rp.text
        status   = None
        division = None
        expires  = None
        # Parse structured license block
        for block in soup.find_all(["td", "div", "p", "li"]):
            t = block.get_text(" ", strip=True)
            if "Status:" in t and "Expire" in t:
                for line in t.split("\n"):
                    line = line.strip()
                    if line.startswith("Status:"):
                        status = line.replace("Status:", "").strip()
                    elif line.startswith("Division:"):
                        division = line.replace("Division:", "").strip()
                    elif line.startswith("Expires") or line.startswith("Expire"):
                        expires = line.split(":")[-1].strip()
                break
        # Fallback: regex scan
        if not status:
            import re
            m = re.search(r"Status[:\s]+([A-Za-z]+)", text)
            if m:
                status = m.group(1)
        wdsf_active = (status or "").lower() == "active"
        return {"status": status, "division": division, "expires": expires,
                "wdsf_active": wdsf_active}
    except Exception as e:
        return {"error": str(e)}


_license_check_status = {"running": False, "processed": 0, "total": 0,
                          "changed": [], "errors": [], "done": False}

def _run_license_check_background(judge_rows):
    import time
    global _license_check_status
    _license_check_status.update({"running": True, "processed": 0,
                                   "total": len(judge_rows), "changed": [],
                                   "errors": [], "done": False})
    for j in judge_rows:
        result = _check_judge_license(j["wdsf_min"])
        if result and "wdsf_active" in result:
            local_active = bool(j["active"])
            wdsf_active  = result["wdsf_active"]
            if local_active != wdsf_active:
                _license_check_status["changed"].append({
                    "id": j["id"],
                    "name": f"{j['first_name']} {j['last_name']}",
                    "local_active": local_active,
                    "wdsf_active":  wdsf_active,
                    "status":  result.get("status"),
                    "expires": result.get("expires"),
                })
        elif result and "error" in result:
            _license_check_status["errors"].append(
                f"{j['first_name']} {j['last_name']}: {result['error']}"
            )
        _license_check_status["processed"] += 1
        time.sleep(0.3)
    _license_check_status["running"] = False
    _license_check_status["done"]    = True


@app.route("/api/judges/check-licenses", methods=["POST"])
def check_judge_licenses():
    """Background check: compare local active flag vs WDSF license status."""
    import threading
    global _license_check_status
    if _license_check_status["running"]:
        return jsonify({"ok": False, "error": "Already running"})
    conn = get_db()
    rows = conn.execute(
        "SELECT id, first_name, last_name, wdsf_min, active FROM judges WHERE wdsf_min IS NOT NULL"
    ).fetchall()
    conn.close()
    t = threading.Thread(target=_run_license_check_background, args=(rows,), daemon=True)
    t.start()
    return jsonify({"ok": True, "started": True, "total": len(rows)})


@app.route("/api/judges/check-licenses/status", methods=["GET"])
def check_licenses_status():
    return jsonify(_license_check_status)


@app.route("/api/judges/check-licenses/apply", methods=["POST"])
def apply_license_changes():
    """Apply the detected license changes to the DB (mark as inactive)."""
    data = request.json or {}
    judge_ids = data.get("ids", [])   # list of judge IDs to mark inactive
    if not judge_ids:
        return jsonify({"ok": False, "error": "No IDs provided"})
    conn = get_db()
    for jid in judge_ids:
        conn.execute("UPDATE judges SET active=0 WHERE id=?", (jid,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "deactivated": len(judge_ids)})


# ── Monday Sync — master weekly task ──────────────────────────────────────────
@app.route("/api/monday-sync", methods=["POST"])
def monday_sync():
    """Weekly sync task (run every Monday):
    1. Scrape weekend competitions + recompute correlations
    2. Check judge license status vs WDSF
    3. Sync WDSF nominations (panel assignments)
    Returns a summary; long steps run inline (may take 5-10 min).
    """
    import threading, subprocess, sys
    data     = request.json or {}
    days_back = int(data.get("days_back", 4))   # Fri → Mon covers ~4 days
    summary  = {}

    # Step 1: Scrape weekend + correlations (inline, returns quickly for small batches)
    try:
        from datetime import datetime, timedelta
        end   = datetime.utcnow()
        start = end - timedelta(days=days_back)
        month_list = []
        cur = start.replace(day=1)
        while cur <= end:
            month_list.append((cur.month, cur.year))
            if cur.month == 12:
                cur = cur.replace(year=cur.year+1, month=1)
            else:
                cur = cur.replace(month=cur.month+1)

        conn = get_db()
        scraped = {r["slug"] for r in conn.execute("SELECT slug FROM scraped_competitions").fetchall()}
        conn.close()
        new_slugs = []
        for month, year in set(month_list):
            for c in _scan_calendar_month(month, year):
                if c["slug"] not in scraped and c["slug"] not in new_slugs:
                    new_slugs.append(c["slug"])
        summary["new_competitions_found"] = len(new_slugs)
        summary["scraped_ok"] = 0
        summary["scraped_errors"] = []
        for slug in new_slugs:
            try:
                officials = _scrape_officials_page(slug)
                marks     = _scrape_results_page(slug) or _scrape_marks_page(slug)
                if officials and marks:
                    summary["scraped_ok"] += 1
            except Exception as e:
                summary["scraped_errors"].append(f"{slug}: {e}")
        # Recompute correlations
        try:
            n_pairs = _compute_all_correlations()
            summary["correlation_pairs"] = n_pairs
        except Exception as e:
            summary["correlation_error"] = str(e)
    except Exception as e:
        summary["scrape_error"] = str(e)

    # Step 2: Launch license check in background
    try:
        global _license_check_status
        if not _license_check_status["running"]:
            conn = get_db()
            rows = conn.execute(
                "SELECT id, first_name, last_name, wdsf_min, active FROM judges WHERE wdsf_min IS NOT NULL"
            ).fetchall()
            conn.close()
            t = threading.Thread(target=_run_license_check_background, args=(rows,), daemon=True)
            t.start()
            summary["license_check"] = f"started for {len(rows)} judges (poll /api/judges/check-licenses/status)"
        else:
            summary["license_check"] = "already running"
    except Exception as e:
        summary["license_check_error"] = str(e)

    # Step 3: Sync nominations from WDSF
    try:
        script = os.path.join(APP_DIR, "sincronizar_nominados.py")
        if os.path.exists(script):
            result = subprocess.run(
                [sys.executable, script], capture_output=True, text=True, timeout=180
            )
            summary["nominations_sync"] = "ok" if result.returncode == 0 else result.stderr[-500:]
        else:
            summary["nominations_sync"] = "script not found"
    except Exception as e:
        summary["nominations_sync_error"] = str(e)

    summary["ok"] = True
    return jsonify(summary)


@app.route("/")
def index():
    return send_from_directory(APP_DIR, "index.html")

if __name__ == "__main__":
    port     = int(os.environ.get("PORT", 5001))
    is_local = (port == 5001)
    print("\n" + "="*55)
    print("  WDSF Panel Assignment System — Starting...")
    print(f"  DB: {DB}")
    if is_local:
        print(f"  Open: http://127.0.0.1:{port}")
    print("="*55 + "\n")
    app.run(debug=is_local, port=port, host="0.0.0.0")
