"""
sincronizar_jueces.py  –  Sincronización completa de jueces desde WDSF API
===========================================================================
Descarga TODOS los jueces que han participado en competiciones WDSF
(no sólo los campeonatos "importantes") e inserta/actualiza su registro
en la base de datos local.

Uso:
    python sincronizar_jueces.py [--years 2024 2025 2026] [--db /ruta/wdsf_panel.db]

Variables de entorno que se usan si no se pasan credenciales en código:
    WDSF_USER, WDSF_PASS
"""

import os, sys, json, re, time, sqlite3, argparse
from datetime import date, timedelta
from requests.auth import HTTPBasicAuth
import requests

# ── Credenciales WDSF ─────────────────────────────────────────────────────────
WDSF_USER = os.environ.get("WDSF_USER", "ValeriIvanov1")
WDSF_PASS = os.environ.get("WDSF_PASS", "sjJ@M9Va7I")
WDSF_BASE = "https://services.worlddancesport.org/api/1"

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

# ── HTTP session ──────────────────────────────────────────────────────────────
_session = requests.Session()
_session.auth = HTTPBasicAuth(WDSF_USER, WDSF_PASS)
_session.headers.update({"Accept": "application/json"})

def wdsf_get(url, retries=2, delay=0.35):
    """GET a WDSF API URL; returns parsed JSON or None on error."""
    time.sleep(delay)
    for attempt in range(retries + 1):
        try:
            r = _session.get(url, timeout=30)
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

def parse_licenses(person_data):
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
                import re as _re
                m = _re.search(r'^([\w\s]+)\s+\(([ABC])\)', d)
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
    mapped = set()
    for d in raw_discs:
        dl = d.lower()
        if "standard" in dl: mapped.add("Standard")
        elif "latin" in dl: mapped.add("Latin")
        elif "ten dance" in dl or "combined" in dl: mapped.add("Combined")
    return list(mapped)

def get_min_from_official(official):
    import re as _re
    for link in official.get("link", []):
        if "person" in link.get("rel", ""):
            m = _re.search(r'/person/(\d+)', link.get("href",""))
            if m: return int(m.group(1))
    return None

def run_sync(years=None, db_path=None, log=print):
    from datetime import date as _date
    import sqlite3 as _sqlite3
    if years is None:
        today = _date.today()
        years = [today.year - 1, today.year]
    years = [str(y) for y in years]
    if db_path is None: db_path = find_db()
    log(f"WDSF Judge Sync started. Years: {years}")
    conn = _sqlite3.connect(db_path)
    conn.row_factory = _sqlite3.Row
    existing_mins = set(r[0] for r in conn.execute("SELECT wdsf_min FROM judges WHERE wdsf_min IS NOT NULL").fetchall())
    log(f"Judges in DB: {len(existing_mins)}")
    competitions = []
    for year in years:
        for status in ["Closed", "Announced"]:
            data = wdsf_get(f"{WDSF_BASE}/competition?status={status}&from={year}-01-01&to={year}-12-31")
            if data: competitions.extend(data)
    seen_ids = set()
    unique_comps = [c for c in competitions if c.get("id") not in seen_ids and not seen_ids.add(c.get("id"))]
    log(f"Competitions: {len(unique_comps)}")
    new_mins = set()
    official_data = {}
    for comp in unique_comps:
        officials = wdsf_get(f"{WDSF_BASE}/official?competitionId={comp.get('id')}")
        if not officials: continue
        for off in officials:
            if "Adjudicator" not in off.get("Name",""): continue
            min_id = get_min_from_official(off)
            if not min_id or min_id in existing_mins: continue
            new_mins.add(min_id)
            official_data[min_id] = {"name": off.get("Name",""), "country": off.get("country","")}
    log(f"New judges to add: {len(new_mins)}")
    inserted = 0
    for min_id in sorted(new_mins):
        person = wdsf_get(f"{WDSF_BASE}/person/{min_id}")
        lic = parse_licenses(person) if person else {"level":None,"disciplines":[],"expires":None,"status":None}
        discs = map_disciplines(lic["disciplines"])
        today_str = _date.today().isoformat()
        is_active = lic.get("status") == "Active" and (not lic.get("expires") or lic["expires"] >= today_str)
        if person:
            record = {"wdsf_min":min_id,"first_name":(person.get("name") or "")[:100],"last_name":(person.get("surname") or "")[:100],"nationality":(person.get("nationality") or "")[:100],"representing":(person.get("country") or "")[:100],"license_type":lic["level"],"license_valid_until":lic["expires"],"disciplines":",".join(discs),"active":1 if is_active else 0,"judging_world_championships":0,"judging_grand_slams":0,"judging_continental_championships":0}
        else:
            off = official_data.get(min_id, {})
            np = off.get("name","").split()
            cc = off.get("country","")
            country_name = COUNTRY_CODE_MAP.get(cc.upper(), cc)
            if not np: continue
            record = {"wdsf_min":min_id,"first_name":np[0][:100],"last_name":" ".join(np[1:])[:100],"nationality":country_name[:100],"representing":country_name[:100],"license_type":None,"license_valid_until":None,"disciplines":"","active":0,"judging_world_championships":0,"judging_grand_slams":0,"judging_continental_championships":0}
        try:
            conn.execute("INSERT INTO judges (wdsf_min,first_name,last_name,nationality,representing,license_type,license_valid_until,disciplines,active,judging_world_championships,judging_grand_slams,judging_continental_championships) VALUES (:wdsf_min,:first_name,:last_name,:nationality,:representing,:license_type,:license_valid_until,:disciplines,:active,:judging_world_championships,:judging_grand_slams,:judging_continental_championships) ON CONFLICT(wdsf_min) DO NOTHING", record)
            conn.commit()
            existing_mins.add(min_id)
            inserted += 1
        except: pass
    unmatched = conn.execute("SELECT id,judge_name,judge_country FROM official_nominations WHERE judge_id IS NULL").fetchall()
    fixed = 0
    for row in unmatched:
        nm=(row["judge_name"] or "").strip()
        cc=(row["judge_country"] or "").strip().upper()
        cf=COUNTRY_CODE_MAP.get(cc,cc)
        parts=nm.split()
        if len(parts)<2: continue
        jid=None
        last,first=parts[-1],parts[0]
        for ln,fn in [(last,first),(first,last)]:
            r=conn.execute("SELECT id FROM judges WHERE UPPER(last_name)=? AND UPPER(first_name) LIKE ? AND (UPPER(representing)=? OR UPPER(nationality)=?)",(ln.upper(),fn.upper()+"%",cf.upper(),cf.upper())).fetchall()
            if len(r)==1: jid=r[0]["id"]; break
        if not jid and len(parts)>=3:
            cln=" ".join(parts[1:])
            for ln,fn in [(cln,first),(first,cln)]:
                r=conn.execute("SELECT id FROM judges WHERE UPPER(last_name)=? AND UPPER(first_name) LIKE ? AND (UPPER(representing)=? OR UPPER(nationality)=?)",(ln.upper(),fn.upper()+"%",cf.upper(),cf.upper())).fetchall()
                if len(r)==1: jid=r[0]["id"]; break
        if not jid:
            for ln in [last,first]:
                r=conn.execute("SELECT id FROM judges WHERE UPPER(last_name)=? AND (UPPER(representing)=? OR UPPER(nationality)=?)",(ln.upper(),cf.upper(),cf.upper())).fetchall()
                if len(r)==1: jid=r[0]["id"]; break
        if jid:
            conn.execute("UPDATE official_nominations SET judge_id=? WHERE id=?",(jid,row["id"]))
            fixed+=1
    if fixed: conn.commit()
    conn.close()
    log(f"Done. New judges: {inserted}, Nominations fixed: {fixed}")
    return {"new_judges":inserted,"unmatched_nominations_fixed":fixed,"competitions_scanned":len(unique_comps)}

if __name__ == "__main__":
    import argparse
    from datetime import date
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", nargs="+", type=int, default=[date.today().year-1,date.today().year])
    parser.add_argument("--db", type=str, default=None)
    args = parser.parse_args()
    run_sync(years=args.years, db_path=args.db)
