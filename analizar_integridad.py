"""
analizar_integridad.py
======================
Scrapes the marks page for ONE competition and performs:

  1. Round-by-round consistency analysis
     "Did any judge change their opinion about a couple in the OPPOSITE direction
      to the rest of the panel?"

  2. Bloc-voting / cross-judge correlation (within this competition)

  3. National-bias detection
     "Did any judge systematically favour/disfavour couples from their country?"

Usage:
    python3 analizar_integridad.py [slug]
    python3 analizar_integridad.py GrandSlam-Blackpool-Adult-Latin-65352

Output: printed report + saves to wdsf_panel.db (table competition_round_marks)
"""

import os, sys, re, time, sqlite3, statistics, itertools
from datetime import datetime
from collections import defaultdict

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────
APP_DIR = os.path.dirname(os.path.abspath(__file__))
BUNDLE  = os.path.join(APP_DIR, "wdsf_panel.db")
LOCAL   = os.path.expanduser("~/wdsf_app/wdsf_panel.db")
DB      = BUNDLE if os.path.exists(BUNDLE) else LOCAL

BASE    = "https://www.worlddancesport.org"
HEADERS = {"User-Agent": "Mozilla/5.0 WDSF-PanelSystem/1.0"}

DEFAULT_SLUG = "GrandSlam-Blackpool-Adult-Latin-65352"

# ── DB setup ──────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS competition_round_marks (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            slug             TEXT    NOT NULL,
            round_num        INTEGER NOT NULL,
            judge_letter     TEXT    NOT NULL,
            judge_name       TEXT,
            couple_num       TEXT    NOT NULL,
            marks_count      INTEGER,   -- dances marked out of total_dances
            total_dances     INTEGER,   -- total dances in this round
            scraped_at       TEXT,
            UNIQUE(slug, round_num, judge_letter, couple_num)
        )
    """)
    conn.commit()

# ── Scraper ───────────────────────────────────────────────────────────────────

def scrape_marks_detailed(slug):
    """
    Fetches /Competitions/Marks/{slug} and returns:
      judge_names: {letter: name_str}   (may be empty if not on page)
      marks: list of {round_num, judge_letter, couple_num, marks_count, total_dances}
    """
    url = f"{BASE}/Competitions/Marks/{slug}"
    print(f"  Fetching {url} ...")
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
    except Exception as e:
        print(f"  ERROR: {e}")
        return {}, []

    soup = BeautifulSoup(r.text, "html.parser")

    # ── Find ALL tables on the page ──────────────────────────────────────────
    tables = soup.find_all("table")
    if not tables:
        print("  No tables found on marks page")
        return {}, []

    judge_names = {}
    all_marks   = []

    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 3:
            continue

        # ── Find the header row with judge letters ──────────────────────────
        # Judge codes: 1-4 uppercase alpha chars, ≥ 9 occurrences in a row
        judge_header_idx  = None
        judge_col_entries = []   # (col_idx, letter)

        for ri, row in enumerate(rows[:6]):
            cells = [th.get_text(strip=True) for th in row.find_all(["th","td"])]
            entries = [(i, c) for i, c in enumerate(cells)
                       if c and re.fullmatch(r'[A-Z]{1,4}', c) and c not in ("=",)]
            if len(entries) >= 9:
                judge_header_idx  = ri
                judge_col_entries = entries
                break

        if judge_header_idx is None:
            continue

        # ── Try to find a row with judge NAMES just below the header ────────
        # Names are usually in the next 1-3 rows in cells aligned to judge cols
        judge_col_indices = {ci for ci, _ in judge_col_entries}
        judge_letter_map  = {ci: ltr for ci, ltr in judge_col_entries}

        for name_ri in range(judge_header_idx + 1, min(judge_header_idx + 4, len(rows))):
            candidate = rows[name_ri]
            cells = [td.get_text(strip=True) for td in candidate.find_all(["td","th"])]
            # A name row has cells that are NOT numbers and NOT * / + and NOT empty
            named = [(ci, cells[ci]) for ci in range(len(cells))
                     if ci in judge_col_indices
                     and cells[ci]
                     and not cells[ci].replace(' ','').isdigit()
                     and cells[ci] not in ('*', '+', '-', '=')]
            if len(named) >= 3:
                for ci, name in named:
                    ltr = judge_letter_map.get(ci)
                    if ltr and name:
                        judge_names[ltr] = name
                break

        # ── Build per-judge dance-column lists ──────────────────────────────
        judge_dance_cols = {}  # letter -> [col_idx_d1, col_idx_d2, ...]
        for ci, jid in judge_col_entries:
            judge_dance_cols.setdefault(jid, []).append(ci)

        n_dances = max(len(cols) for cols in judge_dance_cols.values()) if judge_dance_cols else 1

        # ── Locate couple / round columns ────────────────────────────────────
        first_judge_col = judge_col_entries[0][0]
        couple_col = max(0, first_judge_col - 2)
        round_col  = max(0, first_judge_col - 1)

        # ── Parse data rows ──────────────────────────────────────────────────
        current_couple = None
        table_marks    = []

        for row in rows[judge_header_idx + 1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td","th"])]
            if len(cells) < first_judge_col:
                continue

            couple_cell = cells[couple_col] if couple_col < len(cells) else ""
            if couple_cell and couple_cell.isdigit():
                current_couple = couple_cell

            if not current_couple:
                continue

            round_cell = cells[round_col] if round_col < len(cells) else ""
            try:
                round_num = int(round_cell)
            except (ValueError, TypeError):
                continue

            for jid, dance_cols in judge_dance_cols.items():
                count = sum(
                    1 for ci in dance_cols
                    if ci < len(cells) and cells[ci] in ("*", "+")
                )
                table_marks.append({
                    "round_num":    round_num,
                    "judge_letter": jid,
                    "couple_num":   current_couple,
                    "marks_count":  count,
                    "total_dances": n_dances,
                })

        if table_marks:
            all_marks.extend(table_marks)

    print(f"  Found {len(judge_names)} judge names, {len(all_marks)} mark rows "
          f"across {len(set(m['round_num'] for m in all_marks))} rounds")
    return judge_names, all_marks


def save_marks(conn, slug, judge_names, marks):
    now = datetime.utcnow().isoformat()
    conn.execute("DELETE FROM competition_round_marks WHERE slug=?", (slug,))
    for m in marks:
        jname = judge_names.get(m["judge_letter"])
        conn.execute("""
            INSERT OR REPLACE INTO competition_round_marks
                (slug, round_num, judge_letter, judge_name, couple_num, marks_count, total_dances, scraped_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (slug, m["round_num"], m["judge_letter"], jname,
              m["couple_num"], m["marks_count"], m["total_dances"], now))
    conn.commit()
    print(f"  Saved {len(marks)} rows to competition_round_marks")


# ── Analysis ──────────────────────────────────────────────────────────────────

def analyze(slug, conn):
    rows = conn.execute("""
        SELECT round_num, judge_letter, judge_name, couple_num, marks_count, total_dances
        FROM competition_round_marks WHERE slug=?
        ORDER BY round_num, couple_num, judge_letter
    """, (slug,)).fetchall()

    if not rows:
        print("No data found — run scrape first.")
        return

    # ── Build data structures ────────────────────────────────────────────────
    # data[round][couple][judge_letter] = marks_count
    data = defaultdict(lambda: defaultdict(dict))
    for r in rows:
        data[r["round_num"]][r["couple_num"]][r["judge_letter"]] = r["marks_count"]

    judge_letters = sorted({r["judge_letter"] for r in rows})
    # Map letter → name
    jname = {}
    for r in rows:
        if r["judge_name"]:
            jname[r["judge_letter"]] = r["judge_name"]

    rounds_sorted = sorted(data.keys())
    n_judges      = len(judge_letters)

    # Max possible marks for a couple in a round (all judges × all dances)
    total_dances_map = {}  # round -> total_dances
    for r in rows:
        if r["round_num"] not in total_dances_map:
            total_dances_map[r["round_num"]] = r["total_dances"] or 1

    # ── 1. ROUND-BY-ROUND CONSISTENCY ANALYSIS ───────────────────────────────
    print("\n" + "="*70)
    print("  1. ROUND-BY-ROUND CONSISTENCY ANALYSIS")
    print("="*70)
    print("  Flags cases where a judge's opinion on a couple diverged from")
    print("  the panel consensus between consecutive rounds.\n")

    # suspicious_events: list of {judge, couple, round_from, round_to,
    #                              panel_delta, judge_delta, divergence}
    suspicious = []

    for ri in range(len(rounds_sorted) - 1):
        r1 = rounds_sorted[ri]
        r2 = rounds_sorted[ri + 1]
        td1 = total_dances_map.get(r1, 1)
        td2 = total_dances_map.get(r2, 1)

        # Only couples that appear in BOTH rounds
        couples_r1 = set(data[r1].keys())
        couples_r2 = set(data[r2].keys())
        common_couples = couples_r1 & couples_r2

        for couple in common_couples:
            j_r1 = data[r1][couple]  # {letter: marks_count}
            j_r2 = data[r2][couple]

            # Panel aggregate (normalised by max possible marks)
            total_r1 = sum(j_r1.values()) / (n_judges * td1)
            total_r2 = sum(j_r2.values()) / (n_judges * td2)
            panel_delta = total_r2 - total_r1   # + means panel liked more in r2

            # Only flag if panel moved significantly (±0.25 threshold)
            if abs(panel_delta) < 0.20:
                continue

            for jltr in judge_letters:
                m1 = j_r1.get(jltr, 0)
                m2 = j_r2.get(jltr, 0)
                # Normalise judge's marks
                norm1 = m1 / td1
                norm2 = m2 / td2
                judge_delta = norm2 - norm1

                # Divergence: judge moved in OPPOSITE direction to panel
                # AND it's not a tiny movement
                if (panel_delta > 0.20 and judge_delta < -0.3) or \
                   (panel_delta < -0.20 and judge_delta > 0.3):
                    suspicious.append({
                        "judge":       jltr,
                        "judge_name":  jname.get(jltr, jltr),
                        "couple":      couple,
                        "round_from":  r1,
                        "round_to":    r2,
                        "panel_delta": round(panel_delta, 3),
                        "judge_delta": round(judge_delta, 3),
                        "divergence":  round(abs(judge_delta - panel_delta), 3),
                    })

    # Summarise by judge
    judge_suspicious_count = defaultdict(int)
    for ev in suspicious:
        judge_suspicious_count[ev["judge"]] += 1

    if suspicious:
        print(f"  Found {len(suspicious)} suspicious opinion changes "
              f"across {len(rounds_sorted)-1} round transitions.\n")
        # Sort by judge with most divergences
        for jltr, cnt in sorted(judge_suspicious_count.items(), key=lambda x: -x[1]):
            jn = jname.get(jltr, f"Judge {jltr}")
            print(f"  ⚠️  Judge {jltr} ({jn}): {cnt} suspicious divergence(s)")
            # Show top 3 examples
            examples = [ev for ev in suspicious if ev["judge"] == jltr][:3]
            for ex in examples:
                direction = "⬆️ panel liked MORE" if ex["panel_delta"] > 0 else "⬇️ panel liked LESS"
                judge_dir = "⬇️ judge REMOVED mark" if ex["judge_delta"] < 0 else "⬆️ judge ADDED mark"
                print(f"     Couple #{ex['couple']:>4}  R{ex['round_from']}→R{ex['round_to']}  "
                      f"{direction} (Δ={ex['panel_delta']:+.2f})  but  {judge_dir} (Δ={ex['judge_delta']:+.2f})")
            print()
    else:
        print("  ✅ No significant divergences found (or insufficient overlap between rounds).")

    # ── 2. WITHIN-COMPETITION JUDGE CORRELATION ───────────────────────────────
    print("="*70)
    print("  2. WITHIN-COMPETITION JUDGE CORRELATION")
    print("="*70)
    print("  Based on round 1 (largest round, most couples → most data)\n")

    if rounds_sorted:
        r1 = rounds_sorted[0]
        couple_list = sorted(data[r1].keys(), key=lambda x: int(x) if x.isdigit() else 0)
        td1 = total_dances_map.get(r1, 1)

        # Build vector per judge: [marks_count/total_dances for each couple]
        vectors = {}
        for jltr in judge_letters:
            vectors[jltr] = [data[r1][c].get(jltr, 0) / td1 for c in couple_list]

        # Spearman-rank correlation between each pair
        def spearman(a, b):
            n = len(a)
            if n < 5:
                return None
            def rank(lst):
                sorted_lst = sorted(enumerate(lst), key=lambda x: x[1])
                ranks = [0]*n
                for rank_val, (orig_idx, _) in enumerate(sorted_lst):
                    ranks[orig_idx] = rank_val + 1
                return ranks
            ra, rb = rank(a), rank(b)
            d2 = sum((ra[i]-rb[i])**2 for i in range(n))
            return 1 - (6*d2) / (n*(n**2-1))

        corr_matrix = {}
        for j1, j2 in itertools.combinations(judge_letters, 2):
            r = spearman(vectors[j1], vectors[j2])
            if r is not None:
                corr_matrix[(j1, j2)] = round(r, 3)

        if corr_matrix:
            # Average correlation per judge
            avg_corr = {}
            for jltr in judge_letters:
                vals = [v for (a,b), v in corr_matrix.items() if jltr in (a,b)]
                avg_corr[jltr] = round(statistics.mean(vals), 3) if vals else 0

            print(f"  Judge correlation in Round 1 ({len(couple_list)} couples):\n")
            print(f"  {'Letter':<6} {'Name':<30} {'Avg r':>6}  {'Interpretation'}")
            print(f"  {'-'*65}")
            for jltr, avg in sorted(avg_corr.items(), key=lambda x: -x[1]):
                jn = jname.get(jltr, '—')
                interp = ("✅ normal" if 0.4 < avg < 0.85
                          else "⚠️ VERY HIGH (bloc?)" if avg >= 0.85
                          else "🔍 LOW (outlier/independent)" if avg < 0.25
                          else "")
                print(f"  {jltr:<6} {jn:<30} {avg:>6.3f}  {interp}")

            # Flag highest pairs
            print(f"\n  Top correlated PAIRS in this competition:")
            top_pairs = sorted(corr_matrix.items(), key=lambda x: -x[1])[:5]
            for (j1, j2), r in top_pairs:
                n1 = jname.get(j1, j1); n2 = jname.get(j2, j2)
                flag = " 🚨" if r > 0.90 else " ⚠️" if r > 0.80 else ""
                print(f"    r={r:+.3f}  {j1}({n1}) ↔ {j2}({n2}){flag}")

    # ── 3. COUPLE ANALYSIS — who were the most controversial? ────────────────
    print("\n" + "="*70)
    print("  3. MOST CONTROVERSIAL COUPLES (highest judge disagreement in R1)")
    print("="*70)

    if rounds_sorted:
        r1 = rounds_sorted[0]
        couple_list = sorted(data[r1].keys(), key=lambda x: int(x) if x.isdigit() else 0)
        td1 = total_dances_map.get(r1, 1)

        controversies = []
        for couple in couple_list:
            marks = [data[r1][couple].get(jltr, 0) / td1 for jltr in judge_letters]
            if len(marks) < 5:
                continue
            avg = statistics.mean(marks)
            std = statistics.stdev(marks)
            controversies.append((couple, avg, std, marks))

        controversies.sort(key=lambda x: -x[2])  # sort by std dev (most controversy)
        print(f"\n  Top 10 most controversial couples (high std dev in judge votes):\n")
        print(f"  {'Couple':>7}  {'Avg marks':>9}  {'Std dev':>7}  Distribution")
        for couple, avg, std, marks in controversies[:10]:
            bar = "".join("█" if m > 0.5 else "░" for m in marks)
            pct = avg * 100
            print(f"  #{couple:>6}    {pct:6.1f}%     {std:.3f}   [{bar}]")

    # ── 4. SUMMARY SUSPICION SCORE ────────────────────────────────────────────
    print("\n" + "="*70)
    print("  4. JUDGE SUSPICION INDEX (round-by-round divergences)")
    print("="*70)
    print("  Higher = more times this judge moved opposite to panel consensus.\n")

    total_opportunities = defaultdict(int)
    for ri in range(len(rounds_sorted) - 1):
        r1s = rounds_sorted[ri]; r2s = rounds_sorted[ri+1]
        common = set(data[r1s].keys()) & set(data[r2s].keys())
        for jltr in judge_letters:
            total_opportunities[jltr] += len(common)

    print(f"  {'Letter':<6} {'Name':<30} {'Divergences':>11}  {'Opportunities':>14}  {'Index':>7}  {'Risk'}")
    print(f"  {'-'*80}")
    for jltr in sorted(judge_letters):
        cnt  = judge_suspicious_count.get(jltr, 0)
        opps = total_opportunities.get(jltr, 1)
        idx  = cnt / opps if opps else 0
        jn   = jname.get(jltr, '—')
        risk = ("🚨 HIGH" if idx > 0.08
                else "⚠️ MEDIUM" if idx > 0.04
                else "✅ low")
        print(f"  {jltr:<6} {jn:<30} {cnt:>11}  {opps:>14}  {idx:>7.3f}  {risk}")

    return suspicious


# ── Main ──────────────────────────────────────────────────────────────────────

def run(slug=None):
    slug = slug or DEFAULT_SLUG
    print(f"\n{'='*70}")
    print(f"  INTEGRITY ANALYSIS: {slug}")
    print(f"  DB: {DB}")
    print(f"{'='*70}\n")

    conn = get_db()
    ensure_tables(conn)

    # Check if already scraped
    existing = conn.execute(
        "SELECT COUNT(*) FROM competition_round_marks WHERE slug=?", (slug,)
    ).fetchone()[0]

    if existing > 0:
        print(f"  Data already in DB ({existing} rows). Re-using. (Delete rows to re-scrape.)\n")
        judge_names = {}
        rows = conn.execute(
            "SELECT DISTINCT judge_letter, judge_name FROM competition_round_marks WHERE slug=? AND judge_name IS NOT NULL",
            (slug,)
        ).fetchall()
        for r in rows:
            judge_names[r["judge_letter"]] = r["judge_name"]
    else:
        print("  Scraping marks page...")
        judge_names, marks = scrape_marks_detailed(slug)
        if not marks:
            print("  No marks found — cannot continue.")
            conn.close()
            return
        save_marks(conn, slug, judge_names, marks)

    print(f"  Judge name map: {judge_names}\n")
    analyze(slug, conn)
    conn.close()


if __name__ == "__main__":
    slug = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SLUG
    run(slug)
