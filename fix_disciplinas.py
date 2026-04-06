#!/usr/bin/env python3
"""
fix_disciplinas.py
──────────────────
Corrige eventos con discipline=NULL o vacío deduciendo la disciplina del nombre.
Ejecutar con el servidor PARADO.
"""
import sqlite3, os

DB = os.path.expanduser("~/wdsf_app/wdsf_panel.db")

def detect_discipline(name):
    n = name.lower()
    if "ten dance" in n or "combined" in n:
        return "Ten Dance"
    if "standard" in n:
        return "Standard"
    if "latin" in n:
        return "Latin"
    return None

conn = sqlite3.connect(DB)
events = conn.execute(
    "SELECT id, name, discipline FROM events WHERE discipline IS NULL OR TRIM(discipline)=''"
).fetchall()

if not events:
    print("No hay eventos con disciplina vacía. Todo correcto.")
    conn.close()
    exit()

print(f"Corrigiendo {len(events)} eventos:\n")
for ev in events:
    disc = detect_discipline(ev[1])
    if disc:
        conn.execute("UPDATE events SET discipline=? WHERE id=?", (disc, ev[0]))
        print(f"  ✓ [{ev[0]}] {disc:10} ← {ev[1][:60]}")
    else:
        print(f"  ✗ [{ev[0]}] NO BALLROOM — sin cambio: {ev[1][:60]}")

conn.commit()
conn.close()
print("\nHecho. Ahora reinicia el servidor.")
