"""
Migración: añade columna 'primary_discipline' a la tabla judges.
Rellena automáticamente basándose en:
  1. specialty conocido (Standard/Latin) → esa disciplina
  2. Si solo tiene una disciplina en 'disciplines' → esa
  3. Por defecto: None (el usuario la asigna manualmente)
"""
import sqlite3, os

DB = os.path.expanduser("~/wdsf_app/wdsf_panel.db")
conn = sqlite3.connect(DB)

# 1. Añadir columna si no existe
cols = [r[1] for r in conn.execute("PRAGMA table_info(judges)").fetchall()]
if "primary_discipline" not in cols:
    conn.execute("ALTER TABLE judges ADD COLUMN primary_discipline TEXT")
    print("✓ Columna primary_discipline añadida")
else:
    print("  Columna primary_discipline ya existe")

# 2. Rellenar automáticamente donde sea posible
rows = conn.execute("SELECT id, disciplines, specialty FROM judges").fetchall()
updated = 0
for (jid, discs_raw, specialty) in rows:
    discs = [d.strip() for d in (discs_raw or "").split(",") if d.strip()
             and d.strip() in ("Standard", "Latin", "Combined", "Ten Dance")]

    primary = None
    if specialty == "Standard":
        primary = "Standard"
    elif specialty == "Latin":
        primary = "Latin"
    elif len(discs) == 1:
        primary = discs[0]
    # Si tiene todas o 'Both'/Unknown → dejar None para que el usuario decida

    if primary:
        conn.execute("UPDATE judges SET primary_discipline=? WHERE id=?", (primary, jid))
        updated += 1

conn.commit()
conn.close()

print(f"✓ {updated} jueces con primary_discipline asignada automáticamente")
print(f"  El resto ({len(rows)-updated}) necesitan asignación manual en la interfaz")
print("\nEjecuta 'python3 servidor.py' para arrancar el servidor con los cambios.")
