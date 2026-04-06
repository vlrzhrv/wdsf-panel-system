import sqlite3, os

DB = os.path.expanduser("~/wdsf_app/wdsf_panel.db")
conn = sqlite3.connect(DB)

# Todos los eventos de Events/Granting = pending
conn.execute("UPDATE events SET status='pending' WHERE status != 'nominated'")

# Solo los 6 que tienen panel nominado real = nominated
nominated_locations = [
    ("2026-03-21", "Berlin"),
    ("2026-03-22", "Berlin"),
    ("2026-04-03", "Cambrils"),
    ("2026-04-04", "Cambrils"),
    ("2026-05-10", "Frankfurt am Main"),
]
for date, loc in nominated_locations:
    conn.execute(
        "UPDATE events SET status='nominated' WHERE date=? AND location=?",
        (date, loc)
    )

conn.commit()

# Verificar
pending  = conn.execute("SELECT COUNT(*) FROM events WHERE status='pending'").fetchone()[0]
nominated = conn.execute("SELECT COUNT(*) FROM events WHERE status='nominated'").fetchone()[0]
assigned  = conn.execute("SELECT COUNT(*) FROM events WHERE status='assigned'").fetchone()[0]
conn.close()

print("Estados actualizados:")
print(f"  🟡 Pendientes de asignacion : {pending}")
print(f"  🔵 Con panel nominado       : {nominated}")
print(f"  🟢 Asignados por algoritmo  : {assigned}")