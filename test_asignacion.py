import requests, json

BASE = "http://127.0.0.1:5000"

print("Creando evento de prueba...")
ev = requests.post(f"{BASE}/api/events", json={
    "name": "WDSF World Championship Standard Adult",
    "date": "2025-10-15",
    "location": "Berlin",
    "country": "Germany",
    "discipline": "Standard",
    "age_group": "Adult",
    "division": "General",
    "event_type": "WORLD CHAMPIONSHIP",
    "is_ags": False,
    "coefficient": 2.0
}).json()
print(f"Evento ID={ev['id']}: {ev['name']}")

print("\nEjecutando motor de asignacion...")
result = requests.post(f"{BASE}/api/events/{ev['id']}/assign", json={}).json()

print(f"\n{'='*65}")
print(f"PANEL ASIGNADO — {result['stats']['assigned']} jueces — {ev['name']}")
print(f"{'='*65}")
for j in result["panel"]:
    pais = j.get("representing") or j.get("nationality","?")
    print(f"  {j['first_name']:15} {j['last_name']:20} | {pais:22} | {j['zone']:10} | Score:{j['score']:5} | {j['role']}")

print(f"\nRESERVAS:")
for j in result.get("reserves",[]):
    pais = j.get("representing") or j.get("nationality","?")
    print(f"  {j['first_name']} {j['last_name']} ({pais}) Score:{j['score']}")

print(f"\nZONAS CUBIERTAS : {result['zones']}")
print(f"EXCLUSIONES     : {result['exclusions']}")
print(f"ESTADISTICAS    : {result['stats']}")