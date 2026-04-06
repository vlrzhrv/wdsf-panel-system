#!/usr/bin/env python3
"""
asignar_todos.py
────────────────
Asigna paneles de jueces a TODOS los eventos pendientes en la BD,
llamando al servidor Flask local (debe estar corriendo en puerto 5001).

Uso:
    python3 asignar_todos.py                    # asigna todos los pending
    python3 asignar_todos.py --dry-run          # solo lista, no asigna
    python3 asignar_todos.py --estado pending   # filtrar por estado
    python3 asignar_todos.py --desde 2026-04-01 --hasta 2026-07-31
"""

import requests, sys, argparse, json
from datetime import date

API = "http://127.0.0.1:5001/api"

ASIGNABLE_STATES = {"pending", "nominated"}

def get_events(estado=None, desde=None, hasta=None):
    r = requests.get(f"{API}/events", timeout=10)
    r.raise_for_status()
    events = r.json()
    if estado and estado != "all":
        # "pending" en realidad filtra pending + nominated (ambos sin panel)
        if estado == "pending":
            events = [e for e in events if e.get("status") in ASIGNABLE_STATES]
        else:
            events = [e for e in events if e.get("status") == estado]
    if desde:
        events = [e for e in events if e.get("date", "") >= desde]
    if hasta:
        events = [e for e in events if e.get("date", "") <= hasta]
    return sorted(events, key=lambda e: e.get("date", ""))

def assign_event(eid):
    r = requests.post(f"{API}/events/{eid}/assign", timeout=30)
    return r.status_code, r.json() if r.ok else r.text

def main():
    today        = date.today()
    year_start   = f"{today.year}-01-01"   # desde inicio del año
    default_to   = f"{today.year}-12-31"   # hasta fin del año

    parser = argparse.ArgumentParser(description="Asignación en lote de paneles WDSF")
    parser.add_argument("--estado",   default="pending", help="Estado: pending/nominated/assigned/all")
    parser.add_argument("--desde",    default=year_start, help="Desde fecha YYYY-MM-DD")
    parser.add_argument("--hasta",    default="",        help="Hasta fecha YYYY-MM-DD")
    parser.add_argument("--dry-run",  action="store_true", help="Solo listar, no asignar")
    parser.add_argument("--verbose",  action="store_true", help="Mostrar panel completo")
    args = parser.parse_args()

    estado_filter = None if args.estado == "all" else args.estado

    print("\n" + "═"*70)
    print("  WDSF Panel — Asignación en lote")
    print(f"  Servidor: {API}")
    print(f"  Estado filtro: {args.estado}  |  Desde: {args.desde or '—'}  |  Hasta: {args.hasta or '—'}")
    if args.dry_run:
        print("  MODO: Dry-run (solo listar)")
    print("═"*70 + "\n")

    # Verificar servidor
    try:
        requests.get(f"{API}/stats", timeout=5).raise_for_status()
    except Exception as e:
        print(f"✗ No se puede conectar al servidor en {API}")
        print("  Ejecuta primero: python3 servidor.py")
        sys.exit(1)

    events = get_events(estado_filter, args.desde or None, args.hasta or None)
    if not events:
        print("  No hay eventos con ese filtro.")
        sys.exit(0)

    print(f"  {len(events)} eventos a procesar:\n")
    for e in events:
        disc = (e.get('discipline') or '').ljust(10)
        tp   = (e.get('event_type') or '').ljust(28)
        print(f"  [{e['id']:3}] {e.get('date','')} | {disc} | {tp} | {e.get('name','')[:45]}")

    if args.dry_run:
        print("\n  (dry-run: sin asignar)")
        return

    print(f"\n  Iniciando asignaciones...\n")
    ok = failed = 0

    for e in events:
        eid  = e["id"]
        name = e.get("name", f"Evento {eid}")[:55]
        print(f"  → [{eid}] {e.get('date','')} {name} ...", end=" ", flush=True)

        try:
            status, result = assign_event(eid)
            if status == 200:
                panel    = result.get("panel", [])
                reserves = result.get("reserves", [])
                zones    = result.get("zones", [])
                print(f"✓  {len(panel)} jueces + {len(reserves)} reservas | Zonas: {', '.join(zones)}")
                if args.verbose:
                    for j in panel:
                        role_tag = {"host_country":"[HOST]","top3_required":"[TOP3]","selected":"     "}.get(j.get("role",""), "     ")
                        print(f"       {role_tag} {j.get('last_name',''):18} {j.get('first_name',''):15} "
                              f"{(j.get('representing') or j.get('nationality','')):<15} "
                              f"score={j.get('score',0):.0f}")
                ok += 1
            else:
                print(f"✗ Error {status}: {result}")
                failed += 1
        except Exception as ex:
            print(f"✗ Excepción: {ex}")
            failed += 1

    print("\n" + "─"*70)
    print(f"  ✓ Asignados correctamente: {ok}")
    print(f"  ✗ Con errores:             {failed}")
    print("─"*70)
    if ok:
        print(f"\n  Abre http://127.0.0.1:5001 → Eventos para revisar los paneles.")

if __name__ == "__main__":
    main()
