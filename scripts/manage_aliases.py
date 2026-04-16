"""
manage_aliases.py
-----------------
Gestiona el mapeo de nombres de equipos y ligas entre:
  - Nuestra DB interna  (data/db/equipos.csv, ligas.csv)
  - API Football        (IDs y nombres — nuestra DB YA usa sus IDs)
  - odds-api.io         (nombres de equipo y slugs de liga distintos)

Archivos que mantiene:
  data/db/team_aliases.csv    — team_id | nombre_db | oddsapi_name
  data/db/league_aliases.csv  — liga_id | nombre_db | oddsapi_slug

Uso:
  python manage_aliases.py --status
      Muestra ligas y equipos sin alias de oddsapi.

  python manage_aliases.py --auto-ligas [LIGA_ID ...]
      Descarga eventos activos de cada liga en oddsapi y hace fuzzy-match
      automatico con los equipos de la DB. Muestra sugerencias sin guardar.

  python manage_aliases.py --confirmar-auto [LIGA_ID ...]
      Igual que --auto-ligas pero guarda los matches con similitud >= THRESHOLD.

  python manage_aliases.py --set-equipo TEAM_ID "Nombre en oddsapi"
      Guarda manualmente el alias de un equipo.

  python manage_aliases.py --set-liga LIGA_ID "slug-en-oddsapi"
      Guarda manualmente el slug de una liga.

  python manage_aliases.py --ver-liga LIGA_ID
      Lista todos los equipos que oddsapi conoce para esa liga.

  python manage_aliases.py --borrar-equipo TEAM_ID
      Elimina el alias de un equipo (vuelve a fuzzy-match).
"""

import csv
import json
import sys
import time
import unicodedata
import urllib.request
import urllib.parse
from pathlib import Path
from difflib import SequenceMatcher

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE             = Path(r'C:\Users\Matt\Apuestas Deportivas')
EQUIPOS_PATH     = BASE / 'data/db/equipos.csv'
LIGAS_PATH       = BASE / 'data/db/ligas.csv'
TEAM_ALIASES     = BASE / 'data/db/team_aliases.csv'
LEAGUE_ALIASES   = BASE / 'data/db/league_aliases.csv'

ODDSAPI_KEY  = '042f6b8774e4a4e05fea98b9f997de3a27a33656db6d901e73ae5949e8723a34'
ODDSAPI_BASE = 'https://api.odds-api.io/v3'

AUTO_THRESHOLD = 0.82   # similitud minima para auto-confirmar alias

# ─────────────────────────────────────────────────────────────────────────────
# Normalización
# ─────────────────────────────────────────────────────────────────────────────

def norm(s: str) -> str:
    s = ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    ).lower().strip()
    # quitar prefijos comunes
    for pfx in ['ca ', 'club atletico ', 'club ', 'atletico ', 'fc ', 'cf ']:
        if s.startswith(pfx):
            s = s[len(pfx):]
            break
    # quitar sufijos comunes
    for sfx in [' fc', ' cf', ' sc', ' ac', ' afc', ' fbc']:
        if s.endswith(sfx):
            s = s[:-len(sfx)]
            break
    return s.strip()


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, norm(a), norm(b)).ratio()


# ─────────────────────────────────────────────────────────────────────────────
# Carga de archivos
# ─────────────────────────────────────────────────────────────────────────────

def load_equipos() -> dict:
    """Devuelve {team_id (int): {id, nombre, pais, liga_id_principal}}."""
    result = {}
    with open(EQUIPOS_PATH, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            result[int(r['id'])] = r
    return result


def load_ligas() -> dict:
    """Devuelve {liga_id (int): {id, nombre, pais}}."""
    result = {}
    with open(LIGAS_PATH, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            result[int(r['id'])] = r
    return result


def load_team_aliases() -> dict:
    """Devuelve {team_id (int): oddsapi_name (str)}."""
    result = {}
    if not TEAM_ALIASES.exists():
        return result
    with open(TEAM_ALIASES, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            if r.get('oddsapi_name', '').strip():
                result[int(r['team_id'])] = r['oddsapi_name'].strip()
    return result


def load_league_aliases() -> dict:
    """Devuelve {liga_id (int): oddsapi_slug (str)}."""
    result = {}
    if not LEAGUE_ALIASES.exists():
        return result
    with open(LEAGUE_ALIASES, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            if r.get('oddsapi_slug', '').strip():
                result[int(r['liga_id'])] = r['oddsapi_slug'].strip()
    return result


def save_team_aliases(aliases: dict, equipos: dict):
    """Guarda {team_id: oddsapi_name} al CSV."""
    rows = []
    for tid, row in sorted(equipos.items()):
        rows.append({
            'team_id':     tid,
            'nombre_db':   row['nombre'],
            'oddsapi_name': aliases.get(tid, ''),
        })
    with open(TEAM_ALIASES, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['team_id', 'nombre_db', 'oddsapi_name'])
        w.writeheader()
        w.writerows(rows)


def save_league_aliases(aliases: dict, ligas: dict):
    """Guarda {liga_id: oddsapi_slug} al CSV."""
    rows = []
    for lid, row in sorted(ligas.items()):
        rows.append({
            'liga_id':    lid,
            'nombre_db':  row['nombre'],
            'oddsapi_slug': aliases.get(lid, ''),
        })
    with open(LEAGUE_ALIASES, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['liga_id', 'nombre_db', 'oddsapi_slug'])
        w.writeheader()
        w.writerows(rows)


# ─────────────────────────────────────────────────────────────────────────────
# odds-api.io
# ─────────────────────────────────────────────────────────────────────────────

def oddsapi_get(endpoint: str, params: dict = None) -> list | dict:
    p = {'apiKey': ODDSAPI_KEY}
    if params:
        p.update(params)
    url = f"{ODDSAPI_BASE}/{endpoint}?" + urllib.parse.urlencode(p)
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=15) as r:
        data = json.loads(r.read())
    if isinstance(data, dict):
        return data.get('data', data)
    return data


def get_oddsapi_teams_for_league(slug: str) -> list[str]:
    """Retorna lista de nombres de equipos en events pendientes de esa liga."""
    try:
        events = oddsapi_get('events', {'sport': 'football', 'league': slug, 'status': 'pending'})
        time.sleep(0.3)
    except Exception as e:
        print(f"  [oddsapi] Error: {e}")
        return []
    if isinstance(events, dict):
        events = events.get('data', [])
    names = set()
    for ev in events:
        if ev.get('home'): names.add(ev['home'])
        if ev.get('away'): names.add(ev['away'])
    return sorted(names)


# ─────────────────────────────────────────────────────────────────────────────
# Comandos
# ─────────────────────────────────────────────────────────────────────────────

def cmd_status():
    equipos      = load_equipos()
    ligas        = load_ligas()
    team_aliases = load_team_aliases()
    liga_aliases = load_league_aliases()

    sep = '=' * 68

    print(f"\n{sep}")
    print("  LIGAS — estado de aliases oddsapi")
    print(sep)
    print(f"  {'ID':<6}  {'Nombre DB':<30}  {'oddsapi slug'}")
    print(f"  {'-'*64}")
    for lid, row in sorted(ligas.items()):
        slug = liga_aliases.get(lid, '')
        mark = '  ' if slug else '* '
        print(f"  {mark}{lid:<6}  {row['nombre']:<30}  {slug or '(sin alias)'}")

    ligas_sin = sum(1 for lid in ligas if lid not in liga_aliases)
    print(f"\n  {ligas_sin} liga(s) sin slug de oddsapi")

    print(f"\n{sep}")
    print("  EQUIPOS — estado de aliases oddsapi")
    print(sep)

    # Agrupar por liga principal
    from collections import defaultdict
    by_liga = defaultdict(list)
    for tid, row in equipos.items():
        by_liga[int(row['liga_id_principal'])].append((tid, row))

    ligas_principales = [128, 140, 2, 3, 13, 11, 143, 130]
    sin_alias = con_alias = 0

    for lid in ligas_principales:
        slug = liga_aliases.get(lid, '')
        liga_name = ligas.get(lid, {}).get('nombre', f'Liga {lid}')
        equipos_liga = by_liga.get(lid, [])
        if not equipos_liga:
            continue
        sin = [(tid, r) for tid, r in equipos_liga if tid not in team_aliases]
        con = len(equipos_liga) - len(sin)
        sin_alias += len(sin)
        con_alias += con
        print(f"\n  [{liga_name}]  slug='{slug}'  — {con}/{len(equipos_liga)} con alias")
        if sin:
            for tid, r in sorted(sin, key=lambda x: x[1]['nombre']):
                print(f"    * id={tid:<6}  {r['nombre']}")

    print(f"\n  Resumen: {con_alias} equipos con alias  |  {sin_alias} sin alias")
    print(sep)


def cmd_ver_liga(liga_id: int):
    liga_aliases = load_league_aliases()
    slug = liga_aliases.get(liga_id)
    if not slug:
        print(f"Liga {liga_id} no tiene slug configurado.")
        return
    print(f"\nEquipos en oddsapi para liga_id={liga_id} (slug='{slug}'):")
    names = get_oddsapi_teams_for_league(slug)
    if not names:
        print("  (sin eventos pendientes o slug incorrecto)")
    for n in names:
        print(f"  {n}")


def cmd_auto_ligas(liga_ids: list[int], confirmar: bool):
    equipos      = load_equipos()
    ligas        = load_ligas()
    team_aliases = load_team_aliases()
    liga_aliases = load_league_aliases()

    # Filtrar equipos de las ligas pedidas
    from collections import defaultdict
    by_liga = defaultdict(list)
    for tid, row in equipos.items():
        by_liga[int(row['liga_id_principal'])].append((tid, row))

    nuevos = 0
    for lid in liga_ids:
        slug = liga_aliases.get(lid)
        if not slug:
            print(f"\n  Liga {lid} sin slug — usar --set-liga primero.")
            continue

        liga_name = ligas.get(lid, {}).get('nombre', f'Liga {lid}')
        print(f"\n  [{liga_name}]  slug='{slug}'")

        oddsapi_names = get_oddsapi_teams_for_league(slug)
        if not oddsapi_names:
            print("  Sin equipos disponibles en oddsapi (quiza no hay partidos proximos).")
            continue

        for tid, row in sorted(by_liga[lid], key=lambda x: x[1]['nombre']):
            if tid in team_aliases:
                print(f"  [ya tiene alias]  {row['nombre']} -> '{team_aliases[tid]}'")
                continue

            # Fuzzy match contra los nombres de oddsapi
            best_name  = None
            best_score = 0.0
            for on in oddsapi_names:
                sc = similarity(row['nombre'], on)
                if sc > best_score:
                    best_score = sc
                    best_name  = on

            if best_score >= AUTO_THRESHOLD:
                mark = 'AUTO' if confirmar else 'SUGERENCIA'
                print(f"  [{mark} {best_score:.0%}]  '{row['nombre']}' -> '{best_name}'")
                if confirmar:
                    team_aliases[tid] = best_name
                    nuevos += 1
            elif best_name:
                print(f"  [BAJO {best_score:.0%}] '{row['nombre']}' ~~ '{best_name}'  (manual)")
            else:
                print(f"  [SIN MATCH]  '{row['nombre']}'")

    if confirmar and nuevos:
        save_team_aliases(team_aliases, equipos)
        print(f"\n  {nuevos} alias nuevos guardados en {TEAM_ALIASES.name}")
    elif not confirmar and nuevos == 0:
        print("\n  Usa --confirmar-auto para guardar los matches automaticos.")


def cmd_set_equipo(team_id: int, oddsapi_name: str):
    equipos      = load_equipos()
    team_aliases = load_team_aliases()
    if team_id not in equipos:
        print(f"team_id={team_id} no encontrado en equipos.csv")
        return
    old = team_aliases.get(team_id, '(ninguno)')
    team_aliases[team_id] = oddsapi_name
    save_team_aliases(team_aliases, equipos)
    print(f"  '{equipos[team_id]['nombre']}': '{old}' -> '{oddsapi_name}'  [guardado]")


def cmd_set_liga(liga_id: int, oddsapi_slug: str):
    ligas        = load_ligas()
    liga_aliases = load_league_aliases()
    if liga_id not in ligas:
        print(f"liga_id={liga_id} no encontrado en ligas.csv")
        return
    old = liga_aliases.get(liga_id, '(ninguno)')
    liga_aliases[liga_id] = oddsapi_slug
    save_league_aliases(liga_aliases, ligas)
    print(f"  '{ligas[liga_id]['nombre']}': '{old}' -> '{oddsapi_slug}'  [guardado]")


def cmd_borrar_equipo(team_id: int):
    equipos      = load_equipos()
    team_aliases = load_team_aliases()
    if team_id in team_aliases:
        del team_aliases[team_id]
        save_team_aliases(team_aliases, equipos)
        print(f"  Alias de id={team_id} eliminado.")
    else:
        print(f"  id={team_id} no tenia alias.")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    if not args or '--status' in args:
        cmd_status()

    elif '--ver-liga' in args:
        idx = args.index('--ver-liga')
        cmd_ver_liga(int(args[idx + 1]))

    elif '--auto-ligas' in args or '--confirmar-auto' in args:
        flag = '--confirmar-auto' if '--confirmar-auto' in args else '--auto-ligas'
        idx  = args.index(flag)
        ids  = [int(x) for x in args[idx + 1:] if x.isdigit()]
        if not ids:
            ids = [128, 140]   # default: ligas principales
        cmd_auto_ligas(ids, confirmar=(flag == '--confirmar-auto'))

    elif '--set-equipo' in args:
        idx = args.index('--set-equipo')
        cmd_set_equipo(int(args[idx + 1]), args[idx + 2])

    elif '--set-liga' in args:
        idx = args.index('--set-liga')
        cmd_set_liga(int(args[idx + 1]), args[idx + 2])

    elif '--borrar-equipo' in args:
        idx = args.index('--borrar-equipo')
        cmd_borrar_equipo(int(args[idx + 1]))

    else:
        print(__doc__)


if __name__ == '__main__':
    main()
