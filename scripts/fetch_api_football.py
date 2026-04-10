"""
Actualiza partidos_historicos.csv usando la API de API-Football (premium).

Por cada equipo configurado, descarga sus ultimos N partidos y extrae:
  goles, tiros totales, tiros al arco, corners, posesion, tarjetas.

Uso:
    python fetch_api_football.py              # actualiza todos los equipos
    python fetch_api_football.py --dry-run   # muestra filas sin guardar
    python fetch_api_football.py --team "Boca Juniors"   # solo un equipo
"""

import csv
import json
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Configuracion
# ─────────────────────────────────────────────────────────────────────────────

API_KEY  = '5a7d5d038454c3640c8771ce2274c18c'
BASE_URL = 'https://v3.football.api-sports.io'
CSV_PATH = Path(r'C:\Users\Matt\Apuestas Deportivas\data\historico\partidos_historicos.csv')

LAST_N   = 20   # cuantos ultimos partidos traer por equipo

# Equipos a seguir:  nombre_csv -> { id, leagues }
# leagues: lista de league_id de API-Football que queremos incluir
TEAMS = {
    'Boca Juniors':    {'id': 451, 'leagues': [128, 130, 13]},
    # 128 = Liga Profesional Argentina
    # 130 = Copa Argentina
    # 13  = Copa Libertadores
    'Independiente':   {'id': 453, 'leagues': [128, 130, 13]},
    'Barcelona':       {'id': 529, 'leagues': [140, 2, 848]},
    # 140 = La Liga   2 = UEFA Champions League   848 = Copa del Rey
    'Atletico Madrid': {'id': 530, 'leagues': [140, 2, 848]},
}

# API team name  ->  nombre normalizado para nuestro CSV
API_NAME_MAP = {
    'Boca Juniors':          'Boca Juniors',
    'Independiente':         'Independiente',
    'Barcelona':             'Barcelona',
    'FC Barcelona':          'Barcelona',
    'Atletico Madrid':       'Atletico Madrid',
    'Atletico de Madrid':    'Atletico Madrid',
    'Club Atletico de Madrid': 'Atletico Madrid',
}

# API league name -> nombre en nuestro CSV
LEAGUE_MAP = {
    'Liga Profesional Argentina': 'Liga Profesional',
    'Copa Argentina':             'Copa Argentina',
    'CONMEBOL Libertadores':      'Copa Libertadores',
    'La Liga':                    'La Liga',
    'UEFA Champions League':      'Champions League',
    'Copa del Rey':               'Copa del Rey',
}

CSV_FIELDS = [
    'fecha', 'competicion',
    'equipo_local', 'equipo_visitante',
    'goles_local', 'goles_visitante',
    'tiros_local', 'tiros_visitante',
    'tiros_arco_local', 'tiros_arco_visitante',
    'corners_local', 'corners_visitante',
    'posesion_local', 'posesion_visitante',
    'tarjetas_local', 'tarjetas_visitante',
]

# ─────────────────────────────────────────────────────────────────────────────
# Utilidades de API
# ─────────────────────────────────────────────────────────────────────────────

def api_get(endpoint, params=None):
    """Hace un GET a la API y devuelve el JSON de response[]."""
    url = f"{BASE_URL}/{endpoint}"
    if params:
        url += '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={'x-apisports-key': API_KEY})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read())
    if data.get('errors'):
        raise RuntimeError(f"API error: {data['errors']}")
    return data.get('response', [])


def stat_value(stats_list, stat_type):
    """Extrae el valor de un tipo de estadistica de la lista de la API."""
    for s in stats_list:
        if s['type'] == stat_type:
            v = s['value']
            if v is None:
                return 0
            # Posesion viene como "57%" → devolver solo el numero
            if isinstance(v, str) and v.endswith('%'):
                return int(v.rstrip('%'))
            return int(v)
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# Logica principal
# ─────────────────────────────────────────────────────────────────────────────

def load_existing_csv():
    """Carga el CSV y devuelve (rows, existing_keys)."""
    if not CSV_PATH.exists():
        return [], set()
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    # Clave unica: fecha + local + visitante (normalizado)
    keys = {
        (r['fecha'], r['equipo_local'].lower(), r['equipo_visitante'].lower())
        for r in rows
    }
    return rows, keys


def normalize_team(api_name):
    """Convierte nombre de API a nuestro nombre normalizado."""
    return API_NAME_MAP.get(api_name, api_name)


def normalize_league(api_league_name):
    """Convierte nombre de liga de API a nuestro nombre."""
    return LEAGUE_MAP.get(api_league_name, api_league_name)


def fetch_team_matches(team_csv_name, team_id, allowed_leagues, existing_keys, dry_run=False):
    """
    Descarga los ultimos partidos del equipo, filtra por liga y
    extrae estadisticas. Devuelve lista de filas nuevas.
    """
    print(f"\n[{team_csv_name}]  team_id={team_id}")
    print(f"   Descargando ultimos {LAST_N} fixtures...")

    fixtures = api_get('fixtures', {'team': team_id, 'last': LAST_N})
    print(f"   Encontrados: {len(fixtures)} partidos")

    new_rows = []

    for fix in fixtures:
        league_id   = fix['league']['id']
        league_name = fix['league']['name']
        status      = fix['fixture']['status']['short']
        fixture_id  = fix['fixture']['id']

        # Solo partidos terminados y en las ligas que queremos
        if status not in ('FT', 'AET', 'PEN'):
            continue
        if league_id not in allowed_leagues:
            continue

        # Fecha
        date_str = fix['fixture']['date'][:10]   # YYYY-MM-DD

        # Equipos
        home_api = fix['teams']['home']['name']
        away_api = fix['teams']['away']['name']
        home_csv = normalize_team(home_api)
        away_csv = normalize_team(away_api)
        comp_csv = normalize_league(league_name)

        # Verificar duplicado
        key = (date_str, home_csv.lower(), away_csv.lower())
        if key in existing_keys:
            print(f"   [skip] {date_str}  {home_csv} vs {away_csv}  (ya existe)")
            continue

        # Estadisticas
        print(f"   Descargando stats  fixture={fixture_id}  {date_str}  {home_csv} vs {away_csv}...", end='')
        time.sleep(0.3)   # respetar rate limit (~450 req/min en premium)

        try:
            stats = api_get('fixtures/statistics', {'fixture': fixture_id})
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

        if len(stats) < 2:
            print(f"  sin estadisticas")
            continue

        # Mapear home/away a local/visitante segun team ID
        home_id = fix['teams']['home']['id']
        s_home  = next((s['statistics'] for s in stats if s['team']['id'] == home_id), [])
        s_away  = next((s['statistics'] for s in stats if s['team']['id'] != home_id), [])

        goles_l = fix['goals']['home'] or 0
        goles_v = fix['goals']['away'] or 0

        row = {
            'fecha':                date_str,
            'competicion':          comp_csv,
            'equipo_local':         home_csv,
            'equipo_visitante':     away_csv,
            'goles_local':          goles_l,
            'goles_visitante':      goles_v,
            'tiros_local':          stat_value(s_home, 'Total Shots'),
            'tiros_visitante':      stat_value(s_away, 'Total Shots'),
            'tiros_arco_local':     stat_value(s_home, 'Shots on Goal'),
            'tiros_arco_visitante': stat_value(s_away, 'Shots on Goal'),
            'corners_local':        stat_value(s_home, 'Corner Kicks'),
            'corners_visitante':    stat_value(s_away, 'Corner Kicks'),
            'posesion_local':       stat_value(s_home, 'Ball Possession'),
            'posesion_visitante':   stat_value(s_away, 'Ball Possession'),
            'tarjetas_local':       stat_value(s_home, 'Yellow Cards') + stat_value(s_home, 'Red Cards'),
            'tarjetas_visitante':   stat_value(s_away, 'Yellow Cards') + stat_value(s_away, 'Red Cards'),
        }

        # Correccion: si posesion no suma ~100 usar complemento
        pl = row['posesion_local']
        pv = row['posesion_visitante']
        if pl > 0 and pv == 0:
            row['posesion_visitante'] = 100 - pl
        elif pv > 0 and pl == 0:
            row['posesion_local'] = 100 - pv

        new_rows.append(row)
        existing_keys.add(key)   # evitar duplicar si el mismo partido aparece en 2 equipos

        print(f"  OK  (goles {goles_l}-{goles_v}, tiros {row['tiros_local']}/{row['tiros_visitante']}, "
              f"arco {row['tiros_arco_local']}/{row['tiros_arco_visitante']}, "
              f"corners {row['corners_local']}/{row['corners_visitante']}, "
              f"pos {row['posesion_local']}%/{row['posesion_visitante']}%)")

    print(f"   Filas nuevas para {team_csv_name}: {len(new_rows)}")
    return new_rows


def save_csv(all_rows):
    """Guarda el CSV ordenado por fecha."""
    all_rows.sort(key=lambda r: r['fecha'])
    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"\nCSV guardado: {CSV_PATH}  ({len(all_rows)} filas total)")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    dry_run     = '--dry-run' in sys.argv
    only_team   = None
    if '--team' in sys.argv:
        idx = sys.argv.index('--team')
        only_team = sys.argv[idx + 1]

    if dry_run:
        print("=== DRY RUN — no se guardara nada ===")

    existing_rows, existing_keys = load_existing_csv()
    print(f"CSV actual: {len(existing_rows)} filas")

    all_new = []

    for team_name, cfg in TEAMS.items():
        if only_team and team_name.lower() != only_team.lower():
            continue
        new = fetch_team_matches(
            team_name, cfg['id'], cfg['leagues'],
            existing_keys, dry_run=dry_run
        )
        all_new.extend(new)

    print(f"\nTotal filas nuevas: {len(all_new)}")

    if all_new and not dry_run:
        combined = existing_rows + all_new
        save_csv(combined)
        print("\nListo. Podes correr analizar_partido.py para usar los datos actualizados.")
    elif all_new and dry_run:
        print("\n[DRY RUN] Filas que se agregarian:")
        for r in all_new:
            print(f"  {r['fecha']}  {r['equipo_local']} vs {r['equipo_visitante']}  "
                  f"{r['goles_local']}-{r['goles_visitante']}")
    else:
        print("No hay filas nuevas para agregar. El CSV ya esta al dia.")
