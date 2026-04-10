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
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Configuracion
# ─────────────────────────────────────────────────────────────────────────────

API_KEY      = '5a7d5d038454c3640c8771ce2274c18c'
BASE_URL     = 'https://v3.football.api-sports.io'
CSV_PATH     = Path(r'C:\Users\Matt\Apuestas Deportivas\data\historico\partidos_historicos.csv')
EQUIPOS_PATH = Path(r'C:\Users\Matt\Apuestas Deportivas\data\db\equipos.csv')
LIGAS_PATH   = Path(r'C:\Users\Matt\Apuestas Deportivas\data\db\ligas.csv')

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
    'fecha', 'liga_id',
    'equipo_local_id', 'equipo_visitante_id',
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
# Base de datos de equipos y ligas
# ─────────────────────────────────────────────────────────────────────────────

def load_db():
    """Carga equipos.csv y ligas.csv. Devuelve (api_name→team_id, api_league→liga_id)."""
    # Equipos: id, nombre, pais, liga_id_principal
    team_by_id   = {}   # id_int -> nombre
    team_by_name = {}   # nombre_lower -> id_int
    with open(EQUIPOS_PATH, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            tid  = int(row['id'])
            name = row['nombre']
            team_by_id[tid]              = name
            team_by_name[name.lower()]   = tid

    # Ligas: id, nombre, pais
    liga_by_id   = {}
    liga_by_name = {}
    with open(LIGAS_PATH, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            lid  = int(row['id'])
            name = row['nombre']
            liga_by_id[lid]            = name
            liga_by_name[name.lower()] = lid

    return team_by_id, team_by_name, liga_by_id, liga_by_name


def register_team(api_id, api_name, country, primary_league_id):
    """Agrega un equipo nuevo a equipos.csv si no existe."""
    with open(EQUIPOS_PATH, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    existing_ids = {int(r['id']) for r in rows}
    if api_id in existing_ids:
        return False  # ya existe
    rows.append({
        'id': api_id, 'nombre': api_name, 'pais': country,
        'liga_id_principal': primary_league_id,
    })
    rows.sort(key=lambda r: int(r['id']))
    with open(EQUIPOS_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['id','nombre','pais','liga_id_principal'])
        w.writeheader(); w.writerows(rows)
    print(f"   [DB] Equipo nuevo registrado: {api_id}  {api_name}")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Logica principal
# ─────────────────────────────────────────────────────────────────────────────

def load_existing_csv():
    """Carga el CSV y devuelve (rows, existing_keys)."""
    if not CSV_PATH.exists():
        return [], set()
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    # Clave unica: fecha + local_id + visitante_id + liga_id
    keys = {
        (r['fecha'], int(r['equipo_local_id']),
         int(r['equipo_visitante_id']), int(r['liga_id']))
        for r in rows
    }
    return rows, keys


def fetch_team_matches(team_csv_name, team_id, allowed_leagues,
                       existing_keys, team_by_id, team_by_name,
                       liga_by_id, liga_by_name, dry_run=False):
    """
    Descarga los ultimos partidos del equipo, filtra por liga y
    extrae estadisticas. Devuelve lista de filas nuevas (con IDs).
    Si el rival no está en la DB, lo registra automaticamente.
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

        # Si la liga no está en nuestra DB, la saltamos
        if league_id not in liga_by_id:
            continue

        # Fecha
        date_str = fix['fixture']['date'][:10]

        # IDs de equipo directo desde la API
        home_api_id   = fix['teams']['home']['id']
        away_api_id   = fix['teams']['away']['id']
        home_api_name = fix['teams']['home']['name']
        away_api_name = fix['teams']['away']['name']

        # Registrar rivales desconocidos en la DB automaticamente
        for api_id, api_name in [(home_api_id, home_api_name), (away_api_id, away_api_name)]:
            if api_id not in team_by_id:
                country = fix['teams']['home']['name'] if api_id == home_api_id else fix['teams']['away']['name']
                register_team(api_id, api_name, '?', league_id)
                team_by_id[api_id]             = api_name
                team_by_name[api_name.lower()] = api_id

        # Verificar duplicado por IDs
        key = (date_str, home_api_id, away_api_id, league_id)
        if key in existing_keys:
            h = team_by_id.get(home_api_id, home_api_name)
            a = team_by_id.get(away_api_id, away_api_name)
            print(f"   [skip] {date_str}  {h} vs {a}  (ya existe)")
            continue

        # Estadisticas
        h_name = team_by_id.get(home_api_id, home_api_name)
        a_name = team_by_id.get(away_api_id, away_api_name)
        print(f"   Descargando stats  {date_str}  {h_name} vs {a_name}...", end='')
        time.sleep(0.3)

        try:
            stats = api_get('fixtures/statistics', {'fixture': fixture_id})
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

        if len(stats) < 2:
            print(f"  sin estadisticas")
            continue

        s_home = next((s['statistics'] for s in stats if s['team']['id'] == home_api_id), [])
        s_away = next((s['statistics'] for s in stats if s['team']['id'] != home_api_id), [])

        goles_l = fix['goals']['home'] or 0
        goles_v = fix['goals']['away'] or 0

        pl = stat_value(s_home, 'Ball Possession')
        pv = stat_value(s_away, 'Ball Possession')
        if pl > 0 and pv == 0:
            pv = 100 - pl
        elif pv > 0 and pl == 0:
            pl = 100 - pv

        row = {
            'fecha':                date_str,
            'liga_id':              league_id,
            'equipo_local_id':      home_api_id,
            'equipo_visitante_id':  away_api_id,
            'goles_local':          goles_l,
            'goles_visitante':      goles_v,
            'tiros_local':          stat_value(s_home, 'Total Shots'),
            'tiros_visitante':      stat_value(s_away, 'Total Shots'),
            'tiros_arco_local':     stat_value(s_home, 'Shots on Goal'),
            'tiros_arco_visitante': stat_value(s_away, 'Shots on Goal'),
            'corners_local':        stat_value(s_home, 'Corner Kicks'),
            'corners_visitante':    stat_value(s_away, 'Corner Kicks'),
            'posesion_local':       pl,
            'posesion_visitante':   pv,
            'tarjetas_local':       stat_value(s_home, 'Yellow Cards') + stat_value(s_home, 'Red Cards'),
            'tarjetas_visitante':   stat_value(s_away, 'Yellow Cards') + stat_value(s_away, 'Red Cards'),
        }

        new_rows.append(row)
        existing_keys.add(key)

        print(f"  OK  (goles {goles_l}-{goles_v}, tiros {row['tiros_local']}/{row['tiros_visitante']}, "
              f"arco {row['tiros_arco_local']}/{row['tiros_arco_visitante']}, "
              f"corners {row['corners_local']}/{row['corners_visitante']}, "
              f"pos {pl}%/{pv}%)")

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

    team_by_id, team_by_name, liga_by_id, liga_by_name = load_db()
    existing_rows, existing_keys = load_existing_csv()
    print(f"CSV actual: {len(existing_rows)} filas  |  DB: {len(team_by_id)} equipos, {len(liga_by_id)} ligas")

    all_new = []

    for team_name, cfg in TEAMS.items():
        if only_team and team_name.lower() != only_team.lower():
            continue
        new = fetch_team_matches(
            team_name, cfg['id'], cfg['leagues'],
            existing_keys, team_by_id, team_by_name,
            liga_by_id, liga_by_name, dry_run=dry_run
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
