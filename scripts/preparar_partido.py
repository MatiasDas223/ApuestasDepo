"""
Prepara los datos para analizar un partido.

Uso:
    python preparar_partido.py "Boca Juniors" "Independiente"
    python preparar_partido.py "Real Madrid" "Barcelona"
    python preparar_partido.py "Boca Juniors" "Independiente" --fixture 1492015

Para cada equipo:
  - Si no esta en la DB lo busca en la API y lo registra.
  - Descarga solo los partidos nuevos (los que ya estan en el CSV se saltean).
  - Agrega las filas al CSV historico.

Al terminar busca el proximo fixture entre los dos equipos, descarga las odds
de Bet365 y muestra el dict ODDS listo para pegar en analizar_partido.py.
Si no encuentra el fixture automaticamente, pasar --fixture <id>.
"""

import csv
import json
import sys
import time
import urllib.request
import urllib.parse
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE           = Path(r'C:\Users\Matt\Apuestas Deportivas')
CSV_PATH       = BASE / 'data/historico/partidos_historicos.csv'
EQUIPOS_PATH   = BASE / 'data/db/equipos.csv'
LIGAS_PATH     = BASE / 'data/db/ligas.csv'
ANALIZAR_PATH  = BASE / 'scripts/analizar_partido.py'

MARKER_BEGIN = '# ── BEGIN PARTIDO CONFIG ─'
MARKER_END   = '# ── END PARTIDO CONFIG ─'

API_KEY  = '5a7d5d038454c3640c8771ce2274c18c'
BASE_URL = 'https://v3.football.api-sports.io'
LAST_N   = 20   # ultimos partidos a traer por equipo

CSV_FIELDS = [
    'fixture_id', 'fecha', 'liga_id',
    'equipo_local_id', 'equipo_visitante_id',
    'goles_local', 'goles_visitante',
    'tiros_local', 'tiros_visitante',
    'tiros_arco_local', 'tiros_arco_visitante',
    'corners_local', 'corners_visitante',
    'posesion_local', 'posesion_visitante',
    'tarjetas_local', 'tarjetas_visitante',
]

# ── API ───────────────────────────────────────────────────────────────────────

def api_get(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    if params:
        url += '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={'x-apisports-key': API_KEY})
    with urllib.request.urlopen(req, timeout=15) as r:
        data = json.loads(r.read())
    if data.get('errors'):
        raise RuntimeError(f"API error en /{endpoint}: {data['errors']}")
    return data.get('response', [])


def stat_value(stats_list, stat_type):
    for s in stats_list:
        if s['type'] == stat_type:
            v = s['value']
            if v is None:
                return 0
            if isinstance(v, str) and v.endswith('%'):
                return int(v.rstrip('%'))
            return int(v)
    return 0

# ── DB helpers ────────────────────────────────────────────────────────────────

def load_equipos():
    """Devuelve {id: row_dict} y {nombre_lower: id}."""
    by_id, by_name = {}, {}
    with open(EQUIPOS_PATH, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            tid = int(r['id'])
            by_id[tid]              = r
            by_name[r['nombre'].lower()] = tid
    return by_id, by_name


def load_ligas():
    """Devuelve {id: row_dict} y {nombre_lower: id}."""
    by_id, by_name = {}, {}
    with open(LIGAS_PATH, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            lid = int(r['id'])
            by_id[lid]              = r
            by_name[r['nombre'].lower()] = lid
    return by_id, by_name


def save_equipos(by_id):
    rows = sorted(by_id.values(), key=lambda r: int(r['id']))
    with open(EQUIPOS_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['id','nombre','pais','liga_id_principal'])
        w.writeheader(); w.writerows(rows)


def save_ligas(by_id):
    rows = sorted(by_id.values(), key=lambda r: int(r['id']))
    with open(LIGAS_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['id','nombre','pais'])
        w.writeheader(); w.writerows(rows)


def load_csv_fixture_ids():
    """Devuelve set de fixture_ids ya en el CSV."""
    if not CSV_PATH.exists():
        return set()
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        return {int(r['fixture_id']) for r in csv.DictReader(f) if int(r['fixture_id']) > 0}


def append_to_csv(new_rows):
    """Agrega filas nuevas al CSV y reordena por fecha."""
    existing = []
    if CSV_PATH.exists():
        with open(CSV_PATH, newline='', encoding='utf-8') as f:
            existing = list(csv.DictReader(f))
    all_rows = existing + new_rows
    all_rows.sort(key=lambda r: (r['fecha'], int(r.get('fixture_id', 0))))
    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader(); w.writerows(all_rows)
    return len(all_rows)

# ── Busqueda y registro de equipos ────────────────────────────────────────────

def find_or_register_team(name, equipos_by_id, equipos_by_name, ligas_by_id):
    """
    Busca el equipo en la DB local. Si no esta, lo busca en la API,
    lo muestra al usuario y lo registra.
    Devuelve (team_id, allowed_league_ids).
    """
    key = name.lower().strip()

    # Busqueda exacta en DB
    if key in equipos_by_name:
        tid  = equipos_by_name[key]
        row  = equipos_by_id[tid]
        liga = int(row['liga_id_principal'])
        print(f"  [{name}] encontrado en DB  id={tid}  liga_principal={ligas_by_id.get(liga, {}).get('nombre', liga)}")
        return tid, _leagues_for_team(tid, liga)

    # Busqueda parcial en DB (contiene el nombre)
    matches = [(k, v) for k, v in equipos_by_name.items() if key in k or k in key]
    if len(matches) == 1:
        tid  = matches[0][1]
        row  = equipos_by_id[tid]
        liga = int(row['liga_id_principal'])
        print(f"  [{name}] -> '{row['nombre']}' en DB  id={tid}")
        return tid, _leagues_for_team(tid, liga)

    # No esta en DB: buscar en API
    print(f"  [{name}] no encontrado en DB, buscando en API...")
    results = api_get('teams', {'search': name})
    time.sleep(0.3)

    if not results:
        raise ValueError(f"Equipo '{name}' no encontrado en la API. Verificar el nombre.")

    # Mostrar opciones si hay varias
    if len(results) > 1:
        print(f"  Multiples resultados para '{name}':")
        for i, r in enumerate(results[:5]):
            t = r['team']
            print(f"    [{i}] id={t['id']}  {t['name']}  ({t['country']})")
        choice = input(f"  Elegir numero [0]: ").strip()
        idx = int(choice) if choice.isdigit() else 0
    else:
        idx = 0

    chosen = results[idx]
    t = chosen['team']
    tid  = t['id']
    tname = t['name']
    tpais = t['country']

    # Detectar liga principal del equipo
    seasons = api_get('teams/seasons', {'team': tid})
    time.sleep(0.3)
    # Buscar en que ligas juega este season
    season = max(seasons) if seasons else 2025
    leagues_resp = api_get('leagues', {'team': tid, 'season': season, 'current': 'true'})
    time.sleep(0.3)

    liga_principal = 0
    allowed = []
    for lr in leagues_resp:
        lid = lr['league']['id']
        lname = lr['league']['name']
        lpais = lr['league']['country']
        allowed.append(lid)
        if liga_principal == 0:
            liga_principal = lid
        # Registrar liga si no existe
        if lid not in ligas_by_id:
            ligas_by_id[lid] = {'id': lid, 'nombre': lname, 'pais': lpais}
            save_ligas(ligas_by_id)
            print(f"  [DB] Liga nueva registrada: {lid}  {lname}")

    # Registrar equipo
    equipos_by_id[tid] = {'id': tid, 'nombre': tname, 'pais': tpais,
                          'liga_id_principal': liga_principal}
    equipos_by_name[tname.lower()] = tid
    save_equipos(equipos_by_id)
    print(f"  [DB] Equipo nuevo registrado: id={tid}  {tname}  ({tpais})  liga={liga_principal}")

    return tid, allowed if allowed else [liga_principal]


def _leagues_for_team(team_id, liga_principal):
    """Devuelve la lista de ligas relevantes para el equipo según su liga principal."""
    # Ligas principales + copas asociadas
    LIGA_EXTRAS = {
        128: [128, 130, 13],    # Liga Profesional + Copa Arg + Libertadores
        140: [140, 848, 2],     # La Liga + Copa del Rey + Champions
        39:  [39, 45, 2],       # Premier League + FA Cup + Champions
        61:  [61, 65, 2],       # Ligue 1 + Coupe de France + Champions
        78:  [78, 81, 2],       # Bundesliga + DFB Pokal + Champions
        135: [135, 136, 2],     # Serie A + Coppa Italia + Champions
    }
    return LIGA_EXTRAS.get(liga_principal, [liga_principal])

# ── Fetch de partidos ─────────────────────────────────────────────────────────

def fetch_new_matches(team_id, team_name, allowed_leagues,
                      existing_fixture_ids, equipos_by_id, ligas_by_id):
    """
    Descarga los ultimos LAST_N partidos del equipo.
    Agrega solo los que no estan en el CSV (deduplicacion por fixture_id).
    Devuelve lista de filas nuevas.
    """
    fixtures = api_get('fixtures', {'team': team_id, 'last': LAST_N})
    time.sleep(0.3)

    new_rows = []

    for fix in fixtures:
        league_id  = fix['league']['id']
        status     = fix['fixture']['status']['short']
        fixture_id = fix['fixture']['id']

        if status not in ('FT', 'AET', 'PEN'):
            continue
        if league_id not in allowed_leagues:
            continue
        if fixture_id in existing_fixture_ids:
            h = equipos_by_id.get(fix['teams']['home']['id'], {}).get('nombre', '?')
            a = equipos_by_id.get(fix['teams']['away']['id'], {}).get('nombre', '?')
            print(f"    [skip] fixture={fixture_id}  {h} vs {a}  (ya en CSV)")
            continue

        date_str     = fix['fixture']['date'][:10]
        home_api_id  = fix['teams']['home']['id']
        away_api_id  = fix['teams']['away']['id']
        home_api_name = fix['teams']['home']['name']
        away_api_name = fix['teams']['away']['name']

        # Registrar rivales desconocidos en DB
        for api_id, api_name in [(home_api_id, home_api_name), (away_api_id, away_api_name)]:
            if api_id not in equipos_by_id:
                equipos_by_id[api_id] = {'id': api_id, 'nombre': api_name,
                                          'pais': '?', 'liga_id_principal': league_id}
                save_equipos(equipos_by_id)
                print(f"    [DB] Rival nuevo: id={api_id}  {api_name}")

        h_name = equipos_by_id[home_api_id]['nombre']
        a_name = equipos_by_id[away_api_id]['nombre']
        print(f"    Descargando  fixture={fixture_id}  {date_str}  {h_name} vs {a_name}...", end='')
        time.sleep(0.3)

        try:
            stats = api_get('fixtures/statistics', {'fixture': fixture_id})
        except Exception as e:
            print(f"  ERROR stats: {e}")
            continue

        if len(stats) < 2:
            print(f"  sin estadisticas")
            continue

        s_home = next((s['statistics'] for s in stats if s['team']['id'] == home_api_id), [])
        s_away = next((s['statistics'] for s in stats if s['team']['id'] != home_api_id), [])

        pl = stat_value(s_home, 'Ball Possession')
        pv = stat_value(s_away, 'Ball Possession')
        if pl > 0 and pv == 0: pv = 100 - pl
        if pv > 0 and pl == 0: pl = 100 - pv

        row = {
            'fixture_id':           fixture_id,
            'fecha':                date_str,
            'liga_id':              league_id,
            'equipo_local_id':      home_api_id,
            'equipo_visitante_id':  away_api_id,
            'goles_local':          fix['goals']['home'] or 0,
            'goles_visitante':      fix['goals']['away'] or 0,
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

        gl, gv = row['goles_local'], row['goles_visitante']
        print(f"  OK  {gl}-{gv}  tiros {row['tiros_local']}/{row['tiros_visitante']}  "
              f"arco {row['tiros_arco_local']}/{row['tiros_arco_visitante']}  "
              f"corners {row['corners_local']}/{row['corners_visitante']}  pos {pl}%")

        new_rows.append(row)
        existing_fixture_ids.add(fixture_id)

    return new_rows

# ── Escritura de config en analizar_partido.py ───────────────────────────────

def build_config_block(team_local, team_visita, competition, fixture_id, odds, team_local_full=None, team_visita_full=None):
    """
    Genera el bloque completo de configuracion del partido (entre los marcadores).
    odds puede ser {} si no hay fixture disponible: todos los valores quedan None.
    """
    import importlib.util as _ilu
    _spec = _ilu.spec_from_file_location('fetch_odds', Path(__file__).parent / 'fetch_odds.py')
    _fo = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_fo)

    odds_str  = _fo.build_odds_dict_str(odds, team_local_full or team_local,
                                         team_visita_full or team_visita)
    fix_str   = str(fixture_id) if fixture_id else 'None'

    lines = [
        MARKER_BEGIN,
        f"TEAM_LOCAL  = '{team_local}'",
        f"TEAM_VISITA = '{team_visita}'",
        f"COMPETITION = '{competition}'",
        f"FIXTURE_ID  = {fix_str}",
        "N_SIM = 200_000",
        "",
        odds_str,
        MARKER_END,
    ]
    return "\n".join(lines)


def write_config_to_analizar(team_local, team_visita, competition, fixture_id, odds,
                              team_local_full=None, team_visita_full=None):
    """
    Reemplaza el bloque de configuracion en analizar_partido.py entre los marcadores.
    Si los marcadores no existen los agrega antes de la seccion de EJECUCION.
    """
    with open(ANALIZAR_PATH, encoding='utf-8') as f:
        content = f.read()

    new_block = build_config_block(team_local, team_visita, competition, fixture_id, odds,
                                   team_local_full, team_visita_full)

    i_begin = content.find(MARKER_BEGIN)
    i_end   = content.find(MARKER_END)

    if i_begin != -1 and i_end != -1:
        # Reemplazar desde inicio de la linea del BEGIN hasta el fin de la linea del END
        line_start = content.rfind('\n', 0, i_begin) + 1
        line_end   = content.find('\n', i_end)
        line_end   = line_end + 1 if line_end != -1 else len(content)
        new_content = content[:line_start] + new_block + "\n" + content[line_end:]
    else:
        # Insertar antes de la seccion EJECUCION
        exec_marker = '# ─' * 5
        idx = content.find('\n# EJECUCIÓN')
        if idx == -1:
            idx = content.find('\nif __name__')
        if idx == -1:
            idx = len(content)
        new_content = content[:idx] + "\n" + new_block + "\n" + content[idx:]

    with open(ANALIZAR_PATH, 'w', encoding='utf-8') as f:
        f.write(new_content)
    print(f"  analizar_partido.py actualizado  ({team_local} vs {team_visita}  {competition})")


# ── Busqueda de fixture proximo ───────────────────────────────────────────────

def find_upcoming_fixture(local_id, visita_id, next_n=10):
    """
    Busca el proximo fixture entre local_id y visita_id en la API.
    Devuelve (fixture_id, league_id, fecha) o (None, None, None) si no encuentra.
    """
    resp = api_get('fixtures', {'team': local_id, 'next': next_n})
    time.sleep(0.3)
    for fix in resp:
        h = fix['teams']['home']['id']
        a = fix['teams']['away']['id']
        if (h == local_id and a == visita_id) or (h == visita_id and a == local_id):
            fid   = fix['fixture']['id']
            lid   = fix['league']['id']
            fecha = fix['fixture']['date'][:10]
            return fid, lid, fecha
    return None, None, None


# ── Resumen historico ─────────────────────────────────────────────────────────

def resumen_equipo(team_id, team_name, ligas_by_id):
    rows = list(csv.DictReader(open(CSV_PATH, encoding='utf-8')))
    partidos = [r for r in rows
                if int(r['equipo_local_id']) == team_id
                or int(r['equipo_visitante_id']) == team_id]
    partidos.sort(key=lambda r: r['fecha'])
    n = len(partidos)
    ultimo = partidos[-1]['fecha'] if partidos else '-'
    primero = partidos[0]['fecha'] if partidos else '-'
    print(f"  {team_name:<25}  {n:>2} partidos en CSV  ({primero} -> {ultimo})")

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    raw_args = sys.argv[1:]

    # Parsear argumentos: posicionales y --fixture <id>
    fixture_arg = None
    pos_args    = []
    skip_next   = False
    for i, a in enumerate(raw_args):
        if skip_next:
            skip_next = False
            continue
        if a == '--fixture':
            if i + 1 < len(raw_args):
                fixture_arg = int(raw_args[i + 1])
                skip_next   = True
        elif a.startswith('--'):
            pass   # flags desconocidos: ignorar
        else:
            pos_args.append(a)

    if len(pos_args) < 2:
        print("Uso: python preparar_partido.py \"Equipo Local\" \"Equipo Visitante\" [--fixture <id>]")
        sys.exit(1)

    team_local_name  = pos_args[0]
    team_visita_name = pos_args[1]

    print(f"\n{'='*60}")
    print(f"  Preparando: {team_local_name} vs {team_visita_name}")
    print(f"{'='*60}\n")

    equipos_by_id, equipos_by_name = load_equipos()
    ligas_by_id, _                 = load_ligas()
    existing_fixture_ids           = load_csv_fixture_ids()

    print(f"CSV actual: {len(existing_fixture_ids)} fixtures registrados\n")

    all_new      = []
    local_id_res = None
    visita_id_res = None

    for i, team_name in enumerate([team_local_name, team_visita_name]):
        print(f"[{team_name}]")
        team_id, allowed = find_or_register_team(
            team_name, equipos_by_id, equipos_by_name, ligas_by_id
        )
        if i == 0:
            local_id_res  = team_id
        else:
            visita_id_res = team_id

        new = fetch_new_matches(
            team_id, team_name, allowed,
            existing_fixture_ids, equipos_by_id, ligas_by_id
        )
        print(f"  -> {len(new)} partidos nuevos agregados\n")
        all_new.extend(new)

    if all_new:
        total = append_to_csv(all_new)
        print(f"CSV actualizado: {total} filas totales ({len(all_new)} nuevas)\n")
    else:
        print("No hay partidos nuevos. El CSV ya esta al dia.\n")

    print("Historico disponible:")
    equipos_by_id2, equipos_by_name2 = load_equipos()
    for name in [team_local_name, team_visita_name]:
        tid = equipos_by_name2.get(name.lower().strip())
        if tid:
            resumen_equipo(tid, name, ligas_by_id)

    # ── Odds ──────────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  BUSCANDO ODDS BET365")
    print(f"{'='*60}")

    # Determinar fixture_id
    upcoming_fixture_id = fixture_arg
    upcoming_league_id  = None
    upcoming_fecha      = None

    if not upcoming_fixture_id and local_id_res and visita_id_res:
        print(f"\n  Buscando proximo fixture {team_local_name} vs {team_visita_name}...")
        upcoming_fixture_id, upcoming_league_id, upcoming_fecha = find_upcoming_fixture(
            local_id_res, visita_id_res
        )
        if upcoming_fixture_id:
            liga_nombre = ligas_by_id.get(upcoming_league_id, {}).get('nombre', upcoming_league_id)
            print(f"  Encontrado: fixture={upcoming_fixture_id}  {upcoming_fecha}  {liga_nombre}")
        else:
            print(f"  No se encontro fixture proximo entre los dos equipos.")
            print(f"  Pasa el ID manualmente con: --fixture <id>")

    # ── Odds y escritura de config ────────────────────────────────────────────
    odds_dict     = {}
    comp_sugerida = ligas_by_id.get(upcoming_league_id, {}).get('nombre', '???') \
                    if upcoming_league_id else '???'

    # Cargar fetch_odds una sola vez
    import importlib.util as _ilu
    _spec = _ilu.spec_from_file_location(
        'fetch_odds', Path(__file__).parent / 'fetch_odds.py'
    )
    _fo = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_fo)

    if upcoming_fixture_id:
        # Fuente 1: API Football (goles, BTTS, 1X2, handicaps)
        try:
            odds_dict, resumen = _fo.get_odds(upcoming_fixture_id, force=False)
            _fo.print_odds_report(odds_dict, resumen, team_local_name, team_visita_name)
        except Exception as e:
            print(f"  Error al obtener odds API Football: {e}")

    # Fuente 2: odds-api.io (corners, tiros, arco, tarjetas — por equipo y totales)
    print(f"\n{'='*60}")
    print(f"  BUSCANDO ODDS odds-api.io")
    print(f"{'='*60}")
    try:
        oddsapi_event_id, home_api, away_api = _fo.find_event_oddsapi(
            team_local_name, team_visita_name
        )
        if oddsapi_event_id:
            print(f"  Evento encontrado: id={oddsapi_event_id}  {home_api} vs {away_api}")
            oddsapi_odds, oddsapi_resumen = _fo.get_odds_oddsapi(oddsapi_event_id, force=False)
            if oddsapi_odds:
                print(f"\n  Mercados obtenidos de odds-api.io:")
                for m in oddsapi_resumen.get('mapeados', []):
                    print(f"    + {m}")
                # Merge: odds-api.io completa los mercados que API Football no trae
                merged = 0
                for k, v in oddsapi_odds.items():
                    if k not in odds_dict:
                        odds_dict[k] = v
                        merged += 1
                print(f"  -> {merged} claves nuevas agregadas al ODDS dict")
    except Exception as e:
        print(f"  Error al obtener odds de odds-api.io: {e}")

    # ── Escribir config en analizar_partido.py ────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  ACTUALIZANDO analizar_partido.py")
    print(f"{'='*60}")

    # Nombre completo del equipo para los comentarios del ODDS dict
    local_full  = equipos_by_id2.get(equipos_by_name2.get(team_local_name.lower().strip()), {}).get('nombre', team_local_name)
    visita_full = equipos_by_id2.get(equipos_by_name2.get(team_visita_name.lower().strip()), {}).get('nombre', team_visita_name)

    write_config_to_analizar(
        team_local=team_local_name,
        team_visita=team_visita_name,
        competition=comp_sugerida,
        fixture_id=upcoming_fixture_id,
        odds=odds_dict,
        team_local_full=local_full,
        team_visita_full=visita_full,
    )

    print(f"\n  Listo. Correr:")
    print(f"    python analizar_partido.py\n")
