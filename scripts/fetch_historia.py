"""
fetch_historia.py
-----------------
Descarga masiva de partidos historicos para engrosar el CSV de historia.

Fuentes:
  - Liga Profesional Argentina (128): temporada 2025 completa + 2026 actual
  - La Liga Espana           (140): temporada 2023 (23/24) + 2024 (24/25) + 2025 actual
  - Copa Libertadores        ( 13): 2025 — solo fixtures de equipos en nuestra DB
  - Copa Sudamericana        ( 11): 2025 — idem
  - Copa Argentina           (130): 2024 + 2025 — idem
  - UEFA Champions League    (  2): 2024 (24/25) — solo equipos espanoles en DB
  - UEFA Europa League       (  3): 2024 — idem
  - Copa del Rey             (143): 2024 + 2025 — idem

Estrategia de requests:
  - Se obtiene la LISTA de fixtures por (liga, season) en 1 sola llamada.
  - Se filtra: solo terminados (FT/AET/PEN), no ya descargados.
  - Para competiciones internacionales: solo si al menos 1 equipo esta en DB.
  - Se descargan las estadisticas de a 1 fixture por vez.
  - Se guarda progreso en fetch_historia_progress.json para poder retomar.
  - MAX_PER_RUN controla cuantos fixtures se procesan por ejecucion.

Uso:
  python fetch_historia.py                # procesa hasta MAX_PER_RUN fixtures
  python fetch_historia.py --max 50       # sobreescribe MAX_PER_RUN
  python fetch_historia.py --status       # muestra pendientes sin procesar nada
  python fetch_historia.py --reset        # borra el progreso y empieza de cero
"""

import csv
import json
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path
from collections import defaultdict

# Forzar UTF-8 en la consola de Windows para nombres con caracteres especiales
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

BASE          = Path(r'C:\Users\Matt\Apuestas Deportivas')
CSV_PATH      = BASE / 'data/historico/partidos_historicos.csv'
EQUIPOS_PATH  = BASE / 'data/db/equipos.csv'
LIGAS_PATH    = BASE / 'data/db/ligas.csv'
PROGRESS_FILE = BASE / 'data/historico/fetch_historia_progress.json'

API_KEY  = '5a7d5d038454c3640c8771ce2274c18c'
BASE_URL = 'https://v3.football.api-sports.io'

MAX_PER_RUN   = 99999  # sin limite por defecto — descargar todo lo posible
SLEEP_BETWEEN = 0.4   # segundos entre requests

CSV_FIELDS = [
    'fixture_id', 'fecha', 'liga_id',
    'equipo_local_id', 'equipo_visitante_id',
    'goles_local', 'goles_visitante',
    'tiros_local', 'tiros_visitante',
    'tiros_arco_local', 'tiros_arco_visitante',
    'corners_local', 'corners_visitante',
    'posesion_local', 'posesion_visitante',
    'tarjetas_local', 'tarjetas_visitante',
    # ── Stats extendidas (antes en backfill_stats.py, ahora integradas) ──
    'xg_local', 'xg_visitante',
    'tiros_dentro_local', 'tiros_dentro_visitante',
    'tiros_fuera_local', 'tiros_fuera_visitante',
    'tiros_bloqueados_local', 'tiros_bloqueados_visitante',
    'atajadas_local', 'atajadas_visitante',
    'goles_prevenidos_local', 'goles_prevenidos_visitante',
    'referee',
]

# ─────────────────────────────────────────────────────────────────────────────
# Fuentes: (liga_id, season, descripcion, solo_equipos_en_db)
# ─────────────────────────────────────────────────────────────────────────────
# Orden = prioridad de descarga (primero las ligas principales)

FUENTES = [
    # Orden: temporadas actuales PRIMERO, luego hacia atrás.
    # Esto garantiza que los partidos más recientes se descarguen antes.
    #
    # ── Temporadas actuales (prioridad máxima) ────────────────────────────────
    (128, 2026, 'Liga Profesional 2026 (actual)',   False),
    (140, 2025, 'La Liga 2025/26 (actual)',          False),
    ( 39, 2025, 'Premier League 2025/26 (actual)',  False),
    ( 78, 2025, 'Bundesliga 2025/26 (actual)',       False),
    (135, 2025, 'Serie A 2025/26 (actual)',          False),
    ( 71, 2025, 'Brasileirao 2025 (actual)',         False),
    # ── Ligas exoticas (test de mercados menos eficientes) ───────────────────
    (113, 2026, 'Allsvenskan 2026 (actual)',         False),  # Suecia, calendario
    (141, 2025, 'LaLiga 2 2025/26 (actual)',         False),  # Espana 2a
    (203, 2025, 'Super Lig 2025/26 (actual)',        False),  # Turquia
    # ── Copas / internacionales actuales — solo equipos en DB ────────────────
    ( 13, 2025, 'Copa Libertadores 2025',           True),
    ( 11, 2025, 'Copa Sudamericana 2025',           True),
    (130, 2025, 'Copa Argentina 2025',              True),
    (  2, 2024, 'Champions League 2024/25',         True),
    (  3, 2024, 'Europa League 2024/25',            True),
    (143, 2025, 'Copa del Rey 2025/26',             True),
    # ── Temporadas anteriores (segundo nivel) ─────────────────────────────────
    (128, 2025, 'Liga Profesional 2025',            False),
    (140, 2024, 'La Liga 2024/25',                  False),
    ( 39, 2024, 'Premier League 2024/25',           False),
    ( 78, 2024, 'Bundesliga 2024/25',               False),
    (135, 2024, 'Serie A 2024/25',                  False),
    ( 71, 2024, 'Brasileirao 2024',                 False),
    (130, 2024, 'Copa Argentina 2024',              True),
    (143, 2024, 'Copa del Rey 2024/25',             True),
    # ── Ligas exoticas — temporadas previas ─────────────────────────────────
    (113, 2025, 'Allsvenskan 2025',                 False),
    (141, 2024, 'LaLiga 2 2024/25',                 False),
    (203, 2024, 'Super Lig 2024/25',                False),
    # ── Temporadas más viejas (tercer nivel) ──────────────────────────────────
    (140, 2023, 'La Liga 2023/24',                  False),
    ( 39, 2023, 'Premier League 2023/24',           False),
    ( 78, 2023, 'Bundesliga 2023/24',               False),
    (135, 2023, 'Serie A 2023/24',                  False),
]

FINISHED_STATUSES = {'FT', 'AET', 'PEN'}

# ─────────────────────────────────────────────────────────────────────────────
# API
# ─────────────────────────────────────────────────────────────────────────────

_requests_this_run = 0

def api_get(endpoint, params=None):
    global _requests_this_run
    url = f"{BASE_URL}/{endpoint}"
    if params:
        url += '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={'x-apisports-key': API_KEY})
    with urllib.request.urlopen(req, timeout=20) as r:
        data = json.loads(r.read())
    _requests_this_run += 1
    if data.get('errors'):
        raise RuntimeError(f"API error: {data['errors']}")
    return data.get('response', [])


def stat_value(stats_list, stat_type, as_float=False):
    for s in stats_list:
        if s['type'] == stat_type:
            v = s['value']
            if v is None:
                return '' if as_float else 0
            if isinstance(v, str) and v.endswith('%'):
                return int(v.rstrip('%'))
            try:
                return str(float(v)) if as_float else int(float(v))
            except (ValueError, TypeError):
                return str(v) if as_float else 0
    return '' if as_float else 0


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def load_equipos():
    by_id, by_name = {}, {}
    if not EQUIPOS_PATH.exists():
        return by_id, by_name
    with open(EQUIPOS_PATH, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            tid = int(r['id'])
            by_id[tid]                   = r
            by_name[r['nombre'].lower()] = tid
    return by_id, by_name


def load_ligas():
    by_id, by_name = {}, {}
    if not LIGAS_PATH.exists():
        return by_id, by_name
    with open(LIGAS_PATH, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            lid = int(r['id'])
            by_id[lid]                   = r
            by_name[r['nombre'].lower()] = lid
    return by_id, by_name


def save_equipos(by_id):
    rows = sorted(by_id.values(), key=lambda r: int(r['id']))
    with open(EQUIPOS_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['id','nombre','pais','liga_id_principal'])
        w.writeheader()
        w.writerows(rows)


def save_ligas(by_id):
    rows = sorted(by_id.values(), key=lambda r: int(r['id']))
    with open(LIGAS_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['id','nombre','pais'])
        w.writeheader()
        w.writerows(rows)


def load_existing_fids():
    if not CSV_PATH.exists():
        return set()
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        return {int(r['fixture_id']) for r in csv.DictReader(f)}


def append_rows(new_rows):
    existing = []
    file_fields = None
    if CSV_PATH.exists():
        with open(CSV_PATH, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            file_fields = list(reader.fieldnames) if reader.fieldnames else None
            existing = list(reader)
    # Preservar columnas existentes (ej. stats extendidas de backfill)
    if file_fields:
        merged = list(file_fields)
        for col in CSV_FIELDS:
            if col not in merged:
                merged.append(col)
    else:
        merged = list(CSV_FIELDS)
    all_rows = existing + new_rows
    all_rows.sort(key=lambda r: (r.get('fecha') or '0000-00-00', int(r.get('fixture_id') or 0)))
    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=merged, extrasaction='ignore')
        w.writeheader()
        w.writerows(all_rows)


# ─────────────────────────────────────────────────────────────────────────────
# Progreso
# ─────────────────────────────────────────────────────────────────────────────

def load_progress():
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, encoding='utf-8') as f:
            return json.load(f)
    return {'fetched': [], 'failed': [], 'skipped_no_stats': []}


def save_progress(prog):
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(prog, f, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
# Construccion de la lista de fixtures pendientes
# ─────────────────────────────────────────────────────────────────────────────

def build_pending(equipos_by_id, existing_fids, prog):
    """
    Para cada fuente, descarga la lista de fixtures y retorna los pendientes
    como lista de dicts: {fixture_id, liga_id, fecha, home_id, away_id, ...}
    Ordena: ligas principales primero, luego internacionales.
    """
    done_fids = set(prog['fetched']) | set(prog['failed']) | set(prog['skipped_no_stats'])
    team_ids_in_db = set(equipos_by_id.keys())

    pending = []
    already_seen = set()   # para no duplicar fixture_id entre fuentes

    for liga_id, season, desc, only_db_teams in FUENTES:
        print(f"  Consultando {desc}...", end=' ', flush=True)
        try:
            fixtures = api_get('fixtures', {'league': liga_id, 'season': season})
            time.sleep(SLEEP_BETWEEN)
        except Exception as e:
            print(f"ERROR: {e}")
            continue

        total = len(fixtures)
        nuevos = 0
        for fix in fixtures:
            fid    = fix['fixture']['id']
            status = fix['fixture']['status']['short']

            if status not in FINISHED_STATUSES:
                continue
            if fid in existing_fids:
                continue
            if fid in done_fids:
                continue
            if fid in already_seen:
                continue

            home_id = fix['teams']['home']['id']
            away_id = fix['teams']['away']['id']

            # Para internacionales: al menos un equipo en DB
            if only_db_teams and home_id not in team_ids_in_db and away_id not in team_ids_in_db:
                continue

            already_seen.add(fid)
            nuevos += 1
            # Goles: vienen del fixture principal, NO del endpoint de estadisticas
            gh = fix.get('goals', {}).get('home') or 0
            ga = fix.get('goals', {}).get('away') or 0
            pending.append({
                'fixture_id':  fid,
                'liga_id':     liga_id,
                'fecha':       fix['fixture']['date'][:10],
                'home_id':     home_id,
                'home_name':   fix['teams']['home']['name'],
                'away_id':     away_id,
                'away_name':   fix['teams']['away']['name'],
                'home_goals':  gh,
                'away_goals':  ga,
                'referee':     (fix['fixture'].get('referee') or '').strip(),
                'desc':        desc,
            })

        print(f"{total} total  ->  {nuevos} pendientes")

    # Ordenar por fecha DESCENDENTE: más recientes primero
    pending.sort(key=lambda x: x['fecha'], reverse=True)
    return pending


# ─────────────────────────────────────────────────────────────────────────────
# Descarga de estadisticas de un fixture
# ─────────────────────────────────────────────────────────────────────────────

def fetch_fixture_stats(fix_info, equipos_by_id, ligas_by_id):
    """
    Descarga estadisticas del fixture y devuelve una fila CSV lista.
    Registra equipos/ligas nuevos en DB si hace falta.
    Retorna None si no hay stats.
    """
    fid      = fix_info['fixture_id']
    liga_id  = fix_info['liga_id']
    home_id  = fix_info['home_id']
    away_id  = fix_info['away_id']

    # Registrar liga si no existe
    if liga_id not in ligas_by_id:
        try:
            resp = api_get('leagues', {'id': liga_id})
            time.sleep(SLEEP_BETWEEN)
            if resp:
                lg = resp[0]['league']
                ligas_by_id[liga_id] = {
                    'id': liga_id,
                    'nombre': lg['name'],
                    'pais': resp[0]['country']['name'],
                }
                save_ligas(ligas_by_id)
                print(f"  [liga nueva] {lg['name']}")
        except Exception:
            ligas_by_id[liga_id] = {'id': liga_id, 'nombre': f'Liga {liga_id}', 'pais': '?'}

    # Registrar equipos si no existen
    for tid, tname in [(home_id, fix_info['home_name']), (away_id, fix_info['away_name'])]:
        if tid not in equipos_by_id:
            equipos_by_id[tid] = {
                'id': tid,
                'nombre': tname,
                'pais': '?',
                'liga_id_principal': liga_id,
            }
            save_equipos(equipos_by_id)
            print(f"    [equipo nuevo] id={tid}  {tname}")

    # Bajar estadisticas
    try:
        stats = api_get('fixtures/statistics', {'fixture': fid})
        time.sleep(SLEEP_BETWEEN)
    except urllib.error.HTTPError as e:
        if e.code == 429:
            raise  # propagar 429 para que main loop pare
        print(f"  ERROR stats fixture={fid}: {e}")
        return None
    except Exception as e:
        print(f"  ERROR stats fixture={fid}: {e}")
        return None

    if len(stats) < 2:
        return None   # sin stats completas

    # Identificar local y visitante en la respuesta
    home_stats = away_stats = None
    for entry in stats:
        tid = entry['team']['id']
        if tid == home_id:
            home_stats = entry['statistics']
        elif tid == away_id:
            away_stats = entry['statistics']

    if home_stats is None or away_stats is None:
        return None

    return {
        'fixture_id':           fid,
        'fecha':                fix_info['fecha'],
        'liga_id':              liga_id,
        'equipo_local_id':      home_id,
        'equipo_visitante_id':  away_id,
        # Goles vienen del fixture principal (guardados en fix_info), no de statistics
        'goles_local':          fix_info.get('home_goals', 0),
        'goles_visitante':      fix_info.get('away_goals', 0),
        'tiros_local':          stat_value(home_stats, 'Total Shots'),
        'tiros_visitante':      stat_value(away_stats, 'Total Shots'),
        'tiros_arco_local':     stat_value(home_stats, 'Shots on Goal'),
        'tiros_arco_visitante': stat_value(away_stats, 'Shots on Goal'),
        'corners_local':        stat_value(home_stats, 'Corner Kicks'),
        'corners_visitante':    stat_value(away_stats, 'Corner Kicks'),
        'posesion_local':       stat_value(home_stats, 'Ball Possession'),
        'posesion_visitante':   stat_value(away_stats, 'Ball Possession'),
        'tarjetas_local':       (stat_value(home_stats, 'Yellow Cards') +
                                 stat_value(home_stats, 'Red Cards')),
        'tarjetas_visitante':   (stat_value(away_stats, 'Yellow Cards') +
                                 stat_value(away_stats, 'Red Cards')),
        # ── Stats extendidas (misma respuesta de /fixtures/statistics) ──
        'xg_local':              stat_value(home_stats, 'expected_goals', as_float=True),
        'xg_visitante':          stat_value(away_stats, 'expected_goals', as_float=True),
        'tiros_dentro_local':    stat_value(home_stats, 'Shots insidebox'),
        'tiros_dentro_visitante':stat_value(away_stats, 'Shots insidebox'),
        'tiros_fuera_local':     stat_value(home_stats, 'Shots outsidebox'),
        'tiros_fuera_visitante': stat_value(away_stats, 'Shots outsidebox'),
        'tiros_bloqueados_local':    stat_value(home_stats, 'Blocked Shots'),
        'tiros_bloqueados_visitante':stat_value(away_stats, 'Blocked Shots'),
        'atajadas_local':        stat_value(home_stats, 'Goalkeeper Saves'),
        'atajadas_visitante':    stat_value(away_stats, 'Goalkeeper Saves'),
        'goles_prevenidos_local':    stat_value(home_stats, 'goals_prevented', as_float=True),
        'goles_prevenidos_visitante':stat_value(away_stats, 'goals_prevented', as_float=True),
        'referee':                   fix_info.get('referee', ''),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def show_status(prog, existing_fids):
    print(f"\n  Progreso actual:")
    print(f"    En CSV               : {len(existing_fids)} partidos")
    print(f"    Procesados (prog)    : {len(prog['fetched'])} OK  "
          f"/ {len(prog['failed'])} errores  "
          f"/ {len(prog['skipped_no_stats'])} sin stats")


def main():
    global MAX_PER_RUN

    # Flags
    status_only = '--status' in sys.argv
    reset       = '--reset'  in sys.argv
    if '--max' in sys.argv:
        idx = sys.argv.index('--max')
        MAX_PER_RUN = int(sys.argv[idx + 1])

    print("=" * 68)
    print("  FETCH HISTORIA — descarga masiva de partidos")
    print("=" * 68)

    equipos_by_id, _ = load_equipos()
    ligas_by_id, _   = load_ligas()
    existing_fids    = load_existing_fids()
    prog             = load_progress()

    if reset:
        prog = {'fetched': [], 'failed': [], 'skipped_no_stats': []}
        save_progress(prog)
        print("  Progreso reiniciado.\n")

    show_status(prog, existing_fids)

    print(f"\n  Obteniendo lista de fixtures pendientes ({len(FUENTES)} fuentes)...")
    pending = build_pending(equipos_by_id, existing_fids, prog)

    print(f"\n  Total pendientes a descargar: {len(pending)}")
    if not pending or status_only:
        if not pending:
            print("  Todo al dia! No hay fixtures nuevos.")
        return

    # Agrupar por fuente para mostrar resumen
    by_desc = defaultdict(int)
    for p in pending:
        by_desc[p['desc']] += 1
    print()
    for desc, n in by_desc.items():
        print(f"    {desc:<40}: {n} partidos")

    est_dias = max(1, (len(pending) - MAX_PER_RUN) // MAX_PER_RUN + 1)
    print(f"\n  Max por esta ejecucion : {MAX_PER_RUN}")
    print(f"  Estimado total dias    : ~{est_dias} ejecucion(es)")
    print()

    # Procesar
    batch     = []
    processed = 0
    nuevos    = 0

    rate_limited = False
    for fix_info in pending:
        if processed >= MAX_PER_RUN:
            break

        fid = fix_info['fixture_id']
        print(f"  [{processed+1:>3}/{min(MAX_PER_RUN, len(pending))}] "
              f"fid={fid}  {fix_info['fecha']}  "
              f"{fix_info['home_name']} vs {fix_info['away_name']}"
              f"  ({fix_info['desc']})", end='  ')

        try:
            row = fetch_fixture_stats(fix_info, equipos_by_id, ligas_by_id)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                print("RATE LIMIT — guardando y parando...")
                rate_limited = True
                break
            raise
        processed += 1

        if row is None:
            print("sin stats")
            prog['skipped_no_stats'].append(fid)
        else:
            print(f"OK  {row['goles_local']}-{row['goles_visitante']}  "
                  f"tiros={int(row['tiros_local'])+int(row['tiros_visitante'])}  "
                  f"corners={int(row['corners_local'])+int(row['corners_visitante'])}")
            batch.append(row)
            prog['fetched'].append(fid)
            nuevos += 1

        # Guardar progreso cada 10 fixtures
        if processed % 10 == 0:
            if batch:
                append_rows(batch)
                batch = []
            save_progress(prog)

    # Flush final
    if batch:
        append_rows(batch)
    save_progress(prog)

    if rate_limited:
        print(f"\n  *** CUPO AGOTADO (429) — se guardaron {nuevos} partidos nuevos.")
        print(f"  *** Volver a correr mañana para continuar.")

    remaining = len(pending) - processed
    print()
    print("=" * 68)
    print(f"  Descargados esta ejecucion : {nuevos} nuevos partidos")
    print(f"  Sin stats (saltados)       : {processed - nuevos}")
    print(f"  Pendientes para proxima    : {remaining}")

    existing_fids_new = load_existing_fids()
    print(f"  Total en CSV ahora         : {len(existing_fids_new)} partidos")
    print("=" * 68)

    if remaining > 0:
        proxima = max(1, remaining // MAX_PER_RUN)
        print(f"\n  Volver a correr para seguir. ~{proxima} ejecucion(es) mas.")
    else:
        print("\n  Descarga completa.")
    print()


if __name__ == '__main__':
    main()
