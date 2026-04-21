"""
pipeline.py — Pipeline completo de value bets.

Pasos:
  1. Actualiza resultados W/L en value_bets.csv (partidos ya jugados)
  2. Busca partidos proximos en Liga Profesional + La Liga (default: 24h)
  3. Descarga historico de los equipos involucrados
  4. Calcula value bets de cada partido (simulacion Monte Carlo)
  5. Guarda nuevas predicciones en value_bets.csv

Uso:
    python pipeline.py              # corre todos los pasos
    python pipeline.py --solo-wl    # solo actualiza W/L (paso 1)
    python pipeline.py --horas 48   # ampliar ventana de busqueda
    python pipeline.py --force      # re-descarga odds aunque esten en cache
"""

import builtins
import csv
import re
import sys
import time
import importlib.util
from datetime import datetime, timezone, timedelta
from pathlib import Path

# -- Setup ----------------------------------------------------------------------
BASE    = Path(r'C:\Users\Matt\Apuestas Deportivas')
SCRIPTS = BASE / 'scripts'
sys.path.insert(0, str(SCRIPTS))

# Carga dinamica de modulos locales (evita conflictos con el bloque __main__)
def _load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f'{name}.py')
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_ap = _load('analizar_partido')
_fo = _load('fetch_odds')
_pp = _load('preparar_partido')
_cal = _load('modelo_v3_calibrado')
_inj = _load('fetch_injuries')
_lin = _load('fetch_lineups')
_ref = _load('modelo_v3_ref')

from modelo_v3 import (load_csv as load_hist, compute_match_params,
                        run_simulation)

VB_CSV       = BASE / 'data/apuestas/value_bets.csv'
VB_CAL_CSV   = BASE / 'data/apuestas/value_bets_calibrado.csv'
VB_FIL_CSV   = BASE / 'data/apuestas/value_bets_filtrados.csv'
VB_V33_CSV   = BASE / 'data/apuestas/value_bets_v33ref.csv'
PRON_CAL_CSV = BASE / 'data/apuestas/pronosticos_calibrado.csv'
PRON_V33_CSV = BASE / 'data/apuestas/pronosticos_v33ref.csv'

# Alpha del ajuste por árbitro en v3.3-ref (intensidad del multiplicador).
# 0=desactivar, 0.5=half-effect, 1.0=full. Configurable por env var.
import os
V33_REF_ALPHA = float(os.environ.get('V33_REF_ALPHA', '0.5'))
N_SIM  = 100_000   # menos iteraciones que analizar_partido.py para velocidad

# Liga API Football id → (nombre, solo_equipos_en_db)
#   solo_equipos_en_db=False  → analizar TODOS los partidos de la liga
#   solo_equipos_en_db=True   → analizar solo si al menos un equipo esta en nuestra DB
#
# La temporada se detecta automaticamente con _season_for()
LIGAS = {
    # ── Ligas principales ─────────────────────────────────────────────────────
    128: ('Liga Profesional',    False),
    140: ('La Liga',             False),
     39: ('Premier League',      False),
     78: ('Bundesliga',          False),
     71: ('Brasileirao Serie A', False),
    135: ('Serie A',             False),
    # ── Copas europeas ────────────────────────────────────────────────────────
      2: ('Champions League',    True),
      3: ('Europa League',       True),
    848: ('Conference League',   True),
    # ── Copas domesticas ─────────────────────────────────────────────────────
    143: ('Copa del Rey',        True),
     45: ('FA Cup',              True),
     81: ('DFB Pokal',           True),
    136: ('Coppa Italia',        True),
    # ── Copas sudamericanas ───────────────────────────────────────────────────
     13: ('Copa Libertadores',   True),
     11: ('Copa Sudamericana',   True),
    130: ('Copa Argentina',      True),
    # ── Ligas exoticas (test: mercados menos eficientes) ─────────────────────
    113: ('Allsvenskan',         False),  # Suecia, temporada Abr-Nov (calendario)
    141: ('LaLiga 2',            False),  # 2a division Espana, Ago-May
    203: ('Super Lig',           False),  # Turquia, Ago-May
}

# Ligas europeas: temporada = año en que empieza (Aug-Jul)
# Resto (Argentina, Brasil, Libertadores, Allsvenskan...): temporada = año calendario
_EUROPEAN_LEAGUES = {140, 39, 61, 78, 135, 2, 3, 848, 45, 65, 81, 136, 143, 141, 203}

def _season_for(liga_id: int) -> int:
    from datetime import date
    today = date.today()
    if liga_id in _EUROPEAN_LEAGUES:
        return today.year if today.month >= 7 else today.year - 1
    return today.year


def _liga_nombre(liga_id: int) -> str:
    return LIGAS[liga_id][0] if liga_id in LIGAS else f'Liga {liga_id}'


def _liga_solo_db(liga_id: int) -> bool:
    return LIGAS[liga_id][1] if liga_id in LIGAS else True


# -----------------------------------------------------------------------------
# Paso 0 — Actualizar historico con partidos recien terminados
# -----------------------------------------------------------------------------

FINISHED_STATUSES = {'FT', 'AET', 'PEN'}
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


def actualizar_historico(dias=7):
    """
    Paso 0: Descarga partidos terminados de los ultimos N dias para todas las
    ligas en LIGAS. Agrega solo los que no estan en el historico.
    Limita a 30 fixtures nuevos por liga para no bloquear el pipeline.
    """
    from_dt  = (datetime.now(timezone.utc) - timedelta(days=dias)).strftime('%Y-%m-%d')
    to_dt    = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    MAX_POR_LIGA = 30

    equipos_by_id, _  = _pp.load_equipos()
    ligas_by_id, _    = _pp.load_ligas()
    existing_fids     = _pp.load_csv_fixture_ids()

    sv       = _pp.stat_value
    nuevos   = 0
    batch    = []

    for liga_id, liga_nombre in LIGAS.items():
        season = _season_for(liga_id)
        try:
            resp = _pp.api_get('fixtures', {
                'league': liga_id,
                'season': season,
                'from':   from_dt,
                'to':     to_dt,
            })
            time.sleep(0.3)
        except Exception as e:
            print(f"  [hist0] Error {liga_nombre}: {e}")
            continue

        pendientes = [
            f for f in resp
            if f['fixture']['status']['short'] in FINISHED_STATUSES
            and f['fixture']['id'] not in existing_fids
        ]

        if not pendientes:
            print(f"  [hist0] {liga_nombre}: al dia")
            continue

        print(f"  [hist0] {liga_nombre}: {len(pendientes)} partido(s) nuevos "
              f"(ultimos {dias} dias)", end='', flush=True)

        procesados = 0
        for fix in pendientes[:MAX_POR_LIGA]:
            fid     = fix['fixture']['id']
            home_id = fix['teams']['home']['id']
            away_id = fix['teams']['away']['id']

            # Registrar equipos desconocidos
            for tid, tname in [(home_id, fix['teams']['home']['name']),
                               (away_id, fix['teams']['away']['name'])]:
                if tid not in equipos_by_id:
                    equipos_by_id[tid] = {
                        'id': tid, 'nombre': tname,
                        'pais': '?', 'liga_id_principal': liga_id,
                    }
                    _pp.save_equipos(equipos_by_id)

            # Registrar liga si no existe
            if liga_id not in ligas_by_id:
                ligas_by_id[liga_id] = {
                    'id': liga_id, 'nombre': liga_nombre, 'pais': '?'
                }
                _pp.save_ligas(ligas_by_id)

            try:
                stats = _pp.api_get('fixtures/statistics', {'fixture': fid})
                time.sleep(0.3)
            except Exception:
                continue

            if len(stats) < 2:
                continue

            s_home = next((s['statistics'] for s in stats
                           if s['team']['id'] == home_id), [])
            s_away = next((s['statistics'] for s in stats
                           if s['team']['id'] == away_id), [])

            batch.append({
                'fixture_id':           fid,
                'fecha':                fix['fixture']['date'][:10],
                'liga_id':              liga_id,
                'equipo_local_id':      home_id,
                'equipo_visitante_id':  away_id,
                'goles_local':          fix['goals']['home'] or 0,
                'goles_visitante':      fix['goals']['away'] or 0,
                'tiros_local':          sv(s_home, 'Total Shots'),
                'tiros_visitante':      sv(s_away, 'Total Shots'),
                'tiros_arco_local':     sv(s_home, 'Shots on Goal'),
                'tiros_arco_visitante': sv(s_away, 'Shots on Goal'),
                'corners_local':        sv(s_home, 'Corner Kicks'),
                'corners_visitante':    sv(s_away, 'Corner Kicks'),
                'posesion_local':       sv(s_home, 'Ball Possession'),
                'posesion_visitante':   sv(s_away, 'Ball Possession'),
                'tarjetas_local':       (sv(s_home, 'Yellow Cards') +
                                         sv(s_home, 'Red Cards')),
                'tarjetas_visitante':   (sv(s_away, 'Yellow Cards') +
                                         sv(s_away, 'Red Cards')),
            })
            existing_fids.add(fid)
            procesados += 1
            nuevos    += 1

        print(f"  -> {procesados} descargados")

    if batch:
        _pp.append_to_csv(batch)

    print(f"  [hist0] Total nuevos en historico: {nuevos}")


# -----------------------------------------------------------------------------
# Paso 1 — Actualizar W/L en value_bets.csv
# -----------------------------------------------------------------------------

def _get_fixture_stats(fixture_id):
    """
    Devuelve el dict de estadisticas del partido.
    Busca primero en el CSV historico; si no esta y el partido ya termino,
    lo descarga de la API y lo almacena en el historico.
    Retorna None si el partido aun no termino o falla el fetch.
    """
    hist_rows = load_hist()
    match = next((r for r in hist_rows if int(r['fixture_id']) == fixture_id), None)
    if match:
        return match

    print(f"    [wl] fixture={fixture_id} no en historico — consultando API...")
    try:
        resp = _pp.api_get('fixtures', {'id': fixture_id})
        time.sleep(0.3)
    except Exception as e:
        print(f"    [wl] Error API: {e}")
        return None

    if not resp:
        return None

    fix    = resp[0]
    status = fix['fixture']['status']['short']
    if status not in ('FT', 'AET', 'PEN'):
        return None   # No terminado

    home_id = fix['teams']['home']['id']
    away_id = fix['teams']['away']['id']

    try:
        stats = _pp.api_get('fixtures/statistics', {'fixture': fixture_id})
        time.sleep(0.3)
    except Exception:
        return None

    if len(stats) < 2:
        return None

    s_home = next((s['statistics'] for s in stats if s['team']['id'] == home_id), [])
    s_away = next((s['statistics'] for s in stats if s['team']['id'] != home_id),  [])

    sv = _pp.stat_value
    row = {
        'fixture_id':            fixture_id,
        'fecha':                 fix['fixture']['date'][:10],
        'liga_id':               fix['league']['id'],
        'equipo_local_id':       home_id,
        'equipo_visitante_id':   away_id,
        'goles_local':           fix['goals']['home'] or 0,
        'goles_visitante':       fix['goals']['away'] or 0,
        'tiros_local':           sv(s_home, 'Total Shots'),
        'tiros_visitante':       sv(s_away, 'Total Shots'),
        'tiros_arco_local':      sv(s_home, 'Shots on Goal'),
        'tiros_arco_visitante':  sv(s_away, 'Shots on Goal'),
        'corners_local':         sv(s_home, 'Corner Kicks'),
        'corners_visitante':     sv(s_away, 'Corner Kicks'),
        'posesion_local':        sv(s_home, 'Ball Possession'),
        'posesion_visitante':    sv(s_away, 'Ball Possession'),
        'tarjetas_local':        sv(s_home, 'Yellow Cards') + sv(s_home, 'Red Cards'),
        'tarjetas_visitante':    sv(s_away, 'Yellow Cards') + sv(s_away, 'Red Cards'),
    }
    _pp.append_to_csv([row])
    print(f"    [wl] Guardado en historico: {row['goles_local']}-{row['goles_visitante']}  "
          f"corners {row['corners_local']}/{row['corners_visitante']}  "
          f"tiros {row['tiros_local']}/{row['tiros_visitante']}")
    return row


def _resolver_resultado(mercado, lado, partido, stats):
    """
    Determina 'W' o 'L' para una apuesta dadas las estadisticas reales.
    Retorna None si no puede determinarse.
    """
    es_over = lado.startswith('Over') or lado.startswith('Si')

    partes = partido.split(' vs ')
    local  = partes[0].strip() if len(partes) > 1 else ''
    visita = partes[1].strip() if len(partes) > 1 else ''

    gl = int(stats.get('goles_local',          0))
    gv = int(stats.get('goles_visitante',       0))
    tl = int(stats.get('tiros_local',           0))
    tv = int(stats.get('tiros_visitante',       0))
    cl = int(stats.get('corners_local',         0))
    cv = int(stats.get('corners_visitante',     0))
    al = int(stats.get('tiros_arco_local',      0))
    av = int(stats.get('tiros_arco_visitante',  0))
    yl = int(stats.get('tarjetas_local',        0))
    yv = int(stats.get('tarjetas_visitante',    0))

    # 1X2
    if mercado.startswith('1X2'):
        if 'Empate' in mercado:
            ganó = gl == gv
        elif local in mercado:
            ganó = gl > gv
        elif visita in mercado:
            ganó = gv > gl
        else:
            return None
        return 'W' if ganó else 'L'

    # Doble oportunidad
    if mercado.startswith('DC'):
        if '(1X)' in mercado:
            ganó = gl >= gv   # local gana o empate
        elif '(12)' in mercado:
            ganó = gl != gv   # local o visita gana (no empate)
        elif '(X2)' in mercado:
            ganó = gv >= gl   # visita gana o empate
        else:
            return None
        return 'W' if ganó else 'L'

    # BTTS
    if 'BTTS' in mercado:
        btts = gl > 0 and gv > 0
        return 'W' if (btts == es_over) else 'L'

    # Extraer threshold del mercado (ej. "O/U 8.5" → 8.5)
    m = re.search(r'O/U\s+(\d+\.?\d*)', mercado)
    if not m:
        return None
    thr = float(m.group(1))

    # Identificar estadistica
    stat = None
    m_low = mercado.lower()

    if 'goles tot.'   in m_low:                      stat = gl + gv
    elif 'goles'      in m_low and local  in mercado: stat = gl
    elif 'goles'      in m_low and visita in mercado: stat = gv
    elif 'tiros tot.' in m_low:                       stat = tl + tv
    elif 'tiros'      in m_low and local  in mercado: stat = tl
    elif 'tiros'      in m_low and visita in mercado: stat = tv
    elif 'arco tot.'  in m_low:                       stat = al + av
    elif 'arco'       in m_low and local  in mercado: stat = al
    elif 'arco'       in m_low and visita in mercado: stat = av
    elif 'corners tot.' in m_low:                     stat = cl + cv
    elif 'corners'    in m_low and local  in mercado: stat = cl
    elif 'corners'    in m_low and visita in mercado: stat = cv
    elif 'tarjetas tot.' in m_low:                   stat = yl + yv
    elif 'tarjetas'  in m_low and local  in mercado: stat = yl
    elif 'tarjetas'  in m_low and visita in mercado: stat = yv

    if stat is None:
        return None

    return 'W' if (stat > thr) == es_over else 'L'


def actualizar_resultados(csv_path=None, label='value_bets.csv'):
    """Paso 1: Completa la columna 'resultado' en un CSV de value_bets."""
    vb_csv = Path(csv_path) if csv_path else VB_CSV
    if not vb_csv.exists():
        print(f"  {label} no encontrado — nada que actualizar")
        return

    with open(vb_csv, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    pendientes = [r for r in rows if not r.get('resultado', '').strip()]
    if not pendientes:
        print(f"  [{label}] Todas las apuestas ya tienen resultado")
        return

    print(f"  [{label}] {len(pendientes)} apuesta(s) pendientes de resultado")

    stats_cache = {}
    actualizadas = 0

    for row in rows:
        if row.get('resultado', '').strip():
            continue
        fid_str = str(row.get('fixture_id', '')).strip()
        if not fid_str.isdigit():
            continue
        fid = int(fid_str)

        if fid not in stats_cache:
            stats_cache[fid] = _get_fixture_stats(fid)

        stats = stats_cache[fid]
        if stats is None:
            continue   # Partido no terminado o sin datos

        res = _resolver_resultado(row['mercado'], row['lado'], row['partido'], stats)
        if res:
            row['resultado'] = res
            actualizadas += 1

    fieldnames = list(rows[0].keys()) if rows else []
    with open(vb_csv, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"  [{label}] -> {actualizadas} resultado(s) completados")


PRON_CSV = BASE / 'data/apuestas/pronosticos.csv'


def actualizar_resultados_pronosticos(csv_path=None, label='pron'):
    """Completa la columna 'resultado' en un CSV de pronósticos."""
    pron_csv = Path(csv_path) if csv_path else PRON_CSV
    if not pron_csv.exists():
        print(f"  {pron_csv.name} no encontrado — nada que actualizar")
        return

    with open(pron_csv, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    pendientes = [r for r in rows if not r.get('resultado', '').strip()]
    if not pendientes:
        print(f"  [{label}] Todos los pronosticos ya tienen resultado")
        return

    # Agrupar por fixture_id para no repetir consultas
    fids_pendientes = set()
    for r in pendientes:
        fid_str = str(r.get('fixture_id', '')).strip()
        if fid_str.isdigit():
            fids_pendientes.add(int(fid_str))

    print(f"  [{label}] {len(pendientes)} pronostico(s) pendientes en {len(fids_pendientes)} fixture(s)")

    stats_cache = {}
    actualizadas = 0
    no_resueltos = 0

    for fid in fids_pendientes:
        if fid not in stats_cache:
            stats_cache[fid] = _get_fixture_stats(fid)

    for row in rows:
        if row.get('resultado', '').strip():
            continue
        fid_str = str(row.get('fixture_id', '')).strip()
        if not fid_str.isdigit():
            continue
        fid = int(fid_str)

        stats = stats_cache.get(fid)
        if stats is None:
            no_resueltos += 1
            continue

        res = _resolver_resultado(row['mercado'], row['lado'], row['partido'], stats)
        if res:
            row['resultado'] = res
            actualizadas += 1
        else:
            no_resueltos += 1

    fieldnames = list(rows[0].keys()) if rows else []
    with open(pron_csv, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"  [{label}] -> {actualizadas} resultado(s) completados, {no_resueltos} sin resolver")


# -----------------------------------------------------------------------------
# Paso 2 — Buscar partidos proximos
# -----------------------------------------------------------------------------

def get_upcoming_matches(horas=48):
    """Paso 2: Retorna partidos en las proximas N horas en las ligas configuradas."""
    now    = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=horas)
    found  = []

    from_str = now.strftime('%Y-%m-%d')
    to_str   = cutoff.strftime('%Y-%m-%d')

    for liga_id, liga_nombre in LIGAS.items():
        season       = _season_for(liga_id)
        oddsapi_slug = _fo.get_league_slug(liga_id) or ''

        print(f"  Consultando {liga_nombre} (season={season}, "
              f"from={from_str} to={to_str})...")
        try:
            resp = _pp.api_get('fixtures', {
                'league':  liga_id,
                'season':  season,
                'from':    from_str,
                'to':      to_str,
            })
            time.sleep(0.3)
        except Exception as e:
            print(f"  Error {liga_nombre}: {e}")
            continue

        print(f"    -> {len(resp)} fixture(s) en el rango de fechas")

        for fix in resp:
            try:
                fecha = datetime.fromisoformat(
                    fix['fixture']['date'].replace('Z', '+00:00')
                )
            except ValueError:
                continue

            # Solo partidos que NO hayan empezado (whitelist, no blacklist — evita
            # aceptar in-play como 1H/HT/2H/ET/BT/P/SUSP/INT con cuotas ajustadas
            # al resultado parcial)
            status = fix['fixture']['status']['short']
            if status not in ('NS', 'TBD'):
                continue

            # Guardia extra: KO debe ser futuro (un NS con KO pasado es estado raro)
            if fecha <= datetime.now(timezone.utc):
                continue

            found.append({
                'fixture_id':   fix['fixture']['id'],
                'fecha':        fix['fixture']['date'][:10],
                'hora':         fecha.astimezone().strftime('%H:%M'),
                'liga_id':      liga_id,
                'liga_nombre':  liga_nombre,
                'oddsapi_slug': oddsapi_slug,
                'home_id':      fix['teams']['home']['id'],
                'home_name':    fix['teams']['home']['name'],
                'away_id':      fix['teams']['away']['id'],
                'away_name':    fix['teams']['away']['name'],
                'referee':      (fix['fixture'].get('referee') or '').strip(),
                'ko_dt':        fecha,   # datetime aware UTC
            })

    return found


# -----------------------------------------------------------------------------
# Contexto pre-partido (referee, injuries, lineups)
# -----------------------------------------------------------------------------

def fetch_contexto(fix, force=False):
    """
    Baja referee + injuries + lineups para un fixture y deja todo cacheado
    en data/contexto/. No falla si la API responde vacío.

    Política de timing:
      - Injuries: TTL 3h (manejado por fetch_injuries internamente)
      - Lineups:  TTL 5min si pending, 24h si confirmado
      - Lineups solo se intenta si el KO está a < 4h (antes nunca está publicado)
    """
    fid    = fix['fixture_id']
    home   = fix['home_name']
    away   = fix['away_name']
    ref    = fix.get('referee') or ''
    ko_dt  = fix.get('ko_dt')

    print(f"  [ctx] referee : {ref or '(sin asignar)'}")

    # Injuries: siempre que se procese el partido (cache TTL 3h)
    try:
        _, inj_res = _inj.get_injuries(fixture_id=fid, force=force)
        print(f"  [ctx] injuries: home={inj_res['home_total']} "
              f"({inj_res['home_missing']} missing, {inj_res['home_questionable']} ?)  "
              f"away={inj_res['away_total']} "
              f"({inj_res['away_missing']} missing, {inj_res['away_questionable']} ?)")
    except Exception as e:
        print(f"  [ctx] injuries error: {e}")

    # Lineups: solo intentar si KO está cerca (no malgastar requests)
    horas_al_ko = None
    if ko_dt:
        delta = ko_dt - datetime.now(timezone.utc)
        horas_al_ko = delta.total_seconds() / 3600

    if horas_al_ko is not None and horas_al_ko > 4:
        print(f"  [ctx] lineups : skip (KO en {horas_al_ko:.1f}h, lineups suelen "
              f"publicarse a 30-90min)")
    else:
        try:
            _, lin_res = _lin.get_lineups(fid, force=force)
            if lin_res['confirmed']:
                print(f"  [ctx] lineups : CONFIRMADO  "
                      f"home={lin_res['home_formation']} ({lin_res['home_coach']})  "
                      f"away={lin_res['away_formation']} ({lin_res['away_coach']})")
            else:
                print(f"  [ctx] lineups : pending  "
                      f"(home XI={lin_res['home_xi_size']}, away XI={lin_res['away_xi_size']})")
        except Exception as e:
            print(f"  [ctx] lineups error: {e}")


# -----------------------------------------------------------------------------
# Pasos 3–5 — Procesar un partido
# -----------------------------------------------------------------------------

def _register_and_fetch(team_name, existing_ids, equipos_by_id, equipos_by_name,
                        ligas_by_id, known_id=None):
    """
    Registra el equipo si no existe y trae sus ultimos partidos.
    Si known_id se provee (ID de API Football del fixture), se usa directamente
    sin busqueda por nombre — evita ambiguedad con nombres como
    'Central Cordoba de Santiago' vs 'Central Cordoba'.
    Usa input() automatico (elige indice 0) para no bloquear el pipeline.
    """
    _orig_input = builtins.input

    def _auto(prompt=''):
        print(f"{prompt}0  [auto-pipeline]")
        return '0'

    builtins.input = _auto
    try:
        if known_id is not None and known_id in equipos_by_id:
            # ID ya conocido y en DB — usar directamente, sin busqueda por nombre
            team_id = known_id
            liga    = int(equipos_by_id[team_id]['liga_id_principal'])
            leagues = _pp._leagues_for_team(team_id, liga)
            print(f"  [{team_name}] ID={team_id} (fixture API)  "
                  f"liga={equipos_by_id[team_id].get('liga_id_principal')}")
        else:
            # Fallback: busqueda por nombre (para cuando no tenemos el ID)
            team_id, leagues = _pp.find_or_register_team(
                team_name, equipos_by_id, equipos_by_name, ligas_by_id
            )
        new_rows = _pp.fetch_new_matches(
            team_id, team_name, leagues,
            existing_ids, equipos_by_id, ligas_by_id
        )
        return new_rows, team_id
    except Exception as e:
        print(f"  [hist] Error {team_name}: {e}")
        return [], None
    finally:
        builtins.input = _orig_input


def procesar_partido(fix, force_odds=False):
    """Pasos 3–5 para un unico partido."""
    home  = fix['home_name']
    away  = fix['away_name']
    fid   = fix['fixture_id']
    liga  = fix['liga_nombre']
    slug  = fix['oddsapi_slug']

    print(f"\n  {home} vs {away}  [{liga}]  {fix['fecha']} {fix['hora']}")

    # -- 3. Historico ---------------------------------------------------------
    equipos_by_id, equipos_by_name = _pp.load_equipos()
    ligas_by_id, _                 = _pp.load_ligas()
    existing_ids                   = _pp.load_csv_fixture_ids()

    all_new = []
    # Usar los IDs que ya vienen del fixture de API Football — evita ambiguedad
    # de nombres (ej. 'Central Cordoba de Santiago' vs 'Central Cordoba')
    api_home_id = fix.get('home_id')
    api_away_id = fix.get('away_id')
    home_id = away_id = None
    for team, attr, kid in [(home, 'home_id', api_home_id),
                             (away, 'away_id', api_away_id)]:
        rows, team_id = _register_and_fetch(team, existing_ids, equipos_by_id,
                                            equipos_by_name, ligas_by_id,
                                            known_id=kid)
        all_new.extend(rows)
        if attr == 'home_id':
            home_id = team_id
        else:
            away_id = team_id

    if all_new:
        _pp.append_to_csv(all_new)
        print(f"  [hist] {len(all_new)} partido(s) nuevos agregados")

    # -- 4. Odds ---------------------------------------------------------------
    odds = {}

    # Fuente 1: API Football (goles, 1X2, BTTS, handicaps)
    try:
        odds, _ = _fo.get_odds(fid, force=force_odds)
        print(f"  [odds] API Football: {len(odds)} claves")
    except Exception as e:
        print(f"  [odds] API Football error: {e}")

    # Fuente 2: odds-api.io (corners, tiros, arco — por equipo y totales)
    try:
        ev_id, h_api, a_api = _fo.find_event_oddsapi(home, away, slug,
                                                      home_id=home_id,
                                                      away_id=away_id)
        if ev_id:
            oa, oa_res = _fo.get_odds_oddsapi(ev_id, force=force_odds)
            nuevas = sum(1 for k in oa if k not in odds)
            odds.update({k: v for k, v in oa.items() if k not in odds})
            mkts = len(oa_res.get('mapeados', []))
            print(f"  [odds] odds-api.io ({h_api} vs {a_api}): "
                  f"{mkts} mercados  +{nuevas} claves")
        else:
            print(f"  [odds] odds-api.io: evento no encontrado en '{slug}'")
    except Exception as e:
        print(f"  [odds] odds-api.io error: {e}")

    if not odds:
        print("  Sin odds disponibles — partido saltado")
        return

    # -- 4b. Contexto pre-partido (referee, injuries, lineups) ----------------
    # Se cachea en data/contexto/ — el modelo aún no lo usa, pero queda
    # disponible para análisis posterior y para integrarlo en una próxima fase.
    try:
        fetch_contexto(fix, force=force_odds)
    except Exception as e:
        print(f"  [ctx] error inesperado: {e}")

    # -- 5. Simulacion + value bets --------------------------------------------
    try:
        hist   = load_hist()
        sim_home = home_id if home_id is not None else home
        sim_away = away_id if away_id is not None else away
        params = compute_match_params(sim_home, sim_away, hist, liga)
        sim    = run_simulation(params, N_SIM)
        sim['team_local']  = home
        sim['team_visita'] = away

        # arco ya se simula dentro de run_simulation como Binomial(tiros, precision)
        sim['arco_params'] = {
            'prec_local': params['prec_local'],
            'prec_vis':   params['prec_vis'],
        }

        probs     = _ap.compute_all_probs(sim)
        probs_cal = _cal.calibrar_probs(probs)
        vbs       = _ap.analizar_value_bets(probs, odds, home, away)
        vbs_cal   = _ap.analizar_value_bets(probs_cal, odds, home, away)
        vbs_fil   = _ap.filtrar_estrategia(vbs, home, away)

        # ── v3.3-ref shadow: re-simular con factor del árbitro en tarjetas ──
        # Solo afecta mu_tarjetas_*; las demás distribuciones quedan iguales.
        # Si no hay árbitro asignado en el fixture, queda igual que v3.2 raw.
        vbs_v33 = []
        probs_v33 = None
        try:
            params_ref = dict(params)   # shallow copy — alteramos solo mus
            _ref.apply_referee_factor(params_ref,
                                       referee=fix.get('referee'),
                                       alpha=V33_REF_ALPHA,
                                       verbose=False)
            sim_v33 = run_simulation(params_ref, N_SIM)
            sim_v33['team_local']  = home
            sim_v33['team_visita'] = away
            sim_v33['arco_params'] = sim['arco_params']
            probs_v33 = _ap.compute_all_probs(sim_v33)
            vbs_v33   = _ap.analizar_value_bets(probs_v33, odds, home, away)
        except Exception as e:
            print(f"  [v3.3-ref] error: {e}")

    except Exception as e:
        print(f"  [sim] Error: {e}")
        return

    # -- Modelo raw (v3.2) -----------------------------------------------------
    if vbs:
        print(f"\n  VALUE BETS RAW ({len(vbs)}):")
        hdr = f"  {'Mercado':<38} {'Lado':<10} {'Odds':>5}  {'Edge':>7}  {'EV%':>7}"
        print(hdr)
        print(f"  {'-'*70}")
        for vb in vbs:
            print(f"  {vb['market']:<38} {vb['lado']:<10} "
                  f"{vb['odds']:>5.2f}  {vb['edge']:>+6.1%}  {vb['EV_%']:>+6.1f}%")
        _ap.guardar_value_bets(vbs, home, away, liga, fid, metodo='v3.2')
    else:
        print("  Sin value bets detectadas (raw)")

    try:
        _ap.guardar_pronosticos(probs, odds, home, away, liga, fid, metodo='v3.2')
    except Exception as e:
        print(f"  [pronosticos] Error al guardar: {e}")

    # -- Modelo calibrado (v3.2-cal) -------------------------------------------
    if vbs_cal:
        print(f"\n  VALUE BETS CALIBRADO ({len(vbs_cal)}):")
        for vb in vbs_cal:
            print(f"  {vb['market']:<38} {vb['lado']:<10} "
                  f"{vb['odds']:>5.2f}  {vb['edge']:>+6.1%}  {vb['EV_%']:>+6.1f}%")
        _ap.guardar_value_bets(vbs_cal, home, away, liga, fid,
                               metodo='v3.2-cal', csv_path=VB_CAL_CSV)
    else:
        print("  Sin value bets detectadas (calibrado)")

    try:
        _ap.guardar_pronosticos(probs_cal, odds, home, away, liga, fid,
                                 metodo='v3.2-cal', csv_path=PRON_CAL_CSV)
    except Exception as e:
        print(f"  [pronosticos-cal] Error al guardar: {e}")

    # -- Estrategia filtrada (v3.2-fil) ----------------------------------------
    if vbs_fil:
        print(f"\n  VALUE BETS FILTRADAS ({len(vbs_fil)}):")
        for vb in vbs_fil:
            print(f"  {vb['market']:<38} {vb['lado']:<10} "
                  f"{vb['odds']:>5.2f}  {vb['edge']:>+6.1%}  {vb['EV_%']:>+6.1f}%")
        _ap.guardar_value_bets(vbs_fil, home, away, liga, fid,
                               metodo='v3.2-fil', csv_path=VB_FIL_CSV)
    else:
        print("  Sin value bets detectadas (filtrado)")

    # -- Modelo v3.3-ref (shadow — multiplicador por árbitro en tarjetas) -----
    if vbs_v33:
        ref_tag = fix.get('referee') or '(sin árbitro)'
        print(f"\n  VALUE BETS v3.3-REF ({len(vbs_v33)})  α={V33_REF_ALPHA}  ref={ref_tag}:")
        for vb in vbs_v33:
            print(f"  {vb['market']:<38} {vb['lado']:<10} "
                  f"{vb['odds']:>5.2f}  {vb['edge']:>+6.1%}  {vb['EV_%']:>+6.1f}%")
        _ap.guardar_value_bets(vbs_v33, home, away, liga, fid,
                               metodo='v3.3-ref', csv_path=VB_V33_CSV)
    else:
        print("  Sin value bets detectadas (v3.3-ref)")

    if probs_v33 is not None:
        try:
            _ap.guardar_pronosticos(probs_v33, odds, home, away, liga, fid,
                                     metodo='v3.3-ref', csv_path=PRON_V33_CSV)
        except Exception as e:
            print(f"  [pronosticos-v33] Error al guardar: {e}")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    args       = sys.argv[1:]
    solo_wl    = '--solo-wl'  in args
    force_odds = '--force'    in args
    horas      = 24
    dias_hist  = 7
    if '--horas' in args:
        idx = args.index('--horas')
        if idx + 1 < len(args):
            horas = int(args[idx + 1])
    if '--dias-hist' in args:
        idx = args.index('--dias-hist')
        if idx + 1 < len(args):
            dias_hist = int(args[idx + 1])

    sep = '=' * 60
    print(f"\n{sep}")
    print(f"  PIPELINE AUTO VALUE BETS")
    print(f"{sep}")

    # -- Paso 0: Actualizar historico con partidos recientes ------------------
    print(f"\n{'-'*60}")
    print(f"  [0/3] Actualizando historico (ultimos {dias_hist} dias)")
    print(f"{'-'*60}")
    actualizar_historico(dias=dias_hist)

    # -- Paso 1: W/L ----------------------------------------------------------
    print(f"\n{'-'*60}")
    print(f"  [1/3] Actualizando resultados W/L")
    print(f"{'-'*60}")
    actualizar_resultados(label='raw')
    actualizar_resultados(csv_path=VB_CAL_CSV, label='cal')
    actualizar_resultados(csv_path=VB_FIL_CSV, label='fil')
    actualizar_resultados(csv_path=VB_V33_CSV, label='v33ref')
    actualizar_resultados_pronosticos(label='pron raw')
    actualizar_resultados_pronosticos(csv_path=PRON_CAL_CSV, label='pron cal')
    actualizar_resultados_pronosticos(csv_path=PRON_V33_CSV, label='pron v33')

    if solo_wl:
        print("\nModo --solo-wl completado.\n")
        return

    # -- Paso 2: Partidos proximos ---------------------------------------------
    print(f"\n{'-'*60}")
    print(f"  [2/3] Partidos proximas {horas}h")
    print(f"{'-'*60}")
    matches = get_upcoming_matches(horas)

    if not matches:
        print(f"  No hay partidos en las proximas {horas} horas.")
        print(f"\n{sep}\n")
        return

    print(f"\n  {len(matches)} partido(s) encontrados:")
    for m in matches:
        print(f"    {m['fecha']} {m['hora']}  "
              f"{m['home_name']} vs {m['away_name']}  [{m['liga_nombre']}]")

    # -- Pasos 3–5: Procesar ---------------------------------------------------
    print(f"\n{'-'*60}")
    print(f"  [3/3] Procesando partidos")
    print(f"{'-'*60}")
    for fix in matches:
        try:
            procesar_partido(fix, force_odds=force_odds)
        except Exception as e:
            print(f"  ERROR {fix['home_name']} vs {fix['away_name']}: {e}")

    # -- Fallback: snapshot de closing line para bets con KO cercano ----------
    print(f"\n{'-'*60}")
    print(f"  [fallback] Snapshot cuotas de cierre (CLV)")
    print(f"{'-'*60}")
    try:
        _sc = _load('snapshot_cierre')
        _sc.run(window_min=60, verbose=False)
    except Exception as e:
        print(f"  [snapshot] error: {e}")

    print(f"\n{sep}")
    print(f"  Pipeline completado")
    print(f"{sep}\n")


if __name__ == '__main__':
    main()
