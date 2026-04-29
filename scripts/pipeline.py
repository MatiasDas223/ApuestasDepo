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
_shr = _load('modelo_v3_shrink')

from modelo_v3 import (load_csv as load_hist, compute_match_params,
                        run_simulation)

VB_CSV       = BASE / 'data/apuestas/value_bets.csv'
VB_CAL_CSV   = BASE / 'data/apuestas/value_bets_calibrado.csv'
VB_FIL_CSV   = BASE / 'data/apuestas/value_bets_filtrados.csv'
VB_V33_CSV   = BASE / 'data/apuestas/value_bets_v33ref.csv'
VB_V34_CSV   = BASE / 'data/apuestas/value_bets_v34shrink.csv'
VB_V35_CSV   = BASE / 'data/apuestas/value_bets_v35dedup.csv'
VB_V36_CSV   = BASE / 'data/apuestas/value_bets_v36.csv'
PRON_CAL_CSV = BASE / 'data/apuestas/pronosticos_calibrado.csv'
PRON_V33_CSV = BASE / 'data/apuestas/pronosticos_v33ref.csv'
PRON_V34_CSV = BASE / 'data/apuestas/pronosticos_v34shrink.csv'

# Alpha del ajuste por árbitro en v3.3-ref (intensidad del multiplicador).
# 0=desactivar, 0.5=half-effect, 1.0=full. Configurable por env var.
import os
V33_REF_ALPHA = float(os.environ.get('V33_REF_ALPHA', '0.5'))

# Alpha del shrinkage de ratings atk/def en v3.4-shrink (rating -> 1 + alpha*(rating-1)).
# 1.0 = desactivar (igual que v3.2). 0.30 = valor validado OOS (ver
# project_regresion_media_extremos.md / scripts/fix_regresion_media.py --oos).
V34_SHRINK_ALPHA = float(os.environ.get('V34_SHRINK_ALPHA', '0.30'))

# v3.5-dedup: dedup de bets correlacionadas en escalera (O/U en varios thresholds del mismo
# variable subyacente). Dentro de un cluster (fixture, categoria, alcance) se queda con el
# de mayor edge. 1X2/DC/BTTS no deduplica (no forman escalera).
# Nota histórica: se probó Max Kelly en Goles/Tiros/Arco/Tarjetas (mejor ROI backtest
# -2.2% vs -9.3%) pero concentra 49% en cuotas <1.20 con payout 0.30u/bet → fragilidad
# inaceptable en tail calibration y drawdown. Max edge: payout 0.95u/bet, cuota mediana 1.83.

# v3.6: estrategia derivada del sweep MIN_EDGE×α (backtest 500 fixtures, 2026-04-23).
# Usa α de shrinkage específico por mercado + umbral de edge elevado para vencer el
# winner's curse. Regla por (categoria, alcance):
#   ('Goles', 'Total')     → α=0.30, edge>12%  (backtest ROI +22.7%, N=34)
#   ('Goles', 'Visitante') → α=0.00, edge>10%  (backtest ROI +20.3%, N=75)
#   ('BTTS',  'Total')     → α=0.15, edge>4%   (backtest ROI +24.5%, N=28)
# Mercados descartados: Local (siempre pierde -20/-33% ROI), 1X2/DC/Corners/Tiros/Arco/Tarjetas.
V36_RULES = [
    # (categoria, alcance, alpha_shrink, min_edge)
    ('Goles', 'Total',     0.30, 0.12),
    ('Goles', 'Visitante', 0.00, 0.10),
    ('BTTS',  'Total',     0.15, 0.04),
]
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


def _dedup_max_edge_vbs(vbs, partido):
    """
    Deduplica bets correlacionadas en escalera (O/U en varios thresholds del mismo
    variable subyacente) agrupando por (categoria, alcance). Se queda con la de mayor
    edge dentro del cluster. 1X2/DC/BTTS no deduplica (no forman escalera).
    """
    no_ladder_cats = {'1X2', 'Doble Oportunidad', 'BTTS'}
    best = {}
    pass_thru = []
    for vb in vbs:
        cat, alc = _ap._clasificar_mercado(vb['market'], partido)
        if cat in no_ladder_cats:
            pass_thru.append(vb)
            continue
        key = (cat, alc)
        if key not in best or vb['edge'] > best[key]['edge']:
            best[key] = vb
    return pass_thru + list(best.values())


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

_LIGAS_NAMES = {nombre for nombre, _ in LIGAS.values()}


def _liga_name_from_competicion(comp):
    """
    Normaliza el campo 'competicion' del CSV. value_bets guarda 'La Liga',
    pronosticos guarda "('Champions League', True)" (tupla literal).
    """
    s = (comp or '').strip()
    if s.startswith('('):
        m = re.match(r"\(\s*['\"]([^'\"]+)['\"]", s)
        if m:
            return m.group(1)
    return s


def _get_fixture_stats(fixture_id, hist_rows=None, skip_api=False):
    """
    Devuelve el dict de estadisticas del partido.
    Busca primero en el CSV historico; si no esta y el partido ya termino,
    lo descarga de la API y lo almacena en el historico.
    Retorna None si el partido aun no termino o falla el fetch.

    Si hist_rows se pasa, se usa ese índice en memoria en vez de releer el CSV.
    Cuando se baja un fixture nuevo de la API, se agrega a hist_rows (si se pasó)
    para que quede disponible en llamadas posteriores.

    Si skip_api=True, no consulta la API si el fixture no está en histórico
    (asume que paso 0 ya cubrió la ventana relevante y el partido aún no terminó).
    """
    if hist_rows is None:
        hist_rows = load_hist()
    match = next((r for r in hist_rows if int(r['fixture_id']) == fixture_id), None)
    if match:
        return match

    if skip_api:
        return None

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
    # Mantener el hist en memoria en sync con el CSV
    if hist_rows is not None:
        hist_rows.append({k: str(v) for k, v in row.items()})
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


PRON_CSV = BASE / 'data/apuestas/pronosticos.csv'


def actualizar_todos_resultados(csvs, hist_rows=None, dias_paso0=7):
    """
    Paso 1 unificado: completa 'resultado' en todos los CSVs pasados, compartiendo
    stats_cache y hist_rows.

    csvs: iterable de (path, label). Se saltan en silencio los que no existen.

    Ahorra re-consultas: el mismo fixture_id suele aparecer en varios CSVs
    (raw/cal/fil/v33/v34/v35 para value_bets; raw/cal/v33/v34 para pronosticos).
    Antes cada CSV construía su propio stats_cache y además _get_fixture_stats
    releía el historico entero por cada lookup.

    Optimización API: si el fixture no está en histórico pero (a) la liga está
    configurada en LIGAS y (b) la fecha_analisis cae dentro de los últimos
    `dias_paso0` días, asumimos que el partido aún no terminó y NO consultamos
    la API (paso 0 ya cubrió esa ventana). Para bets más viejas o de ligas no
    configuradas seguimos llamando a la API para recuperar edge cases.
    """
    if hist_rows is None:
        hist_rows = load_hist()

    # 1. Cargar todos los CSVs existentes
    csv_states = []   # [(path, label, rows)]
    for path, label in csvs:
        path = Path(path)
        if not path.exists():
            print(f"  [{label}] archivo no encontrado — saltado")
            continue
        with open(path, newline='', encoding='utf-8') as f:
            rows = list(csv.DictReader(f))
        csv_states.append((path, label, rows))

    if not csv_states:
        return hist_rows

    # 2. Unión de fixture_ids pendientes en todos los CSVs + meta por fid
    pending_fids = set()
    total_pend   = 0
    fid_meta     = {}   # fid -> {'fecha_max': date|None, 'liga': str}
    for _, _, rows in csv_states:
        for r in rows:
            if r.get('resultado', '').strip():
                continue
            fid_str = str(r.get('fixture_id', '')).strip()
            if not fid_str.isdigit():
                continue
            fid = int(fid_str)
            pending_fids.add(fid)
            total_pend += 1
            fa_str = (r.get('fecha_analisis') or '')[:10]
            try:
                fa_date = datetime.strptime(fa_str, '%Y-%m-%d').date()
            except ValueError:
                fa_date = None
            liga = _liga_name_from_competicion(r.get('competicion', ''))
            meta = fid_meta.get(fid)
            if meta is None:
                fid_meta[fid] = {'fecha_max': fa_date, 'liga': liga}
            else:
                if fa_date and (meta['fecha_max'] is None
                                or fa_date > meta['fecha_max']):
                    meta['fecha_max'] = fa_date
                if not meta['liga'] and liga:
                    meta['liga'] = liga

    if not pending_fids:
        for _, label, _ in csv_states:
            print(f"  [{label}] todo con resultado")
        return hist_rows

    print(f"  [wl] {total_pend} fila(s) pendientes en {len(pending_fids)} "
          f"fixture(s) únicos (unión de {len(csv_states)} CSVs)")

    # 3. Resolver stats una sola vez por fixture
    today = datetime.now(timezone.utc).date()
    hist_fids = {int(r['fixture_id']) for r in hist_rows}
    stats_cache = {}
    skipped_api = 0
    for fid in pending_fids:
        meta    = fid_meta.get(fid, {})
        fa_date = meta.get('fecha_max')
        liga    = meta.get('liga', '')
        skip_api = False
        if fid not in hist_fids and liga in _LIGAS_NAMES and fa_date is not None:
            if (today - fa_date).days <= dias_paso0:
                skip_api = True
                skipped_api += 1
        stats_cache[fid] = _get_fixture_stats(
            fid, hist_rows=hist_rows, skip_api=skip_api)

    if skipped_api:
        print(f"  [wl] {skipped_api} fixture(s) skipeados de API "
              f"(liga en LIGAS y bet ≤{dias_paso0}d — partido aún no terminó)")

    resueltos_fids = sum(1 for v in stats_cache.values() if v is not None)
    print(f"  [wl] {resueltos_fids}/{len(pending_fids)} fixture(s) con stats disponibles")

    # 4. Aplicar a cada CSV
    for path, label, rows in csv_states:
        actualizadas = 0
        no_resueltos = 0
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
            res = _resolver_resultado(row['mercado'], row['lado'],
                                      row['partido'], stats)
            if res:
                row['resultado'] = res
                actualizadas += 1
            else:
                no_resueltos += 1

        if actualizadas and rows:
            fieldnames = list(rows[0].keys())
            with open(path, 'w', newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                w.writerows(rows)

        print(f"  [{label}] -> {actualizadas} completados, {no_resueltos} sin resolver")

    return hist_rows


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


def procesar_partido(fix, force_odds=False,
                     equipos_by_id=None, equipos_by_name=None,
                     ligas_by_id=None, existing_ids=None, hist=None):
    """
    Pasos 3–5 para un unico partido.

    Los contextos (equipos_by_id, equipos_by_name, ligas_by_id, existing_ids, hist)
    pueden pasarse desde main() para evitar recargar los CSVs en cada fixture.
    Si alguno es None se carga internamente (back-compat).
    """
    home  = fix['home_name']
    away  = fix['away_name']
    fid   = fix['fixture_id']
    liga  = fix['liga_nombre']
    slug  = fix['oddsapi_slug']

    print(f"\n  {home} vs {away}  [{liga}]  {fix['fecha']} {fix['hora']}")

    # -- 3. Historico ---------------------------------------------------------
    if equipos_by_id is None or equipos_by_name is None:
        equipos_by_id, equipos_by_name = _pp.load_equipos()
    if ligas_by_id is None:
        ligas_by_id, _ = _pp.load_ligas()
    if existing_ids is None:
        existing_ids = _pp.load_csv_fixture_ids()

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
        # Si se agregaron partidos nuevos al CSV en el Paso 3, refrescar el hist
        # en memoria para que la simulación los considere. Si no hubo cambios,
        # reutilizar el hist pasado desde main() (evita relectura del CSV).
        if all_new or hist is None:
            hist = load_hist()
        sim_home = home_id if home_id is not None else home
        sim_away = away_id if away_id is not None else away
        params = compute_match_params(sim_home, sim_away, hist, liga)
        # Seed = fixture_id para reproducibilidad + CRN parcial entre las 3 variantes
        # del mismo fixture (v3.2, v3.3-ref, v3.4-shrink). Mercados cuyos params no
        # cambian entre variantes quedan sincronizados; los que sí cambian, divergen.
        sim    = run_simulation(params, N_SIM, seed=fid)
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
        vbs_v35   = _dedup_max_edge_vbs(vbs, f"{home} vs {away}")

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
            sim_v33 = run_simulation(params_ref, N_SIM, seed=fid)
            sim_v33['team_local']  = home
            sim_v33['team_visita'] = away
            sim_v33['arco_params'] = sim['arco_params']
            probs_v33 = _ap.compute_all_probs(sim_v33)
            vbs_v33   = _ap.analizar_value_bets(probs_v33, odds, home, away)
        except Exception as e:
            print(f"  [v3.3-ref] error: {e}")

        # ── v3.4-shrink shadow: shrinkage de ratings atk/def hacia 1.0 ──────────
        # Solo afecta lambda_local y lambda_vis (goles). Mitigación del sesgo
        # de regresión a la media validado OOS con alpha=0.30.
        vbs_v34 = []
        probs_v34 = None
        try:
            params_shr = _shr.apply_shrink_to_ratings(params, alpha=V34_SHRINK_ALPHA)
            sim_v34 = run_simulation(params_shr, N_SIM, seed=fid)
            sim_v34['team_local']  = home
            sim_v34['team_visita'] = away
            sim_v34['arco_params'] = sim['arco_params']
            probs_v34 = _ap.compute_all_probs(sim_v34)
            vbs_v34   = _ap.analizar_value_bets(probs_v34, odds, home, away)
        except Exception as e:
            print(f"  [v3.4-shrink] error: {e}")

        # ── v3.6 shadow: estrategia derivada del sweep MIN_EDGE×α ──────────────
        # Usa shrinkage + edge threshold específico por (categoria, alcance).
        # Reusa sim_v34 (α=0.30) para Total; simula α=0.15 (BTTS) y α=0.00 (Vis).
        vbs_v36 = []
        partido_str = f"{home} vs {away}"
        try:
            # Probs por alpha: agrupamos reglas por alpha para minimizar sims
            probs_by_alpha = {V34_SHRINK_ALPHA: probs_v34}  # 0.30 ya disponible
            alphas_needed = {r[2] for r in V36_RULES}
            for alpha in alphas_needed:
                if alpha in probs_by_alpha:
                    continue
                p_a = _shr.apply_shrink_to_ratings(params, alpha=alpha)
                sim_a = run_simulation(p_a, N_SIM, seed=fid)
                sim_a['team_local']  = home
                sim_a['team_visita'] = away
                sim_a['arco_params'] = sim['arco_params']
                probs_by_alpha[alpha] = _ap.compute_all_probs(sim_a)

            # Aplicar reglas: para cada (cat, alc, α, edge_min) filtrar vbs
            for cat_rule, alc_rule, alpha_rule, edge_min in V36_RULES:
                probs_r = probs_by_alpha.get(alpha_rule)
                if probs_r is None:
                    continue
                vbs_r = _ap.analizar_value_bets(probs_r, odds, home, away,
                                                min_edge=edge_min)
                for vb in vbs_r:
                    cat, alc = _ap._clasificar_mercado(vb['market'], partido_str)
                    if cat == cat_rule and alc == alc_rule:
                        vbs_v36.append(vb)
        except Exception as e:
            print(f"  [v3.6] error: {e}")

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

    # -- Modelo v3.4-shrink (shadow — shrinkage de ratings hacia 1.0) ----------
    if vbs_v34:
        mu_old = params['lambda_local'] + params['lambda_vis']
        mu_new = params_shr['lambda_local'] + params_shr['lambda_vis']
        print(f"\n  VALUE BETS v3.4-SHRINK ({len(vbs_v34)})  alpha={V34_SHRINK_ALPHA}  "
              f"mu_total: {mu_old:.2f}->{mu_new:.2f}:")
        for vb in vbs_v34:
            print(f"  {vb['market']:<38} {vb['lado']:<10} "
                  f"{vb['odds']:>5.2f}  {vb['edge']:>+6.1%}  {vb['EV_%']:>+6.1f}%")
        _ap.guardar_value_bets(vbs_v34, home, away, liga, fid,
                               metodo='v3.4-shrink', csv_path=VB_V34_CSV)
    else:
        print("  Sin value bets detectadas (v3.4-shrink)")

    if probs_v34 is not None:
        try:
            _ap.guardar_pronosticos(probs_v34, odds, home, away, liga, fid,
                                     metodo='v3.4-shrink', csv_path=PRON_V34_CSV)
        except Exception as e:
            print(f"  [pronosticos-v34] Error al guardar: {e}")

    # -- Modelo v3.5-dedup (shadow — max edge dentro de clusters correlacionados) --
    if vbs_v35:
        print(f"\n  VALUE BETS v3.5-DEDUP ({len(vbs_v35)})  "
              f"(de {len(vbs)} raw tras dedup max edge):")
        for vb in sorted(vbs_v35, key=lambda v: -v['edge']):
            print(f"  {vb['market']:<38} {vb['lado']:<10} "
                  f"{vb['odds']:>5.2f}  {vb['edge']:>+6.1%}  {vb['EV_%']:>+6.1f}%")
        _ap.guardar_value_bets(vbs_v35, home, away, liga, fid,
                               metodo='v3.5-dedup', csv_path=VB_V35_CSV)
    else:
        print("  Sin value bets detectadas (v3.5-dedup)")

    # -- Modelo v3.6 (shadow — alpha y edge_min por mercado, del sweep backtest) --
    if vbs_v36:
        print(f"\n  VALUE BETS v3.6 ({len(vbs_v36)})  "
              f"(Total α=0.30 edge>12%, Vis α=0.00 edge>10%, BTTS α=0.15 edge>4%):")
        for vb in sorted(vbs_v36, key=lambda v: -v['edge']):
            print(f"  {vb['market']:<38} {vb['lado']:<10} "
                  f"{vb['odds']:>5.2f}  {vb['edge']:>+6.1%}  {vb['EV_%']:>+6.1f}%")
        _ap.guardar_value_bets(vbs_v36, home, away, liga, fid,
                               metodo='v3.6', csv_path=VB_V36_CSV)
    else:
        print("  Sin value bets detectadas (v3.6)")


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
    # Carga hist una sola vez y la reutiliza entre todos los CSVs. Ademas usa
    # un stats_cache unico compartido por value_bets y pronósticos (los mismos
    # fixtures aparecen en varios CSVs).
    hist = actualizar_todos_resultados([
        (VB_CSV,       'raw'),
        (VB_CAL_CSV,   'cal'),
        (VB_FIL_CSV,   'fil'),
        (VB_V33_CSV,   'v33ref'),
        (VB_V34_CSV,   'v34shrink'),
        (VB_V35_CSV,   'v35dedup'),
        (VB_V36_CSV,   'v36'),
        (PRON_CSV,     'pron raw'),
        (PRON_CAL_CSV, 'pron cal'),
        (PRON_V33_CSV, 'pron v33'),
        (PRON_V34_CSV, 'pron v34'),
    ], dias_paso0=dias_hist)

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
    # Cargar contextos UNA sola vez; todos los fixtures comparten y mutan
    # las mismas referencias (equipos_by_id, ligas_by_id y existing_ids se
    # actualizan en memoria al registrar equipos nuevos y bajar partidos).
    equipos_by_id, equipos_by_name = _pp.load_equipos()
    ligas_by_id, _                 = _pp.load_ligas()
    existing_ids                   = _pp.load_csv_fixture_ids()

    for fix in matches:
        try:
            procesar_partido(fix, force_odds=force_odds,
                             equipos_by_id=equipos_by_id,
                             equipos_by_name=equipos_by_name,
                             ligas_by_id=ligas_by_id,
                             existing_ids=existing_ids,
                             hist=hist)
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
