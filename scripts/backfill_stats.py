"""
backfill_stats.py
-----------------
Agrega 12 columnas nuevas al CSV histórico para todos los partidos existentes:

  xg_local / xg_visitante               — Expected Goals
  tiros_dentro_local / visitante         — Shots insidebox
  tiros_fuera_local / visitante          — Shots outsidebox
  tiros_bloqueados_local / visitante     — Blocked Shots
  atajadas_local / visitante             — Goalkeeper Saves
  goles_prevenidos_local / visitante     — goals_prevented

Consume 1 llamada a la API por partido. Con 6705 partidos y límite de 7500/día
entra en una sola corrida (si el cupo diario está disponible).

Uso:
    python backfill_stats.py               # procesa todos los pendientes
    python backfill_stats.py --max 500     # máximo 500 llamadas en esta corrida
    python backfill_stats.py --dry-run     # solo muestra cuántos pendientes hay
    python backfill_stats.py --retry-dashes  # reintenta filas marcadas con '-'

Progreso: se guarda en el CSV en tiempo real cada --batch filas (default 50).
Se puede interrumpir y retomar: las filas ya procesadas se omiten.
Filas marcadas '-' (API sin datos en ese momento) se pueden reintentar con
--retry-dashes ahora que la cuota se renovó.
"""

import csv
import json
import sys
import time
import urllib.request
import urllib.parse
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

API_KEY  = '5a7d5d038454c3640c8771ce2274c18c'
BASE_URL = 'https://v3.football.api-sports.io'

CSV_PATH = Path(r'C:\Users\Matt\Apuestas Deportivas\data\historico\partidos_historicos.csv')

# Nuevas columnas (en el orden que se agregarán al CSV)
NEW_COLS = [
    'xg_local',             'xg_visitante',
    'tiros_dentro_local',   'tiros_dentro_visitante',
    'tiros_fuera_local',    'tiros_fuera_visitante',
    'tiros_bloqueados_local','tiros_bloqueados_visitante',
    'atajadas_local',       'atajadas_visitante',
    'goles_prevenidos_local','goles_prevenidos_visitante',
]

# Mapeo: tipo API → (col_local, col_visitante)
STAT_MAP = {
    'expected_goals':   ('xg_local',              'xg_visitante'),
    'Shots insidebox':  ('tiros_dentro_local',     'tiros_dentro_visitante'),
    'Shots outsidebox': ('tiros_fuera_local',      'tiros_fuera_visitante'),
    'Blocked Shots':    ('tiros_bloqueados_local',  'tiros_bloqueados_visitante'),
    'Goalkeeper Saves': ('atajadas_local',          'atajadas_visitante'),
    'goals_prevented':  ('goles_prevenidos_local',  'goles_prevenidos_visitante'),
}

SLEEP_BETWEEN = 0.13   # segundos entre llamadas (~7.5 req/s, seguro para 7500/día)
BATCH_SIZE    = 50      # guardar CSV cada N filas procesadas


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
    if data.get('errors') and data['errors'] not in ({}, []):
        raise RuntimeError(f"API error: {data['errors']}")
    return data.get('response', [])


def quota_restante():
    url = f"{BASE_URL}/status"
    r = urllib.request.Request(url, headers={'x-apisports-key': API_KEY})
    with urllib.request.urlopen(r, timeout=20) as resp:
        data = json.loads(resp.read())
    resp_data = data.get('response', {})
    if isinstance(resp_data, list):
        resp_data = resp_data[0] if resp_data else {}
    req = resp_data.get('requests', {})
    current = req.get('current', 0)
    limit   = req.get('limit_day', 7500)
    return limit - current


def fetch_stats(fixture_id: str) -> tuple[dict, str]:
    """
    Llama /fixtures/statistics?fixture=ID y devuelve (dict_valores, status).
    status: 'ok'      -> datos obtenidos
            'no_data' -> API respondió pero no hay stats (genuino)
            'error'   -> fallo de red / cupo / error de API (reintentar luego)
    """
    result = {c: '' for c in NEW_COLS}
    try:
        response = api_get('fixtures/statistics', {'fixture': fixture_id})
    except Exception:
        return result, 'error'   # cupo agotado u otro error — NO marcar como '-'

    if len(response) < 2:
        return result, 'no_data'   # API respondió pero sin stats — marcar como '-'

    home_stats = response[0]['statistics']
    away_stats = response[1]['statistics']

    def get_val(stats_list, stat_type, as_float=False):
        for s in stats_list:
            if s['type'] == stat_type:
                v = s['value']
                if v is None:
                    return ''
                if isinstance(v, str) and v.endswith('%'):
                    return str(int(v.rstrip('%')))
                try:
                    return str(float(v)) if as_float else str(int(float(v)))
                except (ValueError, TypeError):
                    return str(v)
        return ''

    for stat_type, (col_local, col_vis) in STAT_MAP.items():
        is_float = stat_type in ('expected_goals', 'goals_prevented')
        result[col_local] = get_val(home_stats, stat_type, as_float=is_float)
        result[col_vis]   = get_val(away_stats, stat_type, as_float=is_float)

    return result, 'ok'


# ─────────────────────────────────────────────────────────────────────────────
# CSV helpers
# ─────────────────────────────────────────────────────────────────────────────

def load_csv():
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        cols = list(reader.fieldnames)
        rows = list(reader)
    return cols, rows


def save_csv(cols, rows):
    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def add_new_cols(cols, rows):
    """Agrega las columnas nuevas al schema y a cada fila (vacías) si no existen."""
    added = []
    for col in NEW_COLS:
        if col not in cols:
            cols.append(col)
            added.append(col)
            for r in rows:
                r[col] = ''
    return added


def is_pending(row, include_dashes=False):
    """
    Una fila está pendiente si tiros_dentro_local está vacío.
    Se usa tiros_dentro en lugar de xg_local porque xG no siempre está
    disponible en la API (muchos partidos lo tienen vacío aunque el resto
    de los stats sí se descargaron correctamente).
    """
    val = row.get('tiros_dentro_local', '')
    return val == '' or (include_dashes and val == '-')


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    args          = sys.argv[1:]
    dry_run       = '--dry-run'      in args
    retry_dashes  = '--retry-dashes' in args
    max_calls     = None
    batch         = BATCH_SIZE

    if '--max' in args:
        max_calls = int(args[args.index('--max') + 1])
    if '--batch' in args:
        batch = int(args[args.index('--batch') + 1])

    # ── Cargar CSV ────────────────────────────────────────────────────────────
    print(f"Cargando {CSV_PATH.name}...")
    cols, rows = load_csv()

    # ── Agregar columnas nuevas si faltan ─────────────────────────────────────
    added = add_new_cols(cols, rows)
    if added:
        print(f"  Columnas nuevas agregadas: {', '.join(added)}")
    else:
        print(f"  Columnas ya presentes en el CSV.")

    # ── --retry-dashes: resetear '-' a '' para que sean tratadas como pendientes
    if retry_dashes:
        reseteadas = 0
        for r in rows:
            if r.get('xg_local', '') == '-':
                for col in NEW_COLS:
                    r[col] = ''
                reseteadas += 1
        print(f"  --retry-dashes: {reseteadas} filas reseteadas de '-' a '' para reintento")

    # ── Contar pendientes ─────────────────────────────────────────────────────
    pendientes = [r for r in rows if is_pending(r) and r.get('fixture_id', '').strip()]
    ya_hechos  = len(rows) - len(pendientes)

    print(f"\n  Total partidos  : {len(rows)}")
    print(f"  Ya procesados   : {ya_hechos}")
    print(f"  Pendientes      : {len(pendientes)}")

    if dry_run:
        print("\n[dry-run] Sin cambios. Saliendo.")
        return

    if not pendientes:
        print("\nTodo ya está procesado. Nada que hacer.")
        return

    # ── Verificar cupo API ────────────────────────────────────────────────────
    print(f"\nVerificando cupo API...")
    restante = quota_restante()
    print(f"  Llamadas disponibles hoy: {restante}")
    a_procesar = min(len(pendientes), restante - 10)  # reserva 10 de margen
    if max_calls:
        a_procesar = min(a_procesar, max_calls)

    if a_procesar <= 0:
        print("  Sin cupo disponible hoy. Intentá mañana.")
        return

    print(f"  Se procesarán {a_procesar} partidos en esta corrida.")
    print(f"  Tiempo estimado: ~{a_procesar * SLEEP_BETWEEN / 60:.1f} minutos")
    print()

    # ── Procesar ──────────────────────────────────────────────────────────────
    procesados = 0
    errores    = 0
    fila_idx   = {r['fixture_id']: i for i, r in enumerate(rows)}

    for row in pendientes[:a_procesar]:
        fid = row['fixture_id']
        idx = fila_idx.get(fid)

        stats, status = fetch_stats(fid)

        if status == 'error':
            # Fallo de red o cupo agotado — dejar vacío para reintentar después
            errores += 1
            print(f"  [ERROR red/cupo] fid={fid} — se reintentará la próxima corrida")
        elif status == 'no_data':
            # API respondió correctamente pero no tiene stats para este partido
            errores += 1
            for col in NEW_COLS:
                rows[idx][col] = '-'
        else:
            for col, val in stats.items():
                rows[idx][col] = val

        procesados += 1
        time.sleep(SLEEP_BETWEEN)

        # ── Progreso en consola ───────────────────────────────────────────────
        if procesados % 25 == 0 or procesados == a_procesar:
            pct = procesados / a_procesar * 100
            print(f"  [{procesados:>5}/{a_procesar}]  {pct:5.1f}%  "
                  f"OK={procesados-errores}  sin_datos={errores}  "
                  f"req_run={_requests_this_run}")

        # ── Guardado incremental ──────────────────────────────────────────────
        if procesados % batch == 0:
            save_csv(cols, rows)

    # ── Guardado final ────────────────────────────────────────────────────────
    save_csv(cols, rows)

    pendientes_restantes = sum(1 for r in rows if is_pending(r))
    print(f"\n{'='*60}")
    print(f"  Procesados esta corrida : {procesados}")
    print(f"  Sin datos (marcados '-'): {errores}")
    print(f"  Pendientes restantes    : {pendientes_restantes}")
    print(f"  Llamadas API usadas     : {_requests_this_run}")
    print(f"  CSV guardado en         : {CSV_PATH}")
    if pendientes_restantes > 0:
        print(f"\n  Quedan {pendientes_restantes} partidos. Volvé a correr mañana.")
    else:
        print(f"\n  ¡Backfill completo!")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
