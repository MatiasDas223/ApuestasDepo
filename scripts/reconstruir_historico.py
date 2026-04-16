"""
reconstruir_historico.py
------------------------
Reconstruye el CSV histórico re-descargando los fixtures que están
en fetch_historia_progress.json pero no en el CSV actual.

Usa 2 llamadas por fixture:
  1. /fixtures?id=X → goles, fecha, equipos
  2. /fixtures/statistics?fixture=X → tiros, corners, posesion, tarjetas

Reanudable: guarda cada --batch fixtures. Se puede interrumpir y retomar.

Uso:
    python scripts/reconstruir_historico.py
    python scripts/reconstruir_historico.py --max 1000
    python scripts/reconstruir_historico.py --dry-run
"""

import csv
import json
import sys
import time
import urllib.request
from pathlib import Path

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE      = Path(r'C:\Users\Matt\Apuestas Deportivas')
CSV_PATH  = BASE / 'data/historico/partidos_historicos.csv'
PROG_FILE = BASE / 'data/historico/fetch_historia_progress.json'

API_KEY   = '5a7d5d038454c3640c8771ce2274c18c'
BASE_URL  = 'https://v3.football.api-sports.io'
SLEEP     = 0.15
BATCH     = 50

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

_reqs = 0

def api_get(endpoint, params):
    global _reqs
    url = f"{BASE_URL}/{endpoint}?" + '&'.join(f"{k}={v}" for k, v in params.items())
    req = urllib.request.Request(url, headers={'x-apisports-key': API_KEY})
    with urllib.request.urlopen(req, timeout=20) as r:
        data = json.loads(r.read())
    _reqs += 1
    return data.get('response', [])


def stat_val(stats, name):
    for s in stats:
        if s['type'] == name:
            v = s['value']
            if v is None:
                return 0
            if isinstance(v, str) and v.endswith('%'):
                return int(v.rstrip('%'))
            try:
                return int(v)
            except (ValueError, TypeError):
                return 0
    return 0


def fetch_fixture(fid):
    """Descarga fixture info + stats. Retorna dict row o None."""
    try:
        # 1) Fixture info (goles, equipos, fecha)
        fix_resp = api_get('fixtures', {'id': fid})
        time.sleep(SLEEP)
        if not fix_resp:
            return None
        fix = fix_resp[0]

        status = fix['fixture']['status']['short']
        if status not in ('FT', 'AET', 'PEN'):
            return None

        # 2) Statistics
        stats_resp = api_get('fixtures/statistics', {'fixture': fid})
        time.sleep(SLEEP)

        if len(stats_resp) < 2:
            return None

        home_stats = stats_resp[0]['statistics']
        away_stats = stats_resp[1]['statistics']

        return {
            'fixture_id':          fid,
            'fecha':               fix['fixture']['date'][:10],
            'liga_id':             fix['league']['id'],
            'equipo_local_id':     fix['teams']['home']['id'],
            'equipo_visitante_id': fix['teams']['away']['id'],
            'goles_local':         fix['goals']['home'] or 0,
            'goles_visitante':     fix['goals']['away'] or 0,
            'tiros_local':         stat_val(home_stats, 'Total Shots'),
            'tiros_visitante':     stat_val(away_stats, 'Total Shots'),
            'tiros_arco_local':    stat_val(home_stats, 'Shots on Goal'),
            'tiros_arco_visitante':stat_val(away_stats, 'Shots on Goal'),
            'corners_local':       stat_val(home_stats, 'Corner Kicks'),
            'corners_visitante':   stat_val(away_stats, 'Corner Kicks'),
            'posesion_local':      stat_val(home_stats, 'Ball Possession'),
            'posesion_visitante':  stat_val(away_stats, 'Ball Possession'),
            'tarjetas_local':      stat_val(home_stats, 'Yellow Cards') + stat_val(home_stats, 'Red Cards'),
            'tarjetas_visitante':  stat_val(away_stats, 'Yellow Cards') + stat_val(away_stats, 'Red Cards'),
        }
    except Exception as e:
        print(f"    [error] fid={fid}: {e}")
        return None


def load_csv():
    if not CSV_PATH.exists():
        return []
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def save_csv(rows):
    # Deduplicate by fixture_id
    seen = set()
    unique = []
    for r in rows:
        fid = str(r['fixture_id'])
        if fid not in seen:
            seen.add(fid)
            unique.append(r)
    unique.sort(key=lambda r: (r['fecha'], int(r['fixture_id'])))
    # Preservar columnas existentes (ej. stats extendidas de backfill)
    file_fields = None
    if CSV_PATH.exists():
        with open(CSV_PATH, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            file_fields = list(reader.fieldnames) if reader.fieldnames else None
    if file_fields:
        merged = list(file_fields)
        for col in CSV_FIELDS:
            if col not in merged:
                merged.append(col)
    else:
        merged = list(CSV_FIELDS)
    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=merged, extrasaction='ignore')
        w.writeheader()
        w.writerows(unique)
    return len(unique)


def main():
    args = sys.argv[1:]
    dry_run = '--dry-run' in args
    max_fixes = 999999
    if '--max' in args:
        max_fixes = int(args[args.index('--max') + 1])

    # Load progress
    with open(PROG_FILE, encoding='utf-8') as f:
        prog = json.load(f)
    fetched_fids = set(prog['fetched'])

    # Load current CSV
    existing_rows = load_csv()
    existing_fids = set(str(r['fixture_id']) for r in existing_rows)

    # Find missing
    missing_fids = [fid for fid in fetched_fids if str(fid) not in existing_fids]
    missing_fids.sort()

    print(f"Fixtures en progress: {len(fetched_fids)}")
    print(f"Fixtures en CSV:      {len(existing_fids)}")
    print(f"Faltantes:            {len(missing_fids)}")
    print(f"API calls necesarias: ~{len(missing_fids) * 2}")

    if dry_run:
        print("\n[dry-run] Sin cambios.")
        return

    # Check quota
    try:
        url = f"{BASE_URL}/status"
        req = urllib.request.Request(url, headers={'x-apisports-key': API_KEY})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read())
        r = data['response']
        if isinstance(r, list):
            r = r[0]
        cuota = r.get('requests', {}).get('limit_day', 7500) - r.get('requests', {}).get('current', 0)
        print(f"Cuota disponible:     {cuota}")
    except:
        cuota = 2000

    # Limit by quota (2 calls per fixture + margin)
    can_do = min(len(missing_fids), cuota // 2 - 10, max_fixes)
    if can_do <= 0:
        print("Sin cuota disponible.")
        return

    print(f"Procesando:           {can_do} fixtures")
    print(f"Tiempo estimado:      ~{can_do * 2 * SLEEP / 60:.1f} min")
    print()

    new_rows = []
    errors = 0

    for i, fid in enumerate(missing_fids[:can_do], 1):
        row = fetch_fixture(fid)
        if row:
            new_rows.append(row)
        else:
            errors += 1

        if i % 25 == 0 or i == can_do:
            print(f"  [{i:>5}/{can_do}] OK={len(new_rows)}  errors={errors}  reqs={_reqs}")

        if i % BATCH == 0:
            total = save_csv(existing_rows + new_rows)
            existing_rows = load_csv()  # reload to keep in sync

    # Final save
    total = save_csv(existing_rows + new_rows)

    print(f"\n{'=' * 60}")
    print(f"  Descargados: {len(new_rows)}")
    print(f"  Errores:     {errors}")
    print(f"  CSV total:   {total} filas")
    print(f"  API calls:   {_reqs}")
    remaining = len(missing_fids) - can_do
    if remaining > 0:
        print(f"  Faltan:      {remaining} (correr de nuevo mañana)")
    else:
        print(f"  ¡Reconstrucción completa!")
    print(f"{'=' * 60}")


if __name__ == '__main__':
    main()
