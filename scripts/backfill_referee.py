"""
backfill_referee.py
-------------------
Agrega columna `referee` a partidos_historicos.csv consultando en bulk
el endpoint /fixtures?league=X&season=Y (1 call por liga/temporada en vez
de 1 call por partido).

Para fixtures cuya (liga, season) no se pueda inferir, hace fallback
a /fixtures?ids=X-Y-Z (batches de 20).

Uso:
  python backfill_referee.py              # backfill completo
  python backfill_referee.py --dry-run    # muestra qué bajaría sin escribir
  python backfill_referee.py --max 5      # solo procesa N (liga, season) combos
"""

import csv
import json
import sys
import time
import urllib.request
import urllib.parse
from pathlib import Path
from collections import defaultdict

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE     = Path(r'C:\Users\Matt\Apuestas Deportivas')
CSV_PATH = BASE / 'data/historico/partidos_historicos.csv'

API_KEY  = '5a7d5d038454c3640c8771ce2274c18c'
BASE_URL = 'https://v3.football.api-sports.io'
SLEEP    = 0.4

_requests = 0


def api_get(endpoint, params=None):
    global _requests
    url = f"{BASE_URL}/{endpoint}"
    if params:
        url += '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={'x-apisports-key': API_KEY})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read())
    _requests += 1
    if data.get('errors'):
        # API Football devuelve errors como dict {} cuando no hay errores reales
        if isinstance(data['errors'], dict) and not data['errors']:
            pass
        elif isinstance(data['errors'], list) and not data['errors']:
            pass
        else:
            raise RuntimeError(f"API error: {data['errors']}")
    return data.get('response', [])


def infer_season(liga_id, fecha_str):
    """
    Devuelve la season que corresponde según API Football.
    Ligas europeas (agosto-mayo): si mes>=7 -> año, si <7 -> año-1
    Resto (calendario): año
    """
    EUROPEAS = {39, 140, 141, 78, 135, 136, 61, 88, 94, 2, 3, 143, 45, 81, 848}  # top-5 + cup + uefa
    year, month = int(fecha_str[:4]), int(fecha_str[5:7])
    if liga_id in EUROPEAS:
        return year if month >= 7 else year - 1
    return year


def load_csv():
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fields = list(reader.fieldnames)
        rows   = list(reader)
    return rows, fields


def write_csv(rows, fields):
    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)


def main():
    dry_run = '--dry-run' in sys.argv
    max_combos = None
    if '--max' in sys.argv:
        max_combos = int(sys.argv[sys.argv.index('--max') + 1])

    print('=' * 70)
    print('  BACKFILL REFEREE')
    print('=' * 70)

    rows, fields = load_csv()
    print(f'  CSV: {len(rows)} partidos, {len(fields)} columnas')

    # Si ya existe la columna y queremos solo completar faltantes:
    if 'referee' in fields:
        ya_tienen = sum(1 for r in rows if r.get('referee', '').strip())
        print(f'  Columna referee YA existe: {ya_tienen}/{len(rows)} con valor')
    else:
        ya_tienen = 0
        print('  Columna referee NO existe — se va a crear')

    # Construir set de fixture_ids que necesitan referee
    necesitan = {int(r['fixture_id']) for r in rows
                 if not r.get('referee', '').strip()}
    print(f'  Faltan: {len(necesitan)} fixtures')

    if not necesitan:
        print('  Nada que hacer.')
        return

    # Agrupar por (liga, season) usando inferencia
    by_combo = defaultdict(list)  # (liga, season) -> [fixture_id]
    for r in rows:
        fid = int(r['fixture_id'])
        if fid not in necesitan:
            continue
        liga = int(r['liga_id'])
        season = infer_season(liga, r['fecha'])
        by_combo[(liga, season)].append(fid)

    combos = sorted(by_combo.items(), key=lambda kv: -len(kv[1]))
    print(f'  Combos (liga, season): {len(combos)}')
    for (liga, season), fids in combos[:5]:
        print(f'    liga {liga} season {season}: {len(fids)} fixtures')
    if len(combos) > 5:
        print(f'    ...({len(combos)-5} combos más)')

    if dry_run:
        print('\n  DRY RUN — sin descargas')
        return

    skip_fallback = max_combos is not None
    if max_combos:
        combos = combos[:max_combos]
        print(f'  Limitado a {len(combos)} combos por --max (fallback DESACTIVADO)')

    # Descarga bulk por combo
    referee_map = {}  # fixture_id -> referee
    for i, ((liga, season), fids_combo) in enumerate(combos, 1):
        print(f'\n  [{i}/{len(combos)}] liga {liga} season {season} '
              f'({len(fids_combo)} fids esperados)...', end=' ', flush=True)
        try:
            fixtures = api_get('fixtures', {'league': liga, 'season': season})
            time.sleep(SLEEP)
        except Exception as e:
            print(f'ERROR: {e}')
            continue

        nuevos = 0
        for fix in fixtures:
            fid = fix['fixture']['id']
            ref = fix['fixture'].get('referee') or ''
            if fid in necesitan:
                referee_map[fid] = ref.strip()
                nuevos += 1
        print(f'-> {nuevos} matched, {sum(1 for f in fids_combo if referee_map.get(f))} con referee')

    # Fallback: fixtures que no aparecieron en bulk → /fixtures?ids=
    sin_match = [fid for fid in necesitan if fid not in referee_map]
    if sin_match and skip_fallback:
        print(f'\n  Sin match: {len(sin_match)} (fallback skipped por --max)')
        sin_match = []
    if sin_match:
        print(f'\n  Fallback /fixtures?ids para {len(sin_match)} fixtures sin match...')
        for batch_start in range(0, len(sin_match), 20):
            batch = sin_match[batch_start:batch_start + 20]
            ids_str = '-'.join(str(f) for f in batch)
            try:
                fixtures = api_get('fixtures', {'ids': ids_str})
                time.sleep(SLEEP)
            except Exception as e:
                print(f'    batch {batch_start}: ERROR {e}')
                continue
            for fix in fixtures:
                fid = fix['fixture']['id']
                ref = fix['fixture'].get('referee') or ''
                referee_map[fid] = ref.strip()
            print(f'    batch {batch_start}: {len(fixtures)} recuperados')

    # Inyectar en rows
    if 'referee' not in fields:
        fields.append('referee')

    actualizados = 0
    for r in rows:
        fid = int(r['fixture_id'])
        if fid in referee_map:
            r['referee'] = referee_map[fid]
            actualizados += 1
        elif 'referee' not in r:
            r['referee'] = ''

    write_csv(rows, fields)

    con_ref = sum(1 for r in rows if r.get('referee', '').strip())
    print(f'\n  RESULTADO:')
    print(f'    API requests usados : {_requests}')
    print(f'    Fixtures actualizados: {actualizados}')
    print(f'    Total con referee   : {con_ref}/{len(rows)} '
          f'({100*con_ref/len(rows):.1f}%)')
    print(f'    Sin referee         : {len(rows) - con_ref}')


if __name__ == '__main__':
    main()
