"""
snapshot_cierre.py — Captura cuotas de cierre (closing line) y calcula CLV.

Flujo:
  1. Levanta value_bets.csv / value_bets_calibrado.csv / value_bets_filtrados.csv
  2. Colecta fixtures unicos con al menos una fila sin odds_close y sin resultado.
  3. Por cada fixture: consulta /fixtures?id=X en API Football -> lee kickoff.
  4. Si kickoff esta dentro de [now, now+window] min:
       - Refetch odds (force=True) de API Football + odds-api.io
       - Mapea mercado+lado -> clave de odds via market_utils
       - Escribe odds_close, clv_pct, fecha_cierre en la fila
  5. Guarda los CSVs.

Uso:
    python snapshot_cierre.py              # window=20 min (pensado para scheduler)
    python snapshot_cierre.py --window 60  # fallback manual / post-pipeline
    python snapshot_cierre.py --verbose    # log detallado

Se invoca automaticamente desde pipeline.py con window=60 como fallback.
"""

import argparse
import csv
import importlib.util
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

BASE    = Path(r'C:\Users\Matt\Apuestas Deportivas')
SCRIPTS = BASE / 'scripts'
sys.path.insert(0, str(SCRIPTS))

VB_CSV     = BASE / 'data/apuestas/value_bets.csv'
VB_CAL_CSV = BASE / 'data/apuestas/value_bets_calibrado.csv'
VB_FIL_CSV = BASE / 'data/apuestas/value_bets_filtrados.csv'

CSV_PATHS = [
    (VB_CSV,     'raw'),
    (VB_CAL_CSV, 'cal'),
    (VB_FIL_CSV, 'fil'),
]


def _load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f'{name}.py')
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_fo = _load('fetch_odds')
_pp = _load('preparar_partido')
from market_utils import mercado_to_odds_key
from analizar_partido import _VB_COLS


def _now_utc():
    return datetime.now(timezone.utc)


def _load_rows(csv_path):
    if not csv_path.exists():
        return []
    with open(csv_path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def _write_rows(csv_path, rows):
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=_VB_COLS, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)


def _needs_close(row):
    """Fila necesita closing line: sin odds_close y sin resultado final."""
    if (row.get('odds_close') or '').strip():
        return False
    if (row.get('resultado') or '').strip().upper() in ('W', 'L', 'V'):
        return False
    return True


def _fixture_info(fid, cache):
    """
    Devuelve dict con kickoff (datetime UTC), home_id, away_id, home_name,
    away_name, liga_id. Cachea por fixture para no repetir calls.
    """
    if fid in cache:
        return cache[fid]
    try:
        resp = _pp.api_get('fixtures', {'id': fid})
        time.sleep(0.3)
    except Exception as e:
        cache[fid] = {'error': str(e)}
        return cache[fid]

    if not resp:
        cache[fid] = {'error': 'no response'}
        return cache[fid]

    fix = resp[0]
    try:
        ko = datetime.fromisoformat(fix['fixture']['date'].replace('Z', '+00:00'))
    except Exception:
        cache[fid] = {'error': 'bad date'}
        return cache[fid]

    cache[fid] = {
        'kickoff':   ko,
        'status':    fix['fixture']['status']['short'],
        'home_id':   fix['teams']['home']['id'],
        'away_id':   fix['teams']['away']['id'],
        'home_name': fix['teams']['home']['name'],
        'away_name': fix['teams']['away']['name'],
        'liga_id':   fix['league']['id'],
    }
    return cache[fid]


def _refetch_odds(fid, info, verbose=False):
    """
    Refetch forzado de ambas fuentes para un fixture.
    Retorna dict de odds combinado.
    """
    odds = {}

    try:
        o1, _ = _fo.get_odds(fid, force=True)
        odds.update(o1)
        if verbose:
            print(f"    [refetch] API Football: {len(o1)} claves")
    except Exception as e:
        if verbose:
            print(f"    [refetch] API Football error: {e}")

    slug = _fo.get_league_slug(info['liga_id']) or ''
    if slug:
        try:
            ev_id, _, _ = _fo.find_event_oddsapi(
                info['home_name'], info['away_name'], slug,
                home_id=info['home_id'], away_id=info['away_id'])
            if ev_id:
                o2, _ = _fo.get_odds_oddsapi(ev_id, force=True)
                nuevas = sum(1 for k in o2 if k not in odds)
                odds.update({k: v for k, v in o2.items() if k not in odds})
                if verbose:
                    print(f"    [refetch] odds-api.io: +{nuevas} claves")
            elif verbose:
                print(f"    [refetch] odds-api.io: evento no encontrado (puede haber iniciado)")
        except Exception as e:
            if verbose:
                print(f"    [refetch] odds-api.io error: {e}")

    return odds


def run(window_min=20, verbose=False):
    """
    Ejecuta la captura de closing lines.
    window_min: ventana en minutos hacia adelante desde now para considerar
                un fixture como "proximo al KO".
    """
    now = _now_utc()
    print(f"  [snapshot] ventana: proximos {window_min} min  (now UTC: {now:%Y-%m-%d %H:%M})")

    # Cargar los 3 CSVs y agrupar filas por fixture
    csv_data = {}   # path -> rows (lista mutable)
    fid_rows = {}   # fid -> [(path, row), ...]

    for path, label in CSV_PATHS:
        rows = _load_rows(path)
        csv_data[path] = rows
        for r in rows:
            if not _needs_close(r):
                continue
            fid = (r.get('fixture_id') or '').strip()
            if not fid:
                continue
            fid_rows.setdefault(fid, []).append((path, r))

    if not fid_rows:
        print(f"  [snapshot] Ninguna fila pendiente de closing line")
        return

    print(f"  [snapshot] {len(fid_rows)} fixture(s) con filas pendientes")

    fix_cache   = {}
    updated     = 0
    skipped_ko  = 0
    skipped_err = 0

    for fid, pairs in fid_rows.items():
        info = _fixture_info(fid, fix_cache)
        if 'error' in info:
            if verbose:
                print(f"  [fid={fid}] error consulta: {info['error']}")
            skipped_err += 1
            continue

        ko      = info['kickoff']
        mins_to = (ko - now).total_seconds() / 60
        status  = info.get('status', '')

        # Solo pre-match: un partido in-play tiene cuotas ajustadas al resultado
        # parcial y rompe el concepto de closing line.
        if status not in ('NS', 'TBD'):
            if verbose:
                print(f"  [fid={fid}] status={status} — no pre-match, skip")
            skipped_ko += 1
            continue

        if mins_to < 0:
            if verbose:
                print(f"  [fid={fid}] KO ya paso ({-mins_to:.0f} min atras) — sin captura")
            skipped_ko += 1
            continue
        if mins_to > window_min:
            if verbose:
                print(f"  [fid={fid}] KO en {mins_to:.0f} min — fuera de ventana")
            skipped_ko += 1
            continue

        print(f"  [fid={fid}] {info['home_name']} vs {info['away_name']}  "
              f"KO en {mins_to:.0f} min -> refetch")

        odds = _refetch_odds(fid, info, verbose=verbose)
        if not odds:
            print(f"    [refetch] sin odds disponibles")
            skipped_err += 1
            continue

        stamp = _now_utc().strftime('%Y-%m-%d %H:%M')
        rows_hit = 0
        rows_miss = 0

        for path, row in pairs:
            if not _needs_close(row):
                continue
            key = mercado_to_odds_key(
                row['mercado'], row['lado'],
                info['home_name'], info['away_name'])
            if key is None:
                rows_miss += 1
                if verbose:
                    print(f"    [miss] mapeo fallido: {row['mercado']} | {row['lado']}")
                continue

            odds_close = odds.get(key)
            if odds_close is None:
                rows_miss += 1
                if verbose:
                    print(f"    [miss] key {key} no disponible en close")
                continue

            try:
                odds_open = float(row['odds'])
            except (TypeError, ValueError):
                continue

            if odds_close <= 0:
                continue

            clv = (odds_open / odds_close - 1) * 100
            row['odds_close']   = f"{odds_close:.2f}"
            row['clv_pct']      = f"{clv:+.2f}"
            row['fecha_cierre'] = stamp
            rows_hit += 1

        print(f"    [write] {rows_hit} fila(s) con closing  ({rows_miss} sin match)")
        updated += rows_hit

    # Reescribir CSVs (aunque no haya cambios — idempotente)
    for path, label in CSV_PATHS:
        rows = csv_data[path]
        if rows:
            _write_rows(path, rows)

    print(f"  [snapshot] total: {updated} fila(s) con closing line  "
          f"({skipped_ko} fixture(s) fuera de ventana, {skipped_err} error)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--window', type=int, default=20,
                    help='Ventana en min hacia adelante desde now (default 20)')
    ap.add_argument('--verbose', action='store_true')
    args = ap.parse_args()

    print(f"\n{'='*60}")
    print(f"  SNAPSHOT CIERRE (CLV)")
    print(f"{'='*60}")
    run(window_min=args.window, verbose=args.verbose)
    print()


if __name__ == '__main__':
    main()
