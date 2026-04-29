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
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

BASE    = Path(r'C:\Users\Matt\Apuestas Deportivas')
SCRIPTS = BASE / 'scripts'
sys.path.insert(0, str(SCRIPTS))

VB_CSV        = BASE / 'data/apuestas/value_bets.csv'
VB_CAL_CSV    = BASE / 'data/apuestas/value_bets_calibrado.csv'
VB_FIL_CSV    = BASE / 'data/apuestas/value_bets_filtrados.csv'
VB_V33_CSV    = BASE / 'data/apuestas/value_bets_v33ref.csv'
VB_V34_CSV    = BASE / 'data/apuestas/value_bets_v34shrink.csv'
VB_V35_CSV    = BASE / 'data/apuestas/value_bets_v35dedup.csv'
VB_V36_CSV    = BASE / 'data/apuestas/value_bets_v36.csv'

# Cache persistente de kickoffs: evita que el Task Scheduler gaste 1 call/fixture
# cada 5 min solo para chequear que el KO sigue fuera de ventana.
KICKOFF_CACHE = BASE / 'data/odds/kickoffs_cache.json'

CSV_PATHS = [
    (VB_CSV,     'raw'),
    (VB_CAL_CSV, 'cal'),
    (VB_FIL_CSV, 'fil'),
    (VB_V33_CSV, 'v33ref'),
    (VB_V34_CSV, 'v34shrink'),
    (VB_V35_CSV, 'v35dedup'),
    (VB_V36_CSV, 'v36'),
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
        rows = list(csv.DictReader(f))
    # Backfill: filas pre-existentes con odds_close pero sin clv_method
    # se etiquetan como 'exact' (eran capturas directas del codigo viejo).
    for r in rows:
        if (r.get('odds_close') or '').strip() and not (r.get('clv_method') or '').strip():
            r['clv_method'] = 'exact'
    return rows


def _write_rows(csv_path, rows):
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=_VB_COLS, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)


def _needs_close(row):
    """Fila necesita closing line: sin captura previa (exacta o estimada)
    y sin resultado final."""
    if (row.get('odds_close') or '').strip():
        return False
    if (row.get('odds_close_est') or '').strip():
        return False
    if (row.get('resultado') or '').strip().upper() in ('W', 'L', 'V'):
        return False
    return True


_THR_KEY = re.compile(r'^([a-z]+)_(over|under)_(\d+(?:\.\d+)?)$')


def _estimate_close_odds(target_key, close_odds):
    """
    Estima odds de cierre para target_key cuando NO existe la clave exacta
    pero sí otros thresholds del mismo prefix/lado.

    Estrategia:
      1. Recolecta todos los pares (over,under) del mismo prefix en close_odds.
      2. De-vig: prob_devig por threshold + vig por threshold.
      3. Si target_thr esta en [min, max] -> interpolacion lineal en prob.
      4. Si esta fuera con >=2 puntos -> extrapolacion lineal con los 2 mas cercanos.
      5. Si solo hay 1 punto -> heuristica de 10pp por unidad de threshold.
      6. Reaplica vig promedio para devolver odds (no fair).

    Retorna (odds_est: float | None, method: str).
    method in {'interp','extrap','single_heur','no_threshold','no_pair'}.
    """
    m = _THR_KEY.match(target_key)
    if not m:
        return None, 'no_threshold'
    prefix, side, target_thr = m.group(1), m.group(2), float(m.group(3))
    other_side = 'under' if side == 'over' else 'over'

    pairs = {}  # threshold -> {'over': odds, 'under': odds}
    for k, v in close_odds.items():
        mm = _THR_KEY.match(k)
        if not mm:
            continue
        if mm.group(1) != prefix:
            continue
        try:
            v = float(v)
        except (TypeError, ValueError):
            continue
        if v <= 1.0:
            continue
        pairs.setdefault(float(mm.group(3)), {})[mm.group(2)] = v

    points = []  # (threshold, p_side_devig, vig)
    for t, d in sorted(pairs.items()):
        if 'over' not in d or 'under' not in d:
            continue
        p_o = 1.0 / d['over']
        p_u = 1.0 / d['under']
        total = p_o + p_u
        if total <= 0:
            continue
        p_devig = (p_o if side == 'over' else p_u) / total
        vig = total - 1.0
        points.append((t, p_devig, vig))

    if not points:
        return None, 'no_pair'

    thresholds = [p[0] for p in points]
    avg_vig = sum(p[2] for p in points) / len(points)

    if len(points) >= 2 and min(thresholds) <= target_thr <= max(thresholds):
        below = max((p for p in points if p[0] <= target_thr), key=lambda p: p[0])
        above = min((p for p in points if p[0] >= target_thr), key=lambda p: p[0])
        if below[0] == above[0]:
            p_target = below[1]
        else:
            t1, p1, _ = below
            t2, p2, _ = above
            p_target = p1 + (p2 - p1) * (target_thr - t1) / (t2 - t1)
        method = 'interp'
    elif len(points) >= 2:
        sorted_pts = sorted(points, key=lambda p: abs(p[0] - target_thr))[:2]
        sorted_pts.sort(key=lambda p: p[0])
        t1, p1, _ = sorted_pts[0]
        t2, p2, _ = sorted_pts[1]
        if t2 == t1:
            p_target = p1
        else:
            p_target = p1 + (p2 - p1) * (target_thr - t1) / (t2 - t1)
        method = 'extrap'
    else:
        # 1 punto: heuristica 10pp por unidad de threshold
        t1, p1, _ = points[0]
        delta = target_thr - t1
        shift = 0.10 * delta
        p_target = (p1 - shift) if side == 'over' else (p1 + shift)
        method = 'single_heur'

    p_target = max(0.01, min(0.99, p_target))
    p_with_vig = min(0.99, p_target * (1.0 + avg_vig))
    return 1.0 / p_with_vig, method


def _load_kickoff_cache():
    """Lee el cache de disco. Descarta entries cuyo KO es >30d atras."""
    if not KICKOFF_CACHE.exists():
        return {}
    try:
        with open(KICKOFF_CACHE, encoding='utf-8') as f:
            raw = json.load(f)
    except Exception:
        return {}
    now = _now_utc()
    cache = {}
    for fid, entry in raw.items():
        try:
            entry['kickoff'] = datetime.fromisoformat(entry['kickoff'])
        except Exception:
            continue
        if (now - entry['kickoff']).total_seconds() > 30 * 24 * 3600:
            continue
        cache[fid] = entry
    return cache


def _save_kickoff_cache(cache):
    KICKOFF_CACHE.parent.mkdir(parents=True, exist_ok=True)
    raw = {}
    for fid, entry in cache.items():
        if 'error' in entry:
            continue
        e = dict(entry)
        ko = e.get('kickoff')
        if isinstance(ko, datetime):
            e['kickoff'] = ko.isoformat()
        raw[fid] = e
    with open(KICKOFF_CACHE, 'w', encoding='utf-8') as f:
        json.dump(raw, f, ensure_ascii=False, indent=2)


def _needs_api_refresh(entry, now, window_min):
    """
    Decide si vale la pena llamar a la API para este fixture.
      - Sin cache: sí
      - Status ya no pre-match (FT/AET/PEN/LIVE/...): no (closing line ya no aplica)
      - KO fuera de [now-2h, now+window+15min]: no (cache confiable, no consultar)
      - En caso contrario: sí (refrescar por si cambió status/KO)
    """
    if entry is None or 'error' in entry:
        return True
    status = entry.get('status', '')
    if status not in ('NS', 'TBD'):
        return False
    ko = entry.get('kickoff')
    if not isinstance(ko, datetime):
        return True
    mins_to = (ko - now).total_seconds() / 60
    if mins_to > window_min + 15:
        return False
    if mins_to < -120:
        return False
    return True


def _fixture_info(fid, cache, now, window_min):
    """
    Devuelve dict con kickoff (datetime UTC), home_id, away_id, home_name,
    away_name, liga_id. Usa cache persistente: solo llama a la API si el KO
    cacheado cae en ventana de interes.
    """
    entry = cache.get(fid)
    if entry is not None and not _needs_api_refresh(entry, now, window_min):
        return entry

    try:
        resp = _pp.api_get('fixtures', {'id': fid})
        time.sleep(0.3)
    except Exception as e:
        return {'error': str(e)}

    if not resp:
        return {'error': 'no response'}

    fix = resp[0]
    try:
        ko = datetime.fromisoformat(fix['fixture']['date'].replace('Z', '+00:00'))
    except Exception:
        return {'error': 'bad date'}

    info = {
        'kickoff':   ko,
        'status':    fix['fixture']['status']['short'],
        'home_id':   fix['teams']['home']['id'],
        'away_id':   fix['teams']['away']['id'],
        'home_name': fix['teams']['home']['name'],
        'away_name': fix['teams']['away']['name'],
        'liga_id':   fix['league']['id'],
    }
    cache[fid] = info
    return info


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

    fix_cache = _load_kickoff_cache()
    cached_skip = sum(
        1 for fid in fid_rows
        if not _needs_api_refresh(fix_cache.get(fid), now, window_min)
    )
    print(f"  [snapshot] {len(fid_rows)} fixture(s) con filas pendientes "
          f"({cached_skip} skip por cache KO)")

    updated     = 0
    skipped_ko  = 0
    skipped_err = 0

    for fid, pairs in fid_rows.items():
        info = _fixture_info(fid, fix_cache, now, window_min)
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
        rows_hit_exact = 0
        rows_hit_est   = 0
        rows_miss      = 0

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

            try:
                odds_open = float(row['odds'])
            except (TypeError, ValueError):
                continue

            odds_close = odds.get(key)
            if odds_close is not None and odds_close > 0:
                clv = (odds_open / odds_close - 1) * 100
                row['odds_close']   = f"{odds_close:.2f}"
                row['clv_pct']      = f"{clv:+.2f}"
                row['clv_method']   = 'exact'
                row['fecha_cierre'] = stamp
                rows_hit_exact += 1
                continue

            # Match exacto fallo: intentar estimacion por interpolacion
            odds_est, method = _estimate_close_odds(key, odds)
            if odds_est is None:
                rows_miss += 1
                if verbose:
                    print(f"    [miss] key {key} no disponible en close ({method})")
                continue

            clv_est = (odds_open / odds_est - 1) * 100
            row['odds_close_est'] = f"{odds_est:.2f}"
            row['clv_pct_est']    = f"{clv_est:+.2f}"
            row['clv_method']     = method
            row['fecha_cierre']   = stamp
            rows_hit_est += 1
            if verbose:
                print(f"    [est] {key} -> {odds_est:.2f} ({method}) clv_est={clv_est:+.2f}")

        print(f"    [write] {rows_hit_exact} exact + {rows_hit_est} est  "
              f"({rows_miss} sin match)")
        updated += rows_hit_exact + rows_hit_est

    # Reescribir CSVs (aunque no haya cambios — idempotente)
    for path, label in CSV_PATHS:
        rows = csv_data[path]
        if rows:
            _write_rows(path, rows)

    try:
        _save_kickoff_cache(fix_cache)
    except Exception as e:
        print(f"  [snapshot] error guardando cache KO: {e}")

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
