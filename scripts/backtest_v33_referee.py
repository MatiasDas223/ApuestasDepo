"""
backtest_v33_referee.py
-----------------------
Backtest walk-forward del ajuste por árbitro sobre el mercado de tarjetas.

Para cada bet de Tarjetas con resultado:
  1. Sacar fixture_id, fecha, mercado (con threshold), lado, odds, modelo_prob, resultado
  2. Cargar referee del partido desde partidos_historicos.csv
  3. Computar ratings de árbitros usando SOLO partidos anteriores a la fecha
     de la apuesta (walk-forward — evita data leakage)
  4. Resolver mu Poisson tal que CDF coincida con modelo_prob original
  5. Aplicar factor del árbitro con varios alpha
  6. Recalcular nueva prob → edge → si >= MIN_EDGE, contabiliza

Compara ROI v3.2 (sin ajuste) vs v3.3 (con ajuste) por alpha.

Uso:
    python backtest_v33_referee.py
    python backtest_v33_referee.py --csv raw       # solo value_bets.csv
    python backtest_v33_referee.py --csv cal       # solo calibrado
    python backtest_v33_referee.py --alphas 0.3 0.5 0.7 1.0
"""
import csv
import math
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE = Path(r'C:\Users\Matt\Apuestas Deportivas')
HIST_CSV   = BASE / 'data/historico/partidos_historicos.csv'
VB_PATHS   = {
    'raw': BASE / 'data/apuestas/value_bets.csv',
    'cal': BASE / 'data/apuestas/value_bets_calibrado.csv',
    'fil': BASE / 'data/apuestas/value_bets_filtrados.csv',
}

MIN_EDGE = 0.04
STAKE    = 1.0
K_REF    = 10  # shrinkage de ratings de árbitro


# ─────────────────────────────────────────────────────────────────────────────
# Poisson helpers (sin scipy)
# ─────────────────────────────────────────────────────────────────────────────

def poisson_cdf(mu, k):
    """P(X <= k) para X ~ Poisson(mu). k entero >=0."""
    if mu <= 0: return 1.0
    s, term = 0.0, math.exp(-mu)
    s += term
    for i in range(1, k + 1):
        term *= mu / i
        s += term
    return min(s, 1.0)


def poisson_sf(mu, k):
    """P(X > k) = 1 - CDF(mu, k)."""
    return max(0.0, 1.0 - poisson_cdf(mu, k))


def solve_mu_from_prob(prob, threshold, lado):
    """
    Devuelve el mu Poisson tal que la prob del lado coincida con `prob`.
    threshold: ej 4.5 (línea O/U)
    lado: 'Over' / 'Under' (acepta prefijos 'Over/Si', 'Under/No')
    """
    es_over = lado.startswith('Over') or lado.startswith('Si')
    k = int(threshold)  # threshold típicamente .5 — el "menor o igual a k" cubre exactamente Under

    def f(mu):
        return poisson_sf(mu, k) if es_over else poisson_cdf(mu, k)

    # Bisección
    lo, hi = 0.001, 50.0
    target = prob
    if f(hi) < target and not es_over:
        return hi
    if f(lo) > target and es_over:
        return lo
    for _ in range(80):
        mid = (lo + hi) / 2
        if f(mid) < target:
            if es_over:
                lo = mid
            else:
                hi = mid
        else:
            if es_over:
                hi = mid
            else:
                lo = mid
    return (lo + hi) / 2


def prob_from_mu(mu, threshold, lado):
    es_over = lado.startswith('Over') or lado.startswith('Si')
    k = int(threshold)
    return poisson_sf(mu, k) if es_over else poisson_cdf(mu, k)


# ─────────────────────────────────────────────────────────────────────────────
# Histórico → referee por fixture, ratings walk-forward
# ─────────────────────────────────────────────────────────────────────────────

def load_history():
    rows = []
    with open(HIST_CSV, encoding='utf-8') as f:
        for r in csv.DictReader(f):
            try:
                fid    = int(r['fixture_id'])
                fecha  = r['fecha']
                liga   = int(r['liga_id'])
                ref    = (r.get('referee') or '').strip()
                yl     = int(r.get('tarjetas_local')     or 0)
                yv     = int(r.get('tarjetas_visitante') or 0)
            except (ValueError, KeyError):
                continue
            rows.append({'fid': fid, 'fecha': fecha, 'liga': liga,
                         'ref': ref, 'cards': yl + yv})
    rows.sort(key=lambda r: r['fecha'])
    return rows


def fixture_index(hist):
    """Devuelve {fixture_id: row}."""
    return {r['fid']: r for r in hist}


def compute_ratings_walkforward(hist, until_date, k_ref=K_REF):
    """
    Computa ratings de árbitros con shrinkage hacia 1.0, usando solo
    partidos con fecha < until_date.
    """
    by_liga = defaultdict(list)
    by_ref  = defaultdict(lambda: {'cards': [], 'ligas_n': defaultdict(int)})

    for r in hist:
        if r['fecha'] >= until_date:
            break  # hist está ordenado por fecha
        if not r['ref']:
            continue
        by_liga[r['liga']].append(r['cards'])
        by_ref[r['ref']]['cards'].append((r['cards'], r['liga']))
        by_ref[r['ref']]['ligas_n'][r['liga']] += 1

    liga_avgs = {l: sum(v) / len(v) if v else 4.5
                 for l, v in by_liga.items()}

    ratings = {}
    for ref, d in by_ref.items():
        n = len(d['cards'])
        if n == 0:
            continue
        avg_ref = sum(c for c, _ in d['cards']) / n
        liga_avg_w = sum(liga_avgs.get(l, 4.5) * cnt
                         for l, cnt in d['ligas_n'].items()) / n
        if liga_avg_w <= 0:
            continue
        f_raw = avg_ref / liga_avg_w
        f_shr = (n * f_raw + k_ref * 1.0) / (n + k_ref)
        ratings[ref] = {'n': n, 'factor': f_shr}
    return ratings


# ─────────────────────────────────────────────────────────────────────────────
# Backtest core
# ─────────────────────────────────────────────────────────────────────────────

_threshold_re = re.compile(r'O/U\s+(\d+\.?\d*)')


def parse_bet(row):
    """Devuelve (threshold, lado_normalizado, scope) o None si no es Tarjetas tot."""
    if row.get('categoria', '') != 'Tarjetas':
        return None
    mercado = row.get('mercado', '')
    m = _threshold_re.search(mercado)
    if not m:
        return None
    threshold = float(m.group(1))
    lado_raw  = row.get('lado', '')
    return threshold, lado_raw, row.get('alcance', 'Total')


def pnl(result, odds):
    if result == 'W': return odds - STAKE
    if result == 'L': return -STAKE
    return 0.0


def backtest_csv(csv_path, hist, fixt_idx, alphas):
    """Devuelve dict {alpha: {bets, won, lost, pnl, ...}} + breakdown."""
    bets = []
    skipped_no_ref = 0
    skipped_no_fixture = 0
    skipped_total_only = 0  # Solo backtesteamos Tarjetas tot. (donde el lado es claro)

    with open(csv_path, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            res = (row.get('resultado') or '').strip().upper()
            if res not in ('W', 'L', 'V'):
                continue
            parsed = parse_bet(row)
            if not parsed:
                continue
            threshold, lado, alcance = parsed
            if alcance != 'Total':
                skipped_total_only += 1
                continue
            try:
                fid   = int(row['fixture_id'])
                odds  = float(row['odds'])
                prob  = float(row['modelo_prob'])
            except (ValueError, KeyError):
                continue

            fix = fixt_idx.get(fid)
            if not fix:
                skipped_no_fixture += 1
                continue
            if not fix['ref']:
                skipped_no_ref += 1
                # Aún así contamos como bet de baseline (sin ajuste posible)
                bets.append({
                    'fid': fid, 'fecha': fix['fecha'], 'odds': odds,
                    'prob': prob, 'threshold': threshold, 'lado': lado,
                    'res': res, 'ref': '', 'mu_base': None,
                })
                continue

            mu_base = solve_mu_from_prob(prob, threshold, lado)
            bets.append({
                'fid': fid, 'fecha': fix['fecha'], 'odds': odds,
                'prob': prob, 'threshold': threshold, 'lado': lado,
                'res': res, 'ref': fix['ref'], 'mu_base': mu_base,
            })

    print(f'    Total bets considerados: {len(bets)}  '
          f'(skip sin_fixture={skipped_no_fixture}, '
          f'sin_ref={skipped_no_ref}, no_total={skipped_total_only})')

    # Ratings walk-forward por fecha (cacheo por fecha para no recomputar)
    fechas = sorted({b['fecha'] for b in bets})
    print(f'    Computando ratings walk-forward para {len(fechas)} fechas...',
          end=' ', flush=True)
    ratings_by_date = {d: compute_ratings_walkforward(hist, d, k_ref=K_REF)
                       for d in fechas}
    print('OK')

    # Baseline v3.2 (sin ajuste)
    baseline = {'bets': 0, 'W': 0, 'L': 0, 'V': 0, 'pnl': 0.0}
    for b in bets:
        baseline['bets'] += 1
        baseline[b['res']] = baseline.get(b['res'], 0) + 1
        baseline['pnl']  += pnl(b['res'], b['odds'])
    baseline['roi'] = baseline['pnl'] / baseline['bets'] if baseline['bets'] else 0

    # Por alpha
    results = {'baseline': baseline}
    for alpha in alphas:
        out = {'bets': 0, 'W': 0, 'L': 0, 'V': 0, 'pnl': 0.0,
               'changed_to_skip': 0, 'kept': 0,
               'edge_old_total': 0.0, 'edge_new_total': 0.0}
        for b in bets:
            if b['mu_base'] is None or not b['ref']:
                # Sin árbitro o sin mu_base resoluble — apuesta tal cual baseline
                out['bets'] += 1
                out[b['res']] += 1
                out['pnl']    += pnl(b['res'], b['odds'])
                out['kept']   += 1
                continue
            ratings = ratings_by_date.get(b['fecha'], {})
            ref_meta = ratings.get(b['ref'])
            if not ref_meta:
                # Árbitro sin histórico previo a esa fecha — usar 1.0
                factor = 1.0
            else:
                factor = ref_meta['factor'] ** alpha
            mu_new = b['mu_base'] * factor
            prob_new = prob_from_mu(mu_new, b['threshold'], b['lado'])
            edge_old = b['prob'] - 1.0 / b['odds']
            edge_new = prob_new - 1.0 / b['odds']

            out['edge_old_total'] += edge_old
            out['edge_new_total'] += edge_new

            if edge_new >= MIN_EDGE:
                out['bets'] += 1
                out[b['res']] += 1
                out['pnl']    += pnl(b['res'], b['odds'])
                out['kept']   += 1
            else:
                out['changed_to_skip'] += 1
        out['roi'] = out['pnl'] / out['bets'] if out['bets'] else 0
        results[alpha] = out

    return results, bets


# ─────────────────────────────────────────────────────────────────────────────
# Print
# ─────────────────────────────────────────────────────────────────────────────

def fmt_pct(x): return f'{x:+.1%}' if x is not None else '   -  '


def print_results(label, results):
    print(f'\n  {"-" * 78}')
    print(f'  RESULTADOS — {label}')
    print(f'  {"-" * 78}')
    base = results['baseline']
    print(f'  baseline (v3.2, sin ajuste):  N={base["bets"]:>3}  '
          f'W={base.get("W",0):>3}  L={base.get("L",0):>3}  '
          f'P&L={base["pnl"]:+7.2f}u  ROI={fmt_pct(base["roi"])}')

    print(f'\n  {"alpha":>6}  {"N":>4}  {"skip":>4}  {"W":>3}  {"L":>3}  '
          f'{"P&L":>8}  {"ROI":>7}  {"ΔROI":>7}  edgeAvg(old→new)')
    for alpha, out in sorted((k, v) for k, v in results.items()
                             if isinstance(k, float)):
        n = out['bets']
        droi = out['roi'] - base['roi'] if n else None
        e_old = out['edge_old_total'] / max(1, base['bets'])
        e_new = out['edge_new_total'] / max(1, base['bets'])
        print(f'  {alpha:>6.2f}  {n:>4}  {out["changed_to_skip"]:>4}  '
              f'{out.get("W",0):>3}  {out.get("L",0):>3}  '
              f'{out["pnl"]:>+7.2f}u  {fmt_pct(out["roi"]):>7}  '
              f'{fmt_pct(droi):>7}   {e_old:+.2%} → {e_new:+.2%}')


def main():
    args = sys.argv[1:]
    csvs = ['raw', 'cal', 'fil']
    if '--csv' in args:
        csvs = [args[args.index('--csv') + 1]]
    alphas = [0.3, 0.5, 0.7, 1.0]
    if '--alphas' in args:
        idx = args.index('--alphas')
        alphas = [float(x) for x in args[idx + 1:]]

    print('=' * 80)
    print(f'  BACKTEST v3.3 (referee adjustment) vs v3.2 (baseline) — Tarjetas')
    print(f'  K_REF={K_REF}, MIN_EDGE={MIN_EDGE:.0%}, alphas={alphas}')
    print('=' * 80)

    print('\n  Cargando histórico...', end=' ', flush=True)
    hist = load_history()
    fidx = fixture_index(hist)
    print(f'{len(hist)} partidos, {sum(1 for r in hist if r["ref"])} con referee')

    for tag in csvs:
        path = VB_PATHS[tag]
        if not path.exists():
            continue
        print(f'\n  ──── {tag.upper()}  ({path.name}) ────')
        results, _ = backtest_csv(path, hist, fidx, alphas)
        print_results(tag, results)


if __name__ == '__main__':
    main()
