"""
sweep_k_shrink.py
Sweep K_SHRINK (shrinkage bayesiano hacia promedio de liga) via walk-forward.
"""
import sys
import csv
from pathlib import Path
from collections import defaultdict

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).parent))

import modelo_v3
from modelo_v3 import load_csv, compute_match_params, run_simulation, poisson_sample
from analizar_partido import compute_all_probs, compute_arco_params
from backtest_v3 import _mercado_to_prob_key

VB_CSV   = BASE / 'data/apuestas/value_bets.csv'
HIST_CSV = BASE / 'data/historico/partidos_historicos.csv'

K_VALUES = [4, 8, 12, 16, 24, 32]
MIN_EDGE = 0.04
N_SIM    = 20_000


def load_resolved_bets():
    rows = []
    with open(VB_CSV, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            if r.get('resultado', '').strip().upper() in ('W', 'L'):
                rows.append(r)
    return rows


def run_one_k(k_value, bets_by_fixture, hist_all):
    modelo_v3.K_SHRINK = k_value
    results = []
    for fid, bets in bets_by_fixture.items():
        sample = bets[0]
        partido = sample['partido']
        competicion = sample['competicion']
        parts = partido.split(' vs ', 1)
        if len(parts) != 2:
            continue
        home, away = parts[0].strip(), parts[1].strip()

        hist_sin = [r for r in hist_all if r['fixture_id'] != fid]
        if len(hist_sin) < 5:
            continue

        try:
            params = compute_match_params(home, away, hist_sin, competicion)
            sim = run_simulation(params, N_SIM)
            sim['team_local']  = home
            sim['team_visita'] = away
            try:
                arco_p = compute_arco_params(home, away, hist_sin, competicion)
                sim['sla_arco'] = [poisson_sample(arco_p['mu_arco_local']) for _ in range(N_SIM)]
                sim['sva_arco'] = [poisson_sample(arco_p['mu_arco_vis'])   for _ in range(N_SIM)]
                sim['arco_params'] = arco_p
            except Exception:
                pass
            probs = compute_all_probs(sim)
        except Exception as e:
            continue

        for bet in bets:
            prob_key = _mercado_to_prob_key(bet['mercado'], bet['lado'], home, away)
            if prob_key is None or prob_key not in probs:
                continue
            new_prob = probs[prob_key]
            odds     = float(bet['odds'])
            new_edge = new_prob * odds - 1
            results.append({
                'fid': fid,
                'categoria': bet.get('categoria', ''),
                'mercado':   bet['mercado'],
                'lado':      bet['lado'],
                'odds':      odds,
                'resultado': bet['resultado'].strip().upper(),
                'new_prob':  new_prob,
                'new_edge':  new_edge,
            })
    return results


def summarize(k, results):
    taken = [r for r in results if r['new_edge'] >= MIN_EDGE]
    if not taken:
        return {'k': k, 'N_total': 0, 'N_taken': 0, 'W': 0, 'L': 0, 'pnl': 0.0, 'roi': 0.0, 'taken': []}
    w = sum(1 for r in taken if r['resultado'] == 'W')
    l = sum(1 for r in taken if r['resultado'] == 'L')
    pnl = sum((r['odds']-1) if r['resultado']=='W' else -1 for r in taken)
    return {'k': k, 'N_total': len(results), 'N_taken': len(taken),
            'W': w, 'L': l, 'pnl': pnl, 'roi': pnl/len(taken)*100, 'taken': taken}


def category_breakdown(taken):
    by_cat = defaultdict(lambda: {'n':0,'w':0,'pnl':0.0})
    for r in taken:
        cat = r['categoria'] or 'Other'
        by_cat[cat]['n']   += 1
        by_cat[cat]['w']   += (r['resultado']=='W')
        by_cat[cat]['pnl'] += (r['odds']-1) if r['resultado']=='W' else -1
    return dict(by_cat)


def main():
    print(f"Cargando bets resueltas...")
    bets = load_resolved_bets()
    bets_by_fixture = defaultdict(list)
    for r in bets:
        bets_by_fixture[r['fixture_id']].append(r)
    print(f"  {len(bets)} bets | {len(bets_by_fixture)} fixtures")

    hist_all = load_csv(str(HIST_CSV))
    print(f"  {len(hist_all)} partidos historico")

    print(f"\nSweep K_SHRINK = {K_VALUES}")
    print(f"N_SIM={N_SIM}, MIN_EDGE={MIN_EDGE}\n")

    summaries = []
    all_taken = {}
    for k in K_VALUES:
        print(f"  [K={k}] corriendo...")
        results = run_one_k(k, bets_by_fixture, hist_all)
        s = summarize(k, results)
        summaries.append(s)
        all_taken[k] = s['taken']
        print(f"    N_taken={s['N_taken']}/{s['N_total']}  W={s['W']}  L={s['L']}  PnL={s['pnl']:+.2f}  ROI={s['roi']:+.1f}%")

    print(f"\n{'='*70}")
    print(f"  RESUMEN GLOBAL")
    print(f"{'='*70}")
    print(f"{'K':>4s}  {'N bets':>8s}  {'W':>4s}  {'L':>4s}  {'PnL':>8s}  {'ROI':>8s}")
    for s in summaries:
        print(f"{s['k']:>4d}  {s['N_taken']:>8d}  {s['W']:>4d}  {s['L']:>4d}  {s['pnl']:>+8.2f}  {s['roi']:>+7.1f}%")

    print(f"\n{'='*70}")
    print(f"  ROI por CATEGORIA x K_SHRINK")
    print(f"{'='*70}")
    all_cats = set()
    cat_data = {}
    for k in K_VALUES:
        cb = category_breakdown(all_taken[k])
        cat_data[k] = cb
        all_cats.update(cb.keys())

    header = f"{'Categoria':>12s}" + "".join(f"  K={k:<2d}          " for k in K_VALUES)
    print(header)
    for cat in sorted(all_cats):
        row = f"{cat:>12s}"
        for k in K_VALUES:
            d = cat_data[k].get(cat, {'n':0,'w':0,'pnl':0.0})
            roi = (d['pnl']/d['n']*100) if d['n'] else 0
            row += f"  {d['n']:>3d}/{d['w']:<3d} {roi:>+6.1f}%"
        print(row)


if __name__ == '__main__':
    main()
