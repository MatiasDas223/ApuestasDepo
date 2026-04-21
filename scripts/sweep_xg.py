"""
sweep_xg.py
Walk-forward: prueba distintos valores de XG_BLEND para ratings de goles.
Compara ROI sobre los bets historicos.
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

CONFIGS = [
    ('goles_puro',   False, 0.0),
    ('xG_25',        True,  0.25),
    ('xG_50',        True,  0.50),
    ('xG_75',        True,  0.75),
    ('xG_85',        True,  0.85),
    ('xG_puro',      True,  1.0),
]
MIN_EDGE = 0.04
N_SIM    = 20_000


def load_resolved_bets():
    rows = []
    with open(VB_CSV, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            if r.get('resultado', '').strip().upper() in ('W', 'L'):
                rows.append(r)
    return rows


def run_one(use_xg, blend, bets_by_fixture, hist_all):
    modelo_v3.USE_XG_FOR_GOALS = use_xg
    modelo_v3.XG_BLEND = blend
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
        except Exception:
            continue

        for bet in bets:
            prob_key = _mercado_to_prob_key(bet['mercado'], bet['lado'], home, away)
            if prob_key is None or prob_key not in probs:
                continue
            new_prob = probs[prob_key]
            odds     = float(bet['odds'])
            new_edge = new_prob * odds - 1
            results.append({
                'categoria': bet.get('categoria', ''),
                'lado':      bet['lado'],
                'odds':      odds,
                'resultado': bet['resultado'].strip().upper(),
                'new_prob':  new_prob,
                'new_edge':  new_edge,
            })
    return results


def summarize(label, results):
    taken = [r for r in results if r['new_edge'] >= MIN_EDGE]
    if not taken:
        return {'label': label, 'N': 0, 'W': 0, 'L': 0, 'pnl': 0.0, 'roi': 0.0, 'taken': []}
    w = sum(1 for r in taken if r['resultado'] == 'W')
    l = sum(1 for r in taken if r['resultado'] == 'L')
    pnl = sum((r['odds']-1) if r['resultado']=='W' else -1 for r in taken)
    return {'label': label, 'N_total': len(results), 'N': len(taken),
            'W': w, 'L': l, 'pnl': pnl, 'roi': pnl/len(taken)*100, 'taken': taken}


def category_breakdown(taken):
    d = defaultdict(lambda: {'n':0,'w':0,'pnl':0.0})
    for r in taken:
        cat = r['categoria'] or 'Other'
        d[cat]['n'] += 1
        d[cat]['w'] += (r['resultado']=='W')
        d[cat]['pnl'] += (r['odds']-1) if r['resultado']=='W' else -1
    return dict(d)


def main():
    bets = load_resolved_bets()
    bets_by_fixture = defaultdict(list)
    for r in bets:
        bets_by_fixture[r['fixture_id']].append(r)
    print(f"Bets resueltas: {len(bets)} | fixtures: {len(bets_by_fixture)}")

    hist_all = load_csv(str(HIST_CSV))
    print(f"Historico: {len(hist_all)} partidos")

    print(f"\nSweep XG_BLEND: {[c[0] for c in CONFIGS]}")
    print(f"N_SIM={N_SIM}, MIN_EDGE={MIN_EDGE}\n")

    summaries = []
    takens = {}
    for label, use_xg, blend in CONFIGS:
        print(f"  [{label}] corriendo...")
        res = run_one(use_xg, blend, bets_by_fixture, hist_all)
        s = summarize(label, res)
        summaries.append(s); takens[label] = s['taken']
        print(f"    N={s['N']}  W={s['W']}  L={s['L']}  PnL={s['pnl']:+.2f}  ROI={s['roi']:+.1f}%")

    print(f"\n{'='*70}\n  RESUMEN GLOBAL\n{'='*70}")
    print(f"{'config':>12s}  {'N':>5s} {'W':>4s} {'L':>4s}  {'PnL':>8s}  {'ROI':>8s}")
    for s in summaries:
        print(f"{s['label']:>12s}  {s['N']:>5d} {s['W']:>4d} {s['L']:>4d}  {s['pnl']:>+8.2f}  {s['roi']:>+7.1f}%")

    print(f"\n{'='*70}\n  GOLES solo (categoria donde XG importa mas)\n{'='*70}")
    print(f"{'config':>12s}  {'N':>5s} {'W':>4s} {'L':>4s}  {'ROI':>8s}")
    for s in summaries:
        g = [t for t in s['taken'] if t['categoria']=='Goles']
        if not g: continue
        w = sum(1 for t in g if t['resultado']=='W'); l = len(g)-w
        pnl = sum((t['odds']-1) if t['resultado']=='W' else -1 for t in g)
        roi = pnl/len(g)*100
        print(f"{s['label']:>12s}  {len(g):>5d} {w:>4d} {l:>4d}  {roi:>+7.1f}%")

    print(f"\n{'='*70}\n  ROI por categoria\n{'='*70}")
    cats = set()
    cat_data = {}
    for s in summaries:
        cb = category_breakdown(s['taken'])
        cat_data[s['label']] = cb; cats.update(cb.keys())
    header = f"{'cat':>12s}" + ''.join(f"  {s['label']:>12s}" for s in summaries)
    print(header)
    for c in sorted(cats):
        row = f"{c:>12s}"
        for s in summaries:
            d = cat_data[s['label']].get(c, {'n':0,'pnl':0})
            if d['n'] == 0: row += f"  {'-':>12s}"
            else:
                roi = d['pnl']/d['n']*100
                row += f"  {d['n']:>3d}/{roi:>+6.1f}%"
        print(row)


if __name__ == '__main__':
    main()
