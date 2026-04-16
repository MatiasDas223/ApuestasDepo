"""
calibracion_tiros_negbin.py
---------------------------
Walk-forward sobre TODAS las bets de Tiros en value_bets.csv:
re-simula con NegBin (k por equipo) y analiza calibracion y ROI.

Secciones:
  1. Calibracion: prob NegBin vs win rate real (por rango de prob)
  2. ROI por rango de cuota
  3. ROI por rango de edge NegBin
  4. Desglose por sub-mercado (total, local, visita)
  5. Desglose por lado (Over vs Under)
  6. Desglose por threshold
"""

import csv
import re
import sys
import math
from pathlib import Path
from collections import defaultdict

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE     = Path(__file__).resolve().parent.parent
VB_CSV   = BASE / 'data/apuestas/value_bets.csv'
HIST_CSV = BASE / 'data/historico/partidos_historicos.csv'

sys.path.insert(0, str(Path(__file__).parent))

from modelo_v3 import (
    load_csv, compute_match_params, run_simulation, MIN_EDGE,
)
from analizar_partido import compute_all_probs

N_SIM = 50_000


def _parse_prob(s):
    s = s.strip().replace('%', '').replace('+', '')
    if not s:
        return None
    v = float(s)
    return v / 100 if v > 1.5 else v


def _mercado_to_prob_key(mercado, lado, home, away):
    m = mercado.strip()
    l = lado.strip()
    is_over = l in ('Over/Si', 'Si', 'Over')
    side = 'over' if is_over else 'under'

    match = re.search(r'O/U\s+([\d.]+)', m)
    thr = float(match.group(1)) if match else None

    if m.startswith('Tiros') and thr is not None:
        if 'tot.' in m:
            return f'ts_{side}_{thr}'
        for team_name in [home, away]:
            if team_name in m:
                prefix = 'sl' if team_name == home else 'sv'
                return f'{prefix}_{side}_{thr}'
    return None


def _sub_mercado(mercado, home, away):
    if 'tot.' in mercado:
        return 'Totales'
    for team_name in [home, away]:
        if team_name in mercado:
            return 'Local' if team_name == home else 'Visita'
    return 'Otro'


def _lado(lado_str):
    l = lado_str.strip()
    if l in ('Over/Si', 'Si', 'Over'):
        return 'Over'
    return 'Under'


def _threshold(mercado):
    match = re.search(r'O/U\s+([\d.]+)', mercado)
    return float(match.group(1)) if match else None


# ─────────────────────────────────────────────────────────────────────────────
# Carga y simulacion
# ─────────────────────────────────────────────────────────────────────────────

def load_and_simulate():
    all_bets = []
    with open(VB_CSV, newline='', encoding='utf-8') as f:
        all_bets = list(csv.DictReader(f))

    tiros_bets = [b for b in all_bets
                  if ('tiro' in b['mercado'].lower() or 'Tiros' in b['mercado'])
                  and b.get('resultado', '').strip().upper() in ('W', 'L')]

    hist_all = load_csv(HIST_CSV)

    by_fixture = defaultdict(list)
    for b in tiros_bets:
        by_fixture[b['fixture_id']].append(b)

    print(f"  Fixtures: {len(by_fixture)}   Bets de Tiros con resultado: {len(tiros_bets)}")
    print(f"  Simulando con NegBin k-por-equipo (walk-forward)...\n")

    results = []
    errors = 0

    for fid, bets in sorted(by_fixture.items()):
        sample = bets[0]
        partido = sample['partido']
        competicion = sample['competicion']
        parts = partido.split(' vs ', 1)
        if len(parts) != 2:
            errors += 1
            continue
        home, away = parts[0].strip(), parts[1].strip()
        hist_sin = [r for r in hist_all if r['fixture_id'] != fid]

        try:
            params = compute_match_params(home, away, hist_sin, competicion)
            sim = run_simulation(params, N_SIM)
            probs = compute_all_probs(sim)
        except Exception:
            errors += 1
            continue

        for bet in bets:
            mercado = bet['mercado']
            lado = bet['lado']
            odds = float(bet['odds'])
            resultado = bet['resultado'].strip().upper()
            old_prob = _parse_prob(bet['modelo_prob']) or 0
            implied_p = _parse_prob(bet['implied_prob']) or (1.0 / odds)

            prob_key = _mercado_to_prob_key(mercado, lado, home, away)
            if prob_key is None or prob_key not in probs:
                continue

            nb_prob = probs[prob_key]
            nb_edge = nb_prob - implied_p
            pnl = (odds - 1) if resultado == 'W' else -1.0
            is_win = resultado == 'W'

            results.append({
                'mercado': mercado, 'lado': _lado(lado),
                'sub': _sub_mercado(mercado, home, away),
                'threshold': _threshold(mercado),
                'odds': odds,
                'old_prob': old_prob,
                'nb_prob': nb_prob,
                'implied_p': implied_p,
                'nb_edge': nb_edge,
                'pnl': pnl,
                'is_win': is_win,
                'k_local': params['k_shots_local'],
                'k_vis': params['k_shots_vis'],
            })

    if errors:
        print(f"  ({errors} fixtures con error, omitidos)\n")

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Reportes
# ─────────────────────────────────────────────────────────────────────────────

def _print_table(title, rows_data, cols, col_fmts):
    print(f"  {title}")
    header = '  '.join(f'{c:>{w}}' for c, w in cols)
    print(f"  {header}")
    print(f"  {'-' * len(header)}")
    for row in rows_data:
        line = '  '.join(fmt(row.get(c[0], '')) for (c, _), fmt in zip(cols, col_fmts))
        print(f"  {line}")
    print()


def report_calibracion(results):
    print("=" * 78)
    print("  1. CALIBRACION: prob NegBin vs win rate real")
    print("=" * 78)
    print()

    bins = [
        ('<0.30',    lambda p: p < 0.30),
        ('0.30-0.40', lambda p: 0.30 <= p < 0.40),
        ('0.40-0.50', lambda p: 0.40 <= p < 0.50),
        ('0.50-0.60', lambda p: 0.50 <= p < 0.60),
        ('0.60-0.70', lambda p: 0.60 <= p < 0.70),
        ('0.70-0.80', lambda p: 0.70 <= p < 0.80),
        ('>=0.80',    lambda p: p >= 0.80),
    ]

    print(f"  {'Rango prob':<12s}  {'N':>4s}  {'Win%':>6s}  {'Prob NB':>8s}  {'Prob old':>8s}  {'Delta NB':>9s}  {'ROI':>7s}  {'P&L':>8s}")
    print(f"  {'-'*72}")

    for label, fn in bins:
        subset = [r for r in results if fn(r['nb_prob'])]
        if not subset:
            continue
        n = len(subset)
        win_rate = sum(r['is_win'] for r in subset) / n
        avg_nb = sum(r['nb_prob'] for r in subset) / n
        avg_old = sum(r['old_prob'] for r in subset) / n
        delta = win_rate - avg_nb
        pnl = sum(r['pnl'] for r in subset)
        roi = pnl / n

        flag = ' <<' if abs(delta) > 0.05 else ''
        print(f"  {label:<12s}  {n:>4d}  {win_rate:>5.1%}  {avg_nb:>7.1%}  {avg_old:>7.1%}  {delta:>+8.1%}  {roi:>+6.1%}  {pnl:>+7.2f}u{flag}")

    print()

    # Same but for old model
    print(f"  Comparacion: calibracion del modelo ANTERIOR (Normal)")
    print(f"  {'Rango prob':<12s}  {'N':>4s}  {'Win%':>6s}  {'Prob old':>8s}  {'Delta old':>9s}")
    print(f"  {'-'*50}")
    for label, fn in bins:
        # Use old prob ranges
        fn_old = lambda p, lo=float(label.split('-')[0].replace('<','0').replace('>=','')) if '-' in label or '>=' in label else 0, \
                        hi=float(label.split('-')[1]) if '-' in label else (0.30 if '<' in label else 1.01): lo <= p < hi
        subset_old = [r for r in results if fn(r['old_prob'])]
        if not subset_old:
            continue
        n = len(subset_old)
        win_rate = sum(r['is_win'] for r in subset_old) / n
        avg_old = sum(r['old_prob'] for r in subset_old) / n
        delta = win_rate - avg_old
        flag = ' <<' if abs(delta) > 0.05 else ''
        print(f"  {label:<12s}  {n:>4d}  {win_rate:>5.1%}  {avg_old:>7.1%}  {delta:>+8.1%}{flag}")

    print()


def report_cuotas(results):
    print("=" * 78)
    print("  2. ROI POR RANGO DE CUOTA")
    print("=" * 78)
    print()

    bins = [
        ('1.10-1.40', 1.10, 1.40),
        ('1.40-1.70', 1.40, 1.70),
        ('1.70-2.00', 1.70, 2.00),
        ('2.00-2.50', 2.00, 2.50),
        ('2.50-3.50', 2.50, 3.50),
        ('>3.50',     3.50, 999),
    ]

    print(f"  {'Rango cuota':<12s}  {'N':>4s}  {'W':>3s}  {'L':>3s}  {'Hit%':>6s}  {'Odds avg':>8s}  {'Edge NB':>8s}  {'P&L':>8s}  {'ROI':>7s}")
    print(f"  {'-'*72}")

    for label, lo, hi in bins:
        subset = [r for r in results if lo <= r['odds'] < hi]
        if not subset:
            continue
        n = len(subset)
        w = sum(r['is_win'] for r in subset)
        pnl = sum(r['pnl'] for r in subset)
        roi = pnl / n
        avg_odds = sum(r['odds'] for r in subset) / n
        avg_edge = sum(r['nb_edge'] for r in subset) / n

        print(f"  {label:<12s}  {n:>4d}  {w:>3d}  {n-w:>3d}  {w/n:>5.1%}  {avg_odds:>8.2f}  {avg_edge:>+7.1%}  {pnl:>+7.2f}u  {roi:>+6.1%}")

    # NegBin filtered
    print()
    print(f"  Solo bets donde NegBin edge >= 4%:")
    print(f"  {'Rango cuota':<12s}  {'N':>4s}  {'W':>3s}  {'L':>3s}  {'Hit%':>6s}  {'P&L':>8s}  {'ROI':>7s}")
    print(f"  {'-'*55}")

    for label, lo, hi in bins:
        subset = [r for r in results if lo <= r['odds'] < hi and r['nb_edge'] >= MIN_EDGE]
        if not subset:
            continue
        n = len(subset)
        w = sum(r['is_win'] for r in subset)
        pnl = sum(r['pnl'] for r in subset)
        roi = pnl / n
        print(f"  {label:<12s}  {n:>4d}  {w:>3d}  {n-w:>3d}  {w/n:>5.1%}  {pnl:>+7.2f}u  {roi:>+6.1%}")

    print()


def report_edge(results):
    print("=" * 78)
    print("  3. ROI POR RANGO DE EDGE NegBin")
    print("=" * 78)
    print()

    bins = [
        ('<0%',    -999, 0.00),
        ('0-4%',   0.00, 0.04),
        ('4-8%',   0.04, 0.08),
        ('8-12%',  0.08, 0.12),
        ('12-20%', 0.12, 0.20),
        ('>20%',   0.20, 999),
    ]

    print(f"  {'Edge NB':<10s}  {'N':>4s}  {'W':>3s}  {'L':>3s}  {'Hit%':>6s}  {'Odds':>6s}  {'P&L':>8s}  {'ROI':>7s}  {'Hubiera apostado?'}")
    print(f"  {'-'*72}")

    for label, lo, hi in bins:
        subset = [r for r in results if lo <= r['nb_edge'] < hi]
        if not subset:
            continue
        n = len(subset)
        w = sum(r['is_win'] for r in subset)
        pnl = sum(r['pnl'] for r in subset)
        roi = pnl / n
        avg_odds = sum(r['odds'] for r in subset) / n
        apuesta = 'SI' if lo >= 0.04 else 'NO'

        print(f"  {label:<10s}  {n:>4d}  {w:>3d}  {n-w:>3d}  {w/n:>5.1%}  {avg_odds:>6.2f}  {pnl:>+7.2f}u  {roi:>+6.1%}  {apuesta}")

    print()


def report_sub_mercado(results):
    print("=" * 78)
    print("  4. DESGLOSE POR SUB-MERCADO (NegBin edge >= 4%)")
    print("=" * 78)
    print()

    for sub in ['Totales', 'Local', 'Visita']:
        subset_all = [r for r in results if r['sub'] == sub]
        subset_nb = [r for r in subset_all if r['nb_edge'] >= MIN_EDGE]

        if not subset_all:
            continue

        n_all = len(subset_all)
        pnl_all = sum(r['pnl'] for r in subset_all)
        w_all = sum(r['is_win'] for r in subset_all)

        print(f"  --- Tiros {sub} ---")
        print(f"  Todas las bets originales: N={n_all}  W={w_all}  P&L={pnl_all:+.2f}u  ROI={pnl_all/n_all:+.1%}")

        if subset_nb:
            n_nb = len(subset_nb)
            pnl_nb = sum(r['pnl'] for r in subset_nb)
            w_nb = sum(r['is_win'] for r in subset_nb)
            print(f"  Con filtro NegBin:         N={n_nb}  W={w_nb}  P&L={pnl_nb:+.2f}u  ROI={pnl_nb/n_nb:+.1%}")
        else:
            print(f"  Con filtro NegBin:         N=0")

        # Calibracion por sub
        print(f"  Calibracion NegBin:")
        cal_bins = [
            ('<0.40',    lambda p: p < 0.40),
            ('0.40-0.55', lambda p: 0.40 <= p < 0.55),
            ('0.55-0.70', lambda p: 0.55 <= p < 0.70),
            ('>=0.70',    lambda p: p >= 0.70),
        ]
        print(f"    {'Rango':>10s}  {'N':>4s}  {'Win%':>6s}  {'Prob NB':>8s}  {'Delta':>7s}  {'ROI':>7s}")
        for label, fn in cal_bins:
            ss = [r for r in subset_all if fn(r['nb_prob'])]
            if not ss:
                continue
            sn = len(ss)
            wr = sum(r['is_win'] for r in ss) / sn
            ap = sum(r['nb_prob'] for r in ss) / sn
            roi_s = sum(r['pnl'] for r in ss) / sn
            flag = ' <<' if abs(wr - ap) > 0.05 else ''
            print(f"    {label:>10s}  {sn:>4d}  {wr:>5.1%}  {ap:>7.1%}  {wr-ap:>+6.1%}  {roi_s:>+6.1%}{flag}")

        print()


def report_lado(results):
    print("=" * 78)
    print("  5. OVER vs UNDER")
    print("=" * 78)
    print()

    for lado in ['Over', 'Under']:
        subset = [r for r in results if r['lado'] == lado]
        if not subset:
            continue
        n = len(subset)
        w = sum(r['is_win'] for r in subset)
        pnl = sum(r['pnl'] for r in subset)

        nb_kept = [r for r in subset if r['nb_edge'] >= MIN_EDGE]
        if nb_kept:
            nk = len(nb_kept)
            wk = sum(r['is_win'] for r in nb_kept)
            pk = sum(r['pnl'] for r in nb_kept)
            print(f"  {lado}:  All: N={n} W={w} ROI={pnl/n:+.1%} P&L={pnl:+.2f}u  |  NB kept: N={nk} W={wk} ROI={pk/nk:+.1%} P&L={pk:+.2f}u")
        else:
            print(f"  {lado}:  All: N={n} W={w} ROI={pnl/n:+.1%} P&L={pnl:+.2f}u  |  NB kept: N=0")

    print()


def report_threshold(results):
    print("=" * 78)
    print("  6. POR THRESHOLD (todas las bets)")
    print("=" * 78)
    print()

    thresholds = sorted(set(r['threshold'] for r in results if r['threshold'] is not None))

    print(f"  {'Thr':>6s}  {'N':>4s}  {'W':>3s}  {'Hit%':>6s}  {'ROI all':>8s}  {'N kept':>6s}  {'ROI kept':>8s}  {'Sub-mercados'}")
    print(f"  {'-'*72}")

    for thr in thresholds:
        subset = [r for r in results if r['threshold'] == thr]
        if not subset:
            continue
        n = len(subset)
        w = sum(r['is_win'] for r in subset)
        pnl = sum(r['pnl'] for r in subset)

        kept = [r for r in subset if r['nb_edge'] >= MIN_EDGE]
        nk = len(kept)
        pk = sum(r['pnl'] for r in kept) if kept else 0
        roi_k = pk / nk if nk else 0

        subs = defaultdict(int)
        for r in subset:
            subs[r['sub']] += 1
        sub_str = ', '.join(f'{s}:{c}' for s, c in sorted(subs.items(), key=lambda x: -x[1]))

        print(f"  {thr:>6.1f}  {n:>4d}  {w:>3d}  {w/n:>5.1%}  {pnl/n:>+7.1%}  {nk:>6d}  {roi_k:>+7.1%}  {sub_str}")

    print()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 78)
    print("  CALIBRACION TIROS NegBin (k por equipo)")
    print("=" * 78)
    print()

    results = load_and_simulate()
    n = len(results)
    if n == 0:
        print("  Sin datos.")
        return

    w = sum(r['is_win'] for r in results)
    pnl = sum(r['pnl'] for r in results)
    print(f"  Total: {n} bets  ({w}W / {n-w}L)  P&L={pnl:+.2f}u  ROI={pnl/n:+.1%}")
    print()

    report_calibracion(results)
    report_cuotas(results)
    report_edge(results)
    report_sub_mercado(results)
    report_lado(results)
    report_threshold(results)


if __name__ == '__main__':
    main()
