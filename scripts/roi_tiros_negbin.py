"""
roi_tiros_negbin.py
-------------------
Re-simula TODAS las apuestas de tiros (value_bets.csv) con NegBin k-por-equipo.
Genera tablas de ROI por cuota, EV y probabilidad predicha,
separadas por mercado (Total, Local, Visita).
"""

import csv
import re
import sys
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


def _prob_key(mercado, lado, home, away):
    m = mercado.strip()
    is_over = lado.strip() in ('Over/Si', 'Si', 'Over')
    side = 'over' if is_over else 'under'
    match = re.search(r'O/U\s+([\d.]+)', m)
    thr = float(match.group(1)) if match else None
    if not m.startswith('Tiros') or thr is None:
        return None
    if 'tot.' in m:
        return f'ts_{side}_{thr}'
    for t in [home, away]:
        if t in m:
            return f'{"sl" if t == home else "sv"}_{side}_{thr}'
    return None


def _sub(mercado, home, away):
    if 'tot.' in mercado:
        return 'TOTAL'
    for t in [home, away]:
        if t in mercado:
            return 'LOCAL' if t == home else 'VISITA'
    return '?'


def _lado(s):
    return 'Over' if s.strip() in ('Over/Si', 'Si', 'Over') else 'Under'


# ─────────────────────────────────────────────────────────────────────────────
# Load & simulate
# ─────────────────────────────────────────────────────────────────────────────

def load_data():
    with open(VB_CSV, newline='', encoding='utf-8') as f:
        all_vb = list(csv.DictReader(f))

    tiros = [b for b in all_vb
             if b['mercado'].startswith('Tiros')
             and b.get('resultado', '').strip().upper() in ('W', 'L')]

    hist_all = load_csv(HIST_CSV)

    by_fixture = defaultdict(list)
    for b in tiros:
        by_fixture[b['fixture_id']].append(b)

    print(f"  Value bets de Tiros con resultado: {len(tiros)}  ({len(by_fixture)} fixtures)")
    print(f"  Simulando NegBin k-por-equipo (walk-forward)...\n")

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
            implied_p = _parse_prob(bet['implied_prob']) or (1.0 / odds)

            pk = _prob_key(mercado, lado, home, away)
            if pk is None or pk not in probs:
                continue

            nb_prob = probs[pk]
            nb_edge = nb_prob - implied_p
            nb_ev = nb_prob * odds - 1
            pnl = (odds - 1) if resultado == 'W' else -1.0

            results.append({
                'sub': _sub(mercado, home, away),
                'lado': _lado(lado),
                'odds': odds,
                'prob': nb_prob,
                'edge': nb_edge,
                'ev': nb_ev,
                'pnl': pnl,
                'win': resultado == 'W',
            })

    if errors:
        print(f"  ({errors} fixtures con error)\n")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Reporting helpers
# ─────────────────────────────────────────────────────────────────────────────

SEP = '-' * 74

def _table(rows, label_col, label_w=12):
    """Print a formatted table from list of dicts."""
    if not rows:
        print(f"  (sin datos)\n")
        return

    print(f"  {label_col:<{label_w}s}  {'N':>4s}  {'W':>3s}  {'L':>3s}  {'Hit%':>6s}  {'Odds':>6s}  {'Edge':>6s}  {'EV':>7s}  {'P&L':>8s}  {'ROI':>7s}")
    print(f"  {SEP}")

    for r in rows:
        n = r['n']
        if n == 0:
            continue
        w = r['w']
        hit = w / n
        avg_odds = r['sum_odds'] / n
        avg_edge = r['sum_edge'] / n
        avg_ev = r['sum_ev'] / n
        pnl = r['pnl']
        roi = pnl / n

        print(f"  {r['label']:<{label_w}s}  {n:>4d}  {w:>3d}  {n-w:>3d}  {hit:>5.1%}  {avg_odds:>6.2f}  {avg_edge:>+5.1%}  {avg_ev:>+6.1%}  {pnl:>+7.2f}u  {roi:>+6.1%}")

    # Total
    tn = sum(r['n'] for r in rows)
    tw = sum(r['w'] for r in rows)
    tp = sum(r['pnl'] for r in rows)
    if tn > 0:
        print(f"  {SEP}")
        to = sum(r['sum_odds'] for r in rows) / tn
        te = sum(r['sum_edge'] for r in rows) / tn
        tv = sum(r['sum_ev'] for r in rows) / tn
        print(f"  {'TOTAL':<{label_w}s}  {tn:>4d}  {tw:>3d}  {tn-tw:>3d}  {tw/tn:>5.1%}  {to:>6.2f}  {te:>+5.1%}  {tv:>+6.1%}  {tp:>+7.2f}u  {tp/tn:>+6.1%}")
    print()


def _bucket(data, key, bins):
    """Bucket data by key into bins, return table rows."""
    rows = []
    for label, lo, hi in bins:
        subset = [d for d in data if lo <= d[key] < hi]
        n = len(subset)
        rows.append({
            'label': label, 'n': n,
            'w': sum(d['win'] for d in subset),
            'pnl': sum(d['pnl'] for d in subset),
            'sum_odds': sum(d['odds'] for d in subset),
            'sum_edge': sum(d['edge'] for d in subset),
            'sum_ev': sum(d['ev'] for d in subset),
        })
    return rows


# ─────────────────────────────────────────────────────────────────────────────

CUOTA_BINS = [
    ('1.01-1.30', 1.01, 1.30),
    ('1.30-1.50', 1.30, 1.50),
    ('1.50-1.80', 1.50, 1.80),
    ('1.80-2.10', 1.80, 2.10),
    ('2.10-2.50', 2.10, 2.50),
    ('2.50-3.50', 2.50, 3.50),
    ('>3.50',     3.50, 999),
]

EV_BINS = [
    ('<-10%',    -999, -0.10),
    ('-10% a 0%', -0.10, 0.00),
    ('0% a 5%',  0.00, 0.05),
    ('5% a 10%', 0.05, 0.10),
    ('10% a 20%',0.10, 0.20),
    ('20% a 40%',0.20, 0.40),
    ('>40%',     0.40, 999),
]

PROB_BINS = [
    ('<20%',     0.00, 0.20),
    ('20-30%',   0.20, 0.30),
    ('30-40%',   0.30, 0.40),
    ('40-50%',   0.40, 0.50),
    ('50-60%',   0.50, 0.60),
    ('60-70%',   0.60, 0.70),
    ('70-80%',   0.70, 0.80),
    ('80-90%',   0.80, 0.90),
    ('>=90%',    0.90, 1.01),
]

EDGE_BINS = [
    ('<0%',      -999, 0.00),
    ('0-4%',     0.00, 0.04),
    ('4-8%',     0.04, 0.08),
    ('8-15%',    0.08, 0.15),
    ('15-25%',   0.15, 0.25),
    ('>25%',     0.25, 999),
]


def report_mercado(data, nombre):
    n = len(data)
    if n == 0:
        return
    w = sum(d['win'] for d in data)
    pnl = sum(d['pnl'] for d in data)

    print("=" * 78)
    print(f"  TIROS {nombre}")
    print(f"  {n} apuestas  ({w}W / {n-w}L)  P&L={pnl:+.2f}u  ROI={pnl/n:+.1%}")
    print("=" * 78)
    print()

    # A. ROI por cuota
    print(f"  A. ROI por rango de cuota")
    _table(_bucket(data, 'odds', CUOTA_BINS), 'Cuota')

    # B. ROI por EV
    print(f"  B. ROI por rango de EV predicho")
    _table(_bucket(data, 'ev', EV_BINS), 'EV predicho')

    # C. ROI por probabilidad predicha
    print(f"  C. ROI por rango de probabilidad NegBin")
    _table(_bucket(data, 'prob', PROB_BINS), 'Prob NegBin')

    # D. ROI por edge
    print(f"  D. ROI por rango de edge NegBin")
    _table(_bucket(data, 'edge', EDGE_BINS), 'Edge NB')

    # E. Over vs Under
    print(f"  E. Over vs Under")
    for lado in ['Over', 'Under']:
        ss = [d for d in data if d['lado'] == lado]
        if ss:
            sn = len(ss)
            sw = sum(d['win'] for d in ss)
            sp = sum(d['pnl'] for d in ss)
            print(f"    {lado:<6s}: N={sn:>3d}  W={sw:>2d}  Hit%={sw/sn:>5.1%}  P&L={sp:>+7.2f}u  ROI={sp/sn:>+6.1%}")
    print()


def main():
    print()
    print("=" * 78)
    print("  ROI TIROS NegBin (k por equipo) — value_bets re-simuladas")
    print("=" * 78)
    print()

    results = load_data()
    if not results:
        print("  Sin datos.")
        return

    # Separar por mercado
    total   = [r for r in results if r['sub'] == 'TOTAL']
    local   = [r for r in results if r['sub'] == 'LOCAL']
    visita  = [r for r in results if r['sub'] == 'VISITA']

    report_mercado(total,  'TOTAL')
    report_mercado(local,  'LOCAL')
    report_mercado(visita, 'VISITA')

    # Resumen comparativo
    print("=" * 78)
    print("  RESUMEN COMPARATIVO")
    print("=" * 78)
    print()
    print(f"  {'Mercado':<10s}  {'N':>4s}  {'W':>3s}  {'Hit%':>6s}  {'ROI':>7s}  {'P&L':>8s}  {'Edge avg':>9s}")
    print(f"  {SEP}")
    for nombre, data in [('TOTAL', total), ('LOCAL', local), ('VISITA', visita), ('TODOS', results)]:
        if not data:
            continue
        n = len(data)
        w = sum(d['win'] for d in data)
        pnl = sum(d['pnl'] for d in data)
        ae = sum(d['edge'] for d in data) / n
        print(f"  {nombre:<10s}  {n:>4d}  {w:>3d}  {w/n:>5.1%}  {pnl/n:>+6.1%}  {pnl:>+7.2f}u  {ae:>+8.1%}")
    print()


if __name__ == '__main__':
    main()
