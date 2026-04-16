"""
impacto_negbin.py
-----------------
Re-simula todas las apuestas de Tiros en value_bets.csv con el modelo NegBin
para medir el impacto vs el modelo Normal anterior.

Walk-forward: excluye el fixture evaluado de la historia.
Compara: que bets se hubieran hecho, cuales no, y ROI hipotetico.
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
    load_csv, compute_match_params, run_simulation,
    load_teams_db, load_leagues_db, resolve_team_id,
    MIN_EDGE,
)
from analizar_partido import compute_all_probs

N_SIM = 50_000
MIN_EDGE_THRESHOLD = MIN_EDGE  # 0.04


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
        # Individual: el nombre del equipo esta en el mercado
        # Ej: "Tiros Boca Juniors O/U 9.5" -> determinar si es local o visita
        for team_name in [home, away]:
            if team_name in m:
                prefix = 'sl' if team_name == home else 'sv'
                return f'{prefix}_{side}_{thr}'
    return None


def _sub_mercado(mercado, home, away):
    if 'tot.' in mercado:
        return 'Tiros totales'
    for team_name in [home, away]:
        if team_name in mercado:
            return 'Tiros local' if team_name == home else 'Tiros visita'
    return 'Tiros otro'


def main():
    print()
    print("=" * 78)
    print("  IMPACTO NEGBIN EN TIROS  -  Walk-forward sobre bets historicas")
    print("=" * 78)

    # Load value bets - solo Tiros con resultado
    all_bets = []
    with open(VB_CSV, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            all_bets.append(r)

    tiros_bets = [b for b in all_bets
                  if ('tiro' in b['mercado'].lower() or 'Tiros' in b['mercado'])
                  and b.get('resultado', '').strip().upper() in ('W', 'L')]

    print(f"  Total bets de Tiros con resultado: {len(tiros_bets)}")

    # Load history
    hist_all = load_csv(HIST_CSV)
    print(f"  Historico: {len(hist_all)} partidos")

    # Group bets by fixture
    by_fixture = defaultdict(list)
    for b in tiros_bets:
        by_fixture[b['fixture_id']].append(b)

    print(f"  Fixtures unicos: {len(by_fixture)}")
    print()

    # Results accumulators
    results_old = []  # (pnl, edge, ev, resultado, mercado, odds)
    results_nb = []   # same for new model
    filtered_out = 0  # bets that NegBin would NOT have flagged
    filtered_in = 0   # bets that NegBin would have flagged

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

        # Walk-forward: history without this fixture
        hist_sin = [r for r in hist_all if r['fixture_id'] != fid]

        try:
            params = compute_match_params(home, away, hist_sin, competicion)
            sim = run_simulation(params, N_SIM)
            probs = compute_all_probs(sim)
        except Exception as e:
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
            nb_ev = nb_prob * odds - 1

            old_edge = old_prob - implied_p
            old_ev = old_prob * odds - 1
            pnl = (odds - 1) if resultado == 'W' else -1.0

            sub = _sub_mercado(mercado, home, away)
            results_old.append({
                'mercado': mercado, 'odds': odds, 'resultado': resultado,
                'prob': old_prob, 'edge': old_edge, 'ev': old_ev, 'pnl': pnl,
                'sub': sub,
            })
            results_nb.append({
                'mercado': mercado, 'odds': odds, 'resultado': resultado,
                'prob': nb_prob, 'edge': nb_edge, 'ev': nb_ev, 'pnl': pnl,
                'sub': sub,
            })

            if nb_edge >= MIN_EDGE_THRESHOLD:
                filtered_in += 1
            else:
                filtered_out += 1

    # ── Report ────────────────────────────────────────────────────────────────
    n = len(results_old)
    print(f"  Bets analizadas: {n}  (errores/saltados: {errors})")
    print()

    if n == 0:
        print("  No hay datos para analizar.")
        return

    # Overall comparison
    total_pnl = sum(r['pnl'] for r in results_old)
    total_w = sum(1 for r in results_old if r['resultado'] == 'W')
    total_l = n - total_w
    roi_real = total_pnl / n

    old_avg_edge = sum(r['edge'] for r in results_old) / n
    nb_avg_edge = sum(r['edge'] for r in results_nb) / n
    old_avg_ev = sum(r['ev'] for r in results_old) / n
    nb_avg_ev = sum(r['ev'] for r in results_nb) / n

    print("=" * 78)
    print(f"  RESUMEN GLOBAL  -  {n} bets  ({total_w}W / {total_l}L)")
    print("=" * 78)
    print(f"  {'Metrica':<35s}  {'Normal (viejo)':>14s}  {'NegBin (nuevo)':>14s}")
    print(f"  {'-'*68}")
    print(f"  {'P&L real':<35s}  {total_pnl:>+13.2f}u  {total_pnl:>+13.2f}u")
    print(f"  {'ROI real':<35s}  {roi_real:>13.1%}  {roi_real:>13.1%}")
    print(f"  {'Edge promedio':<35s}  {old_avg_edge:>13.1%}  {nb_avg_edge:>13.1%}")
    print(f"  {'EV promedio':<35s}  {old_avg_ev:>13.1%}  {nb_avg_ev:>13.1%}")
    print()
    print(f"  (P&L y ROI real son iguales porque las apuestas ya se hicieron)")
    print()

    # Filtering effect
    print("=" * 78)
    print(f"  EFECTO FILTRO: que bets hubiera hecho NegBin? (edge >= {MIN_EDGE_THRESHOLD:.0%})")
    print("=" * 78)
    print(f"  NegBin SI hubiera apostado: {filtered_in}")
    print(f"  NegBin NO hubiera apostado: {filtered_out}")
    print()

    # ROI of the filtered set
    if filtered_in > 0:
        kept = [results_old[i] for i in range(n) if results_nb[i]['edge'] >= MIN_EDGE_THRESHOLD]
        kept_pnl = sum(r['pnl'] for r in kept)
        kept_w = sum(1 for r in kept if r['resultado'] == 'W')
        print(f"  ROI de las {filtered_in} bets que NegBin SI hubiera hecho:")
        print(f"    W/L: {kept_w}/{filtered_in - kept_w}  Hit%: {kept_w/filtered_in:.1%}")
        print(f"    P&L: {kept_pnl:+.2f}u   ROI: {kept_pnl/filtered_in:+.1%}")

    if filtered_out > 0:
        dropped = [results_old[i] for i in range(n) if results_nb[i]['edge'] < MIN_EDGE_THRESHOLD]
        dropped_pnl = sum(r['pnl'] for r in dropped)
        dropped_w = sum(1 for r in dropped if r['resultado'] == 'W')
        print(f"\n  ROI de las {filtered_out} bets que NegBin hubiera DESCARTADO:")
        print(f"    W/L: {dropped_w}/{filtered_out - dropped_w}  Hit%: {dropped_w/filtered_out:.1%}")
        print(f"    P&L: {dropped_pnl:+.2f}u   ROI: {dropped_pnl/filtered_out:+.1%}")

    print()

    # By market sub-type (total, local, visita)
    print("=" * 78)
    print("  DESGLOSE POR SUB-MERCADO")
    print("=" * 78)

    by_sub = defaultdict(lambda: {'old': [], 'nb': []})
    for i in range(n):
        sub = results_old[i].get('sub', 'Tiros otro')
        by_sub[sub]['old'].append(results_old[i])
        by_sub[sub]['nb'].append(results_nb[i])

    print(f"  {'Sub-mercado':<18s}  {'N':>4s}  {'W':>3s}  {'ROI real':>9s}  {'Kept':>4s}  {'Drop':>4s}  {'ROI kept':>9s}  {'ROI drop':>9s}")
    print(f"  {'-'*72}")

    for sub in ['Tiros totales', 'Tiros local', 'Tiros visita']:
        if sub not in by_sub:
            continue
        old_list = by_sub[sub]['old']
        nb_list = by_sub[sub]['nb']
        sn = len(old_list)
        sw = sum(1 for r in old_list if r['resultado'] == 'W')
        spnl = sum(r['pnl'] for r in old_list)
        sroi = spnl / sn if sn else 0

        kept_idx = [i for i in range(sn) if nb_list[i]['edge'] >= MIN_EDGE_THRESHOLD]
        drop_idx = [i for i in range(sn) if nb_list[i]['edge'] < MIN_EDGE_THRESHOLD]

        if kept_idx:
            kept_pnl = sum(old_list[i]['pnl'] for i in kept_idx)
            kept_roi = kept_pnl / len(kept_idx)
        else:
            kept_roi = 0.0

        if drop_idx:
            drop_pnl = sum(old_list[i]['pnl'] for i in drop_idx)
            drop_roi = drop_pnl / len(drop_idx)
        else:
            drop_roi = 0.0

        print(f"  {sub:<18s}  {sn:>4d}  {sw:>3d}  {sroi:>+8.1%}  {len(kept_idx):>4d}  {len(drop_idx):>4d}  {kept_roi:>+8.1%}  {drop_roi:>+8.1%}")

    # Distribution of edge shift
    print()
    print("=" * 78)
    print("  DISTRIBUCION DEL CAMBIO DE EDGE (NegBin - Normal)")
    print("=" * 78)

    deltas = [results_nb[i]['edge'] - results_old[i]['edge'] for i in range(n)]
    avg_delta = sum(deltas) / n
    print(f"  Delta edge promedio: {avg_delta:+.2%}")
    print(f"  (negativo = NegBin es mas conservador, reduce la confianza)")

    buckets = defaultdict(int)
    for d in deltas:
        bucket = round(d * 100 / 2) * 2  # 2pp buckets
        buckets[bucket] += 1

    print(f"\n  {'Delta edge (pp)':>16s}  {'N':>5s}  Histograma")
    for b in sorted(buckets.keys()):
        bar = '#' * (buckets[b] // 1)
        print(f"  {b:>+14d}pp  {buckets[b]:>5d}  {bar}")

    print()


if __name__ == '__main__':
    main()
