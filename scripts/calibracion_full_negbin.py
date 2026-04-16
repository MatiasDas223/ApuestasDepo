"""
calibracion_full_negbin.py
--------------------------
Walk-forward sobre TODOS los partidos del historico:
  1. Para cada fixture, computa NegBin probs (excluyendo ese fixture)
  2. Resuelve W/L contra tiros reales
  3. Calibracion SIN sesgo de seleccion (todos los pronosticos, no solo value bets)
  4. Simula deteccion de value bets usando las odds de value_bets.csv donde estan disponibles

Esto da la foto MAS limpia de como calibra el modelo NegBin.
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
HIST_CSV = BASE / 'data/historico/partidos_historicos.csv'
VB_CSV   = BASE / 'data/apuestas/value_bets.csv'

sys.path.insert(0, str(Path(__file__).parent))

from modelo_v3 import (
    load_csv, compute_match_params, run_simulation,
    load_teams_db, load_leagues_db, MIN_EDGE,
)
from analizar_partido import compute_all_probs

N_SIM = 30_000
MIN_HISTORY_MATCHES = 5  # min partidos de cada equipo para generar pronostico

# Thresholds de tiros a evaluar
THRESHOLDS_TOTAL = [16.5, 18.5, 20.5, 22.5, 24.5, 26.5, 28.5]
THRESHOLDS_INDIV = [5.5, 7.5, 9.5, 11.5, 13.5, 15.5]


def safe_int(v, default=0):
    if v in (None, '', '-'):
        return default
    try:
        return int(float(v))
    except:
        return default


def load_vb_odds():
    """Carga odds de value_bets.csv indexadas por (fixture_id, mercado, lado)."""
    odds_map = {}
    with open(VB_CSV, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            if not r['mercado'].startswith('Tiros'):
                continue
            key = (r['fixture_id'], r['mercado'], r['lado'])
            try:
                odds_map[key] = float(r['odds'])
            except:
                pass
    return odds_map


def main():
    print()
    print("=" * 78)
    print("  CALIBRACION FULL NegBin (k por equipo) — TODOS los partidos")
    print("=" * 78)
    print()

    hist_all = load_csv(HIST_CSV)
    print(f"  Historico: {len(hist_all)} partidos")

    # Pre-count: how many matches per team
    team_counts = defaultdict(int)
    for r in hist_all:
        team_counts[int(r['equipo_local_id'])] += 1
        team_counts[int(r['equipo_visitante_id'])] += 1

    equipos_by_id, _ = load_teams_db()
    ligas_by_id, _ = load_leagues_db()

    # Collect all predictions
    all_preds = []
    fixtures_ok = 0
    fixtures_skip = 0
    fixtures_err = 0

    # Sort by date to process chronologically
    hist_sorted = sorted(hist_all, key=lambda r: r['fecha'])

    for idx, match in enumerate(hist_sorted):
        fid = match['fixture_id']
        hid = int(match['equipo_local_id'])
        aid = int(match['equipo_visitante_id'])

        # Skip if teams don't have enough history
        if team_counts.get(hid, 0) < MIN_HISTORY_MATCHES + 1:
            fixtures_skip += 1
            continue
        if team_counts.get(aid, 0) < MIN_HISTORY_MATCHES + 1:
            fixtures_skip += 1
            continue

        # Actual shots
        sl_real = safe_int(match['tiros_local'])
        sv_real = safe_int(match['tiros_visitante'])
        if sl_real + sv_real == 0:
            fixtures_skip += 1
            continue

        st_real = sl_real + sv_real

        # Team names
        home = equipos_by_id.get(hid)
        away = equipos_by_id.get(aid)
        if not home or not away:
            fixtures_skip += 1
            continue

        liga = ligas_by_id.get(int(match['liga_id']), '?')

        # Walk-forward: history without this fixture
        hist_sin = [r for r in hist_all if r['fixture_id'] != fid]

        try:
            params = compute_match_params(home, away, hist_sin, liga)
            sim = run_simulation(params, N_SIM)
            probs = compute_all_probs(sim)
        except Exception:
            fixtures_err += 1
            continue

        fixtures_ok += 1

        # Generate predictions for all thresholds
        # Totales
        for thr in THRESHOLDS_TOTAL:
            key_o = f'ts_over_{thr}'
            key_u = f'ts_under_{thr}'
            if key_o in probs:
                actual_over = st_real > thr
                all_preds.append({
                    'fid': fid, 'tipo': 'Total', 'thr': thr,
                    'lado': 'Over', 'prob': probs[key_o],
                    'win': actual_over, 'home': home, 'away': away,
                })
                all_preds.append({
                    'fid': fid, 'tipo': 'Total', 'thr': thr,
                    'lado': 'Under', 'prob': probs[key_u],
                    'win': not actual_over, 'home': home, 'away': away,
                })

        # Local
        for thr in THRESHOLDS_INDIV:
            key_o = f'sl_over_{thr}'
            key_u = f'sl_under_{thr}'
            if key_o in probs:
                actual_over = sl_real > thr
                all_preds.append({
                    'fid': fid, 'tipo': 'Local', 'thr': thr,
                    'lado': 'Over', 'prob': probs[key_o],
                    'win': actual_over, 'home': home, 'away': away,
                })
                all_preds.append({
                    'fid': fid, 'tipo': 'Local', 'thr': thr,
                    'lado': 'Under', 'prob': probs[key_u],
                    'win': not actual_over, 'home': home, 'away': away,
                })

        # Visita
        for thr in THRESHOLDS_INDIV:
            key_o = f'sv_over_{thr}'
            key_u = f'sv_under_{thr}'
            if key_o in probs:
                actual_over = sv_real > thr
                all_preds.append({
                    'fid': fid, 'tipo': 'Visita', 'thr': thr,
                    'lado': 'Over', 'prob': probs[key_o],
                    'win': actual_over, 'home': home, 'away': away,
                })
                all_preds.append({
                    'fid': fid, 'tipo': 'Visita', 'thr': thr,
                    'lado': 'Under', 'prob': probs[key_u],
                    'win': not actual_over, 'home': home, 'away': away,
                })

        if fixtures_ok % 50 == 0:
            print(f"  ... {fixtures_ok} fixtures procesados ({len(all_preds)} predicciones)", flush=True)

    print(f"\n  Fixtures OK: {fixtures_ok}  Skip: {fixtures_skip}  Error: {fixtures_err}")
    print(f"  Predicciones totales: {len(all_preds)}")
    print()

    if not all_preds:
        print("  Sin datos.")
        return

    # ══════════════════════════════════════════════════════════════════════════
    # SECCION 1: CALIBRACION GLOBAL (todos los pronosticos)
    # ══════════════════════════════════════════════════════════════════════════
    print("=" * 78)
    print("  1. CALIBRACION GLOBAL — prob NegBin vs win rate real")
    print(f"     (TODOS los pronosticos, sin sesgo de seleccion)")
    print("=" * 78)
    print()

    bins = [
        ('0-10%',     0.00, 0.10),
        ('10-20%',    0.10, 0.20),
        ('20-30%',    0.20, 0.30),
        ('30-40%',    0.30, 0.40),
        ('40-50%',    0.40, 0.50),
        ('50-60%',    0.50, 0.60),
        ('60-70%',    0.60, 0.70),
        ('70-80%',    0.70, 0.80),
        ('80-90%',    0.80, 0.90),
        ('90-100%',   0.90, 1.01),
    ]

    print(f"  {'Rango prob':<10s}  {'N':>6s}  {'Win%':>6s}  {'Prob NB':>8s}  {'Delta':>7s}")
    print(f"  {'-'*45}")

    for label, lo, hi in bins:
        subset = [p for p in all_preds if lo <= p['prob'] < hi]
        if not subset:
            continue
        n = len(subset)
        wr = sum(p['win'] for p in subset) / n
        avg_p = sum(p['prob'] for p in subset) / n
        delta = wr - avg_p
        flag = ' <<' if abs(delta) > 0.05 else (' *' if abs(delta) > 0.03 else '')
        print(f"  {label:<10s}  {n:>6d}  {wr:>5.1%}  {avg_p:>7.1%}  {delta:>+6.1%}{flag}")

    # ══════════════════════════════════════════════════════════════════════════
    # SECCION 2: CALIBRACION POR SUB-MERCADO
    # ══════════════════════════════════════════════════════════════════════════
    print()
    print("=" * 78)
    print("  2. CALIBRACION POR SUB-MERCADO")
    print("=" * 78)
    print()

    for tipo in ['Total', 'Local', 'Visita']:
        subset_tipo = [p for p in all_preds if p['tipo'] == tipo]
        if not subset_tipo:
            continue

        n_total = len(subset_tipo)
        wr_total = sum(p['win'] for p in subset_tipo) / n_total

        print(f"  --- {tipo} (N={n_total}) ---")
        print(f"  {'Rango prob':<10s}  {'N':>6s}  {'Win%':>6s}  {'Prob NB':>8s}  {'Delta':>7s}")
        print(f"  {'-'*45}")

        for label, lo, hi in bins:
            subset = [p for p in subset_tipo if lo <= p['prob'] < hi]
            if len(subset) < 5:
                continue
            n = len(subset)
            wr = sum(p['win'] for p in subset) / n
            avg_p = sum(p['prob'] for p in subset) / n
            delta = wr - avg_p
            flag = ' <<' if abs(delta) > 0.05 else ''
            print(f"  {label:<10s}  {n:>6d}  {wr:>5.1%}  {avg_p:>7.1%}  {delta:>+6.1%}{flag}")
        print()

    # ══════════════════════════════════════════════════════════════════════════
    # SECCION 3: SIMULACION DE VALUE BETS
    # Asumimos cuota justa = 1/prob_modelo. Si edge > 4% sobre cuota de mercado,
    # es value bet. Usamos cuotas reales donde estan disponibles (value_bets.csv).
    # Para el resto, simulamos con implied_p = prob_modelo (sin edge, baseline).
    # ══════════════════════════════════════════════════════════════════════════
    print("=" * 78)
    print("  3. VALUE BET SIMULATION — si tuvieramos cuotas para todos")
    print("     (usando cuota implicita = 1/prob como proxy donde no hay odds reales)")
    print("=" * 78)
    print()

    # Para cada prediccion, calcular "fair odds" y "model edge"
    # Edge = prob_modelo - prob_implied
    # Si no tenemos odds reales, la cuota de mercado tipicamente lleva 5-8% de vig
    # Simulamos: cuota mercado = 1 / (prob_real * (1 + margen))
    # Margen tipico Bet365 para tiros: ~6%

    MARGEN_BOOK = 0.06   # 6% overround por lado

    for p in all_preds:
        # Cuota simulada que el mercado ofreceria
        # Si el evento tiene prob real P, el book ofrece ~1/(P * (1+margin))
        # Esto da una cuota peor que la justa
        actual_p = 1.0 if p['win'] else 0.0  # We don't know market's true prob
        # Use the complementary approach: assume market is perfectly calibrated
        # market_prob = actual historical win rate for that range
        # We'll compute edge as: model_prob - (1 - model_prob) only if one side
        # Actually, let's just bucket by model prob and check if high-prob bets win more
        pass

    # Better approach: for each prediction, check if betting at fair odds
    # (1/prob_modelo) minus vig would have been profitable

    # Even better: simulate what happens if we bet ONLY when model says > X%
    # and the fair odds would be ~ 1/real_win_rate

    print(f"  Estrategia: apostar cuando prob NegBin > threshold, a cuota 1/(prob*(1+{MARGEN_BOOK:.0%}))")
    print(f"  Esto simula apostar contra un mercado con {MARGEN_BOOK:.0%} de vig por lado")
    print()

    # For each prob threshold, show hypothetical ROI
    thresholds = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90]
    print(f"  {'Min prob':<9s}  {'N':>6s}  {'Win%':>6s}  {'Prob avg':>8s}  {'Delta':>7s}  {'ROI sim':>8s}")
    print(f"  {'-'*55}")

    for min_p in thresholds:
        subset = [p for p in all_preds if p['prob'] >= min_p]
        if len(subset) < 10:
            continue
        n = len(subset)
        wr = sum(p['win'] for p in subset) / n
        avg_p = sum(p['prob'] for p in subset) / n

        # Simulated ROI: if market offers 1/(p * 1.06), our P&L per bet is:
        # Win: odds - 1 = 1/(p*1.06) - 1
        # Loss: -1
        # E[PNL] = wr * (1/(avg_p*1.06) - 1) + (1-wr)*(-1)
        sim_odds = 1.0 / (avg_p * (1 + MARGEN_BOOK))
        sim_pnl = wr * (sim_odds - 1) + (1 - wr) * (-1)

        print(f"  >={min_p:<7.0%}  {n:>6d}  {wr:>5.1%}  {avg_p:>7.1%}  {wr-avg_p:>+6.1%}  {sim_pnl:>+7.1%}")

    print()

    # ══════════════════════════════════════════════════════════════════════════
    # SECCION 4: POR THRESHOLD DE TIROS
    # ══════════════════════════════════════════════════════════════════════════
    print("=" * 78)
    print("  4. CALIBRACION POR THRESHOLD")
    print("=" * 78)
    print()

    for tipo in ['Total', 'Local', 'Visita']:
        thrs = THRESHOLDS_TOTAL if tipo == 'Total' else THRESHOLDS_INDIV
        print(f"  --- {tipo} ---")
        print(f"  {'Thr':>6s}  {'N Over':>7s}  {'Win% O':>7s}  {'Prob O':>7s}  {'Delta O':>8s}  {'N Under':>7s}  {'Win% U':>7s}  {'Prob U':>7s}  {'Delta U':>8s}")
        print(f"  {'-'*80}")

        for thr in thrs:
            overs = [p for p in all_preds if p['tipo'] == tipo and p['thr'] == thr and p['lado'] == 'Over']
            unders = [p for p in all_preds if p['tipo'] == tipo and p['thr'] == thr and p['lado'] == 'Under']

            if not overs:
                continue

            no = len(overs)
            wro = sum(p['win'] for p in overs) / no
            po = sum(p['prob'] for p in overs) / no

            nu = len(unders)
            wru = sum(p['win'] for p in unders) / nu
            pu = sum(p['prob'] for p in unders) / nu

            flag_o = ' <<' if abs(wro - po) > 0.05 else ''
            flag_u = ' <<' if abs(wru - pu) > 0.05 else ''

            print(f"  {thr:>6.1f}  {no:>7d}  {wro:>6.1%}  {po:>6.1%}  {wro-po:>+7.1%}{flag_o:3s}  {nu:>7d}  {wru:>6.1%}  {pu:>6.1%}  {wru-pu:>+7.1%}{flag_u}")

        print()

    # ══════════════════════════════════════════════════════════════════════════
    # SECCION 5: SOLO LAS VALUE BETS REALES (con odds y resultado)
    # ══════════════════════════════════════════════════════════════════════════
    print("=" * 78)
    print("  5. VALUE BETS REALES (odds de value_bets.csv, NegBin re-simulado)")
    print("=" * 78)
    print()

    vb_odds = load_vb_odds()

    # Match predictions with value_bets odds
    vb_results = []
    for p in all_preds:
        # Build mercado string to match value_bets format
        if p['tipo'] == 'Total':
            mercado = f"Tiros tot. O/U {p['thr']}"
        elif p['tipo'] == 'Local':
            mercado = f"Tiros {p['home']} O/U {p['thr']}"
        else:
            mercado = f"Tiros {p['away']} O/U {p['thr']}"

        lado = 'Over/Si' if p['lado'] == 'Over' else 'Under/No'
        key = (p['fid'], mercado, lado)

        if key in vb_odds:
            odds = vb_odds[key]
            implied_p = 1.0 / odds  # sin vig removal (proxy)
            edge = p['prob'] - implied_p
            pnl = (odds - 1) if p['win'] else -1.0

            vb_results.append({
                'tipo': p['tipo'], 'thr': p['thr'], 'lado': p['lado'],
                'prob': p['prob'], 'odds': odds, 'implied_p': implied_p,
                'edge': edge, 'pnl': pnl, 'win': p['win'],
            })

    if not vb_results:
        print("  No se encontraron matches con odds reales.")
        return

    # All VB
    n_vb = len(vb_results)
    w_vb = sum(r['win'] for r in vb_results)
    pnl_vb = sum(r['pnl'] for r in vb_results)
    print(f"  Matches con odds reales: {n_vb}  ({w_vb}W / {n_vb-w_vb}L)  P&L={pnl_vb:+.2f}u  ROI={pnl_vb/n_vb:+.1%}")
    print()

    # Filtered by NegBin edge >= 4%
    nb_value = [r for r in vb_results if r['edge'] >= MIN_EDGE]
    nb_no_value = [r for r in vb_results if r['edge'] < MIN_EDGE]

    if nb_value:
        n_v = len(nb_value)
        w_v = sum(r['win'] for r in nb_value)
        pnl_v = sum(r['pnl'] for r in nb_value)
        print(f"  NegBin MARCA como value (edge>=4%): {n_v}  ({w_v}W / {n_v-w_v}L)  P&L={pnl_v:+.2f}u  ROI={pnl_v/n_v:+.1%}")

    if nb_no_value:
        n_nv = len(nb_no_value)
        w_nv = sum(r['win'] for r in nb_no_value)
        pnl_nv = sum(r['pnl'] for r in nb_no_value)
        print(f"  NegBin NO marca value  (edge<4%):   {n_nv}  ({w_nv}W / {n_nv-w_nv}L)  P&L={pnl_nv:+.2f}u  ROI={pnl_nv/n_nv:+.1%}")

    print()

    # ROI by cuota range for NegBin value bets
    if nb_value:
        print(f"  ROI de value bets NegBin por rango de cuota:")
        cuota_bins = [
            ('1.10-1.50', 1.10, 1.50),
            ('1.50-2.00', 1.50, 2.00),
            ('2.00-3.00', 2.00, 3.00),
            ('>3.00',     3.00, 999),
        ]
        print(f"  {'Cuota':<10s}  {'N':>4s}  {'W':>3s}  {'Hit%':>6s}  {'P&L':>8s}  {'ROI':>7s}")
        print(f"  {'-'*45}")
        for label, lo, hi in cuota_bins:
            ss = [r for r in nb_value if lo <= r['odds'] < hi]
            if not ss:
                continue
            sn = len(ss)
            sw = sum(r['win'] for r in ss)
            sp = sum(r['pnl'] for r in ss)
            print(f"  {label:<10s}  {sn:>4d}  {sw:>3d}  {sw/sn:>5.1%}  {sp:>+7.2f}u  {sp/sn:>+6.1%}")

    # ROI by sub-mercado for NegBin value bets
    if nb_value:
        print(f"\n  ROI de value bets NegBin por sub-mercado:")
        print(f"  {'Tipo':<10s}  {'N':>4s}  {'W':>3s}  {'Hit%':>6s}  {'P&L':>8s}  {'ROI':>7s}")
        print(f"  {'-'*45}")
        for tipo in ['Total', 'Local', 'Visita']:
            ss = [r for r in nb_value if r['tipo'] == tipo]
            if not ss:
                continue
            sn = len(ss)
            sw = sum(r['win'] for r in ss)
            sp = sum(r['pnl'] for r in ss)
            print(f"  {tipo:<10s}  {sn:>4d}  {sw:>3d}  {sw/sn:>5.1%}  {sp:>+7.2f}u  {sp/sn:>+6.1%}")

    print()


if __name__ == '__main__':
    main()
