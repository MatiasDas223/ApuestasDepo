"""
backtest_alpha_shrink.py
------------------------
Walk-forward sobre fixtures settled: para cada alpha en una grilla, aplica
shrinkage a los ratings atk/def y mide Brier score + MAE en mercados Goles
(Over/Under a thresholds estandar + BTTS) contra el resultado real.

Objetivo: validar si alpha=0.30 (v3.4-shrink) realmente mejora la calidad
de prediccion en mu_goles, o si alpha=0 (sin shrinkage = raw v3.2) es mejor.

Uso:
    python scripts/backtest_alpha_shrink.py --max 200
    python scripts/backtest_alpha_shrink.py --since 2026-01-01
"""

import sys
import argparse
import json
import pickle
from pathlib import Path
from collections import defaultdict
import math

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).parent))
ODDS_DIR = BASE / 'data/odds'
MIN_EDGE = 0.04
STAKE = 1.0

from modelo_v3 import (
    load_csv, compute_match_params, run_simulation,
)
from analizar_partido import compute_all_probs, compute_arco_params
from modelo_v3_shrink import apply_shrink_to_ratings

HIST_CSV = BASE / 'data/historico/partidos_historicos.csv'
TEAMS_CSV = BASE / 'data/db/equipos.csv'
LIGAS_CSV = BASE / 'data/db/ligas.csv'

N_SIM = 30_000   # menor para velocidad; suficiente para calibracion
ALPHAS = [0.0, 0.15, 0.30, 0.50, 0.70, 1.0]   # 1.0 = sin shrinkage (raw v3.2)
GOLES_THRESHOLDS = [0.5, 1.5, 2.5, 3.5, 4.5]
CORNERS_THRESHOLDS_TEAM  = [1.5, 2.5, 3.5, 4.5, 5.5, 6.5]
CORNERS_THRESHOLDS_TOTAL = [7.5, 8.5, 9.5, 10.5, 11.5, 12.5]
SHOTS_THRESHOLDS_TEAM    = [7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5]


def apply_shrink_to_corners_shots(params: dict, alpha: float) -> dict:
    """Shrinkage intrínseca para corners y shots: achica la asimetría
    preservando el total. Para corners ataca share_local → 0.5. Para shots
    ataca mu_L vs mu_V → promedio. alpha=1.0 = sin efecto."""
    if alpha >= 1.0:
        return params
    out = dict(params)
    # Corners: shrink share_local toward 0.5
    share = params.get('share_corners_loc')
    if share is not None:
        new_share = 0.5 + alpha * (share - 0.5)
        out['share_corners_loc'] = new_share
        mu_total = params.get('mu_corners_total', 0)
        out['mu_corners_local'] = mu_total * new_share
        out['mu_corners_vis']   = mu_total * (1 - new_share)
    # Shots: shrink mu_L and mu_V toward their average
    mu_sl = params.get('mu_shots_local')
    mu_sv = params.get('mu_shots_vis')
    if mu_sl is not None and mu_sv is not None:
        m = (mu_sl + mu_sv) / 2
        out['mu_shots_local'] = m + alpha * (mu_sl - m)
        out['mu_shots_vis']   = m + alpha * (mu_sv - m)
    return out


def _load_id_to_name():
    import csv
    teams = {}
    with open(TEAMS_CSV, encoding='utf-8') as f:
        for r in csv.DictReader(f):
            teams[r['id']] = r['nombre']
    ligas = {}
    with open(LIGAS_CSV, encoding='utf-8') as f:
        for r in csv.DictReader(f):
            ligas[r['id']] = r['nombre']
    return teams, ligas


def _probs_for_alpha(params, alpha: float, fid: int):
    # Aplica shrinkage a goles (via modelo_v3_shrink) + a corners/shots (via helper local)
    if alpha >= 1.0:
        params_use = params
    else:
        params_use = apply_shrink_to_ratings(params, alpha=alpha)
        params_use = apply_shrink_to_corners_shots(params_use, alpha=alpha)
    sim = run_simulation(params_use, N_SIM, seed=fid)
    sim['team_local']  = params['team_local']
    sim['team_visita'] = params['team_visita']
    try:
        arco_p = compute_arco_params(params['team_local'], params['team_visita'],
                                      params.get('_hist_used', []), params.get('_competicion', ''))
        sim['arco_params'] = arco_p
    except Exception:
        pass
    return compute_all_probs(sim)


def _real_outcome_keys(goles_local: int, goles_visita: int,
                        corners_local=None, corners_vis=None,
                        shots_local=None, shots_vis=None):
    """Mapea resultados reales (goles/corners/tiros) a keys de probs para comparacion."""
    gtot = goles_local + goles_visita
    out = {}
    # Goles
    for t in GOLES_THRESHOLDS:
        out[f'ou_total_{t}_over']  = 1 if gtot > t else 0
        out[f'ou_total_{t}_under'] = 1 if gtot <= t else 0
        out[f'ou_local_{t}_over']  = 1 if goles_local  > t else 0
        out[f'ou_local_{t}_under'] = 1 if goles_local  <= t else 0
        out[f'ou_vis_{t}_over']    = 1 if goles_visita > t else 0
        out[f'ou_vis_{t}_under']   = 1 if goles_visita <= t else 0
    out['btts_si'] = 1 if (goles_local > 0 and goles_visita > 0) else 0
    out['btts_no'] = 1 if not (goles_local > 0 and goles_visita > 0) else 0
    # Corners (si hay data)
    if corners_local is not None and corners_vis is not None:
        ctot = corners_local + corners_vis
        for t in CORNERS_THRESHOLDS_TOTAL:
            out[f'corners_total_{t}_over']  = 1 if ctot > t else 0
            out[f'corners_total_{t}_under'] = 1 if ctot <= t else 0
        for t in CORNERS_THRESHOLDS_TEAM:
            out[f'corners_local_{t}_over']  = 1 if corners_local > t else 0
            out[f'corners_local_{t}_under'] = 1 if corners_local <= t else 0
            out[f'corners_vis_{t}_over']    = 1 if corners_vis > t else 0
            out[f'corners_vis_{t}_under']   = 1 if corners_vis <= t else 0
    # Tiros (si hay data)
    if shots_local is not None and shots_vis is not None:
        for t in SHOTS_THRESHOLDS_TEAM:
            out[f'shots_local_{t}_over']  = 1 if shots_local > t else 0
            out[f'shots_local_{t}_under'] = 1 if shots_local <= t else 0
            out[f'shots_vis_{t}_over']    = 1 if shots_vis > t else 0
            out[f'shots_vis_{t}_under']   = 1 if shots_vis <= t else 0
    return out


# Map de key de prob en compute_all_probs a key interna
def _probs_to_compare(probs: dict):
    """Retorna dict {internal_key: (prob, odds_key)} para cruzar con odds json."""
    out = {}
    # Goles
    for t in GOLES_THRESHOLDS:
        out[f'ou_total_{t}_over']  = (probs.get(f'g_over_{t}'),  f'g_over_{t}')
        out[f'ou_total_{t}_under'] = (probs.get(f'g_under_{t}'), f'g_under_{t}')
        out[f'ou_local_{t}_over']  = (probs.get(f'gl_over_{t}'), f'gl_over_{t}')
        out[f'ou_local_{t}_under'] = (probs.get(f'gl_under_{t}'),f'gl_under_{t}')
        out[f'ou_vis_{t}_over']    = (probs.get(f'gv_over_{t}'), f'gv_over_{t}')
        out[f'ou_vis_{t}_under']   = (probs.get(f'gv_under_{t}'),f'gv_under_{t}')
    out['btts_si'] = (probs.get('btts_si'), 'btts_si')
    out['btts_no'] = (probs.get('btts_no'), 'btts_no')
    # Corners
    for t in CORNERS_THRESHOLDS_TOTAL:
        out[f'corners_total_{t}_over']  = (probs.get(f'tc_over_{t}'),  f'tc_over_{t}')
        out[f'corners_total_{t}_under'] = (probs.get(f'tc_under_{t}'), f'tc_under_{t}')
    for t in CORNERS_THRESHOLDS_TEAM:
        out[f'corners_local_{t}_over']  = (probs.get(f'cl_over_{t}'),  f'cl_over_{t}')
        out[f'corners_local_{t}_under'] = (probs.get(f'cl_under_{t}'), f'cl_under_{t}')
        out[f'corners_vis_{t}_over']    = (probs.get(f'cv_over_{t}'),  f'cv_over_{t}')
        out[f'corners_vis_{t}_under']   = (probs.get(f'cv_under_{t}'), f'cv_under_{t}')
    # Tiros
    for t in SHOTS_THRESHOLDS_TEAM:
        out[f'shots_local_{t}_over']  = (probs.get(f'sl_over_{t}'), f'sl_over_{t}')
        out[f'shots_local_{t}_under'] = (probs.get(f'sl_under_{t}'),f'sl_under_{t}')
        out[f'shots_vis_{t}_over']    = (probs.get(f'sv_over_{t}'), f'sv_over_{t}')
        out[f'shots_vis_{t}_under']   = (probs.get(f'sv_under_{t}'),f'sv_under_{t}')
    return out


def _load_odds(fid):
    p = ODDS_DIR / f'{fid}.json'
    if not p.exists(): return None
    try:
        with open(p, encoding='utf-8') as f:
            d = json.load(f)
        return d.get('odds', {})
    except Exception:
        return None


def run(since: str = '2025-01-01', max_fixtures: int = 200):
    teams_map, ligas_map = _load_id_to_name()
    hist_all = load_csv(HIST_CSV)
    # Filtrar por fecha: fixtures settled post `since`
    eligible = [r for r in hist_all if r.get('fecha','') >= since
                and r.get('goles_local','').strip() != ''
                and r.get('goles_visitante','').strip() != '']
    eligible = sorted(eligible, key=lambda r: r['fecha'], reverse=True)[:max_fixtures]

    print(f"Fixtures elegibles: {len(eligible)} (desde {since}, max {max_fixtures})")
    print(f"Alphas a testear: {ALPHAS}")
    print(f"N sim por fixture: {N_SIM}")
    print()

    # Stats: {alpha: {'brier': [...], 'errors':[...], 'probs_calls':[(prob, outcome, market_type)]}}
    stats = {a: defaultdict(list) for a in ALPHAS}

    done = 0
    errors = 0
    for r in eligible:
        fid = r['fixture_id']
        gl = int(r['goles_local'])
        gv = int(r['goles_visitante'])
        home = teams_map.get(r['equipo_local_id'], '')
        away = teams_map.get(r['equipo_visitante_id'], '')
        liga = ligas_map.get(r['liga_id'], '')
        if not home or not away:
            continue

        hist_sin = [h for h in hist_all if h['fixture_id'] != fid]

        try:
            params = compute_match_params(home, away, hist_sin, liga)
        except Exception as e:
            errors += 1
            continue

        # Guardar contexto en params para arco
        params['team_local']  = home
        params['team_visita'] = away
        params['_hist_used']  = hist_sin
        params['_competicion'] = liga

        # Resultados reales para corners/shots (pueden estar vacios en partidos viejos)
        def _int_or_none(v):
            try:
                s = (v or '').strip()
                return int(s) if s else None
            except Exception:
                return None
        cl = _int_or_none(r.get('corners_local'))
        cv = _int_or_none(r.get('corners_visitante'))
        sl = _int_or_none(r.get('tiros_local'))
        sv = _int_or_none(r.get('tiros_visitante'))
        real = _real_outcome_keys(gl, gv, cl, cv, sl, sv)
        odds_map = _load_odds(fid) or {}

        for alpha in ALPHAS:
            try:
                probs = _probs_for_alpha(params, alpha, fid=int(fid))
            except Exception:
                continue
            prob_map = _probs_to_compare(probs)
            for key, val in prob_map.items():
                p_pred, odds_key = val
                if p_pred is None: continue
                if key not in real: continue   # corners/shots pueden faltar
                y = real[key]
                # Categoria: goles / corners / shots / btts
                if key.startswith('corners_'):
                    category = 'corners'
                elif key.startswith('shots_'):
                    category = 'shots'
                elif key.startswith('btts'):
                    category = 'btts'
                else:
                    category = 'goles'
                market_type = 'total' if 'total' in key else ('local' if 'local' in key else ('vis' if 'vis' in key else 'btts'))
                side = 'over' if 'over' in key or key == 'btts_si' else 'under'
                stats[alpha]['brier'].append((p_pred - y)**2)
                stats[alpha]['mae'].append(abs(p_pred - y))
                stats[alpha]['calls'].append((p_pred, y, market_type, side, key, category))

                # === Simulacion de ROI: guardar TODAS las bets con edge>0 para sweep MIN_EDGE ===
                odds = odds_map.get(odds_key)
                if odds and odds > 1.0:
                    implied = 1.0 / odds
                    edge = p_pred - implied
                    if edge > 0:
                        pnl = (odds - 1.0) if y == 1 else -1.0
                        stats[alpha]['bets'].append({
                            'fid': fid, 'key': key, 'market': market_type, 'side': side,
                            'category': category,
                            'prob': p_pred, 'odds': odds, 'edge': edge, 'y': y, 'pnl': pnl
                        })

        done += 1
        if done % 25 == 0:
            print(f"  [{done}/{len(eligible)}] procesados (errors={errors})")

    print(f"\nDone: {done} fixtures, {errors} errores")
    # Save raw data for significance analysis
    out_pkl = BASE / 'scripts/bt_raw.pkl'
    try:
        with open(out_pkl,'wb') as f:
            pickle.dump({a: dict(stats[a]) for a in ALPHAS}, f)
        print(f"Raw stats saved to {out_pkl}")
    except Exception as e:
        print(f"Save failed: {e}")
    print()

    # Reporte
    print("="*100)
    print("RESULTADO POR ALPHA (menor Brier/MAE = mejor calibrado)")
    print("="*100)
    print(f"{'alpha':<8} {'N bets':>8} {'Brier':>10} {'MAE':>10}")
    print("-"*100)
    for a in ALPHAS:
        b = stats[a]['brier']
        m = stats[a]['mae']
        if not b: continue
        print(f"{a:<8.2f} {len(b):>8} {sum(b)/len(b):>10.5f} {sum(m)/len(m):>10.5f}")

    print()
    print("="*100)
    print("BRIER POR CATEGORIA (GOLES vs CORNERS vs SHOTS vs BTTS)")
    print("="*100)
    for cat in ['goles','corners','shots','btts']:
        print(f"\n>>> {cat.upper()}")
        print(f"{'alpha':<8} {'N':>8} {'Brier':>10} {'MAE':>10}")
        for a in ALPHAS:
            calls = [c for c in stats[a]['calls'] if len(c)>=6 and c[5]==cat]
            if not calls: continue
            briers = [(c[0]-c[1])**2 for c in calls]
            maes = [abs(c[0]-c[1]) for c in calls]
            print(f"{a:<8.2f} {len(calls):>8} {sum(briers)/len(briers):>10.5f} {sum(maes)/len(maes):>10.5f}")

    print()
    print("="*100)
    print("BRIER POR CATEGORIA x ALCANCE")
    print("="*100)
    for cat in ['goles','corners','shots']:
        for mkt in ['total','local','vis']:
            label = f"{cat}/{mkt}"
            print(f"\n>>> {label.upper()}")
            print(f"{'alpha':<8} {'N':>8} {'Brier':>10}")
            for a in ALPHAS:
                calls = [c for c in stats[a]['calls'] if len(c)>=6 and c[5]==cat and c[2]==mkt]
                if not calls: continue
                briers = [(c[0]-c[1])**2 for c in calls]
                print(f"{a:<8.2f} {len(calls):>8} {sum(briers)/len(briers):>10.5f}")

    print()
    print("="*100)
    print("POR TIPO DE MERCADO (cualquier categoria)")
    print("="*100)
    for mkt in ['total','local','vis','btts']:
        print(f"\n>>> {mkt.upper()}")
        print(f"{'alpha':<8} {'N':>8} {'Brier':>10} {'MAE':>10}")
        for a in ALPHAS:
            calls = [c for c in stats[a]['calls'] if c[2]==mkt]
            if not calls: continue
            briers = [(c[0]-c[1])**2 for c in calls]
            maes = [abs(c[0]-c[1]) for c in calls]
            print(f"{a:<8.2f} {len(calls):>8} {sum(briers)/len(briers):>10.5f} {sum(maes)/len(maes):>10.5f}")

    print()
    print("="*100)
    print("POR LADO (Over vs Under)")
    print("="*100)
    for side in ['over','under']:
        print(f"\n>>> {side.upper()}")
        print(f"{'alpha':<8} {'N':>8} {'Brier':>10} {'MAE':>10}")
        for a in ALPHAS:
            calls = [c for c in stats[a]['calls'] if c[3]==side]
            if not calls: continue
            briers = [(c[0]-c[1])**2 for c in calls]
            maes = [abs(c[0]-c[1]) for c in calls]
            print(f"{a:<8.2f} {len(calls):>8} {sum(briers)/len(briers):>10.5f} {sum(maes)/len(maes):>10.5f}")

    # === Reporte ROI simulado: SWEEP de MIN_EDGE ===
    print()
    print("="*110)
    print("SWEEP MIN_EDGE x ALPHA — ROI simulado por umbral de edge")
    print("="*110)
    edge_thresholds = [0.02, 0.04, 0.06, 0.08, 0.10, 0.12, 0.15]
    print(f"{'alpha':<6} " + " ".join(f"edge>{int(t*100)}%:N/ROI%".ljust(16) for t in edge_thresholds))
    print("-"*110)
    for a in ALPHAS:
        bets = stats[a]['bets']
        if not bets: continue
        row = f"{a:<6.2f} "
        for t in edge_thresholds:
            filt = [b for b in bets if b['edge'] > t]
            if not filt:
                row += "-".ljust(16) + " "
                continue
            n=len(filt); pnl=sum(b['pnl'] for b in filt); w=sum(1 for b in filt if b['y']==1)
            row += f"{n}/{pnl/n*100:+.1f}%".ljust(16) + " "
        print(row)

    # Tabla con mas detalle por edge threshold
    print()
    print("="*110)
    print("DETALLE POR MIN_EDGE — alpha=0.15 (optimo Brier)")
    print("="*110)
    print(f"{'min_edge':<10} {'N':>5} {'hit%':>7} {'ROI%':>8} {'PnL':>9} {'odds':>6}")
    for t in edge_thresholds:
        bets = [b for b in stats[0.15]['bets'] if b['edge'] > t]
        if not bets: continue
        n=len(bets); pnl=sum(b['pnl'] for b in bets); w=sum(1 for b in bets if b['y']==1)
        odds=sum(b['odds'] for b in bets)/n
        print(f">{int(t*100)}%     {n:>5} {w/n*100:>6.1f}% {pnl/n*100:>+7.2f}% {pnl:>+9.2f} {odds:>6.2f}")

    print()
    print("="*110)
    print("ROI original edge>4% (compatibilidad con corridas previas)")
    print("="*110)
    print(f"{'alpha':<8} {'N bets':>8} {'hit%':>7} {'ROI%':>8} {'PnL':>9} {'odds_avg':>9} {'edge_avg':>9}")
    print("-"*110)
    for a in ALPHAS:
        bets = [b for b in stats[a]['bets'] if b['edge'] > 0.04]
        if not bets: continue
        n = len(bets)
        w = sum(1 for b in bets if b['y']==1)
        pnl = sum(b['pnl'] for b in bets)
        odds_avg = sum(b['odds'] for b in bets)/n
        edge_avg = sum(b['edge'] for b in bets)/n
        print(f"{a:<8.2f} {n:>8} {w/n*100:>6.1f}% {pnl/n*100:>+7.2f}% {pnl:>+9.2f} {odds_avg:>9.2f} {edge_avg*100:>+8.2f}%")

    print()
    print("="*110)
    print("SWEEP MIN_EDGE x ALPHA POR CATEGORIA+ALCANCE (N / ROI%)")
    print("="*110)
    for cat in ['goles','corners','shots','btts']:
        for mkt in (['total','local','vis'] if cat != 'btts' else ['total']):
            label = f"{cat}/{mkt}".upper()
            print(f"\n>>> {label}")
            print(f"{'alpha':<6} " + " ".join(f">{int(t*100)}%".ljust(16) for t in edge_thresholds))
            for a in ALPHAS:
                row = f"{a:<6.2f} "
                for t in edge_thresholds:
                    bets = [b for b in stats[a]['bets']
                            if b.get('category')==cat and b['market']==mkt and b['edge']>t]
                    if not bets:
                        row += "-".ljust(16) + " "
                        continue
                    n=len(bets); pnl=sum(b['pnl'] for b in bets)
                    row += f"{n}/{pnl/n*100:+.1f}%".ljust(16) + " "
                print(row)

    print()
    print("="*110)
    print("SWEEP MIN_EDGE x ALPHA POR MERCADO (legado — sin categoria)")
    print("="*110)
    for mkt in ['total','local','vis','btts']:
        print(f"\n>>> {mkt.upper()}")
        print(f"{'alpha':<6} " + " ".join(f">{int(t*100)}%".ljust(16) for t in edge_thresholds))
        for a in ALPHAS:
            row = f"{a:<6.2f} "
            for t in edge_thresholds:
                bets = [b for b in stats[a]['bets'] if b['market']==mkt and b['edge']>t]
                if not bets:
                    row += "-".ljust(16) + " "
                    continue
                n=len(bets); pnl=sum(b['pnl'] for b in bets)
                row += f"{n}/{pnl/n*100:+.1f}%".ljust(16) + " "
            print(row)

    print()
    print("="*100)
    print("ROI POR TIPO DE MERCADO (edge>4% para compatibilidad)")
    print("="*100)
    for mkt in ['total','local','vis','btts']:
        print(f"\n>>> {mkt.upper()}")
        print(f"{'alpha':<8} {'N':>6} {'hit%':>7} {'ROI%':>8} {'odds':>6}")
        for a in ALPHAS:
            bets = [b for b in stats[a]['bets'] if b['market']==mkt and b['edge']>0.04]
            if not bets: continue
            n=len(bets); w=sum(1 for b in bets if b['y']==1)
            pnl=sum(b['pnl'] for b in bets); odds=sum(b['odds'] for b in bets)/n
            print(f"{a:<8.2f} {n:>6} {w/n*100:>6.1f}% {pnl/n*100:>+7.2f}% {odds:>6.2f}")

    print()
    print("="*100)
    print("ROI POR LADO")
    print("="*100)
    for side in ['over','under']:
        print(f"\n>>> {side.upper()}")
        print(f"{'alpha':<8} {'N':>6} {'hit%':>7} {'ROI%':>8} {'odds':>6}")
        for a in ALPHAS:
            bets = [b for b in stats[a]['bets'] if b['side']==side]
            if not bets: continue
            n=len(bets); w=sum(1 for b in bets if b['y']==1)
            pnl=sum(b['pnl'] for b in bets); odds=sum(b['odds'] for b in bets)/n
            print(f"{a:<8.2f} {n:>6} {w/n*100:>6.1f}% {pnl/n*100:>+7.2f}% {odds:>6.2f}")

    print()
    print("="*100)
    print("CALIBRACION POR BUCKET DE PROB (para alpha=0 y alpha=0.30 y alpha=1)")
    print("="*100)
    for a in [0.0, 0.30, 1.0]:
        print(f"\n>>> alpha={a}")
        bins = [(0,0.1),(0.1,0.2),(0.2,0.3),(0.3,0.4),(0.4,0.5),(0.5,0.6),(0.6,0.7),(0.7,0.8),(0.8,0.9),(0.9,1.01)]
        print(f"{'bucket':<14} {'N':>6} {'prob_avg':>10} {'hit_real':>10} {'gap':>8}")
        for lo, hi in bins:
            calls = [c for c in stats[a]['calls'] if lo<=c[0]<hi]
            if len(calls)<10: continue
            p_avg = sum(c[0] for c in calls)/len(calls)
            hit = sum(c[1] for c in calls)/len(calls)
            print(f"[{lo:.1f},{hi:.1f}){' ':<5} {len(calls):>6} {p_avg*100:>9.1f}% {hit*100:>9.1f}% {(hit-p_avg)*100:>+7.2f}")


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--since', default='2025-01-01')
    ap.add_argument('--max', type=int, default=200)
    args = ap.parse_args()
    run(since=args.since, max_fixtures=args.max)
