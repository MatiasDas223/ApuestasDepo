"""
correlacion_tiros.py
--------------------
Analisis exploratorio para entender que variables pre-partido predicen
el numero de tiros y tiros al arco en un partido.

Estudia:
  1. Correlaciones intra-match (mismas variables del partido)
  2. Correlaciones predictivas (promedios historicos pre-partido vs real)
  3. Pace como predictor
  4. Analisis por liga
"""

import csv
import sys
import math
from collections import defaultdict
from pathlib import Path

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE     = Path(r'C:\Users\Matt\Apuestas Deportivas')
CSV_PATH = BASE / 'data/historico/partidos_historicos.csv'

HALF_LIFE_DAYS = 90
MIN_HISTORY    = 5   # minimo de partidos previos para calcular promedio


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def safe_int(v, default=0):
    if v in (None, '', '-'):
        return default
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return default

def safe_float(v, default=0.0):
    if v in (None, '', '-'):
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default

def pearson(xs, ys):
    """Pearson correlation coefficient."""
    n = len(xs)
    if n < 5:
        return float('nan'), n
    mx = sum(xs) / n
    my = sum(ys) / n
    sx = math.sqrt(sum((x - mx)**2 for x in xs) / n)
    sy = math.sqrt(sum((y - my)**2 for y in ys) / n)
    if sx == 0 or sy == 0:
        return 0.0, n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n
    return cov / (sx * sy), n

def mae(predicted, actual):
    if not predicted:
        return float('nan')
    return sum(abs(p - a) for p, a in zip(predicted, actual)) / len(predicted)


# ─────────────────────────────────────────────────────────────────────────────
# Load data
# ─────────────────────────────────────────────────────────────────────────────

def load_data():
    rows = []
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            rows.append(r)
    # Sort by date
    rows.sort(key=lambda r: r['fecha'])
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# 1. CORRELACIONES INTRA-MATCH
# ─────────────────────────────────────────────────────────────────────────────

def analisis_intra_match(rows):
    """Correlaciones entre variables del MISMO partido con tiros totales."""
    print("=" * 72)
    print("  1. CORRELACIONES INTRA-MATCH (variables del mismo partido vs tiros)")
    print("=" * 72)
    print()

    # Build arrays
    tiros_total = []
    tiros_arco_total = []
    goles_total = []
    corners_total = []
    posesion_diff = []  # abs(pos_local - pos_visitante) = dominio
    tarjetas_total = []
    xg_total = []
    tiros_dentro_total = []
    tiros_fuera_total = []
    tiros_bloq_total = []
    atajadas_total = []

    # For shots on target analysis
    sot_total = []

    for r in rows:
        sl = safe_int(r['tiros_local'])
        sv = safe_int(r['tiros_visitante'])
        st = sl + sv
        if st == 0:
            continue

        tiros_total.append(st)
        sot_l = safe_int(r['tiros_arco_local'])
        sot_v = safe_int(r['tiros_arco_visitante'])
        tiros_arco_total.append(sot_l + sot_v)

        goles_total.append(safe_int(r['goles_local']) + safe_int(r['goles_visitante']))
        corners_total.append(safe_int(r['corners_local']) + safe_int(r['corners_visitante']))
        posesion_diff.append(abs(safe_int(r['posesion_local']) - safe_int(r['posesion_visitante'])))
        tarjetas_total.append(safe_int(r['tarjetas_local']) + safe_int(r['tarjetas_visitante']))

        xg_l = safe_float(r.get('xg_local'))
        xg_v = safe_float(r.get('xg_visitante'))
        if r.get('xg_local', '') not in ('', '-'):
            xg_total.append((st, xg_l + xg_v))

        td_l = safe_int(r.get('tiros_dentro_local'))
        td_v = safe_int(r.get('tiros_dentro_visitante'))
        if r.get('tiros_dentro_local', '') not in ('', '-'):
            tiros_dentro_total.append((st, td_l + td_v))
            tiros_fuera_total.append((st, safe_int(r.get('tiros_fuera_local')) + safe_int(r.get('tiros_fuera_visitante'))))
            tiros_bloq_total.append((st, safe_int(r.get('tiros_bloqueados_local')) + safe_int(r.get('tiros_bloqueados_visitante'))))
            atajadas_total.append((st, safe_int(r.get('atajadas_local')) + safe_int(r.get('atajadas_visitante'))))

    print(f"  {'Variable':<30s}  {'r(tiros_tot)':<12s}  {'r(sot_tot)':<12s}  N")
    print(f"  {'-'*70}")

    pairs = [
        ('Goles totales',       tiros_total, goles_total,    tiros_arco_total, goles_total),
        ('Corners totales',     tiros_total, corners_total,  tiros_arco_total, corners_total),
        ('|Posesion diff|',     tiros_total, posesion_diff,  tiros_arco_total, posesion_diff),
        ('Tarjetas totales',    tiros_total, tarjetas_total, tiros_arco_total, tarjetas_total),
        ('Tiros arco totales',  tiros_total, tiros_arco_total, None, None),
    ]
    for name, xs, ys, xs2, ys2 in pairs:
        r1, n1 = pearson(xs, ys)
        if xs2 is not None:
            r2, n2 = pearson(xs2, ys2)
            print(f"  {name:<30s}  {r1:>+.3f}         {r2:>+.3f}         {n1}")
        else:
            print(f"  {name:<30s}  {r1:>+.3f}         {'—':<12s}  {n1}")

    # Extended stats (subset)
    if xg_total:
        r1, n1 = pearson([x[0] for x in xg_total], [x[1] for x in xg_total])
        # sot vs xg
        sot_sub = [tiros_arco_total[i] for i in range(len(tiros_total)) if i < len(xg_total)]
        print(f"  {'xG total':<30s}  {r1:>+.3f}         {'—':<12s}  {n1}")

    if tiros_dentro_total:
        r1, _ = pearson([x[0] for x in tiros_dentro_total], [x[1] for x in tiros_dentro_total])
        r2, _ = pearson([x[0] for x in tiros_fuera_total], [x[1] for x in tiros_fuera_total])
        r3, _ = pearson([x[0] for x in tiros_bloq_total], [x[1] for x in tiros_bloq_total])
        r4, n4 = pearson([x[0] for x in atajadas_total], [x[1] for x in atajadas_total])
        print(f"  {'Tiros dentro area':<30s}  {r1:>+.3f}         {'—':<12s}  {n4}")
        print(f"  {'Tiros fuera area':<30s}  {r2:>+.3f}         {'—':<12s}  {n4}")
        print(f"  {'Tiros bloqueados':<30s}  {r3:>+.3f}         {'—':<12s}  {n4}")
        print(f"  {'Atajadas':<30s}  {r4:>+.3f}         {'—':<12s}  {n4}")

    # Tiros vs Tiros arco (key relationship)
    r_sot, _ = pearson(tiros_total, tiros_arco_total)
    print(f"\n  Tiros total <-> SOT total: r = {r_sot:+.3f}")

    # Stats descriptivas
    avg_st = sum(tiros_total) / len(tiros_total)
    std_st = math.sqrt(sum((x - avg_st)**2 for x in tiros_total) / len(tiros_total))
    avg_sot = sum(tiros_arco_total) / len(tiros_arco_total)
    std_sot = math.sqrt(sum((x - avg_sot)**2 for x in tiros_arco_total) / len(tiros_arco_total))

    print(f"\n  Tiros totales:    media={avg_st:.1f}  std={std_st:.1f}  (CV={std_st/avg_st:.2f})")
    print(f"  SOT totales:      media={avg_sot:.1f}  std={std_sot:.1f}  (CV={std_sot/avg_sot:.2f})")
    print(f"  Ratio SOT/Tiros:  {avg_sot/avg_st:.2%}")

    # Individual stats
    tiros_local = [safe_int(r['tiros_local']) for r in rows if safe_int(r['tiros_local']) + safe_int(r['tiros_visitante']) > 0]
    tiros_vis = [safe_int(r['tiros_visitante']) for r in rows if safe_int(r['tiros_local']) + safe_int(r['tiros_visitante']) > 0]
    avg_tl = sum(tiros_local) / len(tiros_local)
    avg_tv = sum(tiros_vis) / len(tiros_vis)
    print(f"  Tiros local avg:  {avg_tl:.1f}   Tiros visita avg: {avg_tv:.1f}   (home advantage: {avg_tl/avg_tv:.2f}x)")

    sot_local = [safe_int(r['tiros_arco_local']) for r in rows if safe_int(r['tiros_local']) + safe_int(r['tiros_visitante']) > 0]
    sot_vis = [safe_int(r['tiros_arco_visitante']) for r in rows if safe_int(r['tiros_local']) + safe_int(r['tiros_visitante']) > 0]
    avg_sotl = sum(sot_local) / len(sot_local)
    avg_sotv = sum(sot_vis) / len(sot_vis)
    print(f"  SOT local avg:    {avg_sotl:.1f}   SOT visita avg:   {avg_sotv:.1f}   (home advantage: {avg_sotl/avg_sotv:.2f}x)")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# 2. CORRELACIONES PREDICTIVAS (promedios pre-partido vs real)
# ─────────────────────────────────────────────────────────────────────────────

def compute_team_history(rows):
    """
    Para cada partido, computa promedios historicos del equipo ANTES de ese partido.
    Devuelve lista de dicts con predictors y actuals.
    """
    # Track per-team history: {team_id: [(date, venue, stats_dict), ...]}
    team_history = defaultdict(list)
    results = []

    for r in rows:
        fecha = r['fecha']
        lid   = safe_int(r['liga_id'])
        hid   = safe_int(r['equipo_local_id'])
        aid   = safe_int(r['equipo_visitante_id'])

        sl = safe_int(r['tiros_local'])
        sv = safe_int(r['tiros_visitante'])
        if sl + sv == 0:
            continue

        sot_l = safe_int(r['tiros_arco_local'])
        sot_v = safe_int(r['tiros_arco_visitante'])
        gl = safe_int(r['goles_local'])
        gv = safe_int(r['goles_visitante'])
        cl = safe_int(r['corners_local'])
        cv = safe_int(r['corners_visitante'])
        pl = safe_int(r['posesion_local'])
        pv = safe_int(r['posesion_visitante'])
        tl = safe_int(r['tarjetas_local'])
        tv = safe_int(r['tarjetas_visitante'])

        # Get pre-match averages for home team (as home) and away team (as away)
        hist_home = team_history[hid]
        hist_away = team_history[aid]

        # Filter: same venue + any venue
        home_as_home = [h for h in hist_home if h['venue'] == 'home']
        away_as_away = [h for h in hist_away if h['venue'] == 'away']
        home_all = hist_home
        away_all = hist_away

        if len(home_all) >= MIN_HISTORY and len(away_all) >= MIN_HISTORY:
            def avg_last(history, key, n=None):
                subset = history[-n:] if n else history
                vals = [h[key] for h in subset if key in h]
                return sum(vals) / len(vals) if vals else 0.0

            def wavg(history, key):
                """Weighted average with exponential decay."""
                if not history:
                    return 0.0
                from datetime import datetime
                latest = history[-1]['date']
                try:
                    latest_dt = datetime.strptime(latest, '%Y-%m-%d')
                except:
                    return avg_last(history, key)
                total_w = 0.0
                total_v = 0.0
                for h in history:
                    try:
                        dt = datetime.strptime(h['date'], '%Y-%m-%d')
                        days = (latest_dt - dt).days
                    except:
                        days = 0
                    w = math.exp(-math.log(2) * days / HALF_LIFE_DAYS)
                    total_w += w
                    total_v += w * h.get(key, 0)
                return total_v / total_w if total_w > 0 else 0.0

            # Pace components
            pace_home = wavg(home_all, 'pace')
            pace_away = wavg(away_all, 'pace')

            result = {
                'fecha': fecha,
                'liga_id': lid,
                # Actuals
                'tiros_local': sl,
                'tiros_visitante': sv,
                'tiros_total': sl + sv,
                'sot_local': sot_l,
                'sot_visitante': sot_v,
                'sot_total': sot_l + sot_v,
                'goles_total': gl + gv,
                'corners_total': cl + cv,
                # Predictors - home team
                'hist_shots_for_home': wavg(home_all, 'shots_for'),
                'hist_shots_ag_home': wavg(home_all, 'shots_against'),
                'hist_sot_for_home': wavg(home_all, 'sot_for'),
                'hist_sot_ag_home': wavg(home_all, 'sot_against'),
                'hist_goals_for_home': wavg(home_all, 'goals_for'),
                'hist_goals_ag_home': wavg(home_all, 'goals_against'),
                'hist_corners_for_home': wavg(home_all, 'corners_for'),
                'hist_poss_home': wavg(home_all, 'possession'),
                'hist_cards_home': wavg(home_all, 'cards'),
                # Predictors - away team
                'hist_shots_for_away': wavg(away_all, 'shots_for'),
                'hist_shots_ag_away': wavg(away_all, 'shots_against'),
                'hist_sot_for_away': wavg(away_all, 'sot_for'),
                'hist_sot_ag_away': wavg(away_all, 'sot_against'),
                'hist_goals_for_away': wavg(away_all, 'goals_for'),
                'hist_goals_ag_away': wavg(away_all, 'goals_against'),
                'hist_corners_for_away': wavg(away_all, 'corners_for'),
                'hist_poss_away': wavg(away_all, 'possession'),
                'hist_cards_away': wavg(away_all, 'cards'),
                # Pace
                'pace_home': pace_home,
                'pace_away': pace_away,
                'pace_match_pred': pace_home + pace_away,
                # Venue-specific
                'hist_shots_for_home_venue': wavg(home_as_home, 'shots_for') if len(home_as_home) >= 3 else wavg(home_all, 'shots_for'),
                'hist_shots_ag_home_venue': wavg(home_as_home, 'shots_against') if len(home_as_home) >= 3 else wavg(home_all, 'shots_against'),
                'hist_shots_for_away_venue': wavg(away_as_away, 'shots_for') if len(away_as_away) >= 3 else wavg(away_all, 'shots_for'),
                'hist_shots_ag_away_venue': wavg(away_as_away, 'shots_against') if len(away_as_away) >= 3 else wavg(away_all, 'shots_against'),
                # Form (last 5)
                'form_shots_for_home': avg_last(home_all, 'shots_for', 5),
                'form_shots_for_away': avg_last(away_all, 'shots_for', 5),
                'form_shots_total_home': avg_last(home_all, 'shots_total', 5),
                'form_shots_total_away': avg_last(away_all, 'shots_total', 5),
            }
            results.append(result)

        # Pace for this match
        pace_this = (sl + sv) * 0.467 + (cl + cv) * 0.267 + (gl + gv) * 0.267

        # Record this match for both teams
        match_stats_home = {
            'date': fecha, 'venue': 'home', 'liga_id': lid,
            'shots_for': sl, 'shots_against': sv, 'shots_total': sl + sv,
            'sot_for': sot_l, 'sot_against': sot_v,
            'goals_for': gl, 'goals_against': gv,
            'corners_for': cl, 'corners_against': cv,
            'possession': pl, 'cards': tl,
            'pace': pace_this,
        }
        match_stats_away = {
            'date': fecha, 'venue': 'away', 'liga_id': lid,
            'shots_for': sv, 'shots_against': sl, 'shots_total': sl + sv,
            'sot_for': sot_v, 'sot_against': sot_l,
            'goals_for': gv, 'goals_against': gl,
            'corners_for': cv, 'corners_against': cl,
            'possession': pv, 'cards': tv,
            'pace': pace_this,
        }
        team_history[hid].append(match_stats_home)
        team_history[aid].append(match_stats_away)

    return results


def analisis_predictivo(results):
    print("=" * 72)
    print("  2. CORRELACIONES PREDICTIVAS (promedios historicos -> tiros reales)")
    print(f"     Partidos con suficiente historia: {len(results)}")
    print("=" * 72)
    print()

    # --- A. Predictors for SHOTS TOTAL ---
    print(f"  A. Predictores de TIROS TOTALES")
    print(f"  {'-'*68}")
    print(f"  {'Predictor':<45s} {'r':>7s}  {'N':>5s}")
    print(f"  {'-'*68}")

    actual_st = [r['tiros_total'] for r in results]
    actual_sot = [r['sot_total'] for r in results]

    predictors_total = [
        ('hist_shots_for_home + shots_for_away',
         [r['hist_shots_for_home'] + r['hist_shots_for_away'] for r in results]),
        ('hist_shots_for_home + shots_ag_home',
         [r['hist_shots_for_home'] + r['hist_shots_ag_home'] for r in results]),
        ('(shots_for_H + shots_ag_H + shots_for_A + shots_ag_A)/2',
         [(r['hist_shots_for_home'] + r['hist_shots_ag_home'] + r['hist_shots_for_away'] + r['hist_shots_ag_away']) / 2 for r in results]),
        ('pace_home + pace_away',
         [r['pace_match_pred'] for r in results]),
        ('hist_goals_for_home + goals_for_away',
         [r['hist_goals_for_home'] + r['hist_goals_for_away'] for r in results]),
        ('hist_corners_for_home + corners_for_away',
         [r['hist_corners_for_home'] + r['hist_corners_for_away'] for r in results]),
        ('hist_poss_diff (|home - away|)',
         [abs(r['hist_poss_home'] - r['hist_poss_away']) for r in results]),
        ('hist_cards_home + cards_away',
         [r['hist_cards_home'] + r['hist_cards_away'] for r in results]),
        ('form_shots_total_home + away (ult 5)',
         [r['form_shots_total_home'] + r['form_shots_total_away'] for r in results]),
        ('venue: shots_for_home_H + shots_for_away_A',
         [r['hist_shots_for_home_venue'] + r['hist_shots_for_away_venue'] for r in results]),
        ('venue: (for+ag home H + for+ag away A)/2',
         [(r['hist_shots_for_home_venue'] + r['hist_shots_ag_home_venue'] + r['hist_shots_for_away_venue'] + r['hist_shots_ag_away_venue']) / 2 for r in results]),
    ]

    for name, pred in predictors_total:
        r, n = pearson(pred, actual_st)
        marker = ' ***' if abs(r) > 0.25 else (' **' if abs(r) > 0.15 else '')
        print(f"  {name:<45s} {r:>+.3f}  {n:>5d}{marker}")

    # --- B. Predictors for individual shots ---
    print(f"\n  B. Predictores de TIROS LOCAL (individual)")
    print(f"  {'-'*68}")
    actual_sl = [r['tiros_local'] for r in results]

    predictors_local = [
        ('hist_shots_for_home (tiro ataque local)',
         [r['hist_shots_for_home'] for r in results]),
        ('hist_shots_ag_away (tiro recibido visita)',
         [r['hist_shots_ag_away'] for r in results]),
        ('atk_home * def_away = for_H * ag_A / avg',
         [r['hist_shots_for_home'] * r['hist_shots_ag_away'] / (sum(actual_st) / len(actual_st) * 0.54) for r in results]),
        ('hist_sot_for_home (sot ataque local)',
         [r['hist_sot_for_home'] for r in results]),
        ('hist_goals_for_home',
         [r['hist_goals_for_home'] for r in results]),
        ('hist_poss_home',
         [r['hist_poss_home'] for r in results]),
        ('venue: shots_for home as home',
         [r['hist_shots_for_home_venue'] for r in results]),
        ('form_shots_for_home (ult 5)',
         [r['form_shots_for_home'] for r in results]),
    ]

    print(f"  {'Predictor':<45s} {'r':>7s}  {'N':>5s}")
    print(f"  {'-'*68}")
    for name, pred in predictors_local:
        r, n = pearson(pred, actual_sl)
        marker = ' ***' if abs(r) > 0.25 else (' **' if abs(r) > 0.15 else '')
        print(f"  {name:<45s} {r:>+.3f}  {n:>5d}{marker}")

    # --- C. SOT predictors ---
    print(f"\n  C. Predictores de TIROS AL ARCO TOTALES")
    print(f"  {'-'*68}")
    print(f"  {'Predictor':<45s} {'r':>7s}  {'N':>5s}")
    print(f"  {'-'*68}")

    predictors_sot = [
        ('hist_sot_for_home + sot_for_away',
         [r['hist_sot_for_home'] + r['hist_sot_for_away'] for r in results]),
        ('hist_shots_for_home + shots_for_away',
         [r['hist_shots_for_home'] + r['hist_shots_for_away'] for r in results]),
        ('hist_goals_for_home + goals_for_away',
         [r['hist_goals_for_home'] + r['hist_goals_for_away'] for r in results]),
        ('pace_match_pred',
         [r['pace_match_pred'] for r in results]),
        ('(sot_for + sot_ag) avg ambos / 2',
         [(r['hist_sot_for_home'] + r['hist_sot_ag_home'] + r['hist_sot_for_away'] + r['hist_sot_ag_away']) / 2 for r in results]),
    ]

    for name, pred in predictors_sot:
        r, n = pearson(pred, actual_sot)
        marker = ' ***' if abs(r) > 0.25 else (' **' if abs(r) > 0.15 else '')
        print(f"  {name:<45s} {r:>+.3f}  {n:>5d}{marker}")

    print()

    # --- D. MAE comparison ---
    print(f"  D. MAE de distintos predictores vs tiros totales reales")
    print(f"  {'-'*68}")

    naive_avg = sum(actual_st) / len(actual_st)
    pred_naive = [naive_avg] * len(actual_st)
    pred_hist_shots = [r['hist_shots_for_home'] + r['hist_shots_for_away'] for r in results]
    pred_hist_full = [(r['hist_shots_for_home'] + r['hist_shots_ag_home'] + r['hist_shots_for_away'] + r['hist_shots_ag_away']) / 2 for r in results]
    pred_pace = [r['pace_match_pred'] for r in results]
    pred_venue = [r['hist_shots_for_home_venue'] + r['hist_shots_for_away_venue'] for r in results]
    pred_form = [r['form_shots_total_home'] + r['form_shots_total_away'] for r in results]

    print(f"  {'Metodo':<45s} {'MAE':>7s}")
    print(f"  {'-'*55}")
    print(f"  {'Naive (promedio global)':<45s} {mae(pred_naive, actual_st):>7.2f}")
    print(f"  {'shots_for_home + shots_for_away':<45s} {mae(pred_hist_shots, actual_st):>7.2f}")
    print(f"  {'(for+ag home + for+ag away)/2':<45s} {mae(pred_hist_full, actual_st):>7.2f}")
    print(f"  {'venue specific for_H + for_A':<45s} {mae(pred_venue, actual_st):>7.2f}")
    print(f"  {'form ultimos 5 total_H + total_A':<45s} {mae(pred_form, actual_st):>7.2f}")

    # Scale pace to shots range for MAE
    if pred_pace:
        scale = sum(actual_st) / sum(pred_pace) if sum(pred_pace) > 0 else 1.0
        pred_pace_scaled = [p * scale for p in pred_pace]
        print(f"  {'pace (scaled to shots range)':<45s} {mae(pred_pace_scaled, actual_st):>7.2f}")

    print()
    return results


# ─────────────────────────────────────────────────────────────────────────────
# 3. PACE COMO PREDICTOR
# ─────────────────────────────────────────────────────────────────────────────

def analisis_pace(results):
    print("=" * 72)
    print("  3. PACE COMO PREDICTOR DE TIROS")
    print("=" * 72)
    print()

    # Split by pace quantiles
    sorted_by_pace = sorted(results, key=lambda r: r['pace_match_pred'])
    n = len(sorted_by_pace)
    q_size = n // 4

    print(f"  Cuartiles de pace predicho vs tiros reales:")
    print(f"  {'Cuartil':<12s}  {'Pace pred':>10s}  {'Tiros real':>10s}  {'SOT real':>10s}  {'Goles real':>10s}  N")
    print(f"  {'-'*70}")

    for i, label in enumerate(['Q1 (bajo)', 'Q2', 'Q3', 'Q4 (alto)']):
        start = i * q_size
        end = (i + 1) * q_size if i < 3 else n
        subset = sorted_by_pace[start:end]
        avg_pace = sum(r['pace_match_pred'] for r in subset) / len(subset)
        avg_shots = sum(r['tiros_total'] for r in subset) / len(subset)
        avg_sot = sum(r['sot_total'] for r in subset) / len(subset)
        avg_goals = sum(r['goles_total'] for r in subset) / len(subset)
        print(f"  {label:<12s}  {avg_pace:>10.1f}  {avg_shots:>10.1f}  {avg_sot:>10.1f}  {avg_goals:>10.1f}  {len(subset)}")

    # Ratio high/low
    q1 = sorted_by_pace[:q_size]
    q4 = sorted_by_pace[-q_size:]
    ratio_shots = (sum(r['tiros_total'] for r in q4) / len(q4)) / (sum(r['tiros_total'] for r in q1) / len(q1))
    ratio_sot = (sum(r['sot_total'] for r in q4) / len(q4)) / (sum(r['sot_total'] for r in q1) / len(q1))
    print(f"\n  Ratio Q4/Q1:  tiros={ratio_shots:.2f}x   SOT={ratio_sot:.2f}x")

    # Is pace predictive beyond shots?
    print(f"\n  Correlacion de pace vs tiros, controlando por hist_shots:")
    # Simple: residuals of shots predicted by hist_shots, then correlate with pace
    actual_st = [r['tiros_total'] for r in results]
    pred_shots = [(r['hist_shots_for_home'] + r['hist_shots_ag_home'] + r['hist_shots_for_away'] + r['hist_shots_ag_away']) / 2 for r in results]
    residuals = [a - p for a, p in zip(actual_st, pred_shots)]
    pace_vals = [r['pace_match_pred'] for r in results]
    r_resid, _ = pearson(pace_vals, residuals)
    print(f"  r(pace, residual_shots_after_hist) = {r_resid:+.3f}")
    print(f"  (Si > 0, pace agrega info mas alla del promedio de tiros)")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# 4. VARIABILIDAD - que tan predecible es cada variable?
# ─────────────────────────────────────────────────────────────────────────────

def analisis_variabilidad(rows):
    print("=" * 72)
    print("  4. VARIABILIDAD POR EQUIPO (CV intra-equipo)")
    print("     Cuanto menor el CV, mas predecible es la variable")
    print("=" * 72)
    print()

    team_data = defaultdict(lambda: {'shots': [], 'sot': [], 'goals': [], 'corners': []})
    for r in rows:
        for tid, is_home in [(safe_int(r['equipo_local_id']), True),
                              (safe_int(r['equipo_visitante_id']), False)]:
            if is_home:
                team_data[tid]['shots'].append(safe_int(r['tiros_local']))
                team_data[tid]['sot'].append(safe_int(r['tiros_arco_local']))
                team_data[tid]['goals'].append(safe_int(r['goles_local']))
                team_data[tid]['corners'].append(safe_int(r['corners_local']))
            else:
                team_data[tid]['shots'].append(safe_int(r['tiros_visitante']))
                team_data[tid]['sot'].append(safe_int(r['tiros_arco_visitante']))
                team_data[tid]['goals'].append(safe_int(r['goles_visitante']))
                team_data[tid]['corners'].append(safe_int(r['corners_visitante']))

    # Average CV across teams with enough data
    cvs = {'shots': [], 'sot': [], 'goals': [], 'corners': []}
    for tid, data in team_data.items():
        for var in cvs:
            vals = data[var]
            if len(vals) >= 8:
                avg = sum(vals) / len(vals)
                if avg > 0:
                    std = math.sqrt(sum((v - avg)**2 for v in vals) / len(vals))
                    cvs[var].append(std / avg)

    print(f"  {'Variable':<20s}  {'CV medio':>10s}  {'CV mediana':>10s}  {'Equipos':>8s}")
    print(f"  {'-'*55}")
    for var in ['shots', 'sot', 'goals', 'corners']:
        vals = sorted(cvs[var])
        if vals:
            avg_cv = sum(vals) / len(vals)
            med_cv = vals[len(vals) // 2]
            print(f"  {var:<20s}  {avg_cv:>10.3f}  {med_cv:>10.3f}  {len(vals):>8d}")

    print(f"\n  Interpretacion: CV mas alto = mas ruido = mas dificil de predecir")
    print(f"  Goles tiene CV alto porque son eventos raros (1-2 por equipo)")
    print(f"  Tiros deberia tener CV mas bajo (mas eventos), pero si no mejora")
    print(f"  la prediccion, el problema puede estar en el modelo, no en los datos.")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# 5. ANALISIS POR LIGA
# ─────────────────────────────────────────────────────────────────────────────

def analisis_por_liga(results, rows):
    print("=" * 72)
    print("  5. TIROS POR LIGA - promedios y predictibilidad")
    print("=" * 72)
    print()

    # Load liga names
    ligas = {}
    ligas_path = BASE / 'data/db/ligas.csv'
    if ligas_path.exists():
        with open(ligas_path, newline='', encoding='utf-8') as f:
            for r in csv.DictReader(f):
                ligas[int(r['id'])] = r['nombre']

    # Group by liga
    by_liga = defaultdict(list)
    for r in results:
        by_liga[r['liga_id']].append(r)

    print(f"  {'Liga':<30s}  {'N':>5s}  {'Tiros avg':>9s}  {'SOT avg':>8s}  {'r(pred)':>8s}  {'MAE':>6s}")
    print(f"  {'-'*75}")

    for lid in sorted(by_liga.keys(), key=lambda l: -len(by_liga[l])):
        subset = by_liga[lid]
        if len(subset) < 10:
            continue
        name = ligas.get(lid, f'Liga {lid}')[:28]
        avg_st = sum(r['tiros_total'] for r in subset) / len(subset)
        avg_sot = sum(r['sot_total'] for r in subset) / len(subset)

        pred = [(r['hist_shots_for_home'] + r['hist_shots_ag_home'] + r['hist_shots_for_away'] + r['hist_shots_ag_away']) / 2 for r in subset]
        actual = [r['tiros_total'] for r in subset]
        r_val, _ = pearson(pred, actual)
        mae_val = mae(pred, actual)

        print(f"  {name:<30s}  {len(subset):>5d}  {avg_st:>9.1f}  {avg_sot:>8.1f}  {r_val:>+.3f}   {mae_val:>6.2f}")

    print()


# ─────────────────────────────────────────────────────────────────────────────
# 6. DISTRIBUCION - Es Normal o necesitamos otra cosa?
# ─────────────────────────────────────────────────────────────────────────────

def analisis_distribucion(rows):
    print("=" * 72)
    print("  6. DISTRIBUCION DE TIROS - Es Normal la mejor opcion?")
    print("=" * 72)
    print()

    tiros = []
    sot = []
    for r in rows:
        sl = safe_int(r['tiros_local'])
        sv = safe_int(r['tiros_visitante'])
        if sl + sv > 0:
            tiros.extend([sl, sv])
            sot.extend([safe_int(r['tiros_arco_local']), safe_int(r['tiros_arco_visitante'])])

    # Histogram
    print(f"  Tiros por equipo por partido (n={len(tiros)}):")
    max_val = max(tiros)
    bins = defaultdict(int)
    for v in tiros:
        bins[v] += 1

    print(f"  {'Tiros':>6s}  {'Freq':>6s}  {'%':>6s}  Histograma")
    for v in range(0, min(max_val + 1, 30)):
        freq = bins.get(v, 0)
        pct = freq / len(tiros) * 100
        bar = '#' * int(pct * 2)
        if freq > 0:
            print(f"  {v:>6d}  {freq:>6d}  {pct:>5.1f}%  {bar}")

    avg = sum(tiros) / len(tiros)
    var = sum((v - avg)**2 for v in tiros) / len(tiros)
    skew = sum((v - avg)**3 for v in tiros) / (len(tiros) * var**1.5) if var > 0 else 0

    print(f"\n  Tiros: media={avg:.2f}  var={var:.2f}  var/media={var/avg:.2f}  skew={skew:+.2f}")
    print(f"    Normal: var/media deberia ser libre (es {var/avg:.2f})")
    print(f"    Poisson: var/media deberia ser ~1.0 (es {var/avg:.2f})")
    if var/avg > 1.3:
        print(f"    -> Sobredispersion: NegBin podria ser mejor que Poisson")

    # Same for SOT
    avg_s = sum(sot) / len(sot)
    var_s = sum((v - avg_s)**2 for v in sot) / len(sot)
    skew_s = sum((v - avg_s)**3 for v in sot) / (len(sot) * var_s**1.5) if var_s > 0 else 0
    print(f"\n  SOT: media={avg_s:.2f}  var={var_s:.2f}  var/media={var_s/avg_s:.2f}  skew={skew_s:+.2f}")
    if var_s/avg_s > 1.3:
        print(f"    -> Sobredispersion: NegBin podria ser mejor")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 72)
    print("  ANALISIS EXPLORATORIO: TIROS Y TIROS AL ARCO")
    print("=" * 72)
    print()

    rows = load_data()
    print(f"  Partidos cargados: {len(rows)}")
    print()

    analisis_intra_match(rows)
    results = compute_team_history(rows)
    analisis_predictivo(results)
    analisis_pace(results)
    analisis_variabilidad(rows)
    analisis_por_liga(results, rows)
    analisis_distribucion(rows)


if __name__ == '__main__':
    main()
