"""
analisis_goles.py
-----------------
Análisis de rendimiento del modelo de goles (Poisson doble):
  - Mercados: O/U totales, O/U local, O/U visita, BTTS
  - Rendimiento por rangos de cuota, calibración, EV, edge, threshold, torneo
  - Correlación entre variables del dataset y goles reales

Para cada bet de goles resuelta:
  1. Re-computa probabilidades con el modelo actual
  2. Recalcula edge y EV
  3. Filtra a bets donde el modelo actual ve valor (edge >= MIN_EDGE)
  4. Corre análisis completo

Uso:
    python scripts/analisis_goles.py
    python scripts/analisis_goles.py --n-sim 50000
    python scripts/analisis_goles.py --min-edge 0.04
    python scripts/analisis_goles.py --solo-correlacion
"""

import csv
import sys
import re
import math
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent))
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from modelo_v3 import (
    load_csv, compute_match_params, run_simulation,
    load_teams_db, resolve_team_id, MIN_EDGE,
)
from analizar_partido import compute_all_probs

VB_CSV   = Path(r'C:\Users\Matt\Apuestas Deportivas\data\apuestas\value_bets.csv')
HIST_CSV = Path(r'C:\Users\Matt\Apuestas Deportivas\data\historico\partidos_historicos.csv')
N_SIM    = 100_000
STAKE    = 1.0

SEP  = '=' * 76
SEP2 = '-' * 76

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _clean_comp(raw: str) -> str:
    m = re.match(r"\('(.+?)',\s*(?:True|False)\)", raw.strip())
    return m.group(1) if m else raw.strip()


def _prob_key(mercado: str, lado: str, team_local: str, team_visita: str) -> str | None:
    """Mapea mercado + lado → clave de probabilidad en el dict de probs."""
    lado_up = lado.strip()

    # BTTS
    if 'btts' in mercado.lower():
        if 'Si' in lado_up or 'Over' in lado_up:
            return 'btts_si'
        return 'btts_no'

    # O/U goles
    suffix = 'over' if lado_up == 'Over/Si' else 'under'
    m = re.search(r'(\d+\.5)', mercado)
    if not m:
        return None
    thr = m.group(1)

    if 'tot.' in mercado.lower():
        return f'g_{suffix}_{thr}'
    elif team_local and team_local.lower() in mercado.lower():
        return f'gl_{suffix}_{thr}'
    elif team_visita and team_visita.lower() in mercado.lower():
        return f'gv_{suffix}_{thr}'
    return None


def _pnl(resultado, odds):
    r = str(resultado).strip().upper()
    if r == 'W': return odds - STAKE
    if r == 'L': return -STAKE
    if r == 'V': return 0.0
    return None


# ── Clasificadores ────────────────────────────────────────────────────────────

_LIGAS_LOCALES = {
    'liga profesional', 'la liga', 'premier league', 'bundesliga',
    'serie a', 'brasileirao', 'brasileirao serie a', 'ligue 1',
}
_COPAS_EUR = {'champions league', 'europa league', 'conference league'}
_COPAS_SUD = {'copa libertadores', 'copa sudamericana'}
_COPAS_DOM = {'copa del rey', 'fa cup', 'dfb pokal', 'coppa italia', 'copa argentina'}


def _tipo_torneo(competicion):
    c = competicion.lower().strip()
    if any(l in c for l in _LIGAS_LOCALES):   return 'Ligas locales'
    if any(l in c for l in _COPAS_EUR):        return 'Copas europeas'
    if any(l in c for l in _COPAS_SUD):        return 'Copas sudamericanas'
    if any(l in c for l in _COPAS_DOM):        return 'Copas domesticas'
    return 'Otros'


def _sub_mercado(mercado, team_local='', team_visita=''):
    """Clasifica bet en: Totales / Local / Visita / BTTS."""
    m = mercado.lower()
    if 'btts' in m:
        return 'BTTS'
    if 'tot.' in m:
        return 'Totales'
    if team_local and team_local.lower() in m:
        return 'Local'
    if team_visita and team_visita.lower() in m:
        return 'Visita'
    return 'Individual'


def _sub_mercado_detallado(mercado, lado, team_local='', team_visita=''):
    m = mercado.lower()
    side = 'Over' if 'Over' in lado or 'Si' in lado else 'Under'
    if 'btts' in m:
        return f'BTTS {side}'
    if 'tot.' in m:
        return f'Totales {side}'
    if team_local and team_local.lower() in m:
        return f'Local {side}'
    if team_visita and team_visita.lower() in m:
        return f'Visita {side}'
    return f'Individual {side}'


def _rango_cuota(odds):
    if odds < 1.50:   return '1.01-1.50'
    if odds < 1.80:   return '1.50-1.80'
    if odds < 2.00:   return '1.80-2.00'
    if odds < 2.50:   return '2.00-2.50'
    if odds < 3.00:   return '2.50-3.00'
    return '>3.00'

_ORDEN_CUOTAS = ['1.01-1.50', '1.50-1.80', '1.80-2.00', '2.00-2.50', '2.50-3.00', '>3.00']


def _rango_prob(p):
    if p is None:     return None
    if p < 0.30:      return '<0.30'
    if p < 0.40:      return '0.30-0.40'
    if p < 0.50:      return '0.40-0.50'
    if p < 0.60:      return '0.50-0.60'
    if p < 0.70:      return '0.60-0.70'
    if p < 0.80:      return '0.70-0.80'
    return '>=0.80'

_ORDEN_PROBS = ['<0.30', '0.30-0.40', '0.40-0.50', '0.50-0.60',
                '0.60-0.70', '0.70-0.80', '>=0.80']


def _rango_ev(ev):
    if ev is None:  return None
    if ev < 0.05:   return '0-5%'
    if ev < 0.10:   return '5-10%'
    if ev < 0.15:   return '10-15%'
    if ev < 0.20:   return '15-20%'
    if ev < 0.30:   return '20-30%'
    return '>30%'

_ORDEN_EV = ['0-5%', '5-10%', '10-15%', '15-20%', '20-30%', '>30%']


def _rango_edge(edge):
    if edge is None: return None
    if edge < 0.04:  return '<4%'
    if edge < 0.06:  return '4-6%'
    if edge < 0.08:  return '6-8%'
    if edge < 0.10:  return '8-10%'
    if edge < 0.15:  return '10-15%'
    return '>15%'

_ORDEN_EDGE = ['<4%', '4-6%', '6-8%', '8-10%', '10-15%', '>15%']


def _rango_threshold(mercado):
    if 'btts' in mercado.lower():
        return 'BTTS'
    m = re.search(r'(\d+\.5)', mercado)
    return f'O/U {m.group(1)}' if m else 'Otro'


# ─────────────────────────────────────────────────────────────────────────────
# Estadísticas
# ─────────────────────────────────────────────────────────────────────────────

def _stats(bets):
    if not bets:
        return None
    total  = len(bets)
    wins   = sum(1 for a in bets if a['resultado'] == 'W')
    losses = sum(1 for a in bets if a['resultado'] == 'L')
    voids  = sum(1 for a in bets if a['resultado'] == 'V')
    pnl    = sum(a['pnl'] for a in bets)
    ev_esp = sum(a['ev_recalc'] for a in bets if a['ev_recalc'] is not None)
    edges  = [a['edge_recalc'] for a in bets if a['edge_recalc'] is not None]
    edge_m = sum(edges) / len(edges) if edges else 0.0
    odds_m = sum(a['odds'] for a in bets) / total
    probs  = [a['prob_recalc'] for a in bets if a['prob_recalc'] is not None]
    prob_m = sum(probs) / len(probs) if probs else 0.0
    base   = wins + losses
    return {
        'total': total, 'wins': wins, 'losses': losses, 'voids': voids,
        'pnl': pnl, 'ev_esp': ev_esp,
        'hit_rate': wins / base if base else 0.0,
        'roi_real': pnl / (total * STAKE) if total else 0.0,
        'roi_ev':   ev_esp / (total * STAKE) if total else 0.0,
        'edge_m': edge_m, 'odds_m': odds_m, 'prob_m': prob_m,
    }


def _fmt(v, pct=False, sign=True):
    if v is None: return '  -  '
    if pct:
        return f"{'+' if sign and v >= 0 else ''}{v:.1%}"
    return f"{'+' if sign and v >= 0 else ''}{v:.3f}u"


def _barra(val, max_val=0.50, width=18):
    filled = int(min(abs(val) / max_val, 1.0) * width)
    bar = '#' * filled + '.' * (width - filled)
    return f"[{bar}]" if val >= 0 else f"[{'.' * (width - filled)}{'#' * filled}](−)"


# ─────────────────────────────────────────────────────────────────────────────
# Tabla genérica
# ─────────────────────────────────────────────────────────────────────────────

def _tabla(grupos, orden, label='Grupo', width=18, min_n=1):
    hdr = (f"  {label:<{width}}  {'N':>4}  {'W':>3}  {'L':>3}  "
           f"{'Hit%':>6}  {'Odds':>5}  {'Edge%':>6}  "
           f"{'P&L':>8}  {'EV':>8}  {'ROI%':>7}  {'dROI':>7}")
    print(hdr)
    print(f"  {SEP2}")
    for k in orden:
        bets = grupos.get(k, [])
        if len(bets) < min_n:
            continue
        s = _stats(bets)
        delta = s['roi_real'] - s['roi_ev']
        print(
            f"  {str(k):<{width}}  {s['total']:>4}  {s['wins']:>3}  {s['losses']:>3}  "
            f"{s['hit_rate']:>5.1%}  {s['odds_m']:>5.2f}  {s['edge_m']:>5.1%}  "
            f"{_fmt(s['pnl']):>8}  {_fmt(s['ev_esp']):>8}  "
            f"{s['roi_real']:>+6.1%}  {delta:>+6.1%}"
        )


def _tabla_calibracion(bets, rangos, min_n=2):
    print(f"  {'Rango prob':<12}  {'N':>4}  {'Win% real':>10}  "
          f"{'Prob media':>10}  {'Delta':>7}  {'ROI%':>7}")
    print(f"  {SEP2}")
    grp = defaultdict(list)
    for a in bets:
        rp = a.get('rango_prob')
        if rp:
            grp[rp].append(a)
    for rango in rangos:
        g = grp.get(rango, [])
        if len(g) < min_n:
            continue
        wins = sum(1 for a in g if a['resultado'] == 'W')
        base = sum(1 for a in g if a['resultado'] in ('W', 'L'))
        wr   = wins / base if base else 0.0
        probs = [a['prob_recalc'] for a in g if a['prob_recalc'] is not None]
        pm   = sum(probs) / len(probs) if probs else 0.0
        delta = wr - pm
        roi  = _stats(g)['roi_real']
        flag = '  <<' if abs(delta) > 0.08 else ''
        print(f"  {rango:<12}  {len(g):>4}  {wr:>9.1%}  "
              f"{pm:>9.1%}  {delta:>+6.1%}  {roi:>+6.1%}{flag}")


def _tabla_ev_edge(bets, campo, orden, label, min_n=2):
    grp = defaultdict(list)
    for a in bets:
        k = a.get(campo)
        if k:
            grp[k].append(a)
    print(f"  {label:<12}  {'N':>4}  {'Win%':>7}  {'ROI real':>9}  "
          f"{'EV medio':>9}  {'dROI':>7}  Trend")
    print(f"  {SEP2}")
    prev_roi = None
    for rango in orden:
        g = grp.get(rango, [])
        if len(g) < min_n:
            continue
        s = _stats(g)
        evs = [a['ev_recalc'] for a in g if a['ev_recalc'] is not None]
        ev_m = (sum(evs) / len(evs)) if evs else 0.0
        delta = s['roi_real'] - s['roi_ev']
        if prev_roi is None:
            trend = '  --'
        elif s['roi_real'] > prev_roi + 0.005:
            trend = '  UP'
        elif s['roi_real'] < prev_roi - 0.005:
            trend = '  DOWN  <<'
        else:
            trend = '  ~'
        prev_roi = s['roi_real']
        print(f"  {rango:<12}  {s['total']:>4}  {s['hit_rate']:>6.1%}  "
              f"{s['roi_real']:>+8.1%}  {ev_m:>+8.1%}  {delta:>+6.1%}{trend}")


# ─────────────────────────────────────────────────────────────────────────────
# Correlación variables históricas vs goles
# ─────────────────────────────────────────────────────────────────────────────

def _pearson(xs, ys):
    n = len(xs)
    if n < 3:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    sx = math.sqrt(sum((x - mx)**2 for x in xs) / n)
    sy = math.sqrt(sum((y - my)**2 for y in ys) / n)
    if sx == 0 or sy == 0:
        return None
    cov = sum((xs[i] - mx) * (ys[i] - my) for i in range(n)) / n
    return cov / (sx * sy)


def analisis_correlacion(hist_rows):
    """Calcula correlación Pearson entre variables del dataset y goles."""
    print(f"\n{SEP}")
    print("  CORRELACIÓN VARIABLES HISTÓRICAS vs GOLES")
    print(SEP)

    # Preparar datos
    datos = []
    for r in hist_rows:
        try:
            gl = int(r['goles_local'])
            gv = int(r['goles_visitante'])
            gt = gl + gv
            tl = int(r['tiros_local'])
            tv = int(r['tiros_visitante'])
            tal = int(r['tiros_arco_local'])
            tav = int(r['tiros_arco_visitante'])
            cl = int(r['corners_local'])
            cv = int(r['corners_visitante'])
            pl = int(r['posesion_local'])
            pv = int(r['posesion_visitante'])
            yl = int(r['tarjetas_local'])
            yv = int(r['tarjetas_visitante'])
        except (ValueError, KeyError):
            continue

        datos.append({
            'goles_total': gt,
            'goles_local': gl,
            'goles_visitante': gv,
            'diff_goles': gl - gv,
            'tiros_local': tl,
            'tiros_visitante': tv,
            'tiros_total': tl + tv,
            'tiros_arco_local': tal,
            'tiros_arco_visitante': tav,
            'tiros_arco_total': tal + tav,
            'corners_local': cl,
            'corners_visitante': cv,
            'corners_total': cl + cv,
            'posesion_local': pl,
            'posesion_visitante': pv,
            'diff_posesion': pl - pv,
            'tarjetas_local': yl,
            'tarjetas_visitante': yv,
            'tarjetas_total': yl + yv,
            # Ratios derivados
            'precision_local': tal / tl if tl > 0 else 0,
            'precision_visitante': tav / tv if tv > 0 else 0,
            'diff_tiros': tl - tv,
            'diff_tiros_arco': tal - tav,
            'diff_corners': cl - cv,
        })

    n = len(datos)
    print(f"\n  Partidos analizados: {n}")

    # ── 1. Correlaciones con goles totales ────────────────────────────────────
    print(f"\n  1. CORRELACIÓN CON GOLES TOTALES")
    print(f"  {SEP2}")

    variables_total = [
        ('tiros_total',          'Tiros totales'),
        ('tiros_arco_total',     'Tiros al arco totales'),
        ('corners_total',        'Corners totales'),
        ('tarjetas_total',       'Tarjetas totales'),
        ('tiros_local',          'Tiros local'),
        ('tiros_visitante',      'Tiros visitante'),
        ('tiros_arco_local',     'Tiros arco local'),
        ('tiros_arco_visitante', 'Tiros arco visitante'),
        ('corners_local',        'Corners local'),
        ('corners_visitante',    'Corners visitante'),
        ('diff_posesion',        'Diff posesion (L-V)'),
        ('diff_tiros',           'Diff tiros (L-V)'),
        ('diff_tiros_arco',      'Diff tiros arco (L-V)'),
        ('diff_corners',         'Diff corners (L-V)'),
    ]

    gt = [d['goles_total'] for d in datos]
    results_total = []
    for var_key, var_label in variables_total:
        xs = [d[var_key] for d in datos]
        r_val = _pearson(xs, gt)
        if r_val is not None:
            results_total.append((var_label, r_val))

    results_total.sort(key=lambda x: -abs(x[1]))
    print(f"  {'Variable':<28}  {'Pearson r':>10}  {'|r|':>5}  Fuerza")
    print(f"  {SEP2}")
    for label, r_val in results_total:
        absv = abs(r_val)
        if absv >= 0.5:   fuerza = 'FUERTE'
        elif absv >= 0.3: fuerza = 'MODERADA'
        elif absv >= 0.1: fuerza = 'DÉBIL'
        else:             fuerza = 'NULA'
        bar_len = int(absv * 40)
        bar = '#' * bar_len + '.' * (40 - bar_len)
        print(f"  {label:<28}  {r_val:>+10.4f}  {absv:>5.3f}  {fuerza:<10} [{bar}]")

    # ── 2. Correlaciones con goles LOCAL ──────────────────────────────────────
    print(f"\n  2. CORRELACIÓN CON GOLES LOCAL")
    print(f"  {SEP2}")

    variables_local = [
        ('tiros_local',          'Tiros local'),
        ('tiros_arco_local',     'Tiros arco local'),
        ('corners_local',        'Corners local'),
        ('posesion_local',       'Posesion local'),
        ('tarjetas_local',       'Tarjetas local'),
        ('precision_local',      'Precision local (arco/tiros)'),
        ('tiros_visitante',      'Tiros visitante'),
        ('tiros_arco_visitante', 'Tiros arco visitante'),
        ('corners_visitante',    'Corners visitante'),
        ('tarjetas_visitante',   'Tarjetas visitante'),
    ]

    gl_arr = [d['goles_local'] for d in datos]
    results_local = []
    for var_key, var_label in variables_local:
        xs = [d[var_key] for d in datos]
        r_val = _pearson(xs, gl_arr)
        if r_val is not None:
            results_local.append((var_label, r_val))

    results_local.sort(key=lambda x: -abs(x[1]))
    print(f"  {'Variable':<28}  {'Pearson r':>10}  {'|r|':>5}  Fuerza")
    print(f"  {SEP2}")
    for label, r_val in results_local:
        absv = abs(r_val)
        if absv >= 0.5:   fuerza = 'FUERTE'
        elif absv >= 0.3: fuerza = 'MODERADA'
        elif absv >= 0.1: fuerza = 'DÉBIL'
        else:             fuerza = 'NULA'
        bar_len = int(absv * 40)
        bar = '#' * bar_len + '.' * (40 - bar_len)
        print(f"  {label:<28}  {r_val:>+10.4f}  {absv:>5.3f}  {fuerza:<10} [{bar}]")

    # ── 3. Correlaciones con goles VISITANTE ──────────────────────────────────
    print(f"\n  3. CORRELACIÓN CON GOLES VISITANTE")
    print(f"  {SEP2}")

    variables_visita = [
        ('tiros_visitante',      'Tiros visitante'),
        ('tiros_arco_visitante', 'Tiros arco visitante'),
        ('corners_visitante',    'Corners visitante'),
        ('posesion_visitante',   'Posesion visitante'),
        ('tarjetas_visitante',   'Tarjetas visitante'),
        ('precision_visitante',  'Precision vis (arco/tiros)'),
        ('tiros_local',          'Tiros local'),
        ('tiros_arco_local',     'Tiros arco local'),
        ('corners_local',        'Corners local'),
        ('tarjetas_local',       'Tarjetas local'),
    ]

    gv_arr = [d['goles_visitante'] for d in datos]
    results_visita = []
    for var_key, var_label in variables_visita:
        xs = [d[var_key] for d in datos]
        r_val = _pearson(xs, gv_arr)
        if r_val is not None:
            results_visita.append((var_label, r_val))

    results_visita.sort(key=lambda x: -abs(x[1]))
    print(f"  {'Variable':<28}  {'Pearson r':>10}  {'|r|':>5}  Fuerza")
    print(f"  {SEP2}")
    for label, r_val in results_visita:
        absv = abs(r_val)
        if absv >= 0.5:   fuerza = 'FUERTE'
        elif absv >= 0.3: fuerza = 'MODERADA'
        elif absv >= 0.1: fuerza = 'DÉBIL'
        else:             fuerza = 'NULA'
        bar_len = int(absv * 40)
        bar = '#' * bar_len + '.' * (40 - bar_len)
        print(f"  {label:<28}  {r_val:>+10.4f}  {absv:>5.3f}  {fuerza:<10} [{bar}]")

    # ── 4. Matriz de correlación entre variables ──────────────────────────────
    print(f"\n  4. CORRELACIONES INTER-VARIABLES (top pares)")
    print(f"  {SEP2}")

    all_vars = [
        'goles_total', 'goles_local', 'goles_visitante',
        'tiros_total', 'tiros_local', 'tiros_visitante',
        'tiros_arco_total', 'tiros_arco_local', 'tiros_arco_visitante',
        'corners_total', 'corners_local', 'corners_visitante',
        'tarjetas_total',
    ]

    pares = []
    for i, v1 in enumerate(all_vars):
        for v2 in all_vars[i+1:]:
            xs = [d[v1] for d in datos]
            ys = [d[v2] for d in datos]
            r_val = _pearson(xs, ys)
            if r_val is not None:
                pares.append((v1, v2, r_val))

    pares.sort(key=lambda x: -abs(x[2]))
    print(f"  {'Variable 1':<24}  {'Variable 2':<24}  {'r':>8}")
    print(f"  {SEP2}")
    for v1, v2, r_val in pares[:25]:
        print(f"  {v1:<24}  {v2:<24}  {r_val:>+7.4f}")

    # ── 5. Estadísticas descriptivas de goles ─────────────────────────────────
    print(f"\n  5. DISTRIBUCIÓN DE GOLES")
    print(f"  {SEP2}")

    from collections import Counter
    gt_counts = Counter(gt)
    gl_counts = Counter(gl_arr)
    gv_counts = Counter(gv_arr)

    avg_gt = sum(gt) / len(gt)
    avg_gl = sum(gl_arr) / len(gl_arr)
    avg_gv = sum(gv_arr) / len(gv_arr)
    std_gt = math.sqrt(sum((x - avg_gt)**2 for x in gt) / len(gt))
    std_gl = math.sqrt(sum((x - avg_gl)**2 for x in gl_arr) / len(gl_arr))
    std_gv = math.sqrt(sum((x - avg_gv)**2 for x in gv_arr) / len(gv_arr))

    print(f"  {'':>12}  {'Media':>6}  {'Std':>6}  {'Min':>4}  {'Max':>4}  {'Moda':>5}")
    print(f"  {SEP2}")
    print(f"  {'G. total':<12}  {avg_gt:>6.2f}  {std_gt:>6.2f}  {min(gt):>4}  {max(gt):>4}  {gt_counts.most_common(1)[0][0]:>5}")
    print(f"  {'G. local':<12}  {avg_gl:>6.2f}  {std_gl:>6.2f}  {min(gl_arr):>4}  {max(gl_arr):>4}  {gl_counts.most_common(1)[0][0]:>5}")
    print(f"  {'G. visita':<12}  {avg_gv:>6.2f}  {std_gv:>6.2f}  {min(gv_arr):>4}  {max(gv_arr):>4}  {gv_counts.most_common(1)[0][0]:>5}")

    print(f"\n  Distribución goles totales:")
    for g in range(max(gt) + 1):
        cnt = gt_counts.get(g, 0)
        pct = cnt / n
        bar = '#' * int(pct * 60)
        print(f"    {g:>2} goles: {cnt:>4} ({pct:>5.1%})  {bar}")

    # ── 6. Promedio de variables según rangos de goles ─────────────────────────
    print(f"\n  6. PERFIL DE PARTIDOS SEGÚN GOLES TOTALES")
    print(f"  {SEP2}")

    rangos_g = [(0, 1, '0-1 goles'), (2, 2, '2 goles'), (3, 3, '3 goles'), (4, 99, '4+ goles')]
    vars_perfil = [
        ('tiros_total', 'Tiros tot'),
        ('tiros_arco_total', 'Arco tot'),
        ('corners_total', 'Corners tot'),
        ('tarjetas_total', 'Tarjetas'),
    ]

    hdr = f"  {'Rango goles':<12}"
    for _, label in vars_perfil:
        hdr += f"  {label:>12}"
    hdr += f"  {'N':>5}"
    print(hdr)
    print(f"  {SEP2}")

    for gmin, gmax, label in rangos_g:
        subset = [d for d in datos if gmin <= d['goles_total'] <= gmax]
        if not subset:
            continue
        line = f"  {label:<12}"
        for var_key, _ in vars_perfil:
            avg = sum(d[var_key] for d in subset) / len(subset)
            line += f"  {avg:>12.1f}"
        line += f"  {len(subset):>5}"
        print(line)

    print()


# ─────────────────────────────────────────────────────────────────────────────
# Carga y re-cálculo
# ─────────────────────────────────────────────────────────────────────────────

def cargar_goles_resueltos():
    with open(VB_CSV, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    return [
        r for r in rows
        if ('gol' in r.get('mercado', '').lower() or 'btts' in r.get('mercado', '').lower())
        and r.get('resultado', '').strip().upper() in ('W', 'L', 'V')
    ]


def recalcular_probs(bets, hist_rows, n_sim, min_edge):
    _, name_to_id = load_teams_db()
    by_fixture = defaultdict(list)
    for b in bets:
        by_fixture[b['fixture_id']].append(b)

    resultado = []
    total_fix = len(by_fixture)

    for i, (fid, group) in enumerate(by_fixture.items(), 1):
        sample = group[0]
        partido = sample['partido']
        comp = _clean_comp(sample['competicion'])
        partes = partido.split(' vs ', 1)

        if len(partes) != 2:
            print(f"  [skip] no se pudo parsear: '{partido}'")
            continue

        team_local, team_visita = partes[0].strip(), partes[1].strip()
        local_id = resolve_team_id(team_local, name_to_id)
        vis_id   = resolve_team_id(team_visita, name_to_id)
        if local_id is None or vis_id is None:
            missing = team_local if local_id is None else team_visita
            print(f"  [skip] equipo no encontrado: '{missing}'")
            continue

        try:
            params = compute_match_params(local_id, vis_id, hist_rows, comp)
            sim    = run_simulation(params, n_sim)
            probs  = compute_all_probs(sim)
        except Exception as e:
            print(f"  [error] {partido}: {e}")
            continue

        lam_l = params['lambda_local']
        lam_v = params['lambda_vis']
        print(f"  [{i:>3}/{total_fix}] {partido:<35}  λL={lam_l:.2f}  λV={lam_v:.2f}  E[tot]={lam_l+lam_v:.2f}")

        for b in group:
            odds = float(b['odds'])
            ip   = float(b.get('implied_prob', 0)) if b.get('implied_prob') else 1.0 / odds
            pk   = _prob_key(b['mercado'], b['lado'], team_local, team_visita)
            p_rc = probs.get(pk) if pk else None

            if p_rc is None:
                continue

            edge_rc = p_rc - ip
            ev_rc   = (p_rc * odds - 1.0)
            pnl_val = _pnl(b['resultado'], odds)

            resultado.append({
                'partido':      partido,
                'competicion':  comp,
                'mercado':      b['mercado'],
                'lado':         b['lado'],
                'fixture_id':   fid,
                'odds':         odds,
                'resultado':    b['resultado'].strip().upper(),
                'pnl':          pnl_val,
                'prob_orig':    float(b.get('modelo_prob', 0)) if b.get('modelo_prob') else None,
                'prob_recalc':  p_rc,
                'implied_prob': ip,
                'edge_recalc':  edge_rc,
                'ev_recalc':    ev_rc,
                'value_recalc': edge_rc >= min_edge,
                'team_local':   team_local,
                'team_visita':  team_visita,
                'lambda_local': lam_l,
                'lambda_vis':   lam_v,
                # Clasificación
                'tipo_torneo':      _tipo_torneo(comp),
                'sub_mercado':      _sub_mercado(b['mercado'], team_local, team_visita),
                'sub_mercado_det':  _sub_mercado_detallado(b['mercado'], b['lado'], team_local, team_visita),
                'rango_cuota':      _rango_cuota(odds),
                'rango_prob':       _rango_prob(p_rc),
                'rango_ev':         _rango_ev(ev_rc),
                'rango_edge':       _rango_edge(edge_rc),
                'rango_threshold':  _rango_threshold(b['mercado']),
            })

    return resultado


# ─────────────────────────────────────────────────────────────────────────────
# Reporte
# ─────────────────────────────────────────────────────────────────────────────

def _reporte_seccion(bets, titulo, min_n=2):
    s = _stats(bets)
    if s is None or s['total'] < 2:
        print(f"\n  {titulo}: solo {len(bets)} bet(s) — se omite")
        return

    BSEP = '█' * 76

    print(f"\n{BSEP}")
    print(f"  {titulo}")
    print(f"  {s['total']} bets  ({s['wins']}W / {s['losses']}L / {s['voids']}V)  "
          f"|  Odds medias: {s['odds_m']:.2f}  |  Edge medio: {s['edge_m']:+.1%}")
    print(f"  ROI real: {s['roi_real']:+.1%}   EV esp: {s['roi_ev']:+.1%}   "
          f"dROI: {(s['roi_real'] - s['roi_ev']):+.1%}   P&L: {_fmt(s['pnl'])}")
    print(BSEP)

    # ── A. Over vs Under ──────────────────────────────────────────────────────
    print(f"\n  A. OVER vs UNDER")
    print(f"  {SEP2}")
    by_side = defaultdict(list)
    for a in bets:
        side = 'Over' if ('Over' in a['lado'] or 'Si' in a['lado']) else 'Under'
        by_side[side].append(a)
    sides = [s for s in ['Over', 'Under'] if s in by_side]
    if sides:
        _tabla(by_side, sides, label='Lado', width=10, min_n=1)

    # ── B. Por rango de cuota ─────────────────────────────────────────────────
    print(f"\n  B. POR RANGO DE CUOTA")
    print(f"  {SEP2}")
    by_cuota = defaultdict(list)
    for a in bets:
        by_cuota[a['rango_cuota']].append(a)
    _tabla(by_cuota, _ORDEN_CUOTAS, label='Rango cuota', width=12, min_n=min_n)
    print()
    for rango in _ORDEN_CUOTAS:
        g = by_cuota.get(rango)
        if not g or len(g) < min_n:
            continue
        st = _stats(g)
        print(f"  {rango:<12}  {_barra(st['roi_real'])}  {st['roi_real']:>+6.1%}  (n={st['total']})")

    # ── C. Calibración de probabilidades ──────────────────────────────────────
    print(f"\n  C. CALIBRACIÓN PROBABILIDADES (Win% real vs Prob modelo)")
    print(f"  {SEP2}")
    _tabla_calibracion(bets, _ORDEN_PROBS, min_n=min_n)

    # ── D. Calibración del EV ─────────────────────────────────────────────────
    print(f"\n  D. CALIBRACIÓN EV  (mayor EV → mayor ROI?)")
    print(f"  {SEP2}")
    _tabla_ev_edge(bets, 'rango_ev', _ORDEN_EV, 'EV predicho', min_n=min_n)

    # ── E. Calibración del edge ───────────────────────────────────────────────
    print(f"\n  E. CALIBRACIÓN EDGE  (edge = P_modelo − 1/odds)")
    print(f"  {SEP2}")
    _tabla_ev_edge(bets, 'rango_edge', _ORDEN_EDGE, 'Edge prob', min_n=min_n)

    # ── F. Por threshold (O/U línea) ──────────────────────────────────────────
    print(f"\n  F. POR THRESHOLD (línea)")
    print(f"  {SEP2}")
    by_thr = defaultdict(list)
    for a in bets:
        by_thr[a['rango_threshold']].append(a)
    orden_thr = sorted(by_thr.keys(),
                       key=lambda k: float(re.search(r'[\d.]+', k).group())
                       if re.search(r'[\d.]+', k) else 0)
    _tabla(by_thr, orden_thr, label='Threshold', width=12, min_n=1)

    # ── G. Por tipo de torneo ─────────────────────────────────────────────────
    print(f"\n  G. POR TIPO DE TORNEO")
    print(f"  {SEP2}")
    by_tipo = defaultdict(list)
    for a in bets:
        by_tipo[a['tipo_torneo']].append(a)
    _ORDEN_TIPO = ['Ligas locales', 'Copas europeas', 'Copas sudamericanas',
                   'Copas domesticas', 'Otros']
    _tabla(by_tipo, _ORDEN_TIPO, label='Tipo torneo', width=22, min_n=1)

    # ── H. Por competición ────────────────────────────────────────────────────
    print(f"\n  H. POR COMPETICIÓN")
    print(f"  {SEP2}")
    by_comp = defaultdict(list)
    for a in bets:
        by_comp[a['competicion']].append(a)
    orden_comp = sorted(by_comp.keys(), key=lambda k: -len(by_comp[k]))
    _tabla(by_comp, orden_comp, label='Competición', width=26, min_n=1)

    # ── I. Top bets por edge ──────────────────────────────────────────────────
    print(f"\n  I. TOP BETS POR EDGE")
    print(f"  {SEP2}")
    sorted_edge = sorted(bets, key=lambda b: b['edge_recalc'], reverse=True)
    print(f"  {'Partido':<30}  {'Mercado':<25}  {'Lado':>8}  "
          f"{'p_mod':>6}  {'edge':>6}  {'odds':>5}  {'Res':>3}  {'P&L':>7}")
    print(f"  {SEP2}")
    for b in sorted_edge[:15]:
        partido_short = b['partido'][:28]
        merc_short = b['mercado'][:23]
        print(f"  {partido_short:<30}  {merc_short:<25}  {b['lado']:>8}  "
              f"{b['prob_recalc']:>5.1%}  {b['edge_recalc']:>5.1%}  "
              f"{b['odds']:>5.2f}  {b['resultado']:>3}  {b['pnl']:>+6.2f}u")

    print()


def reporte(all_bets, min_edge):
    bets_value = [b for b in all_bets if b['value_recalc']]
    bets_no    = [b for b in all_bets if not b['value_recalc']]

    s_all   = _stats(all_bets)
    s_value = _stats(bets_value)
    s_no    = _stats(bets_no)

    # ══════════════════════════════════════════════════════════════════════════
    # ENCABEZADO GLOBAL
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print(f"  ANÁLISIS DE RENDIMIENTO GOLES — MODELO v3 (Poisson doble)")
    print(f"  min_edge={min_edge:.0%}  stake={STAKE:.0f}u  N_SIM={N_SIM:,}")
    print(SEP)

    print(f"\n  Total goal bets mapeadas    : {s_all['total']}")
    print(f"  Modelo confirma valor       : {s_value['total'] if s_value else 0}")
    print(f"  Modelo rechaza              : {s_no['total'] if s_no else 0}")

    print(f"\n  {'Universo':<25}  {'N':>4}  {'W/L':>7}  {'Hit%':>6}  "
          f"{'P&L':>9}  {'ROI%':>7}  {'Edge medio':>10}")
    print(f"  {SEP2}")
    for label, s in [('Todas (prob orig)',   s_all),
                     ('Modelo confirma',     s_value),
                     ('Modelo rechaza',      s_no)]:
        if s is None:
            continue
        print(f"  {label:<25}  {s['total']:>4}  "
              f"{s['wins']}W/{s['losses']}L  {s['hit_rate']:>5.1%}  "
              f"{_fmt(s['pnl']):>9}  {s['roi_real']:>+6.1%}  {s['edge_m']:>+9.1%}")

    bets = bets_value
    if not bets:
        print("\n  No hay bets confirmadas por el modelo para analizar.")
        return

    # ══════════════════════════════════════════════════════════════════════════
    # RESUMEN POR SUB-MERCADO (Totales / Local / Visita / BTTS)
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print("  RESUMEN POR SUB-MERCADO (bets confirmadas)")
    print(SEP)

    by_sub = defaultdict(list)
    for a in bets:
        by_sub[a['sub_mercado']].append(a)
    _tabla(by_sub, ['Totales', 'Local', 'Visita', 'BTTS'], label='Sub-mercado', width=14)

    # Detallado over/under
    print()
    by_sub_det = defaultdict(list)
    for a in bets:
        by_sub_det[a['sub_mercado_det']].append(a)
    orden_det = ['Totales Over', 'Totales Under',
                 'Local Over', 'Local Under',
                 'Visita Over', 'Visita Under',
                 'BTTS Over', 'BTTS Under']
    _tabla(by_sub_det, orden_det, label='Detalle', width=16)

    # ══════════════════════════════════════════════════════════════════════════
    # ANÁLISIS DETALLADO POR SUB-MERCADO
    # ══════════════════════════════════════════════════════════════════════════
    for sub_name in ['Totales', 'Local', 'Visita', 'BTTS']:
        sub_bets = by_sub.get(sub_name, [])
        _reporte_seccion(sub_bets, f'GOLES {sub_name.upper()}')

    # ══════════════════════════════════════════════════════════════════════════
    # ANÁLISIS DE λ (lambda) — parámetros del modelo
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print("  ANÁLISIS DE LAMBDA (parámetros Poisson)")
    print(SEP)

    lambdas_l = [b['lambda_local'] for b in bets]
    lambdas_v = [b['lambda_vis'] for b in bets]
    lambdas_t = [b['lambda_local'] + b['lambda_vis'] for b in bets]

    print(f"\n  {'Parámetro':<15}  {'Media':>6}  {'Min':>6}  {'Max':>6}  {'Std':>6}")
    print(f"  {SEP2}")
    for label, vals in [('λ local', lambdas_l), ('λ visita', lambdas_v), ('λ total', lambdas_t)]:
        avg = sum(vals) / len(vals)
        std = math.sqrt(sum((v - avg)**2 for v in vals) / len(vals))
        print(f"  {label:<15}  {avg:>6.2f}  {min(vals):>6.2f}  {max(vals):>6.2f}  {std:>6.2f}")

    # ROI por rango de lambda total
    print(f"\n  ROI por rango de λ total:")
    print(f"  {SEP2}")
    rangos_lambda = [(0, 1.5, '<1.5'), (1.5, 2.0, '1.5-2.0'), (2.0, 2.5, '2.0-2.5'),
                     (2.5, 3.0, '2.5-3.0'), (3.0, 99, '>3.0')]
    for lmin, lmax, label in rangos_lambda:
        subset = [b for b in bets if lmin <= (b['lambda_local'] + b['lambda_vis']) < lmax]
        if len(subset) < 2:
            continue
        s = _stats(subset)
        print(f"  {label:<10}  N={s['total']:>3}  W={s['wins']}  L={s['losses']}  "
              f"Hit={s['hit_rate']:.1%}  ROI={s['roi_real']:+.1%}  P&L={_fmt(s['pnl'])}")

    # ══════════════════════════════════════════════════════════════════════════
    # RESUMEN FINAL GLOBAL
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print("  RESUMEN FINAL GLOBAL")
    print(SEP)
    s = _stats(bets)
    print(f"  Bets analizadas (confirma): {s['total']}")
    print(f"  Hit rate     : {s['hit_rate']:.1%}")
    print(f"  ROI real     : {s['roi_real']:+.1%}")
    print(f"  ROI EV esp.  : {s['roi_ev']:+.1%}")
    print(f"  dROI         : {(s['roi_real'] - s['roi_ev']):+.1%}")
    print(f"  Edge medio   : {s['edge_m']:+.1%}")
    print(f"  Odds medias  : {s['odds_m']:.2f}")
    print(f"  P&L          : {_fmt(s['pnl'])}")
    print(f"  EV esperado  : {_fmt(s['ev_esp'])}")

    print(f"\n  {'Sub-mercado':<12}  {'N':>4}  {'ROI%':>7}  {'P&L':>9}  {'Edge%':>7}  Veredicto")
    print(f"  {SEP2}")
    for sub_name in ['Totales', 'Local', 'Visita', 'BTTS']:
        sub_bets = by_sub.get(sub_name, [])
        ss = _stats(sub_bets)
        if ss is None:
            continue
        v = 'EN PRODUCCION' if ss['roi_real'] > 0 else 'REVISAR' if ss['roi_real'] > -0.15 else 'NO APOSTAR'
        print(f"  {sub_name:<12}  {ss['total']:>4}  {ss['roi_real']:>+6.1%}  "
              f"{_fmt(ss['pnl']):>9}  {ss['edge_m']:>+6.1%}  {v}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    n_sim    = N_SIM
    min_edge = MIN_EDGE
    solo_corr = False

    args = sys.argv[1:]
    if '--n-sim' in args:
        n_sim = int(args[args.index('--n-sim') + 1])
    if '--min-edge' in args:
        min_edge = float(args[args.index('--min-edge') + 1])
    if '--solo-correlacion' in args:
        solo_corr = True

    print("Cargando datos...")
    hist_rows = load_csv(str(HIST_CSV))
    print(f"  {len(hist_rows)} partidos históricos")

    # Siempre correr correlación
    analisis_correlacion(hist_rows)

    if solo_corr:
        print("\n  (--solo-correlacion: se omite el análisis de bets)")
        sys.exit(0)

    bets_raw = cargar_goles_resueltos()
    print(f"  {len(bets_raw)} bets de goles resueltas")

    print(f"\nRe-calculando probabilidades (n_sim={n_sim:,})...")
    all_bets = recalcular_probs(bets_raw, hist_rows, n_sim, min_edge)
    print(f"\n  {len(all_bets)} bets mapeadas exitosamente")

    reporte(all_bets, min_edge)
