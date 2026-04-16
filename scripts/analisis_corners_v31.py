"""
analisis_corners_v31.py
-----------------------
Análisis de rendimiento exclusivo para corners usando probabilidades v3.1
(NegBin+Binomial) re-computadas con el modelo actual.

Para cada bet de corners resuelta:
  1. Re-computa prob v3.1 (modelo actual)
  2. Recalcula edge y EV con la prob nueva
  3. Filtra a bets donde v3.1 también ve valor (edge >= MIN_EDGE)
  4. Corre el análisis completo: cuota, calibración, EV, edge, torneo, etc.

Uso:
    python scripts/analisis_corners_v31.py
    python scripts/analisis_corners_v31.py --n-sim 50000
    python scripts/analisis_corners_v31.py --min-edge 0.04
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
    lado_up = lado.strip()
    suffix  = 'over' if lado_up == 'Over/Si' else 'under'
    m = re.search(r'(\d+\.5)', mercado)
    if not m:
        return None
    thr = m.group(1)
    if 'tot.' in mercado.lower():
        return f'tc_{suffix}_{thr}'
    elif team_local and team_local.lower() in mercado.lower():
        return f'cl_{suffix}_{thr}'
    elif team_visita and team_visita.lower() in mercado.lower():
        return f'cv_{suffix}_{thr}'
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
    """Clasifica corner bet en: Totales / Local / Visita."""
    m = mercado.lower()
    if 'tot.' in m:
        return 'Totales'
    if team_local and team_local.lower() in m:
        return 'Local'
    if team_visita and team_visita.lower() in m:
        return 'Visita'
    return 'Individual'   # fallback


def _sub_mercado_detallado(mercado, lado, team_local='', team_visita=''):
    """Clasifica en: Totales Over, Local Under, Visita Over, etc."""
    m = mercado.lower()
    side = 'Over' if 'Over' in lado or 'Si' in lado else 'Under'
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
    ev_esp = sum(a['ev_v31'] for a in bets if a['ev_v31'] is not None)
    edges  = [a['edge_v31'] for a in bets if a['edge_v31'] is not None]
    edge_m = sum(edges) / len(edges) if edges else 0.0
    odds_m = sum(a['odds'] for a in bets) / total
    probs  = [a['prob_v31'] for a in bets if a['prob_v31'] is not None]
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
        probs = [a['prob_v31'] for a in g if a['prob_v31'] is not None]
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
        evs = [a['ev_v31'] for a in g if a['ev_v31'] is not None]
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
# Carga y re-cálculo v3.1
# ─────────────────────────────────────────────────────────────────────────────

def cargar_corners_resueltos():
    with open(VB_CSV, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    return [
        r for r in rows
        if 'orner' in r.get('mercado', '')
        and r.get('resultado', '').strip().upper() in ('W', 'L', 'V')
    ]


def recalcular_v31(bets, hist_rows, n_sim, min_edge):
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

        mu_tot = params['mu_corners_total']
        share  = params['share_corners_loc']
        k_c    = params['k_corners']
        print(f"  [{i:>3}/{total_fix}] {partido:<35}  mu={mu_tot:.2f}  share={share:.1%}  k={k_c:.1f}")

        for b in group:
            odds = float(b['odds'])
            ip   = float(b.get('implied_prob', 0)) if b.get('implied_prob') else 1.0 / odds
            pk   = _prob_key(b['mercado'], b['lado'], team_local, team_visita)
            p31  = probs.get(pk) if pk else None

            if p31 is None:
                continue

            edge_v31 = p31 - ip
            ev_v31   = (p31 * odds - 1.0)
            pnl_val  = _pnl(b['resultado'], odds)

            resultado.append({
                'partido':     partido,
                'competicion': comp,
                'mercado':     b['mercado'],
                'lado':        b['lado'],
                'fixture_id':  fid,
                'odds':        odds,
                'resultado':   b['resultado'].strip().upper(),
                'pnl':         pnl_val,
                'prob_v3':     float(b.get('modelo_prob', 0)) if b.get('modelo_prob') else None,
                'prob_v31':    p31,
                'implied_prob': ip,
                'edge_v31':    edge_v31,
                'ev_v31':      ev_v31,
                'value_v31':   edge_v31 >= min_edge,
                'team_local':  team_local,
                'team_visita': team_visita,
                'mu_total':    mu_tot,
                'share_local': share,
                # Campos para clasificación
                'tipo_torneo':     _tipo_torneo(comp),
                'sub_mercado':     _sub_mercado(b['mercado'], team_local, team_visita),
                'sub_mercado_det': _sub_mercado_detallado(b['mercado'], b['lado'], team_local, team_visita),
                'rango_cuota':     _rango_cuota(odds),
                'rango_prob':      _rango_prob(p31),
                'rango_ev':        _rango_ev(ev_v31),
                'rango_edge':      _rango_edge(edge_v31),
                'rango_threshold': _rango_threshold(b['mercado']),
            })

    return resultado


# ─────────────────────────────────────────────────────────────────────────────
# Reporte
# ─────────────────────────────────────────────────────────────────────────────

def _reporte_seccion(bets, titulo, min_n=2):
    """Corre el análisis completo (cuota, calibración, EV, edge, threshold,
    torneo, competición, top bets) para un subset de bets."""
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
    _tabla(by_side, ['Over', 'Under'], label='Lado', width=10, min_n=1)

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
    print(f"\n  C. CALIBRACIÓN PROBABILIDADES (Win% real vs Prob v3.1)")
    print(f"  {SEP2}")
    _tabla_calibracion(bets, _ORDEN_PROBS, min_n=min_n)

    # ── D. Calibración del EV ─────────────────────────────────────────────────
    print(f"\n  D. CALIBRACIÓN EV  (mayor EV → mayor ROI?)")
    print(f"  {SEP2}")
    _tabla_ev_edge(bets, 'rango_ev', _ORDEN_EV, 'EV predicho', min_n=min_n)

    # ── E. Calibración del edge ───────────────────────────────────────────────
    print(f"\n  E. CALIBRACIÓN EDGE  (edge = P_v31 − 1/odds)")
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
    print(f"\n  I. TOP BETS POR EDGE v3.1")
    print(f"  {SEP2}")
    sorted_edge = sorted(bets, key=lambda b: b['edge_v31'], reverse=True)
    print(f"  {'Partido':<30}  {'Mercado':<25}  {'Lado':>8}  "
          f"{'p_v31':>6}  {'edge':>6}  {'odds':>5}  {'Res':>3}  {'P&L':>7}")
    print(f"  {SEP2}")
    for b in sorted_edge[:15]:
        partido_short = b['partido'][:28]
        merc_short = b['mercado'][:23]
        print(f"  {partido_short:<30}  {merc_short:<25}  {b['lado']:>8}  "
              f"{b['prob_v31']:>5.1%}  {b['edge_v31']:>5.1%}  "
              f"{b['odds']:>5.2f}  {b['resultado']:>3}  {b['pnl']:>+6.2f}u")

    print()


def reporte(all_bets, min_edge):
    # Dividir en: v3.1 ve valor / no ve valor
    bets_value = [b for b in all_bets if b['value_v31']]
    bets_no    = [b for b in all_bets if not b['value_v31']]

    s_all   = _stats(all_bets)
    s_value = _stats(bets_value)
    s_no    = _stats(bets_no)

    # ══════════════════════════════════════════════════════════════════════════
    # ENCABEZADO GLOBAL
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print(f"  ANÁLISIS DE RENDIMIENTO CORNERS — MODELO v3.1 (NegBin+Binomial)")
    print(f"  min_edge={min_edge:.0%}  stake={STAKE:.0f}u  N_SIM={N_SIM:,}")
    print(SEP)

    print(f"\n  Total corner bets mapeadas : {s_all['total']}")
    print(f"  v3.1 confirma valor        : {s_value['total']}")
    print(f"  v3.1 rechaza               : {s_no['total']}")

    print(f"\n  {'Universo':<25}  {'N':>4}  {'W/L':>7}  {'Hit%':>6}  "
          f"{'P&L':>9}  {'ROI%':>7}  {'Edge medio':>10}")
    print(f"  {SEP2}")
    for label, s in [('Todas (prob v3 orig)', s_all),
                     ('v3.1 confirma valor', s_value),
                     ('v3.1 rechaza',       s_no)]:
        if s is None:
            continue
        print(f"  {label:<25}  {s['total']:>4}  "
              f"{s['wins']}W/{s['losses']}L  {s['hit_rate']:>5.1%}  "
              f"{_fmt(s['pnl']):>9}  {s['roi_real']:>+6.1%}  {s['edge_m']:>+9.1%}")

    bets = bets_value
    if not bets:
        print("\n  No hay bets confirmadas por v3.1 para analizar.")
        return

    # ══════════════════════════════════════════════════════════════════════════
    # RESUMEN POR SUB-MERCADO (Totales / Local / Visita)
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print("  RESUMEN POR SUB-MERCADO (bets v3.1 confirmadas)")
    print(SEP)

    by_sub = defaultdict(list)
    for a in bets:
        by_sub[a['sub_mercado']].append(a)
    _tabla(by_sub, ['Totales', 'Local', 'Visita'], label='Sub-mercado', width=14)

    # Detallado con over/under
    print()
    by_sub_det = defaultdict(list)
    for a in bets:
        by_sub_det[a['sub_mercado_det']].append(a)
    orden_det = ['Totales Over', 'Totales Under',
                 'Local Over', 'Local Under',
                 'Visita Over', 'Visita Under']
    _tabla(by_sub_det, orden_det, label='Detalle', width=16)

    # ══════════════════════════════════════════════════════════════════════════
    # ANÁLISIS DETALLADO POR SUB-MERCADO
    # ══════════════════════════════════════════════════════════════════════════
    for sub_name in ['Totales', 'Local', 'Visita']:
        sub_bets = by_sub.get(sub_name, [])
        _reporte_seccion(sub_bets, f'CORNERS {sub_name.upper()}')

    # ══════════════════════════════════════════════════════════════════════════
    # RESUMEN FINAL GLOBAL
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print("  RESUMEN FINAL GLOBAL")
    print(SEP)
    s = _stats(bets)
    print(f"  Bets analizadas (v3.1 confirma): {s['total']}")
    print(f"  Hit rate     : {s['hit_rate']:.1%}")
    print(f"  ROI real     : {s['roi_real']:+.1%}")
    print(f"  ROI EV esp.  : {s['roi_ev']:+.1%}")
    print(f"  dROI         : {(s['roi_real'] - s['roi_ev']):+.1%}")
    print(f"  Edge medio   : {s['edge_m']:+.1%}")
    print(f"  Odds medias  : {s['odds_m']:.2f}")
    print(f"  P&L          : {_fmt(s['pnl'])}")
    print(f"  EV esperado  : {_fmt(s['ev_esp'])}")

    # Resumen por sub-mercado
    print(f"\n  {'Sub-mercado':<12}  {'N':>4}  {'ROI%':>7}  {'P&L':>9}  {'Edge%':>7}  Veredicto")
    print(f"  {SEP2}")
    for sub_name in ['Totales', 'Local', 'Visita']:
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

    args = sys.argv[1:]
    if '--n-sim' in args:
        n_sim = int(args[args.index('--n-sim') + 1])
    if '--min-edge' in args:
        min_edge = float(args[args.index('--min-edge') + 1])

    print("Cargando datos...")
    hist_rows = load_csv(str(HIST_CSV))
    bets_raw  = cargar_corners_resueltos()
    print(f"  {len(hist_rows)} partidos históricos  |  {len(bets_raw)} corners resueltos")

    print(f"\nRe-calculando probabilidades v3.1 (n_sim={n_sim:,})...")
    all_bets = recalcular_v31(bets_raw, hist_rows, n_sim, min_edge)
    print(f"\n  {len(all_bets)} bets mapeadas exitosamente")

    reporte(all_bets, min_edge)
