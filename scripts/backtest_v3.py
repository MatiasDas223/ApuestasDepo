"""
backtest_v3.py
--------------
Re-calcula todas las apuestas de value_bets.csv con el modelo V3,
excluyendo los datos de ese fixture de la historia (walk-forward limpio).

Para cada fixture con resultado (W/L):
  1. Carga el historico SIN ese fixture_id
  2. Corre V3: compute_match_params → run_simulation → compute_all_probs
  3. Para cada apuesta v2 de ese fixture, busca la probabilidad V3
  4. Recalcula edge y EV con la misma cuota original
  5. Agrega filas nuevas con metodo='v3_retro' al CSV (no toca las v2)

Al final imprime una comparacion v2 vs v3 por categoria.

Uso:
    python backtest_v3.py
    python backtest_v3.py --dry-run   # solo muestra comparacion, no escribe CSV
"""

import csv
import re
import sys
import math
from pathlib import Path
from collections import defaultdict

BASE   = Path(__file__).resolve().parent.parent
VB_CSV = BASE / 'data/apuestas/value_bets.csv'
HIST_CSV = BASE / 'data/historico/partidos_historicos.csv'

sys.path.insert(0, str(Path(__file__).parent))

from modelo_v3 import (
    load_csv, compute_match_params, run_simulation,
    load_teams_db, load_leagues_db, resolve_team_id
)
from analizar_partido import compute_all_probs, compute_arco_params

N_SIM    = 50_000   # menos sims para velocidad, suficiente para retroanalisis
STAKE    = 1.0
DRY_RUN  = '--dry-run' in sys.argv


# ─────────────────────────────────────────────────────────────────────────────
# Mapeo mercado+lado → clave de probs
# ─────────────────────────────────────────────────────────────────────────────

def _mercado_to_prob_key(mercado: str, lado: str,
                          team_local: str, team_visita: str) -> str | None:
    """
    Convierte el string de mercado y el lado a la clave del dict de probs
    que produce analizar_partido.compute_all_probs().

    Retorna None si no se puede mapear.
    """
    m   = mercado.strip()
    l   = lado.strip()
    is_over = l in ('Over/Si', 'Si', 'Over')

    # Helper: extrae threshold "O/U X.Y"
    def thr():
        match = re.search(r'O/U\s+([\d.]+)', m)
        return float(match.group(1)) if match else None

    # ── 1X2 ──────────────────────────────────────────────────────────────────
    if m.startswith('1X2'):
        if 'Empate' in m:    return 'X'
        if team_visita in m: return '2'
        if team_local  in m: return '1'
        return None

    # ── BTTS ─────────────────────────────────────────────────────────────────
    if m.upper().startswith('BTTS'):
        return 'btts_si' if is_over else 'btts_no'

    # ── Goles ─────────────────────────────────────────────────────────────────
    if m.startswith('Goles'):
        t = thr()
        if t is None: return None
        side = 'over' if is_over else 'under'
        if 'tot.' in m:
            return f'g_{side}_{t}'
        if team_local in m:
            return f'gl_{side}_{t}'
        if team_visita in m:
            return f'gv_{side}_{t}'
        return None

    # ── Tiros ─────────────────────────────────────────────────────────────────
    if m.startswith('Tiros'):
        t = thr()
        if t is None: return None
        side = 'over' if is_over else 'under'
        if 'tot.' in m:
            return f'ts_{side}_{t}'
        if team_local in m:
            return f'sl_{side}_{t}'
        if team_visita in m:
            return f'sv_{side}_{t}'
        return None

    # ── Arco (tiros al arco) ──────────────────────────────────────────────────
    if m.startswith('Arco'):
        t = thr()
        if t is None: return None
        side = 'over' if is_over else 'under'
        if 'tot.' in m:
            return f'ta_{side}_{t}'
        if team_local in m:
            return f'sla_{side}_{t}'
        if team_visita in m:
            return f'sva_{side}_{t}'
        return None

    # ── Corners ───────────────────────────────────────────────────────────────
    if m.startswith('Corners'):
        t = thr()
        if t is None: return None
        side = 'over' if is_over else 'under'
        if 'tot.' in m:
            return f'tc_{side}_{t}'
        if team_local in m:
            return f'cl_{side}_{t}'
        if team_visita in m:
            return f'cv_{side}_{t}'
        return None

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Carga y escritura del CSV
# ─────────────────────────────────────────────────────────────────────────────

VB_COLS = [
    'fecha_analisis', 'fixture_id', 'partido', 'competicion',
    'mercado', 'lado', 'odds', 'modelo_prob', 'implied_prob',
    'edge', 'ev_pct', 'metodo', 'resultado',
]


def _load_vb():
    rows = []
    with open(VB_CSV, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows


def _parse_prob(s):
    """Parsea probabilidad: acepta '0.8000' o '80.0%'."""
    s = s.strip().replace('%', '').replace('+', '')
    if not s:
        return None
    v = float(s)
    return v / 100 if v > 1.5 else v


# ─────────────────────────────────────────────────────────────────────────────
# Backtest principal
# ─────────────────────────────────────────────────────────────────────────────

def run_backtest():
    all_rows = _load_vb()

    # Solo apuestas v2 con resultado resuelto
    v2_rows = [r for r in all_rows
               if r.get('metodo', '').startswith('v2')
               and r.get('resultado', '').strip().upper() in ('W', 'L', 'V')]

    if not v2_rows:
        print("No hay apuestas v2 con resultado para retroanalizar.")
        return

    # Historico completo
    hist_all = load_csv(HIST_CSV)
    hist_by_fid = defaultdict(list)
    for row in hist_all:
        hist_by_fid[row['fixture_id']].append(row)

    # Agrupar apuestas por fixture
    by_fixture = defaultdict(list)
    for r in v2_rows:
        by_fixture[r['fixture_id']].append(r)

    print(f"\nRetroanalisis V3 — {len(by_fixture)} fixtures, {len(v2_rows)} apuestas")
    print("=" * 68)

    v3_new_rows = []
    stats_v2 = defaultdict(lambda: {'n':0,'w':0,'pnl':0.0,'ev':0.0})
    stats_v3 = defaultdict(lambda: {'n':0,'w':0,'pnl':0.0,'ev':0.0})

    for fid, bets in sorted(by_fixture.items()):
        sample = bets[0]
        partido     = sample['partido']          # "Home vs Away"
        competicion = sample['competicion']

        parts = partido.split(' vs ', 1)
        if len(parts) != 2:
            print(f"  [{fid}] No se puede parsear partido: '{partido}'")
            continue
        home, away = parts[0].strip(), parts[1].strip()

        # Historia SIN este fixture
        hist_sin = [r for r in hist_all if r['fixture_id'] != fid]

        if len(hist_sin) < 5:
            print(f"  [{fid}] Muy poca historia disponible, saltado")
            continue

        # Correr V3
        try:
            params = compute_match_params(home, away, hist_sin, competicion)
            sim    = run_simulation(params, N_SIM)
            sim['team_local']  = home
            sim['team_visita'] = away

            # Agregar arco al sim
            try:
                arco_p = compute_arco_params(home, away, hist_sin, competicion)
                from modelo_v3 import poisson_sample
                sim['sla_arco'] = [poisson_sample(arco_p['mu_arco_local']) for _ in range(N_SIM)]
                sim['sva_arco'] = [poisson_sample(arco_p['mu_arco_vis'])   for _ in range(N_SIM)]
                sim['arco_params'] = arco_p
            except Exception:
                pass  # sin arco, no es crítico

            probs = compute_all_probs(sim)

        except Exception as e:
            print(f"  [{fid}] Error V3: {e}")
            continue

        print(f"  [{fid}] {home} vs {away}  ({competicion})  —  {len(bets)} apuestas")

        for bet in bets:
            mercado = bet['mercado']
            lado    = bet['lado']
            odds    = float(bet['odds'])
            resultado = bet['resultado'].strip().upper()

            # Clave de probabilidad en V3
            prob_key = _mercado_to_prob_key(mercado, lado, home, away)
            if prob_key is None or prob_key not in probs:
                continue

            v3_prob    = probs[prob_key]
            implied_p  = _parse_prob(bet['implied_prob'])
            if implied_p is None or implied_p <= 0:
                implied_p = 1.0 / odds

            v3_edge    = v3_prob - implied_p
            v3_ev      = v3_prob * odds - 1   # en decimales (0.125 = 12.5% EV)

            # P&L real (mismo para ambos modelos)
            pnl = (odds - 1) if resultado == 'W' else (-1 if resultado == 'L' else 0)

            # Acumular stats por categoria
            cat = _cat(mercado)

            # V2
            v2_prob = _parse_prob(bet['modelo_prob']) or 0
            v2_ev   = _parse_prob(bet['ev_pct']) or 0
            s2 = stats_v2[cat]
            s2['n']   += 1
            s2['w']   += 1 if resultado == 'W' else 0
            s2['pnl'] += pnl
            s2['ev']  += v2_ev   # ev ya en decimal (0.xx)

            # V3
            s3 = stats_v3[cat]
            s3['n']   += 1
            s3['w']   += 1 if resultado == 'W' else 0
            s3['pnl'] += pnl
            s3['ev']  += v3_ev

            # Nueva fila para el CSV
            v3_new_rows.append({
                'fecha_analisis': bet['fecha_analisis'],
                'fixture_id':     fid,
                'partido':        partido,
                'competicion':    competicion,
                'mercado':        mercado,
                'lado':           lado,
                'odds':           f"{odds:.2f}",
                'modelo_prob':    f"{v3_prob:.4f}",
                'implied_prob':   f"{implied_p:.4f}",
                'edge':           f"{v3_edge:.4f}",
                'ev_pct':         f"{v3_ev:.4f}",
                'metodo':         'v3_retro',
                'resultado':      resultado,
            })

    # ── Comparacion ───────────────────────────────────────────────────────────
    print()
    print("=" * 90)
    print(f"  COMPARACION V2 vs V3  —  stake=1 por apuesta")
    print("=" * 90)
    hdr = (f"  {'Cat':<12}  {'N':>4}  "
           f"{'Hit%':>6}  {'P&L':>8}  "
           f"{'EV v2':>8}  {'EV v3':>8}  "
           f"{'ROI%':>6}  {'EVroi v2':>9}  {'EVroi v3':>9}  "
           f"{'dEV':>8}")
    print(hdr)
    print("  " + "-" * 86)

    order = ['Goles','Tiros','Arco','Corners','Tarjetas','BTTS','1X2','Otros']
    cats  = sorted(set(list(stats_v2.keys()) + list(stats_v3.keys())),
                   key=lambda c: order.index(c) if c in order else 99)

    total_v2 = {'n':0,'w':0,'pnl':0.0,'ev':0.0}
    total_v3 = {'n':0,'w':0,'pnl':0.0,'ev':0.0}

    for cat in cats:
        s2 = stats_v2[cat]
        s3 = stats_v3[cat]
        n   = s2['n']
        if n == 0:
            continue
        hit   = s2['w'] / n
        pnl   = s2['pnl']
        roi   = pnl / n
        ev2   = s2['ev']
        ev3   = s3['ev']
        roi2  = ev2 / n
        roi3  = ev3 / n
        delta = ev3 - ev2
        sign  = '+' if delta >= 0 else ''

        print(f"  {cat:<12}  {n:>4}  "
              f"{hit:>5.1%}  {pnl:>+7.3f}u  "
              f"{ev2:>+7.3f}u  {ev3:>+7.3f}u  "
              f"{roi:>5.1%}  {roi2:>8.1%}  {roi3:>8.1%}  "
              f"{sign}{delta:>7.3f}u")

        for k in ('n','w','pnl','ev'):
            total_v2[k] += s2[k]
        for k in ('n','ev'):
            total_v3[k] += s3[k]
        total_v3['w']   += s3['w']
        total_v3['pnl'] += s3['pnl']

    print("  " + "-" * 86)
    n   = total_v2['n']
    hit = total_v2['w'] / n if n else 0
    pnl = total_v2['pnl']
    roi = pnl / n if n else 0
    ev2 = total_v2['ev']
    ev3 = total_v3['ev']
    delta = ev3 - ev2
    sign  = '+' if delta >= 0 else ''
    print(f"  {'TOTAL':<12}  {n:>4}  "
          f"{hit:>5.1%}  {pnl:>+7.3f}u  "
          f"{ev2:>+7.3f}u  {ev3:>+7.3f}u  "
          f"{roi:>5.1%}  {ev2/n:>8.1%}  {ev3/n:>8.1%}  "
          f"{sign}{delta:>7.3f}u")
    print("=" * 90)

    # ── Nota sobre apuestas que V3 NO señalaria ────────────────────────────────
    v3_would_flag = sum(1 for r in v3_new_rows if float(r['edge']) >= 0.04)
    v3_would_not  = len(v3_new_rows) - v3_would_flag
    print(f"\n  De {len(v3_new_rows)} apuestas retrocalculadas:")
    print(f"    V3 hubiera señalado como value bet : {v3_would_flag}")
    print(f"    V3 NO hubiera señalado (edge < 4%) : {v3_would_not}")

    # ── Escribir CSV ───────────────────────────────────────────────────────────
    if not DRY_RUN and v3_new_rows:
        # Eliminar v3_retro anteriores para no duplicar
        existing = [r for r in all_rows if r.get('metodo') != 'v3_retro']
        final    = existing + v3_new_rows

        with open(VB_CSV, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=VB_COLS)
            w.writeheader()
            w.writerows(final)

        print(f"\n  {len(v3_new_rows)} filas v3_retro escritas en {VB_CSV.name}")
    elif DRY_RUN:
        print("\n  [dry-run] CSV no modificado")
    print()


def _cat(mercado):
    m = mercado.lower()
    if 'corner' in m: return 'Corners'
    if 'arco'   in m: return 'Arco'
    if 'tiro'   in m: return 'Tiros'
    if 'btts'   in m: return 'BTTS'
    if '1x2'    in m: return '1X2'
    if 'gol'    in m: return 'Goles'
    return 'Otros'


if __name__ == '__main__':
    run_backtest()
