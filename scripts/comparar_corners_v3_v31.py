"""
comparar_corners_v3_v31.py
--------------------------
Compara el rendimiento de los corners en v3 (Poisson independiente)
vs v3.1 (NegBin total + Binomial reparto) usando las apuestas ya resueltas
de value_bets.csv.

Metodología:
  - Prob v3     : tomada directamente del CSV (modelo_prob original)
  - Prob v3.1   : re-calculada ahora con el modelo actual corriendo
                  compute_match_params() + run_simulation() para cada fixture
  - ROI v3      : el real, usando las probabilidades y resultados que ya tenemos
  - ROI v3.1A   : mismas apuestas, mismos resultados, pero usando prob v3.1
                  (¿habría sido mejor o peor apostar con v3.1?)
  - ROI v3.1B   : solo las apuestas donde v3.1 también diría "value"
                  (¿filtrar con v3.1 hubiera mejorado el resultado?)

Uso:
    python comparar_corners_v3_v31.py
    python comparar_corners_v3_v31.py --min-edge 0.04
    python comparar_corners_v3_v31.py --n-sim 100000
"""

import csv
import sys
import re
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent))
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from modelo_v3 import (
    load_csv, compute_match_params, run_simulation,
    load_teams_db, resolve_team_id, MIN_EDGE,
)
from analizar_partido import compute_all_probs, poisson_sample

VB_CSV   = Path(r'C:\Users\Matt\Apuestas Deportivas\data\apuestas\value_bets.csv')
HIST_CSV = Path(r'C:\Users\Matt\Apuestas Deportivas\data\historico\partidos_historicos.csv')
N_SIM    = 100_000
STAKE    = 1.0


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _clean_comp(raw: str) -> str:
    """Normaliza el campo competicion (limpia tuples string del pipeline)."""
    # "('Copa Libertadores', True)" → "Copa Libertadores"
    m = re.match(r"\('(.+?)',\s*(?:True|False)\)", raw.strip())
    return m.group(1) if m else raw.strip()


def _prob_key(mercado: str, lado: str, team_local: str, team_visita: str) -> str | None:
    """
    Mapea (mercado, lado) al key del dict de probs.
    Retorna None si no se puede mapear.
    """
    lado_up = lado.strip()
    suffix  = 'over' if lado_up == 'Over/Si' else 'under'

    # Extraer umbral  (e.g. "9.5" de "Corners tot. O/U 9.5")
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


def _pnl(resultado: str, odds: float) -> float | None:
    r = resultado.strip().upper()
    if r == 'W': return odds - STAKE
    if r == 'L': return -STAKE
    if r == 'V': return 0.0
    return None


def _stats(bets: list) -> dict:
    if not bets:
        return {}
    total  = len(bets)
    wins   = sum(1 for b in bets if b['resultado'] == 'W')
    losses = sum(1 for b in bets if b['resultado'] == 'L')
    voids  = sum(1 for b in bets if b['resultado'] == 'V')
    pnl    = sum(b['pnl'] for b in bets)
    base   = wins + losses
    return {
        'n': total, 'W': wins, 'L': losses, 'V': voids,
        'pnl': pnl,
        'hit': wins / base if base else 0.0,
        'roi': pnl / (total * STAKE) if total else 0.0,
    }


def _print_stats(label: str, bets: list, width: int = 40):
    s = _stats(bets)
    if not s:
        print(f"  {label:<{width}}  sin datos")
        return
    print(f"  {label:<{width}}  "
          f"n={s['n']:>4}  {s['W']}W/{s['L']}L/{s['V']}V  "
          f"hit={s['hit']:>5.1%}  P&L={s['pnl']:>+7.3f}u  ROI={s['roi']:>+6.1%}")


# ─────────────────────────────────────────────────────────────────────────────
# Carga y agrupación de bets
# ─────────────────────────────────────────────────────────────────────────────

def cargar_corners_resueltos() -> list[dict]:
    with open(VB_CSV, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    return [
        r for r in rows
        if 'orner' in r.get('mercado', '')
        and r.get('resultado', '').strip().upper() in ('W', 'L', 'V')
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Re-cálculo con v3.1
# ─────────────────────────────────────────────────────────────────────────────

def recalcular_probs_v31(bets: list, hist_rows: list, n_sim: int,
                          min_edge: float) -> tuple[list[dict], dict[str, float]]:
    """
    Para cada bet de corner resuelto, re-corre el modelo actual (v3.1)
    y agrega campos: prob_v31, edge_v31, value_v31.
    """
    _, name_to_id = load_teams_db()

    # Agrupar por fixture para evitar re-simular el mismo partido
    by_fixture: dict[str, list] = defaultdict(list)
    for b in bets:
        by_fixture[b['fixture_id']].append(b)

    resultado:  list[dict]       = []
    fixture_mu: dict[str, float] = {}   # fixture_id → mu_total predicho
    total_fix = len(by_fixture)

    for i, (fid, group) in enumerate(by_fixture.items(), 1):
        # Datos del partido (todos en el mismo grupo tienen el mismo partido/comp)
        sample    = group[0]
        partido   = sample['partido']          # "X vs Y"
        comp_raw  = sample['competicion']
        comp      = _clean_comp(comp_raw)

        partes = partido.split(' vs ', 1)
        if len(partes) != 2:
            print(f"  [skip] no se pudo parsear partido: '{partido}'")
            for b in group:
                resultado.append({**b, 'prob_v31': None, 'edge_v31': None, 'value_v31': False})
            continue

        team_local, team_visita = partes[0].strip(), partes[1].strip()

        # Verificar que los equipos están en la DB
        local_id = resolve_team_id(team_local,  name_to_id)
        vis_id   = resolve_team_id(team_visita, name_to_id)
        if local_id is None or vis_id is None:
            missing = team_local if local_id is None else team_visita
            print(f"  [skip] equipo no encontrado: '{missing}'")
            for b in group:
                resultado.append({**b, 'prob_v31': None, 'edge_v31': None, 'value_v31': False})
            continue

        # Simular con modelo actual (v3.1)
        try:
            params = compute_match_params(local_id, vis_id, hist_rows, comp)
            sim    = run_simulation(params, n_sim)
            probs  = compute_all_probs(sim)
        except Exception as e:
            print(f"  [error] {partido} | {comp}: {e}")
            for b in group:
                resultado.append({**b, 'prob_v31': None, 'edge_v31': None, 'value_v31': False})
            continue

        fixture_mu[fid] = params['mu_corners_total']
        print(f"  [{i:>3}/{total_fix}] {partido:<35}  "
              f"mu_tot={params['mu_corners_total']:.2f}  "
              f"share={params['share_corners_loc']:.1%}  "
              f"k={params['k_corners']:.1f}")

        # Asignar prob v3.1 a cada bet del fixture
        for b in group:
            odds = float(b['odds'])
            ip   = float(b['implied_prob']) if b.get('implied_prob') else 1 / odds

            pk = _prob_key(b['mercado'], b['lado'], team_local, team_visita)
            p31 = probs.get(pk) if pk else None

            if p31 is not None:
                edge_v31  = p31 - ip
                value_v31 = edge_v31 >= min_edge
            else:
                edge_v31  = None
                value_v31 = False

            resultado.append({
                **b,
                'team_local':  team_local,
                'team_visita': team_visita,
                'prob_v31':    p31,
                'edge_v31':    edge_v31,
                'value_v31':   value_v31,
            })

    return resultado, fixture_mu


# ─────────────────────────────────────────────────────────────────────────────
# Reporte
# ─────────────────────────────────────────────────────────────────────────────

def reporte(enriched: list, min_edge: float):
    sep  = '=' * 76
    sep2 = '-' * 76

    print(f"\n{sep}")
    print(f"  COMPARACION CORNERS  v3 (Poisson) vs v3.1 (NegBin+Binomial)")
    print(f"  min_edge={min_edge:.0%}  stake={STAKE:.0f}u  N_SIM={N_SIM:,}")
    print(sep)

    # Separar los que tienen prob v3.1 calculada vs skipped
    mapeados  = [b for b in enriched if b['prob_v31'] is not None]
    no_mapeados = [b for b in enriched if b['prob_v31'] is None]

    print(f"\n  Bets resueltas de corners : {len(enriched)}")
    print(f"  Mapeadas a prob v3.1      : {len(mapeados)}")
    print(f"  Sin mapeo (skip)          : {len(no_mapeados)}")

    if not mapeados:
        print("\n  Sin datos suficientes para comparar.")
        return

    # ── Construir listas para cada escenario ──────────────────────────────────

    # v3 original (todos los mapeados)
    bets_v3 = []
    for b in mapeados:
        pnl = _pnl(b['resultado'], float(b['odds']))
        if pnl is not None:
            bets_v3.append({**b, 'pnl': pnl})

    # v3.1A — mismas apuestas, mismo resultado, P&L idéntico al v3
    # (solo cambia cómo calificamos si fue o no value ex-ante)
    bets_v31a = bets_v3   # mismo conjunto, mismos outcomes

    # v3.1B — solo apuestas que v3.1 también considera value
    bets_v31b = [b for b in bets_v3 if b.get('value_v31')]

    # Apuestas que v3 hizo pero v3.1 no hubiera hecho
    bets_v3_solo = [b for b in bets_v3 if not b.get('value_v31')]

    # ── Resumen global ─────────────────────────────────────────────────────────
    print(f"\n{sep}")
    print(f"  ESCENARIO A — mismas apuestas, mismo resultado")
    print(f"  (diferencia: probabilidades asignadas, no el universo de bets)")
    print(sep2)

    # Comparar prob asignada v3 vs v3.1 para cada bet
    diffs = []
    for b in mapeados:
        p3  = float(b['modelo_prob'])
        p31 = b['prob_v31']
        if p31 is not None:
            diffs.append(p31 - p3)

    if diffs:
        avg_diff = sum(diffs) / len(diffs)
        pos = sum(1 for d in diffs if d > 0.005)
        neg = sum(1 for d in diffs if d < -0.005)
        neu = len(diffs) - pos - neg
        print(f"\n  Diferencia prob v3.1 - v3 (por bet):")
        print(f"    Media  : {avg_diff:+.4f}  ({avg_diff*100:+.2f}pp)")
        print(f"    v3.1 > v3  (sube)  : {pos:>4} bets")
        print(f"    v3.1 ~ v3  (±0.5pp): {neu:>4} bets")
        print(f"    v3.1 < v3  (baja)  : {neg:>4} bets")

    print(f"\n  ROI comparado (sobre el mismo universo de bets):")
    print(f"  {'Escenario':<42}  {'N':>4}  {'W/L/V'}  {'Hit%':>6}  {'P&L':>8}  {'ROI%':>7}")
    print(f"  {sep2}")
    _print_stats("v3 — modelo original", bets_v3)
    # Para v3.1A el resultado financiero es igual (mismas bets, mismos outcomes)
    # lo que cambia es si cada bet habría sido detectada o no
    print(f"\n  Bets que v3.1 TAMBIÉN habría detectado: {len(bets_v31b)} de {len(bets_v3)}")
    print(f"  Bets que v3.1 NO habría detectado     : {len(bets_v3_solo)} de {len(bets_v3)}")

    # ── Escenario B — solo apuestas que ambos modelos aprueban ────────────────
    print(f"\n{sep}")
    print(f"  ESCENARIO B — solo apuestas donde v3.1 también ve valor")
    print(f"  (filtrado más estricto: ambos modelos de acuerdo)")
    print(sep2)
    print(f"  {'Escenario':<42}  {'N':>4}  {'W/L/V'}  {'Hit%':>6}  {'P&L':>8}  {'ROI%':>7}")
    print(f"  {sep2}")
    _print_stats("v3   — todas las bets",            bets_v3)
    _print_stats("v3.1 — solo bets confirmadas",     bets_v31b)
    _print_stats("v3   — bets que v3.1 rechazaría",  bets_v3_solo)

    # ── Detalle por tipo de mercado ───────────────────────────────────────────
    print(f"\n{sep}")
    print(f"  DESGLOSE POR MERCADO")
    print(sep2)
    print(f"  {'Mercado (simplificado)':<28}  {'N':>4}  {'ROI v3':>8}  {'N v3.1':>7}  {'ROI v3.1B':>9}  {'Δ prob media':>12}")
    print(f"  {sep2}")

    by_tipo: dict[str, list] = defaultdict(list)
    for b in bets_v3:
        m = b['mercado']
        if 'tot.' in m.lower():
            tipo = 'Corners TOTALES'
        elif b.get('team_local','') and b['team_local'].lower() in m.lower():
            tipo = 'Corners LOCAL'
        else:
            tipo = 'Corners VISITA'
        by_tipo[tipo].append(b)

    for tipo, grupo in sorted(by_tipo.items()):
        s3  = _stats(grupo)
        sub_v31b = [b for b in grupo if b.get('value_v31')]
        s31 = _stats(sub_v31b)
        dp  = [b['prob_v31'] - float(b['modelo_prob']) for b in grupo if b['prob_v31'] is not None]
        dp_m = sum(dp)/len(dp) if dp else 0.0
        roi31 = f"{s31['roi']:>+.1%}" if s31 else "  -  "
        print(f"  {tipo:<28}  {s3['n']:>4}  {s3['roi']:>+7.1%}  "
              f"{len(sub_v31b):>7}  {roi31:>9}  {dp_m:>+11.4f}")

    # ── Detalle bet a bet (muestra) ───────────────────────────────────────────
    print(f"\n{sep}")
    print(f"  DETALLE — bets donde v3.1 cambia más la probabilidad (top 15)")
    print(sep2)
    sorted_diffs = sorted(
        [b for b in bets_v3 if b.get('prob_v31') is not None],
        key=lambda b: abs(b['prob_v31'] - float(b['modelo_prob'])),
        reverse=True
    )[:15]

    hdr = (f"  {'Partido':<30}  {'Mercado':<26}  {'Lado':<9}  "
           f"{'p_v3':>6}  {'p_v31':>6}  {'Δp':>6}  {'Res':>3}  "
           f"{'v3.1?':>5}")
    print(hdr)
    print(f"  {sep2}")
    for b in sorted_diffs:
        p3  = float(b['modelo_prob'])
        p31 = b['prob_v31']
        dp  = p31 - p3
        partido = b['partido'][:29]
        mercado = b['mercado'][:25]
        v31_tag = 'SI' if b.get('value_v31') else 'NO'
        print(f"  {partido:<30}  {mercado:<26}  {b['lado']:<9}  "
              f"{p3:>5.1%}  {p31:>5.1%}  {dp:>+5.2%}  {b['resultado']:>3}  "
              f"{v31_tag:>5}")

    print(f"\n{sep}\n")


# ─────────────────────────────────────────────────────────────────────────────
# Análisis profundo TOTALES
# ─────────────────────────────────────────────────────────────────────────────

def _get_actual_corners(fid: str, hist_rows: list) -> int | None:
    """Busca corners totales reales del fixture en el histórico."""
    for r in hist_rows:
        if r.get('fixture_id', '') == fid:
            try:
                return int(r['corners_local']) + int(r['corners_visitante'])
            except (ValueError, TypeError):
                return None
    return None


def analisis_profundo_totales(enriched: list, hist_rows: list,
                               fixture_mu: dict, min_edge: float):
    """
    Análisis profundo del mercado de Corners TOTALES:
      1. Sesgo Over vs Under  (ROI y hit rate separados)
      2. Desglose por threshold (8.5 / 9.5 / 10.5 / 11.5 / 12.5)
      3. Calibración de probabilidades (bins de 10pp)
      4. Precisión de mu_total vs corners reales por fixture
    """
    sep  = '=' * 76
    sep2 = '-' * 76

    # Filtrar solo TOTALES con resultado y prob v3.1
    totales = []
    for b in enriched:
        if b.get('prob_v31') is None:
            continue
        if 'tot.' not in b.get('mercado', '').lower():
            continue
        pnl = _pnl(b['resultado'], float(b['odds']))
        if pnl is None:
            continue
        totales.append({**b, 'pnl': pnl})

    if not totales:
        print("  Sin bets de TOTALES con prob v3.1 calculada.")
        return

    print(f"\n{sep}")
    print(f"  ANALISIS PROFUNDO — Corners TOTALES ({len(totales)} bets)")
    print(sep)

    # ── 1. Sesgo Over vs Under ────────────────────────────────────────────────
    print(f"\n  1. SESGO OVER vs UNDER")
    print(sep2)
    print(f"  {'Lado':<12}  {'N':>4}  {'Hit%':>6}  {'Hit% real':>10}  {'ROI v3':>8}  {'ROI v3.1B':>10}  {'Δprob media':>12}")
    print(f"  {sep2}")

    for lado_tag, label in [('Over/Si', 'OVER'), ('Under/No', 'UNDER')]:
        sub = [b for b in totales if b['lado'].strip() == lado_tag]
        if not sub:
            continue
        sub31b = [b for b in sub if b.get('value_v31')]
        s3  = _stats(sub)
        s31 = _stats(sub31b)
        dp  = [b['prob_v31'] - float(b['modelo_prob']) for b in sub]
        dp_m = sum(dp) / len(dp) if dp else 0.0

        # Hit rate "real" = % de Over/Under que efectivamente ocurrieron
        # Para Over: win = el total fue SOBRE el umbral; para Under: al revés
        hit_real = s3['hit']   # ya es la tasa de W
        roi31_str = f"{s31['roi']:>+.1%}" if s31 else "  -  "
        print(f"  {label:<12}  {s3['n']:>4}  {s3['hit']:>6.1%}  {hit_real:>10.1%}  "
              f"{s3['roi']:>+7.1%}  {roi31_str:>10}  {dp_m:>+11.4f}")

    # ── 2. Desglose por threshold ─────────────────────────────────────────────
    print(f"\n  2. DESGLOSE POR THRESHOLD")
    print(sep2)
    print(f"  {'Threshold':<10}  {'N':>4}  {'Hit%':>6}  {'ROI v3':>8}  "
          f"{'N v3.1':>7}  {'ROI v3.1':>8}  {'p_v3 avg':>9}  {'p_v31 avg':>9}")
    print(f"  {sep2}")

    # Extraer threshold de mercado
    by_thr: dict[str, list] = defaultdict(list)
    for b in totales:
        m = re.search(r'(\d+\.5)', b['mercado'])
        if m:
            by_thr[m.group(1)].append(b)

    for thr in sorted(by_thr.keys(), key=float):
        sub  = by_thr[thr]
        sub31b = [b for b in sub if b.get('value_v31')]
        s3   = _stats(sub)
        s31  = _stats(sub31b)
        avg_p3  = sum(float(b['modelo_prob']) for b in sub) / len(sub)
        avg_p31 = sum(b['prob_v31'] for b in sub if b['prob_v31'] is not None)
        avg_p31 /= max(1, sum(1 for b in sub if b['prob_v31'] is not None))
        roi31_str = f"{s31['roi']:>+.1%}" if s31 else "  -  "
        n31_str   = str(len(sub31b)) if sub31b else "0"
        print(f"  {'O/U '+thr:<10}  {s3['n']:>4}  {s3['hit']:>6.1%}  {s3['roi']:>+7.1%}  "
              f"{n31_str:>7}  {roi31_str:>8}  {avg_p3:>8.1%}  {avg_p31:>8.1%}")

    # ── 3. Calibración de probabilidades ─────────────────────────────────────
    print(f"\n  3. CALIBRACION DE PROBABILIDADES (v3.1)")
    print(f"     bin = [prob_v31]; si está bien calibrado, hit% ≈ prob media del bin")
    print(sep2)
    print(f"  {'Bin prob':<14}  {'N':>4}  {'prob media':>10}  {'hit% real':>10}  {'sesgo':>8}  {'ROI':>7}")
    print(f"  {sep2}")

    # Crear bins de 10pp
    bins: dict[str, list] = defaultdict(list)
    for b in totales:
        p31 = b['prob_v31']
        if p31 is None:
            continue
        bin_lo = int(p31 * 10) * 10   # 0, 10, 20, ..., 90
        bin_key = f"{bin_lo}-{bin_lo+10}%"
        bins[bin_key].append(b)

    for bin_key in sorted(bins.keys()):
        sub = bins[bin_key]
        s   = _stats(sub)
        avg_prob = sum(b['prob_v31'] for b in sub) / len(sub)
        sesgo    = s['hit'] - avg_prob   # positivo → modelo subestimó
        print(f"  {bin_key:<14}  {s['n']:>4}  {avg_prob:>10.1%}  {s['hit']:>10.1%}  "
              f"{sesgo:>+7.1%}  {s['roi']:>+6.1%}")

    # ── 4. Precisión mu_total por fixture ────────────────────────────────────
    print(f"\n  4. PRECISION mu_total vs CORNERS REALES (por fixture)")
    print(f"     error = mu_total - corners_reales  (positivo = sobreestimó)")
    print(sep2)
    print(f"  {'Partido':<35}  {'mu_pred':>7}  {'real':>5}  {'error':>7}  {'N bets':>6}  {'ROI bets':>9}")
    print(f"  {sep2}")

    # Agrupar bets de TOTALES por fixture
    by_fix: dict[str, list] = defaultdict(list)
    for b in totales:
        by_fix[b['fixture_id']].append(b)

    errores = []
    for fid, sub in sorted(by_fix.items()):
        mu_pred  = fixture_mu.get(fid)
        ct_real  = _get_actual_corners(fid, hist_rows)
        if mu_pred is None or ct_real is None:
            continue
        error = mu_pred - ct_real
        errores.append(error)
        s = _stats(sub)
        partido = sub[0]['partido'][:34]
        print(f"  {partido:<35}  {mu_pred:>7.2f}  {ct_real:>5}  {error:>+6.2f}  "
              f"{s['n']:>6}  {s['roi']:>+8.1%}")

    if errores:
        print(f"  {sep2}")
        me   = sum(errores) / len(errores)
        mae  = sum(abs(e) for e in errores) / len(errores)
        rmse = (sum(e**2 for e in errores) / len(errores)) ** 0.5
        over_pred = sum(1 for e in errores if e > 0)
        under_pred = sum(1 for e in errores if e < 0)
        exact = sum(1 for e in errores if abs(e) < 0.5)
        print(f"\n  Resumen error mu_total ({len(errores)} fixtures):")
        print(f"    ME (sesgo medio)   : {me:>+.2f}  "
              f"({'sobreestima' if me > 0 else 'subestima'} corners totales)")
        print(f"    MAE (error medio)  : {mae:>6.2f} corners")
        print(f"    RMSE               : {rmse:>6.2f} corners")
        print(f"    Sobreestima (>0)   : {over_pred}/{len(errores)}  ({over_pred/len(errores):.0%})")
        print(f"    Subestima   (<0)   : {under_pred}/{len(errores)}  ({under_pred/len(errores):.0%})")

    print(f"\n{sep}\n")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    args     = sys.argv[1:]
    min_edge = MIN_EDGE
    n_sim    = N_SIM

    if '--min-edge' in args:
        min_edge = float(args[args.index('--min-edge') + 1])
    if '--n-sim' in args:
        n_sim = int(args[args.index('--n-sim') + 1])

    print("Cargando datos...")
    hist_rows = load_csv(HIST_CSV)
    bets      = cargar_corners_resueltos()

    print(f"  {len(hist_rows)} partidos históricos  |  {len(bets)} corners resueltos")
    print(f"\nRe-calculando probabilidades con v3.1 (n_sim={n_sim:,})...")

    enriched, fixture_mu = recalcular_probs_v31(bets, hist_rows, n_sim, min_edge)

    reporte(enriched, min_edge)
    analisis_profundo_totales(enriched, hist_rows, fixture_mu, min_edge)


if __name__ == '__main__':
    main()
