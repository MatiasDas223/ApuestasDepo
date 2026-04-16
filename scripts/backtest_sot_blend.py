"""
backtest_sot_blend.py
---------------------
Backtest walk-forward: ¿mejora la predicción de goles si incorporamos
tiros al arco (shots on target) al cálculo de lambda?

Metodología:
  Para cada partido con suficiente historia:
    1. Construir records SOLO con partidos anteriores
    2. Calcular λ_goals (modelo actual: basado en goles previos)
    3. Calcular λ_sot   (basado en tiros al arco previos, convertido a escala goles)
    4. Blend: λ_final = (1-w)*λ_goals + w*λ_sot
    5. Evaluar contra goles reales: Poisson log-lik, MAE, Brier O/U

Uso:
    python scripts/backtest_sot_blend.py
"""

import csv
import sys
import math
import datetime
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent))
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

HIST_CSV = Path(r'C:\Users\Matt\Apuestas Deportivas\data\historico\partidos_historicos.csv')

# Parámetros del modelo (iguales a modelo_v3.py)
K_SHRINK       = 8
HALF_LIFE_DAYS = 90
N_FORM         = 5
FORM_WEIGHT    = 0.20
MIN_HIST       = 5     # mínimo de partidos previos para incluir

SEP  = '=' * 80
SEP2 = '-' * 80

# ─────────────────────────────────────────────────────────────────────────────
# Carga
# ─────────────────────────────────────────────────────────────────────────────

def cargar_partidos():
    with open(HIST_CSV, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    partidos = []
    for r in rows:
        try:
            partidos.append({
                'fixture_id':   r['fixture_id'],
                'fecha':        r['fecha'],
                'fecha_ord':    datetime.date.fromisoformat(r['fecha']).toordinal(),
                'liga_id':      int(r['liga_id']),
                'local_id':     int(r['equipo_local_id']),
                'visita_id':    int(r['equipo_visitante_id']),
                'gl':           int(r['goles_local']),
                'gv':           int(r['goles_visitante']),
                'tl':           int(r['tiros_local']),
                'tv':           int(r['tiros_visitante']),
                'tal':          int(r['tiros_arco_local']),
                'tav':          int(r['tiros_arco_visitante']),
                'cl':           int(r['corners_local']),
                'cv':           int(r['corners_visitante']),
                'pl':           int(r['posesion_local']),
                'pv':           int(r['posesion_visitante']),
                'yl':           int(r['tarjetas_local']),
                'yv':           int(r['tarjetas_visitante']),
            })
        except (ValueError, KeyError):
            continue
    partidos.sort(key=lambda p: p['fecha_ord'])
    return partidos


# ─────────────────────────────────────────────────────────────────────────────
# Mini-modelo (replicado de modelo_v3 para walk-forward)
# ─────────────────────────────────────────────────────────────────────────────

def _weighted_avg(vals_dates, today_ord):
    """Promedio ponderado por recencia."""
    if not vals_dates:
        return None, 0
    lam = math.log(2) / HALF_LIFE_DAYS
    tw = twv = 0.0
    for val, d_ord in vals_dates:
        days = max(0, today_ord - d_ord)
        w = math.exp(-lam * days)
        tw += w
        twv += w * val
    return (twv / tw if tw > 0 else None), len(vals_dates)


def _form_mult(vals_dates):
    """Factor forma: últimos N_FORM vs promedio total."""
    if len(vals_dates) < N_FORM:
        return 1.0
    sorted_vd = sorted(vals_dates, key=lambda x: x[1])
    recent = [v for v, _ in sorted_vd[-N_FORM:]]
    all_vals = [v for v, _ in sorted_vd]
    form_avg = sum(recent) / len(recent)
    season_avg = sum(all_vals) / len(all_vals)
    if season_avg <= 0:
        return 1.0
    raw = form_avg / season_avg
    return 1.0 + FORM_WEIGHT * (raw - 1.0)


def _shrunk_rating(team_avg, league_avg, n):
    if team_avg is None or league_avg == 0:
        return 1.0
    raw = team_avg / league_avg
    return (n * raw + K_SHRINK * 1.0) / (n + K_SHRINK)


def compute_lambdas(partidos_previos, local_id, visita_id, liga_id, today_ord):
    """
    Calcula lambdas para goles y para tiros al arco (SOT).
    Retorna dict con lambda_goals_l, lambda_goals_v, lambda_sot_l, lambda_sot_v
    y los promedios de liga necesarios para la conversión.
    """
    # Filtrar por liga (con fallback a todo)
    comp_rows = [p for p in partidos_previos if p['liga_id'] == liga_id]
    if len(comp_rows) < 3:
        comp_rows = partidos_previos

    # Promedios de liga
    n_liga = len(comp_rows)
    la_hg = sum(p['gl'] for p in comp_rows) / n_liga   # home goals avg
    la_ag = sum(p['gv'] for p in comp_rows) / n_liga   # away goals avg
    la_hs = sum(p['tal'] for p in comp_rows) / n_liga   # home SOT avg
    la_as = sum(p['tav'] for p in comp_rows) / n_liga   # away SOT avg

    # Construir records para ambos equipos
    def get_records(team_id, ctx, partidos):
        """Extrae (valor, fecha_ord) para un equipo en un contexto."""
        goals = []
        goals_con = []
        sot = []
        sot_con = []
        for p in partidos:
            if ctx == 'home' and p['local_id'] == team_id:
                goals.append((p['gl'], p['fecha_ord']))
                goals_con.append((p['gv'], p['fecha_ord']))
                sot.append((p['tal'], p['fecha_ord']))
                sot_con.append((p['tav'], p['fecha_ord']))
            elif ctx == 'away' and p['visita_id'] == team_id:
                goals.append((p['gv'], p['fecha_ord']))
                goals_con.append((p['gl'], p['fecha_ord']))
                sot.append((p['tav'], p['fecha_ord']))
                sot_con.append((p['tal'], p['fecha_ord']))
        return goals, goals_con, sot, sot_con

    # Buscar registros: primero liga, luego todo
    def get_best_records(team_id, ctx):
        g, gc, s, sc = get_records(team_id, ctx, comp_rows)
        if len(g) >= 1:
            g_all, gc_all, s_all, sc_all = get_records(team_id, ctx, partidos_previos)
            if len(g) < MIN_HIST and len(g_all) >= 1:
                return g_all, gc_all, s_all, sc_all
            return g, gc, s, sc
        return get_records(team_id, ctx, partidos_previos)

    loc_g, loc_gc, loc_s, loc_sc = get_best_records(local_id, 'home')
    vis_g, vis_gc, vis_s, vis_sc = get_best_records(visita_id, 'away')

    n_loc = len(loc_g)
    n_vis = len(vis_g)

    if n_loc < MIN_HIST or n_vis < MIN_HIST:
        return None

    # ── GOLES lambda ──
    avg_atk_l, _ = _weighted_avg(loc_g, today_ord)
    avg_def_v, _ = _weighted_avg(vis_gc, today_ord)   # goles que concede vis como away
    avg_atk_v, _ = _weighted_avg(vis_g, today_ord)
    avg_def_l, _ = _weighted_avg(loc_gc, today_ord)   # goles que concede loc como home

    r_atk_l = _shrunk_rating(avg_atk_l, la_hg, n_loc)
    r_def_v = _shrunk_rating(avg_def_v, la_hg, n_vis)
    r_atk_v = _shrunk_rating(avg_atk_v, la_ag, n_vis)
    r_def_l = _shrunk_rating(avg_def_l, la_ag, n_loc)

    f_atk_l = _form_mult(loc_g)
    f_atk_v = _form_mult(vis_g)

    lam_g_l = max(0.15, la_hg * r_atk_l * r_def_v * f_atk_l)
    lam_g_v = max(0.15, la_ag * r_atk_v * r_def_l * f_atk_v)

    # ── SOT lambda (misma estructura, campo shots_on_target) ──
    avg_sot_atk_l, _ = _weighted_avg(loc_s, today_ord)
    avg_sot_def_v, _ = _weighted_avg(vis_sc, today_ord)
    avg_sot_atk_v, _ = _weighted_avg(vis_s, today_ord)
    avg_sot_def_l, _ = _weighted_avg(loc_sc, today_ord)

    r_sot_atk_l = _shrunk_rating(avg_sot_atk_l, la_hs, n_loc)
    r_sot_def_v = _shrunk_rating(avg_sot_def_v, la_hs, n_vis)
    r_sot_atk_v = _shrunk_rating(avg_sot_atk_v, la_as, n_vis)
    r_sot_def_l = _shrunk_rating(avg_sot_def_l, la_as, n_loc)

    f_sot_l = _form_mult(loc_s)
    f_sot_v = _form_mult(vis_s)

    mu_sot_l = max(0.5, la_hs * r_sot_atk_l * r_sot_def_v * f_sot_l)
    mu_sot_v = max(0.5, la_as * r_sot_atk_v * r_sot_def_l * f_sot_v)

    # Convertir SOT a escala de goles usando conversion rate de liga
    conv_home = la_hg / la_hs if la_hs > 0 else 0.3
    conv_away = la_ag / la_as if la_as > 0 else 0.3

    lam_sot_l = max(0.15, mu_sot_l * conv_home)
    lam_sot_v = max(0.15, mu_sot_v * conv_away)

    return {
        'lam_g_l': lam_g_l, 'lam_g_v': lam_g_v,
        'lam_sot_l': lam_sot_l, 'lam_sot_v': lam_sot_v,
        'n_loc': n_loc, 'n_vis': n_vis,
        'conv_home': conv_home, 'conv_away': conv_away,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Métricas
# ─────────────────────────────────────────────────────────────────────────────

def poisson_pmf(k, lam):
    """P(X=k) para X ~ Poisson(lam)."""
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lam + k * math.log(lam) - math.lgamma(k + 1))


def poisson_cdf(k, lam):
    """P(X <= k) para X ~ Poisson(lam)."""
    return sum(poisson_pmf(i, lam) for i in range(k + 1))


def poisson_log_lik(k, lam):
    """Log-likelihood de observar k goles dado lambda."""
    if lam <= 0:
        return -100 if k > 0 else 0
    return -lam + k * math.log(lam) - math.lgamma(k + 1)


def brier_ou(lam_l, lam_v, gl_real, gv_real, threshold):
    """Brier score para Over/Under threshold en goles totales."""
    gt_real = gl_real + gv_real
    # P(total > threshold) via convolución Poisson
    p_over = 0.0
    max_g = int(threshold) + 15
    for i in range(max_g + 1):
        for j in range(max_g + 1):
            if i + j > threshold:
                p_over += poisson_pmf(i, lam_l) * poisson_pmf(j, lam_v)

    outcome = 1.0 if gt_real > threshold else 0.0
    return (p_over - outcome) ** 2


# ─────────────────────────────────────────────────────────────────────────────
# Backtest principal
# ─────────────────────────────────────────────────────────────────────────────

def run_backtest(partidos, weights):
    print(f"\n{SEP}")
    print(f"  BACKTEST WALK-FORWARD: MODELO GOLES vs GOLES+SOT")
    print(f"  {len(partidos)} partidos | min_hist={MIN_HIST}")
    print(SEP)

    # Para cada peso, acumular métricas
    results = {w: {
        'n': 0,
        'mae_l': 0, 'mae_v': 0, 'mae_t': 0,
        'loglik_l': 0, 'loglik_v': 0,
        'brier_15': 0, 'brier_25': 0, 'brier_35': 0,
        'brier_gl_05': 0, 'brier_gv_05': 0,
        'se_l': 0, 'se_v': 0,  # squared error for RMSE
    } for w in weights}

    evaluated = 0
    skipped = 0

    for idx in range(len(partidos)):
        p = partidos[idx]
        previos = partidos[:idx]
        if len(previos) < 20:
            skipped += 1
            continue

        res = compute_lambdas(previos, p['local_id'], p['visita_id'],
                              p['liga_id'], p['fecha_ord'])
        if res is None:
            skipped += 1
            continue

        evaluated += 1
        gl = p['gl']
        gv = p['gv']
        gt = gl + gv

        for w in weights:
            lam_l = (1 - w) * res['lam_g_l'] + w * res['lam_sot_l']
            lam_v = (1 - w) * res['lam_g_v'] + w * res['lam_sot_v']

            r = results[w]
            r['n'] += 1
            r['mae_l'] += abs(lam_l - gl)
            r['mae_v'] += abs(lam_v - gv)
            r['mae_t'] += abs((lam_l + lam_v) - gt)
            r['se_l']  += (lam_l - gl) ** 2
            r['se_v']  += (lam_v - gv) ** 2
            r['loglik_l'] += poisson_log_lik(gl, lam_l)
            r['loglik_v'] += poisson_log_lik(gv, lam_v)
            r['brier_15'] += brier_ou(lam_l, lam_v, gl, gv, 1.5)
            r['brier_25'] += brier_ou(lam_l, lam_v, gl, gv, 2.5)
            r['brier_35'] += brier_ou(lam_l, lam_v, gl, gv, 3.5)

            # Brier para goles equipo O/U 0.5
            p_gl_over = 1 - poisson_pmf(0, lam_l)
            p_gv_over = 1 - poisson_pmf(0, lam_v)
            r['brier_gl_05'] += (p_gl_over - (1 if gl > 0 else 0)) ** 2
            r['brier_gv_05'] += (p_gv_over - (1 if gv > 0 else 0)) ** 2

    print(f"\n  Partidos evaluados: {evaluated}  (skipped: {skipped})")

    # ── Tabla de resultados ──────────────────────────────────────────────────
    print(f"\n  {'Peso SOT':>9}  {'MAE_L':>6}  {'MAE_V':>6}  {'MAE_T':>6}  "
          f"{'RMSE_L':>7}  {'RMSE_V':>7}  "
          f"{'LogLik':>8}  {'Brier15':>8}  {'Brier25':>8}  {'Brier35':>8}  "
          f"{'B_GL05':>7}  {'B_GV05':>7}")
    print(f"  {SEP2}")

    baseline = None
    best_metric = {}

    for w in weights:
        r = results[w]
        n = r['n']
        if n == 0:
            continue

        mae_l = r['mae_l'] / n
        mae_v = r['mae_v'] / n
        mae_t = r['mae_t'] / n
        rmse_l = math.sqrt(r['se_l'] / n)
        rmse_v = math.sqrt(r['se_v'] / n)
        ll = (r['loglik_l'] + r['loglik_v']) / n
        b15 = r['brier_15'] / n
        b25 = r['brier_25'] / n
        b35 = r['brier_35'] / n
        bgl = r['brier_gl_05'] / n
        bgv = r['brier_gv_05'] / n

        metrics = {
            'mae_l': mae_l, 'mae_v': mae_v, 'mae_t': mae_t,
            'rmse_l': rmse_l, 'rmse_v': rmse_v,
            'll': ll, 'b15': b15, 'b25': b25, 'b35': b35,
            'bgl': bgl, 'bgv': bgv,
        }

        if w == 0:
            baseline = metrics

        # Track best
        for k, v in metrics.items():
            if k == 'll':  # higher is better
                if k not in best_metric or v > best_metric[k][1]:
                    best_metric[k] = (w, v)
            else:  # lower is better
                if k not in best_metric or v < best_metric[k][1]:
                    best_metric[k] = (w, v)

        tag = ' ← actual' if w == 0 else ''
        print(f"  w={w:>5.2f}  {mae_l:>6.3f}  {mae_v:>6.3f}  {mae_t:>6.3f}  "
              f"{rmse_l:>7.4f}  {rmse_v:>7.4f}  "
              f"{ll:>8.4f}  {b15:>8.5f}  {b25:>8.5f}  {b35:>8.5f}  "
              f"{bgl:>7.5f}  {bgv:>7.5f}{tag}")

    # ── Comparación vs baseline ──────────────────────────────────────────────
    if baseline:
        print(f"\n  DIFERENCIA vs BASELINE (w=0, modelo actual)")
        print(f"  {SEP2}")
        print(f"  {'Peso SOT':>9}  {'ΔMAE_L':>7}  {'ΔMAE_V':>7}  {'ΔMAE_T':>7}  "
              f"{'ΔRMSE_L':>8}  {'ΔRMSE_V':>8}  "
              f"{'ΔLogLik':>8}  {'ΔBrier25':>9}  Mejor?")
        print(f"  {SEP2}")

        for w in weights:
            if w == 0:
                continue
            r = results[w]
            n = r['n']
            if n == 0:
                continue

            mae_l = r['mae_l'] / n
            mae_v = r['mae_v'] / n
            mae_t = r['mae_t'] / n
            rmse_l = math.sqrt(r['se_l'] / n)
            rmse_v = math.sqrt(r['se_v'] / n)
            ll = (r['loglik_l'] + r['loglik_v']) / n
            b25 = r['brier_25'] / n

            d_mae_l = mae_l - baseline['mae_l']
            d_mae_v = mae_v - baseline['mae_v']
            d_mae_t = mae_t - baseline['mae_t']
            d_rmse_l = rmse_l - baseline['rmse_l']
            d_rmse_v = rmse_v - baseline['rmse_v']
            d_ll = ll - baseline['ll']
            d_b25 = b25 - baseline['b25']

            # Count improvements (negative = better for MAE/Brier, positive = better for LL)
            improvements = 0
            if d_mae_l < -0.001: improvements += 1
            if d_mae_v < -0.001: improvements += 1
            if d_mae_t < -0.001: improvements += 1
            if d_ll > 0.001: improvements += 1
            if d_b25 < -0.0001: improvements += 1

            tag = 'SI' if improvements >= 3 else 'PARCIAL' if improvements >= 2 else 'NO'

            print(f"  w={w:>5.2f}  {d_mae_l:>+6.4f}  {d_mae_v:>+6.4f}  {d_mae_t:>+6.4f}  "
                  f"{d_rmse_l:>+7.5f}  {d_rmse_v:>+7.5f}  "
                  f"{d_ll:>+7.5f}  {d_b25:>+8.6f}  {tag}")

    # ── Mejor peso por métrica ───────────────────────────────────────────────
    print(f"\n  MEJOR PESO POR MÉTRICA")
    print(f"  {SEP2}")
    labels = {
        'mae_l': 'MAE goles local', 'mae_v': 'MAE goles visita',
        'mae_t': 'MAE goles total',
        'rmse_l': 'RMSE goles local', 'rmse_v': 'RMSE goles visita',
        'll': 'Log-Likelihood (↑)',
        'b15': 'Brier O/U 1.5', 'b25': 'Brier O/U 2.5', 'b35': 'Brier O/U 3.5',
        'bgl': 'Brier GL O/U 0.5', 'bgv': 'Brier GV O/U 0.5',
    }
    for k in ['mae_l', 'mae_v', 'mae_t', 'rmse_l', 'rmse_v', 'll',
              'b15', 'b25', 'b35', 'bgl', 'bgv']:
        if k in best_metric:
            best_w, best_v = best_metric[k]
            base_v = baseline[k] if baseline else 0
            delta = best_v - base_v
            delta_str = f"({delta:+.5f})" if k != 'll' else f"({delta:+.5f})"
            actual = ' ← modelo actual gana' if best_w == 0 else ''
            print(f"  {labels[k]:<22}  w={best_w:.2f}  val={best_v:.5f}  vs base {delta_str}{actual}")

    # ── Resumen ──────────────────────────────────────────────────────────────
    print(f"\n{SEP}")
    print(f"  RESUMEN")
    print(SEP)

    wins_sot = sum(1 for k in best_metric if best_metric[k][0] > 0)
    wins_base = sum(1 for k in best_metric if best_metric[k][0] == 0)

    print(f"\n  Métricas donde SOT blend gana: {wins_sot}/{len(best_metric)}")
    print(f"  Métricas donde baseline gana:  {wins_base}/{len(best_metric)}")

    if wins_sot > wins_base:
        # Find most common best weight
        sot_weights = [best_metric[k][0] for k in best_metric if best_metric[k][0] > 0]
        from collections import Counter
        common_w = Counter(sot_weights).most_common(1)[0][0]
        print(f"\n  VEREDICTO: SOT BLEND MEJORA el modelo")
        print(f"  Peso óptimo más frecuente: w={common_w:.2f}")
    else:
        print(f"\n  VEREDICTO: El modelo actual (solo goles) es igual o mejor")
        print(f"  Agregar SOT no mejora la predicción de goles")

    print()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print("Cargando partidos...")
    partidos = cargar_partidos()
    print(f"  {len(partidos)} partidos ({partidos[0]['fecha']} a {partidos[-1]['fecha']})")

    weights = [0.00, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50, 0.70, 1.00]

    run_backtest(partidos, weights)
