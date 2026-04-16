"""
correlacion_predictiva.py
-------------------------
Análisis de correlación PREDICTIVA: ¿los promedios históricos previos
de cada equipo predicen los goles del próximo partido?

Para cada partido en el dataset:
  1. Calcula promedios históricos de TODAS las variables para ambos equipos
     usando SOLO partidos anteriores a la fecha del partido (look-ahead-free)
  2. Correlaciona esos promedios con los goles reales del partido

Esto responde: ¿agregar tiros, posesión, corners, etc. al modelo mejoraría
la predicción de goles?

Uso:
    python scripts/correlacion_predictiva.py
    python scripts/correlacion_predictiva.py --min-partidos 8
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

SEP  = '=' * 80
SEP2 = '-' * 80

MIN_PARTIDOS = 5   # mínimo de partidos previos para incluir en el análisis
HALF_LIFE    = 90   # días para ponderación exponencial (igual que modelo_v3)


# ─────────────────────────────────────────────────────────────────────────────
# Carga de datos
# ─────────────────────────────────────────────────────────────────────────────

def cargar_partidos():
    with open(HIST_CSV, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    partidos = []
    for r in rows:
        try:
            p = {
                'fixture_id':   r['fixture_id'],
                'fecha':        r['fecha'],
                'fecha_dt':     datetime.date.fromisoformat(r['fecha']),
                'liga_id':      int(r['liga_id']),
                'local_id':     int(r['equipo_local_id']),
                'visita_id':    int(r['equipo_visitante_id']),
                'goles_local':  int(r['goles_local']),
                'goles_visita': int(r['goles_visitante']),
                'goles_total':  int(r['goles_local']) + int(r['goles_visitante']),
                'tiros_local':       int(r['tiros_local']),
                'tiros_visita':      int(r['tiros_visitante']),
                'tiros_arco_local':  int(r['tiros_arco_local']),
                'tiros_arco_visita': int(r['tiros_arco_visitante']),
                'corners_local':     int(r['corners_local']),
                'corners_visita':    int(r['corners_visitante']),
                'posesion_local':    int(r['posesion_local']),
                'posesion_visita':   int(r['posesion_visitante']),
                'tarjetas_local':    int(r['tarjetas_local']),
                'tarjetas_visita':   int(r['tarjetas_visitante']),
            }
            partidos.append(p)
        except (ValueError, KeyError):
            continue

    partidos.sort(key=lambda p: p['fecha_dt'])
    return partidos


# ─────────────────────────────────────────────────────────────────────────────
# Cálculo de promedios históricos previos (con ponderación por recencia)
# ─────────────────────────────────────────────────────────────────────────────

VARS_ATAQUE = [
    'goles', 'tiros', 'tiros_arco', 'corners', 'tarjetas',
]
VARS_DEFENSA = [
    'goles_con', 'tiros_con', 'tiros_arco_con', 'corners_con',
]

def calcular_promedios_previos(partidos, idx, team_id, venue, use_weights=True):
    """
    Para un equipo dado y un partido en idx, calcula promedios ponderados
    de todas las variables usando SOLO partidos anteriores.

    venue: 'home' solo usa partidos como local, 'away' como visita, 'all' ambos
    """
    partido_actual = partidos[idx]
    fecha_actual = partido_actual['fecha_dt']

    registros = []
    for i in range(idx):
        p = partidos[i]
        es_local = p['local_id'] == team_id
        es_visita = p['visita_id'] == team_id
        if not es_local and not es_visita:
            continue
        if venue == 'home' and not es_local:
            continue
        if venue == 'away' and not es_visita:
            continue

        dias = (fecha_actual - p['fecha_dt']).days
        if dias <= 0:
            continue

        w = math.exp(-math.log(2) * dias / HALF_LIFE) if use_weights else 1.0

        if es_local:
            reg = {
                'w': w,
                'goles':          p['goles_local'],
                'goles_con':      p['goles_visita'],
                'tiros':          p['tiros_local'],
                'tiros_con':      p['tiros_visita'],
                'tiros_arco':     p['tiros_arco_local'],
                'tiros_arco_con': p['tiros_arco_visita'],
                'corners':        p['corners_local'],
                'corners_con':    p['corners_visita'],
                'posesion':       p['posesion_local'],
                'tarjetas':       p['tarjetas_local'],
                'tarjetas_con':   p['tarjetas_visita'],
            }
        else:
            reg = {
                'w': w,
                'goles':          p['goles_visita'],
                'goles_con':      p['goles_local'],
                'tiros':          p['tiros_visita'],
                'tiros_con':      p['tiros_local'],
                'tiros_arco':     p['tiros_arco_visita'],
                'tiros_arco_con': p['tiros_arco_local'],
                'corners':        p['corners_visita'],
                'corners_con':    p['corners_local'],
                'posesion':       p['posesion_visita'],
                'tarjetas':       p['tarjetas_visita'],
                'tarjetas_con':   p['tarjetas_local'],
            }
        registros.append(reg)

    if len(registros) < MIN_PARTIDOS:
        return None

    # Promedios ponderados
    total_w = sum(r['w'] for r in registros)
    promedios = {}
    for key in registros[0]:
        if key == 'w':
            continue
        promedios[key] = sum(r[key] * r['w'] for r in registros) / total_w

    promedios['n_partidos'] = len(registros)

    # Ratios derivados
    if promedios['tiros'] > 0:
        promedios['precision'] = promedios['tiros_arco'] / promedios['tiros']
    else:
        promedios['precision'] = 0

    if promedios['tiros_con'] > 0:
        promedios['precision_con'] = promedios['tiros_arco_con'] / promedios['tiros_con']
    else:
        promedios['precision_con'] = 0

    # Ratio goles/tiros arco (conversion rate)
    if promedios['tiros_arco'] > 0:
        promedios['conversion'] = promedios['goles'] / promedios['tiros_arco']
    else:
        promedios['conversion'] = 0

    return promedios


# ─────────────────────────────────────────────────────────────────────────────
# Correlación Pearson
# ─────────────────────────────────────────────────────────────────────────────

def pearson(xs, ys):
    n = len(xs)
    if n < 10:
        return None, None
    mx = sum(xs) / n
    my = sum(ys) / n
    sx = math.sqrt(sum((x - mx)**2 for x in xs) / n)
    sy = math.sqrt(sum((y - my)**2 for y in ys) / n)
    if sx == 0 or sy == 0:
        return None, None
    cov = sum((xs[i] - mx) * (ys[i] - my) for i in range(n)) / n
    r = cov / (sx * sy)
    # t-test significancia
    if abs(r) >= 1:
        t = float('inf')
    else:
        t = r * math.sqrt((n - 2) / (1 - r**2))
    # p-value aproximado (two-tailed) usando distribución normal para n grande
    p = 2 * (1 - _normal_cdf(abs(t)))
    return r, p


def _normal_cdf(x):
    """Aproximación de la CDF normal estándar."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


# ─────────────────────────────────────────────────────────────────────────────
# Regresión lineal simple
# ─────────────────────────────────────────────────────────────────────────────

def linear_reg(xs, ys):
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    ss_xx = sum((x - mx)**2 for x in xs)
    ss_xy = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    if ss_xx == 0:
        return 0, my, 0
    b = ss_xy / ss_xx
    a = my - b * mx
    y_pred = [a + b * x for x in xs]
    ss_res = sum((ys[i] - y_pred[i])**2 for i in range(n))
    ss_tot = sum((y - my)**2 for y in ys)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0
    return b, a, r2


# ─────────────────────────────────────────────────────────────────────────────
# Análisis principal
# ─────────────────────────────────────────────────────────────────────────────

def run_analysis(partidos, min_partidos):
    print(f"\n{SEP}")
    print(f"  ANÁLISIS DE CORRELACIÓN PREDICTIVA")
    print(f"  ¿Los promedios históricos previos predicen los goles del partido?")
    print(f"  Min partidos previos: {min_partidos}  |  Half-life: {HALF_LIFE} días")
    print(SEP)

    # Construir dataset: para cada partido, calcular features previos
    dataset = []
    skipped = 0

    for idx in range(len(partidos)):
        p = partidos[idx]

        # Promedios del LOCAL como local
        avg_loc_h = calcular_promedios_previos(partidos, idx, p['local_id'], 'home')
        # Promedios del VISITA como visita
        avg_vis_a = calcular_promedios_previos(partidos, idx, p['visita_id'], 'away')
        # Promedios generales (all venues)
        avg_loc_all = calcular_promedios_previos(partidos, idx, p['local_id'], 'all')
        avg_vis_all = calcular_promedios_previos(partidos, idx, p['visita_id'], 'all')

        if avg_loc_h is None or avg_vis_a is None or avg_loc_all is None or avg_vis_all is None:
            skipped += 1
            continue

        row = {
            'goles_local':  p['goles_local'],
            'goles_visita': p['goles_visita'],
            'goles_total':  p['goles_total'],
            # ── LOCAL como local (venue-specific) ──
            'loc_h_goles':          avg_loc_h['goles'],
            'loc_h_goles_con':      avg_loc_h['goles_con'],
            'loc_h_tiros':          avg_loc_h['tiros'],
            'loc_h_tiros_arco':     avg_loc_h['tiros_arco'],
            'loc_h_tiros_con':      avg_loc_h['tiros_con'],
            'loc_h_tiros_arco_con': avg_loc_h['tiros_arco_con'],
            'loc_h_corners':        avg_loc_h['corners'],
            'loc_h_posesion':       avg_loc_h['posesion'],
            'loc_h_tarjetas':       avg_loc_h['tarjetas'],
            'loc_h_precision':      avg_loc_h['precision'],
            'loc_h_conversion':     avg_loc_h['conversion'],
            # ── VISITA como visita (venue-specific) ──
            'vis_a_goles':          avg_vis_a['goles'],
            'vis_a_goles_con':      avg_vis_a['goles_con'],
            'vis_a_tiros':          avg_vis_a['tiros'],
            'vis_a_tiros_arco':     avg_vis_a['tiros_arco'],
            'vis_a_tiros_con':      avg_vis_a['tiros_con'],
            'vis_a_tiros_arco_con': avg_vis_a['tiros_arco_con'],
            'vis_a_corners':        avg_vis_a['corners'],
            'vis_a_posesion':       avg_vis_a['posesion'],
            'vis_a_tarjetas':       avg_vis_a['tarjetas'],
            'vis_a_precision':      avg_vis_a['precision'],
            'vis_a_conversion':     avg_vis_a['conversion'],
            # ── LOCAL all venues ──
            'loc_all_goles':        avg_loc_all['goles'],
            'loc_all_goles_con':    avg_loc_all['goles_con'],
            'loc_all_tiros':        avg_loc_all['tiros'],
            'loc_all_tiros_arco':   avg_loc_all['tiros_arco'],
            'loc_all_corners':      avg_loc_all['corners'],
            'loc_all_posesion':     avg_loc_all['posesion'],
            'loc_all_precision':    avg_loc_all['precision'],
            'loc_all_conversion':   avg_loc_all['conversion'],
            # ── VISITA all venues ──
            'vis_all_goles':        avg_vis_all['goles'],
            'vis_all_goles_con':    avg_vis_all['goles_con'],
            'vis_all_tiros':        avg_vis_all['tiros'],
            'vis_all_tiros_arco':   avg_vis_all['tiros_arco'],
            'vis_all_corners':      avg_vis_all['corners'],
            'vis_all_posesion':     avg_vis_all['posesion'],
            'vis_all_precision':    avg_vis_all['precision'],
            'vis_all_conversion':   avg_vis_all['conversion'],
            # ── Ratings multiplicativos (como los usa el modelo) ──
            'n_loc': avg_loc_h['n_partidos'],
            'n_vis': avg_vis_a['n_partidos'],
        }

        dataset.append(row)

    print(f"\n  Partidos en dataset: {len(dataset)}  (skipped: {skipped} por falta de historia)")

    # ══════════════════════════════════════════════════════════════════════════
    # 1. CORRELACIÓN CON GOLES LOCAL
    # ══════════════════════════════════════════════════════════════════════════
    targets = [
        ('goles_local',  'GOLES LOCAL'),
        ('goles_visita', 'GOLES VISITA'),
        ('goles_total',  'GOLES TOTALES'),
    ]

    features_local = [
        # Variable hist. previa                 Label
        ('loc_h_goles',          'Goles prom local (home)'),
        ('loc_h_tiros',          'Tiros prom local (home)'),
        ('loc_h_tiros_arco',     'Tiros arco prom local (home)'),
        ('loc_h_corners',        'Corners prom local (home)'),
        ('loc_h_posesion',       'Posesion prom local (home)'),
        ('loc_h_tarjetas',       'Tarjetas prom local (home)'),
        ('loc_h_precision',      'Precision prom local (home)'),
        ('loc_h_conversion',     'Conversion prom local (home)'),
        ('loc_h_goles_con',      'Goles concedidos local (home)'),
        ('loc_h_tiros_con',      'Tiros concedidos local (home)'),
        ('loc_h_tiros_arco_con', 'Tiros arco concedidos loc (home)'),
        ('loc_all_goles',        'Goles prom local (all)'),
        ('loc_all_tiros',        'Tiros prom local (all)'),
        ('loc_all_tiros_arco',   'Tiros arco prom local (all)'),
        ('loc_all_corners',      'Corners prom local (all)'),
        ('loc_all_posesion',     'Posesion prom local (all)'),
        ('loc_all_precision',    'Precision prom local (all)'),
        ('loc_all_conversion',   'Conversion prom local (all)'),
    ]

    features_visita = [
        ('vis_a_goles',          'Goles prom visita (away)'),
        ('vis_a_tiros',          'Tiros prom visita (away)'),
        ('vis_a_tiros_arco',     'Tiros arco prom visita (away)'),
        ('vis_a_corners',        'Corners prom visita (away)'),
        ('vis_a_posesion',       'Posesion prom visita (away)'),
        ('vis_a_tarjetas',       'Tarjetas prom visita (away)'),
        ('vis_a_precision',      'Precision prom visita (away)'),
        ('vis_a_conversion',     'Conversion prom visita (away)'),
        ('vis_a_goles_con',      'Goles concedidos visita (away)'),
        ('vis_a_tiros_con',      'Tiros concedidos visita (away)'),
        ('vis_a_tiros_arco_con', 'Tiros arco concedidos vis (away)'),
        ('vis_all_goles',        'Goles prom visita (all)'),
        ('vis_all_tiros',        'Tiros prom visita (all)'),
        ('vis_all_tiros_arco',   'Tiros arco prom visita (all)'),
        ('vis_all_corners',      'Corners prom visita (all)'),
        ('vis_all_posesion',     'Posesion prom visita (all)'),
        ('vis_all_precision',    'Precision prom visita (all)'),
        ('vis_all_conversion',   'Conversion prom visita (all)'),
    ]

    features_cruzadas = [
        # Features del rival que podrían predecir goles
        ('vis_a_goles_con',      'Goles que concede visita (away)'),
        ('vis_a_tiros_arco_con', 'Arco que concede visita (away)'),
        ('loc_h_goles_con',      'Goles que concede local (home)'),
        ('loc_h_tiros_arco_con', 'Arco que concede local (home)'),
    ]

    for target_key, target_label in targets:
        print(f"\n{'█' * 80}")
        print(f"  PREDICTORES DE {target_label}")
        print(f"{'█' * 80}")

        y_vals = [d[target_key] for d in dataset]

        if target_key == 'goles_local':
            features = features_local + [
                ('vis_a_goles_con',      'Goles que concede rival (away)'),
                ('vis_a_tiros_arco_con', 'Arco que concede rival (away)'),
            ]
        elif target_key == 'goles_visita':
            features = features_visita + [
                ('loc_h_goles_con',      'Goles que concede rival (home)'),
                ('loc_h_tiros_arco_con', 'Arco que concede rival (home)'),
            ]
        else:
            features = features_local + features_visita

        results = []
        for feat_key, feat_label in features:
            x_vals = [d[feat_key] for d in dataset]
            r, p = pearson(x_vals, y_vals)
            if r is not None:
                b, a, r2 = linear_reg(x_vals, y_vals)
                results.append((feat_label, feat_key, r, p, r2, b, a))

        results.sort(key=lambda x: -abs(x[2]))

        print(f"\n  {'Variable predictora':<36}  {'r':>7}  {'|r|':>5}  {'R²':>6}  "
              f"{'p-val':>8}  {'Sig':>4}  {'β':>7}  Fuerza")
        print(f"  {SEP2}")
        for label, key, r, p, r2, b, a in results:
            if p is None:
                sig = '?'
            elif p < 0.001:
                sig = '***'
            elif p < 0.01:
                sig = '**'
            elif p < 0.05:
                sig = '*'
            else:
                sig = 'ns'

            absv = abs(r)
            if absv >= 0.3:   fuerza = 'MODERADA'
            elif absv >= 0.15: fuerza = 'DEBIL'
            elif absv >= 0.08: fuerza = 'MUY DEBIL'
            else:             fuerza = '-'

            bar_len = int(absv * 40)
            bar = '#' * bar_len + '.' * (40 - bar_len)
            print(f"  {label:<36}  {r:>+6.4f}  {absv:>5.3f}  {r2:>5.3f}  "
                  f"{p:>8.5f}  {sig:>4}  {b:>+6.3f}  {fuerza:<10} [{bar}]")

    # ══════════════════════════════════════════════════════════════════════════
    # 2. COMPARACIÓN: GOLES PREVIOS vs OTRAS VARIABLES
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print("  COMPARACIÓN: ¿QUÉ PREDICE MEJOR QUE LOS GOLES PREVIOS?")
    print(f"  (Variables que aportan información por encima de los goles históricos)")
    print(SEP)

    for target_key, target_label in targets:
        print(f"\n  {target_label}:")
        y_vals = [d[target_key] for d in dataset]

        # Baseline: correlación de goles previos
        if target_key == 'goles_local':
            baseline_key = 'loc_h_goles'
            baseline_label = 'Goles prev local (home)'
            all_feats = features_local + [
                ('vis_a_goles_con', 'Goles que concede rival (away)'),
                ('vis_a_tiros_arco_con', 'Arco que concede rival (away)'),
            ]
        elif target_key == 'goles_visita':
            baseline_key = 'vis_a_goles'
            baseline_label = 'Goles prev visita (away)'
            all_feats = features_visita + [
                ('loc_h_goles_con', 'Goles que concede rival (home)'),
                ('loc_h_tiros_arco_con', 'Arco que concede rival (home)'),
            ]
        else:
            baseline_key = None
            all_feats = features_local + features_visita

        if baseline_key:
            x_base = [d[baseline_key] for d in dataset]
            r_base, _ = pearson(x_base, y_vals)
            print(f"    Baseline ({baseline_label}): r = {r_base:+.4f}")
            print(f"    Variables con |r| > |r_baseline|:")
            print(f"    {'Variable':<36}  {'r':>7}  {'Δr':>7}  {'Mejora?':>8}")
            print(f"    {'-'*70}")

            for feat_key, feat_label in all_feats:
                x_vals = [d[feat_key] for d in dataset]
                r, _ = pearson(x_vals, y_vals)
                if r is not None and abs(r) > abs(r_base):
                    delta_r = abs(r) - abs(r_base)
                    print(f"    {feat_label:<36}  {r:>+6.4f}  {delta_r:>+6.4f}  {'SI' if delta_r > 0.01 else 'marginal':>8}")

    # ══════════════════════════════════════════════════════════════════════════
    # 3. REGRESIÓN MULTIVARIABLE SIMPLE (top features)
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print("  ANÁLISIS DE VALOR INCREMENTAL (correlación parcial)")
    print(f"  ¿Cuánto mejora R² al agregar cada variable a goles previos?")
    print(SEP)

    for target_key, target_label in targets:
        if target_key == 'goles_total':
            continue  # Skip total, es combinación de los otros dos

        y_vals = [d[target_key] for d in dataset]

        if target_key == 'goles_local':
            base_key = 'loc_h_goles'
            candidates = [
                ('loc_h_tiros_arco', 'Tiros arco (home)'),
                ('loc_h_tiros', 'Tiros (home)'),
                ('loc_h_precision', 'Precision (home)'),
                ('loc_h_conversion', 'Conversion (home)'),
                ('loc_h_corners', 'Corners (home)'),
                ('loc_h_posesion', 'Posesion (home)'),
                ('loc_h_tarjetas', 'Tarjetas (home)'),
                ('vis_a_goles_con', 'Goles concede rival (away)'),
                ('vis_a_tiros_arco_con', 'Arco concede rival (away)'),
                ('loc_all_tiros_arco', 'Tiros arco (all)'),
                ('loc_all_precision', 'Precision (all)'),
                ('loc_all_conversion', 'Conversion (all)'),
            ]
        else:
            base_key = 'vis_a_goles'
            candidates = [
                ('vis_a_tiros_arco', 'Tiros arco (away)'),
                ('vis_a_tiros', 'Tiros (away)'),
                ('vis_a_precision', 'Precision (away)'),
                ('vis_a_conversion', 'Conversion (away)'),
                ('vis_a_corners', 'Corners (away)'),
                ('vis_a_posesion', 'Posesion (away)'),
                ('vis_a_tarjetas', 'Tarjetas (away)'),
                ('loc_h_goles_con', 'Goles concede rival (home)'),
                ('loc_h_tiros_arco_con', 'Arco concede rival (home)'),
                ('vis_all_tiros_arco', 'Tiros arco (all)'),
                ('vis_all_precision', 'Precision (all)'),
                ('vis_all_conversion', 'Conversion (all)'),
            ]

        # R² base con solo goles previos
        x_base = [d[base_key] for d in dataset]
        _, _, r2_base = linear_reg(x_base, y_vals)

        print(f"\n  {target_label} — R² base (solo goles previos): {r2_base:.4f}")
        print(f"  {'Variable agregada':<30}  {'R² nuevo':>8}  {'ΔR²':>7}  {'Mejora%':>8}  Util?")
        print(f"  {'-'*72}")

        increments = []
        for feat_key, feat_label in candidates:
            # R² con regresión bivariable (goles_prev + feature)
            x1 = [d[base_key] for d in dataset]
            x2 = [d[feat_key] for d in dataset]

            # Residualizar: regresar y sobre x1, luego ver si x2 predice residuos
            _, _, _ = linear_reg(x1, y_vals)
            b1, a1, _ = linear_reg(x1, y_vals)
            residuos_y = [y_vals[i] - (a1 + b1 * x1[i]) for i in range(len(y_vals))]
            b2, a2, _ = linear_reg(x1, x2)
            residuos_x2 = [x2[i] - (a2 + b2 * x1[i]) for i in range(len(x2))]
            _, _, r2_parcial = linear_reg(residuos_x2, residuos_y)

            # R² total aproximado
            r2_nuevo = r2_base + r2_parcial * (1 - r2_base)
            delta = r2_nuevo - r2_base
            mejora_pct = (delta / r2_base * 100) if r2_base > 0 else 0

            util = 'SI' if delta > 0.005 else 'marginal' if delta > 0.001 else 'NO'
            increments.append((feat_label, r2_nuevo, delta, mejora_pct, util))

        increments.sort(key=lambda x: -x[2])
        for label, r2n, delta, mejora, util in increments:
            print(f"  {label:<30}  {r2n:>7.4f}  {delta:>+6.4f}  {mejora:>+7.1f}%  {util}")

    # ══════════════════════════════════════════════════════════════════════════
    # 4. ANÁLISIS DE PERSISTENCIA (¿son estables los promedios?)
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print("  PERSISTENCIA: ¿CUÁNTO SE MANTIENEN LAS VARIABLES ENTRE PARTIDOS?")
    print(f"  (Correlación entre promedio previo y valor real del partido)")
    print(SEP)

    variables_persistencia = [
        ('loc_h_goles', 'goles_local', 'Goles local'),
        ('loc_h_tiros', 'tiros_local', 'Tiros local'),
        ('loc_h_tiros_arco', 'tiros_arco_local', 'Tiros arco local'),
        ('loc_h_corners', 'corners_local', 'Corners local'),
        ('loc_h_posesion', 'posesion_local', 'Posesion local'),
        ('vis_a_goles', 'goles_visita', 'Goles visita'),
        ('vis_a_tiros', 'tiros_visita', 'Tiros visita'),
        ('vis_a_tiros_arco', 'tiros_arco_visita', 'Tiros arco visita'),
        ('vis_a_corners', 'corners_visita', 'Corners visita'),
        ('vis_a_posesion', 'posesion_visita', 'Posesion visita'),
    ]

    # Necesitamos los valores reales del partido
    real_vals = {}
    for idx, d in enumerate(dataset):
        # Los valores reales ya están en el dataset original
        pass

    # Rebuild with real values
    dataset_full = []
    j = 0
    for idx in range(len(partidos)):
        p = partidos[idx]
        avg_loc_h = calcular_promedios_previos(partidos, idx, p['local_id'], 'home')
        avg_vis_a = calcular_promedios_previos(partidos, idx, p['visita_id'], 'away')
        avg_loc_all = calcular_promedios_previos(partidos, idx, p['local_id'], 'all')
        avg_vis_all = calcular_promedios_previos(partidos, idx, p['visita_id'], 'all')
        if avg_loc_h is None or avg_vis_a is None or avg_loc_all is None or avg_vis_all is None:
            continue

        dataset_full.append({
            'loc_h_goles': avg_loc_h['goles'],
            'loc_h_tiros': avg_loc_h['tiros'],
            'loc_h_tiros_arco': avg_loc_h['tiros_arco'],
            'loc_h_corners': avg_loc_h['corners'],
            'loc_h_posesion': avg_loc_h['posesion'],
            'vis_a_goles': avg_vis_a['goles'],
            'vis_a_tiros': avg_vis_a['tiros'],
            'vis_a_tiros_arco': avg_vis_a['tiros_arco'],
            'vis_a_corners': avg_vis_a['corners'],
            'vis_a_posesion': avg_vis_a['posesion'],
            # Valores reales
            'goles_local': p['goles_local'],
            'goles_visita': p['goles_visita'],
            'tiros_local': p['tiros_local'],
            'tiros_visita': p['tiros_visita'],
            'tiros_arco_local': p['tiros_arco_local'],
            'tiros_arco_visita': p['tiros_arco_visita'],
            'corners_local': p['corners_local'],
            'corners_visita': p['corners_visita'],
            'posesion_local': p['posesion_local'],
            'posesion_visita': p['posesion_visita'],
        })

    print(f"\n  {'Variable':<24}  {'r(prev→real)':>12}  {'R²':>6}  Persistencia")
    print(f"  {SEP2}")

    for prev_key, real_key, label in variables_persistencia:
        xs = [d[prev_key] for d in dataset_full]
        ys = [d[real_key] for d in dataset_full]
        r, p = pearson(xs, ys)
        _, _, r2 = linear_reg(xs, ys)
        if r is not None:
            if abs(r) >= 0.30:
                pers = 'ALTA (estable)'
            elif abs(r) >= 0.15:
                pers = 'MODERADA'
            elif abs(r) >= 0.08:
                pers = 'BAJA'
            else:
                pers = 'MUY BAJA (ruidosa)'
            sig = '***' if p < 0.001 else '**' if p < 0.01 else '*' if p < 0.05 else 'ns'
            print(f"  {label:<24}  {r:>+11.4f}  {r2:>5.3f}  {pers}  ({sig})")

    print(f"\n  Interpretación:")
    print(f"    ALTA persistencia = la variable se mantiene entre partidos → BUEN predictor potencial")
    print(f"    BAJA persistencia = muy ruidosa partido a partido → agregar al modelo no ayudaría")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    min_p = MIN_PARTIDOS
    args = sys.argv[1:]
    if '--min-partidos' in args:
        min_p = int(args[args.index('--min-partidos') + 1])
        MIN_PARTIDOS = min_p

    print("Cargando partidos...")
    partidos = cargar_partidos()
    print(f"  {len(partidos)} partidos cargados ({partidos[0]['fecha']} a {partidos[-1]['fecha']})")

    run_analysis(partidos, min_p)
