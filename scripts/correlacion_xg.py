"""
correlacion_xg.py
-----------------
Compara poder predictivo de: goles previos vs tiros al arco vs xG
para predecir goles del próximo partido. Walk-forward (sin look-ahead).
"""

import csv, math, datetime, sys
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

HIST_CSV = Path(r'C:\Users\Matt\Apuestas Deportivas\data\historico\partidos_historicos.csv')
HALF_LIFE = 90
MIN_P = 3

SEP  = '=' * 80
SEP2 = '-' * 80


def weighted_avg(vals_dates, today_ord):
    if not vals_dates:
        return None, 0
    lam = math.log(2) / HALF_LIFE
    tw = twv = 0.0
    for val, d in vals_dates:
        days = max(0, today_ord - d)
        w = math.exp(-lam * days)
        tw += w
        twv += w * val
    return (twv / tw if tw > 0 else None), len(vals_dates)


def pearson(xs, ys):
    n = len(xs)
    if n < 10:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs) / n)
    sy = math.sqrt(sum((y - my) ** 2 for y in ys) / n)
    if sx == 0 or sy == 0:
        return None
    return sum((xs[i] - mx) * (ys[i] - my) for i in range(n)) / (n * sx * sy)


def cargar():
    with open(HIST_CSV, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    partidos = []
    for r in rows:
        try:
            xg_l = r.get('xg_local', '')
            xg_v = r.get('xg_visitante', '')
            partidos.append({
                'fecha_ord': datetime.date.fromisoformat(r['fecha']).toordinal(),
                'local_id':  int(r['equipo_local_id']),
                'visita_id': int(r['equipo_visitante_id']),
                'gl': int(r['goles_local']),
                'gv': int(r['goles_visitante']),
                'tal': int(r['tiros_arco_local']),
                'tav': int(r['tiros_arco_visitante']),
                'xg_l': float(xg_l) if xg_l not in ('', '-') else None,
                'xg_v': float(xg_v) if xg_v not in ('', '-') else None,
            })
        except (ValueError, KeyError):
            continue
    partidos.sort(key=lambda p: p['fecha_ord'])
    return partidos


def collect_history(partidos, idx, team_id, venue):
    """Collect (value, date) tuples for a team before match idx."""
    p = partidos[idx]
    today = p['fecha_ord']
    goals, sot, xg = [], [], []

    for pp in partidos[:idx]:
        if pp['fecha_ord'] >= today:
            continue
        if venue in ('home', 'all') and pp['local_id'] == team_id:
            goals.append((pp['gl'], pp['fecha_ord']))
            sot.append((pp['tal'], pp['fecha_ord']))
            if pp['xg_l'] is not None:
                xg.append((pp['xg_l'], pp['fecha_ord']))
        if venue in ('away', 'all') and pp['visita_id'] == team_id:
            goals.append((pp['gv'], pp['fecha_ord']))
            sot.append((pp['tav'], pp['fecha_ord']))
            if pp['xg_v'] is not None:
                xg.append((pp['xg_v'], pp['fecha_ord']))
    return goals, sot, xg


def main():
    partidos = cargar()
    n_xg = sum(1 for p in partidos if p['xg_l'] is not None)
    print(f"{len(partidos)} partidos | {n_xg} con xG ({100*n_xg/len(partidos):.0f}%)")

    # Build dataset
    dataset = []
    for idx in range(len(partidos)):
        p = partidos[idx]
        today = p['fecha_ord']

        g_h, s_h, x_h = collect_history(partidos, idx, p['local_id'], 'home')
        g_a, s_a, x_a = collect_history(partidos, idx, p['visita_id'], 'away')
        g_all_l, s_all_l, x_all_l = collect_history(partidos, idx, p['local_id'], 'all')
        g_all_v, s_all_v, x_all_v = collect_history(partidos, idx, p['visita_id'], 'all')

        if len(g_h) < MIN_P or len(g_a) < MIN_P:
            continue

        avg_gl_h, _ = weighted_avg(g_h, today)
        avg_sot_h, _ = weighted_avg(s_h, today)
        avg_xg_h, n_xg_h = weighted_avg(x_h, today)
        avg_gl_all, _ = weighted_avg(g_all_l, today)
        avg_sot_all_l, _ = weighted_avg(s_all_l, today)
        avg_xg_all_l, _ = weighted_avg(x_all_l, today)

        avg_gv_a, _ = weighted_avg(g_a, today)
        avg_sot_a, _ = weighted_avg(s_a, today)
        avg_xg_a, n_xg_a = weighted_avg(x_a, today)
        avg_gv_all, _ = weighted_avg(g_all_v, today)
        avg_sot_all_v, _ = weighted_avg(s_all_v, today)
        avg_xg_all_v, _ = weighted_avg(x_all_v, today)

        dataset.append({
            'gl': p['gl'], 'gv': p['gv'], 'gt': p['gl'] + p['gv'],
            'xg_l_real': p['xg_l'], 'xg_v_real': p['xg_v'],
            # Local como home
            'avg_gl_h': avg_gl_h, 'avg_sot_h': avg_sot_h, 'avg_xg_h': avg_xg_h,
            # Visita como away
            'avg_gv_a': avg_gv_a, 'avg_sot_a': avg_sot_a, 'avg_xg_a': avg_xg_a,
            # All venues
            'avg_gl_all': avg_gl_all, 'avg_sot_all_l': avg_sot_all_l, 'avg_xg_all_l': avg_xg_all_l,
            'avg_gv_all': avg_gv_all, 'avg_sot_all_v': avg_sot_all_v, 'avg_xg_all_v': avg_xg_all_v,
        })

    print(f"{len(dataset)} partidos con historia suficiente")
    n_xg_h = sum(1 for d in dataset if d['avg_xg_h'] is not None)
    n_xg_a = sum(1 for d in dataset if d['avg_xg_a'] is not None)
    print(f"Con xG hist local: {n_xg_h} | Con xG hist visita: {n_xg_a}")

    # ══════════════════════════════════════════════════════════════════════════
    # CORRELACIÓN PREDICTIVA COMPARATIVA
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print(f"  GOLES PREVIOS vs TIROS AL ARCO vs xG — PODER PREDICTIVO")
    print(SEP)

    targets = [
        ('gl', 'GOLES LOCAL', [
            ('avg_gl_h',     'Goles previos (home)'),
            ('avg_sot_h',    'Tiros arco previos (home)'),
            ('avg_xg_h',     'xG previo (home)'),
            ('avg_gl_all',   'Goles previos (all)'),
            ('avg_sot_all_l','Tiros arco previos (all)'),
            ('avg_xg_all_l', 'xG previo (all)'),
        ]),
        ('gv', 'GOLES VISITA', [
            ('avg_gv_a',     'Goles previos (away)'),
            ('avg_sot_a',    'Tiros arco previos (away)'),
            ('avg_xg_a',     'xG previo (away)'),
            ('avg_gv_all',   'Goles previos (all)'),
            ('avg_sot_all_v','Tiros arco previos (all)'),
            ('avg_xg_all_v', 'xG previo (all)'),
        ]),
        ('gt', 'GOLES TOTALES', [
            ('avg_gl_h',     'Goles prev local (home)'),
            ('avg_sot_h',    'Tiros arco prev local (home)'),
            ('avg_xg_h',     'xG prev local (home)'),
            ('avg_gv_a',     'Goles prev visita (away)'),
            ('avg_sot_a',    'Tiros arco prev visita (away)'),
            ('avg_xg_a',     'xG prev visita (away)'),
            ('avg_xg_all_l', 'xG prev local (all)'),
            ('avg_xg_all_v', 'xG prev visita (all)'),
        ]),
    ]

    for target_key, target_label, features in targets:
        print(f"\n  {target_label}:")
        print(f"  {SEP2}")
        print(f"  {'Variable':<32}  {'N':>5}  {'r':>7}  {'R2':>6}  Fuerza")
        print(f"  {SEP2}")

        results = []
        for feat_key, feat_label in features:
            valid = [(d[target_key], d[feat_key]) for d in dataset if d[feat_key] is not None]
            if len(valid) < 10:
                results.append((feat_label, len(valid), None, 0))
                continue
            ys = [v[0] for v in valid]
            xs = [v[1] for v in valid]
            r = pearson(xs, ys)
            results.append((feat_label, len(valid), r, r ** 2 if r else 0))

        results.sort(key=lambda x: -abs(x[2] or 0))
        for label, n, r, r2 in results:
            if r is None:
                print(f"  {label:<32}  {n:>5}  {'n/a':>7}  {'n/a':>6}")
                continue
            absv = abs(r)
            f = 'FUERTE' if absv >= 0.3 else 'MODERADA' if absv >= 0.15 else 'DEBIL' if absv >= 0.08 else '-'
            bar = '#' * int(absv * 40) + '.' * (40 - int(absv * 40))
            print(f"  {label:<32}  {n:>5}  {r:>+6.4f}  {r2:>5.3f}  {f:<10} [{bar}]")

    # ══════════════════════════════════════════════════════════════════════════
    # PERSISTENCIA: xG previo → xG real del partido
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print(f"  PERSISTENCIA: Promedio previo -> Valor real del partido")
    print(SEP)

    persist_tests = [
        ('avg_gl_h',     'gl',        'Goles prev(home) -> Goles real local'),
        ('avg_sot_h',    'tal_real',  'SOT prev(home) -> SOT real local'),
        ('avg_xg_h',     'xg_l_real', 'xG prev(home) -> xG real local'),
        ('avg_xg_h',     'gl',        'xG prev(home) -> GOLES real local'),
        ('avg_gv_a',     'gv',        'Goles prev(away) -> Goles real visita'),
        ('avg_sot_a',    'tav_real',  'SOT prev(away) -> SOT real visita'),
        ('avg_xg_a',     'xg_v_real', 'xG prev(away) -> xG real visita'),
        ('avg_xg_a',     'gv',        'xG prev(away) -> GOLES real visita'),
        ('avg_xg_all_l', 'gl',        'xG prev(all) -> GOLES real local'),
        ('avg_xg_all_v', 'gv',        'xG prev(all) -> GOLES real visita'),
    ]

    # Add real SOT to dataset for persistence check
    for i, idx_match in enumerate(range(len(partidos))):
        if i >= len(dataset):
            break
    # Rebuild real values
    j = 0
    for idx in range(len(partidos)):
        p = partidos[idx]
        g_h, _, _ = collect_history(partidos, idx, p['local_id'], 'home')
        g_a, _, _ = collect_history(partidos, idx, p['visita_id'], 'away')
        if len(g_h) < MIN_P or len(g_a) < MIN_P:
            continue
        if j < len(dataset):
            dataset[j]['tal_real'] = p['tal']
            dataset[j]['tav_real'] = p['tav']
        j += 1

    print(f"\n  {'Test':<42}  {'N':>5}  {'r':>7}  Persistencia")
    print(f"  {SEP2}")

    for prev_key, real_key, label in persist_tests:
        valid = [(d[prev_key], d[real_key]) for d in dataset
                 if d.get(prev_key) is not None and d.get(real_key) is not None]
        if len(valid) < 10:
            print(f"  {label:<42}  {len(valid):>5}  {'n/a':>7}")
            continue
        xs = [v[0] for v in valid]
        ys = [v[1] for v in valid]
        r = pearson(xs, ys)
        if r is None:
            print(f"  {label:<42}  {len(valid):>5}  {'n/a':>7}")
            continue
        absv = abs(r)
        pers = 'MUY ALTA' if absv >= 0.4 else 'ALTA' if absv >= 0.3 else 'MODERADA' if absv >= 0.15 else 'BAJA'
        print(f"  {label:<42}  {len(valid):>5}  {r:>+6.4f}  {pers}")

    # ══════════════════════════════════════════════════════════════════════════
    # RESUMEN COMPARATIVO
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print(f"  RESUMEN: MEJOR PREDICTOR POR TARGET")
    print(SEP)

    print(f"\n  {'Target':<16}  {'Goles prev':>12}  {'SOT prev':>12}  {'xG prev':>12}  Ganador")
    print(f"  {SEP2}")

    comparisons = [
        ('Goles local', 'gl', 'avg_gl_h', 'avg_sot_h', 'avg_xg_h'),
        ('Goles visita', 'gv', 'avg_gv_a', 'avg_sot_a', 'avg_xg_a'),
        ('GL (all)', 'gl', 'avg_gl_all', 'avg_sot_all_l', 'avg_xg_all_l'),
        ('GV (all)', 'gv', 'avg_gv_all', 'avg_sot_all_v', 'avg_xg_all_v'),
    ]

    for label, target, g_key, s_key, x_key in comparisons:
        vals_g = [(d[target], d[g_key]) for d in dataset if d[g_key] is not None]
        vals_s = [(d[target], d[s_key]) for d in dataset if d[s_key] is not None]
        vals_x = [(d[target], d[x_key]) for d in dataset if d[x_key] is not None]

        r_g = pearson([v[1] for v in vals_g], [v[0] for v in vals_g]) if len(vals_g) >= 10 else None
        r_s = pearson([v[1] for v in vals_s], [v[0] for v in vals_s]) if len(vals_s) >= 10 else None
        r_x = pearson([v[1] for v in vals_x], [v[0] for v in vals_x]) if len(vals_x) >= 10 else None

        def fmt_r(r):
            return f"{r:>+6.4f}" if r is not None else "  n/a "

        best = max(
            [('Goles', abs(r_g) if r_g else 0),
             ('SOT', abs(r_s) if r_s else 0),
             ('xG', abs(r_x) if r_x else 0)],
            key=lambda x: x[1]
        )

        print(f"  {label:<16}  {fmt_r(r_g):>12}  {fmt_r(r_s):>12}  {fmt_r(r_x):>12}  {best[0]}")

    print()


if __name__ == '__main__':
    main()
