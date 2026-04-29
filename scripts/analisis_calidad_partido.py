"""
analisis_calidad_partido.py
----------------------------
Hipótesis: cuando dos equipos de bajo nivel se enfrentan, el modelo sobreestima
los goles totales (y otras estadísticas), por lo que Under sería value bet.

Metodología: walk-forward exacto. Para cada partido M del histórico ordenado por
fecha, compute_match_params usa solo history[:idx_de_M]. Se recolectan ratings
atk/def y predicciones mu del modelo v3, junto con goles reales.

Luego se bucketea por distintas métricas de "calidad conjunta" y se reporta:
  - goles actual vs predicho (gap = actual - predicho; negativo ⇒ modelo alto)
  - hit rate Under 2.5 / 1.5 empírico vs Poisson(mu_total)

Uso:
    python scripts/analisis_calidad_partido.py
    python scripts/analisis_calidad_partido.py --skip 1000
    python scripts/analisis_calidad_partido.py --cache data/tmp/calidad.csv
"""
import sys, csv, math, argparse, time
from pathlib import Path
from collections import defaultdict

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).parent))

from modelo_v3 import load_csv, compute_match_params

HIST_CSV = BASE / 'data/historico/partidos_historicos.csv'


def poisson_cdf(k: int, mu: float) -> float:
    """P(X <= k) para X ~ Poisson(mu)."""
    if mu <= 0:
        return 1.0
    s = 0.0
    for i in range(k + 1):
        s += math.exp(-mu) * (mu ** i) / math.factorial(i)
    return s


def walk_forward(hist: list[dict], skip: int, cache_path: Path | None) -> list[dict]:
    """Corre walk-forward sobre el histórico; devuelve lista de resultados."""
    hist.sort(key=lambda r: r.get('fecha', ''))
    total = len(hist)
    print(f'Histórico ordenado: {total} partidos, saltando primeros {skip}')

    results: list[dict] = []
    t0 = time.time()

    for idx in range(skip, total):
        m = hist[idx]
        try:
            home_id = int(m['equipo_local_id'])
            away_id = int(m['equipo_visitante_id'])
            liga    = int(m['liga_id'])
            gl = int(m['goles_local']); gv = int(m['goles_visitante'])
        except (ValueError, KeyError, TypeError):
            continue

        try:
            p = compute_match_params(home_id, away_id, hist[:idx], liga)
        except Exception:
            continue

        r = p['_ratings']
        results.append({
            'fid':    m.get('fixture_id'),
            'fecha':  m.get('fecha'),
            'liga':   liga,
            'atk_L':  r['atk_local'],
            'atk_V':  r['atk_visita'],
            'def_L':  r['def_local'],
            'def_V':  r['def_visita'],
            'mu_L':   p['lambda_local'],
            'mu_V':   p['lambda_vis'],
            'mu_cor': p['mu_corners_total'],
            'mu_sh_L': p['mu_shots_local'],
            'mu_sh_V': p['mu_shots_vis'],
            'g_L':    gl,
            'g_V':    gv,
            'c_L':    int(m.get('corners_local', 0) or 0),
            'c_V':    int(m.get('corners_visitante', 0) or 0),
            's_L':    int(m.get('tiros_local', 0) or 0),
            's_V':    int(m.get('tiros_visitante', 0) or 0),
        })

        n_done = idx - skip + 1
        if n_done % 200 == 0:
            dt = time.time() - t0
            eta = dt * (total - idx) / n_done
            print(f'  [{n_done:>5d}/{total-skip}]  elapsed={dt/60:.1f}min  eta={eta/60:.1f}min  N_useful={len(results)}')

    if cache_path:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            w.writeheader()
            w.writerows(results)
        print(f'Cache guardado en {cache_path}')

    return results


def load_cache(cache_path: Path) -> list[dict]:
    with open(cache_path, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        for k in ['atk_L','atk_V','def_L','def_V','mu_L','mu_V','mu_cor','mu_sh_L','mu_sh_V']:
            r[k] = float(r[k])
        for k in ['g_L','g_V','c_L','c_V','s_L','s_V']:
            r[k] = int(r[k])
    return rows


def enrich(rows: list[dict], apply_cal: bool = False) -> None:
    """Agrega métricas derivadas a cada fila. Si apply_cal, usa probs calibradas."""
    if apply_cal:
        try:
            from modelo_v3_calibrado import calibrar_prob
        except Exception as e:
            print(f'  AVISO: no se pudo cargar calibrador ({e}); usando raw')
            apply_cal = False

    for r in rows:
        r['avg_atk']   = (r['atk_L'] + r['atk_V']) / 2
        r['min_atk']   = min(r['atk_L'], r['atk_V'])
        r['max_atk']   = max(r['atk_L'], r['atk_V'])
        r['avg_def']   = (r['def_L'] + r['def_V']) / 2
        r['quality']   = r['avg_atk'] / r['avg_def']
        r['total_g']   = r['g_L'] + r['g_V']
        r['total_mu']  = r['mu_L'] + r['mu_V']
        r['resid']     = r['total_g'] - r['total_mu']
        p_u25_raw = poisson_cdf(2, r['total_mu'])
        p_u15_raw = poisson_cdf(1, r['total_mu'])
        if apply_cal:
            # calibrador isotonic entrenado con (prob, hit) de ambos Over y Under
            # en (Goles, Total). Aplicar directo a P(Under).
            r['p_u25'] = calibrar_prob(p_u25_raw, 'Goles', 'Total')
            r['p_u15'] = calibrar_prob(p_u15_raw, 'Goles', 'Total')
        else:
            r['p_u25'] = p_u25_raw
            r['p_u15'] = p_u15_raw
        r['total_c']   = r['c_L'] + r['c_V']
        r['total_s']   = r['s_L'] + r['s_V']
        r['mu_sh_tot'] = r['mu_sh_L'] + r['mu_sh_V']


def report_quintiles(rows: list[dict], metric: str, low_first: bool = True) -> None:
    print(f'\n{"="*100}')
    print(f'  QUINTILES POR {metric}  (Q1 = peor calidad)')
    print(f'{"="*100}')

    srt = sorted(rows, key=lambda r: r[metric], reverse=not low_first)
    n = len(srt)
    q_size = n // 5

    header = (f'  {"Q":>2}  {"rango":<17}  {"N":>4}  '
              f'{"goles act":>9}  {"goles pred":>10}  {"gap":>7}  '
              f'{"U2.5 act":>8}  {"U2.5 mod":>8}  {"dU2.5":>7}  '
              f'{"U1.5 act":>8}  {"U1.5 mod":>8}')
    print(header)
    print('  ' + '-' * 96)

    for q in range(5):
        lo = q * q_size
        hi = (q + 1) * q_size if q < 4 else n
        bucket = srt[lo:hi]
        bn = len(bucket)
        if bn == 0:
            continue
        rango = f'{bucket[0][metric]:.2f}–{bucket[-1][metric]:.2f}'
        m_actual = sum(b['total_g']  for b in bucket) / bn
        m_pred   = sum(b['total_mu'] for b in bucket) / bn
        gap      = m_actual - m_pred
        u25_a    = sum(1 for b in bucket if b['total_g'] <= 2) / bn
        u25_m    = sum(b['p_u25'] for b in bucket) / bn
        u15_a    = sum(1 for b in bucket if b['total_g'] <= 1) / bn
        u15_m    = sum(b['p_u15'] for b in bucket) / bn
        print(f'  Q{q+1}  {rango:<17}  {bn:>4d}  '
              f'{m_actual:>9.2f}  {m_pred:>10.2f}  {gap:>+7.3f}  '
              f'{u25_a:>8.1%}  {u25_m:>8.1%}  {u25_a-u25_m:>+6.1%}  '
              f'{u15_a:>8.1%}  {u15_m:>8.1%}')


def report_extremes(rows: list[dict]) -> None:
    """Análisis específico: combinaciones donde AMBOS equipos son débiles."""
    print(f'\n{"="*100}')
    print(f'  FOCO: PARTIDOS ENTRE DOS EQUIPOS DÉBILES (ambos atk < umbral)')
    print(f'{"="*100}')
    for thr in [0.70, 0.80, 0.90, 1.00]:
        sub = [r for r in rows if r['atk_L'] < thr and r['atk_V'] < thr]
        if not sub:
            continue
        bn = len(sub)
        m_actual = sum(b['total_g']  for b in sub) / bn
        m_pred   = sum(b['total_mu'] for b in sub) / bn
        u25_a = sum(1 for b in sub if b['total_g'] <= 2) / bn
        u25_m = sum(b['p_u25'] for b in sub) / bn
        print(f'  atk < {thr:.2f}: N={bn:4d}  actual={m_actual:.2f}  pred={m_pred:.2f}  '
              f'gap={m_actual-m_pred:+.3f}  U2.5 act={u25_a:.1%} mod={u25_m:.1%} '
              f'd={u25_a-u25_m:+.1%}')


def report_mercados(rows: list[dict]) -> None:
    """Revisa el sesgo de regresión a la media en goles, corners y tiros."""
    print(f'\n{"="*100}')
    print(f'  SESGO POR MERCADO — quintiles por mu predicho')
    print(f'{"="*100}')

    mercados = [
        ('GOLES',   'total_mu',  'total_g',  None),
        ('CORNERS', 'mu_cor',    'total_c',  None),
        ('TIROS',   'mu_sh_tot', 'total_s',  None),
    ]

    for nombre, mu_key, actual_key, _ in mercados:
        srt = sorted(rows, key=lambda r: r[mu_key])
        n   = len(srt)
        q_size = n // 5
        print(f'\n  {nombre}  (mu={mu_key}, actual={actual_key})')
        print(f'  {"Q":>2}  {"rango mu":<17}  {"N":>5}  {"act":>7}  {"pred":>7}  {"gap":>8}  {"gap/pred":>9}')
        print(f'  {"-"*70}')
        for q in range(5):
            lo = q * q_size
            hi = (q + 1) * q_size if q < 4 else n
            bucket = srt[lo:hi]
            bn = len(bucket)
            if bn == 0:
                continue
            rango = f'{bucket[0][mu_key]:.2f}-{bucket[-1][mu_key]:.2f}'
            a = sum(b[actual_key] for b in bucket) / bn
            p = sum(b[mu_key]     for b in bucket) / bn
            gap = a - p
            rel = gap / p if p > 0 else 0
            print(f'  Q{q+1}  {rango:<17}  {bn:>5}  {a:>7.2f}  {p:>7.2f}  {gap:>+8.3f}  {rel:>+8.1%}')


def report_by_liga(rows: list[dict]) -> None:
    print(f'\n{"="*100}')
    print(f'  POR LIGA (top 10 por N)')
    print(f'{"="*100}')
    by_liga = defaultdict(list)
    for r in rows:
        by_liga[r['liga']].append(r)
    ordered = sorted(by_liga.items(), key=lambda kv: -len(kv[1]))[:10]
    for liga, sub in ordered:
        bn = len(sub)
        m_actual = sum(b['total_g']  for b in sub) / bn
        m_pred   = sum(b['total_mu'] for b in sub) / bn
        u25_a = sum(1 for b in sub if b['total_g'] <= 2) / bn
        u25_m = sum(b['p_u25'] for b in sub) / bn
        print(f'  liga_id={liga:>4}  N={bn:>4}  act={m_actual:.2f}  pred={m_pred:.2f}  '
              f'gap={m_actual-m_pred:+.3f}  U2.5 act={u25_a:.1%} mod={u25_m:.1%} '
              f'd={u25_a-u25_m:+.1%}')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--skip', type=int, default=1000)
    ap.add_argument('--cache', type=str, default='data/tmp/calidad_walkfwd.csv')
    ap.add_argument('--reuse-cache', action='store_true')
    ap.add_argument('--calibrado', action='store_true',
                    help='Aplica calibrador isotonic a P(Under) antes de reportar')
    args = ap.parse_args()

    cache = BASE / args.cache if args.cache else None

    if args.reuse_cache and cache and cache.exists():
        print(f'Reutilizando cache {cache}')
        rows = load_cache(cache)
    else:
        hist = load_csv(HIST_CSV)
        rows = walk_forward(hist, args.skip, cache)

    enrich(rows, apply_cal=args.calibrado)
    print(f'\n{len(rows)} partidos analizados  modo={"CALIBRADO" if args.calibrado else "RAW"}')

    # Estadísticas globales
    mean_act  = sum(r['total_g']  for r in rows) / len(rows)
    mean_pred = sum(r['total_mu'] for r in rows) / len(rows)
    u25_a = sum(1 for r in rows if r['total_g'] <= 2) / len(rows)
    u25_m = sum(r['p_u25'] for r in rows) / len(rows)
    print(f'GLOBAL: goles act={mean_act:.3f}  pred={mean_pred:.3f}  gap={mean_act-mean_pred:+.3f}')
    print(f'        U2.5 act={u25_a:.1%}  modelo={u25_m:.1%}  delta={u25_a-u25_m:+.1%}')

    report_quintiles(rows, 'min_atk')      # el más débil de los dos
    report_quintiles(rows, 'avg_atk')
    report_quintiles(rows, 'quality')
    report_quintiles(rows, 'total_mu')     # predicho (baseline)

    report_extremes(rows)
    report_mercados(rows)
    report_by_liga(rows)


if __name__ == '__main__':
    main()
