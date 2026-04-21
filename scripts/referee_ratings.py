"""
referee_ratings.py
------------------
Calcula ratings de árbitros sobre el histórico de partidos.

Para cada árbitro computa su "factor multiplicativo" sobre tarjetas, normalizado
por la media de la liga donde dirige. Aplica shrinkage Bayesiano hacia 1.0
(árbitro neutral) controlado por K_REF para árbitros con pocos partidos.

Uso standalone:
    python referee_ratings.py                   # genera CSV + muestra ranking
    python referee_ratings.py --k 10            # otro K_REF
    python referee_ratings.py --min 5           # cambia umbral mostrar

Uso como módulo:
    from referee_ratings import compute_ratings, load_ratings, get_factor

    ratings = compute_ratings()                 # dict {ref_name: {factor, n, ...}}
    factor = get_factor('M. Oliver', ratings)   # 1.05  (5% más severo que la media)
"""
import csv
import sys
from pathlib import Path
from collections import defaultdict

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE         = Path(r'C:\Users\Matt\Apuestas Deportivas')
HIST_CSV     = BASE / 'data/historico/partidos_historicos.csv'
RATINGS_CSV  = BASE / 'data/db/referee_ratings.csv'

K_REF_DEFAULT = 10   # peso del prior (factor=1.0). Mayor K = más conservador.


def _load_history():
    with open(HIST_CSV, encoding='utf-8') as f:
        return list(csv.DictReader(f))


def _safe_int(x):
    try:
        return int(x)
    except (ValueError, TypeError):
        return 0


def compute_liga_averages(rows):
    """Media de tarjetas totales por liga, sobre partidos con referee y stats válidas."""
    by_liga = defaultdict(list)
    for r in rows:
        if not r.get('referee', '').strip():
            continue
        yl = _safe_int(r.get('tarjetas_local'))
        yv = _safe_int(r.get('tarjetas_visitante'))
        total = yl + yv
        if total == 0 and not r.get('tarjetas_local'):
            # row sin stats reales — saltar
            continue
        by_liga[int(r['liga_id'])].append(total)

    return {liga: sum(v) / len(v) if v else 4.5
            for liga, v in by_liga.items()}


def compute_ratings(k_ref=K_REF_DEFAULT):
    """
    Devuelve dict {ref_name: {n, avg_cards, liga_avg, factor_raw, factor, ligas}}
    donde:
      factor_raw = avg_cards_ref / avg_cards_liga_ponderado_por_n_partidos_ref
      factor     = shrinkage:  (n*factor_raw + K*1.0) / (n + K)
    """
    rows = _load_history()
    liga_avgs = compute_liga_averages(rows)

    by_ref = defaultdict(lambda: {'cards': [], 'ligas_n': defaultdict(int)})
    for r in rows:
        ref = r.get('referee', '').strip()
        if not ref:
            continue
        yl = _safe_int(r.get('tarjetas_local'))
        yv = _safe_int(r.get('tarjetas_visitante'))
        total = yl + yv
        liga = int(r['liga_id'])
        by_ref[ref]['cards'].append((total, liga))
        by_ref[ref]['ligas_n'][liga] += 1

    ratings = {}
    for ref, d in by_ref.items():
        n = len(d['cards'])
        if n == 0:
            continue
        avg_ref = sum(c for c, _ in d['cards']) / n
        # Liga "esperada" para este árbitro = promedio ponderado por partidos
        liga_avg_weighted = sum(liga_avgs.get(liga, 4.5) * cnt
                                for liga, cnt in d['ligas_n'].items()) / n
        if liga_avg_weighted <= 0:
            continue
        factor_raw = avg_ref / liga_avg_weighted
        # Shrinkage hacia factor=1.0 (neutral)
        factor = (n * factor_raw + k_ref * 1.0) / (n + k_ref)
        ratings[ref] = {
            'n':                n,
            'avg_cards':        round(avg_ref, 3),
            'liga_avg':         round(liga_avg_weighted, 3),
            'factor_raw':       round(factor_raw, 4),
            'factor':           round(factor, 4),
            'ligas':            ','.join(str(l) for l in sorted(d['ligas_n'].keys())),
        }
    return ratings


def save_ratings(ratings, path=None):
    out = path or RATINGS_CSV
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = ['referee', 'n', 'avg_cards', 'liga_avg',
              'factor_raw', 'factor', 'ligas']
    items = sorted(ratings.items(), key=lambda kv: -kv[1]['n'])
    with open(out, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        w.writeheader()
        for ref, d in items:
            row = {'referee': ref, **d}
            w.writerow(row)


def load_ratings(path=None):
    p = path or RATINGS_CSV
    if not p.exists():
        return {}
    out = {}
    with open(p, encoding='utf-8') as f:
        for r in csv.DictReader(f):
            out[r['referee']] = {
                'n':          int(r['n']),
                'avg_cards':  float(r['avg_cards']),
                'liga_avg':   float(r['liga_avg']),
                'factor_raw': float(r['factor_raw']),
                'factor':     float(r['factor']),
                'ligas':      r.get('ligas', ''),
            }
    return out


def get_factor(referee, ratings, alpha=1.0, default=1.0):
    """
    Devuelve el factor multiplicativo a aplicar a mu_cards.
    alpha controla el peso (0 = ignorar referee, 1 = full effect).
    Si el referee no está en ratings → default (1.0).
    """
    if not referee:
        return default
    r = ratings.get(referee.strip())
    if not r:
        return default
    # f^alpha permite suavizar: alpha=0.5 hace que un factor 1.20 se convierta en 1.10
    return r['factor'] ** alpha


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]
    k_ref = K_REF_DEFAULT
    min_n = 20
    if '--k'   in args: k_ref = int(args[args.index('--k')   + 1])
    if '--min' in args: min_n = int(args[args.index('--min') + 1])

    print('=' * 78)
    print(f'  REFEREE RATINGS  (K_REF={k_ref}, min_n_display={min_n})')
    print('=' * 78)

    ratings = compute_ratings(k_ref=k_ref)
    save_ratings(ratings)

    print(f'\n  Total árbitros : {len(ratings)}')
    n_ge10 = sum(1 for r in ratings.values() if r['n'] >= 10)
    n_ge20 = sum(1 for r in ratings.values() if r['n'] >= 20)
    print(f'  Con n>=10      : {n_ge10}')
    print(f'  Con n>=20      : {n_ge20}')
    print(f'  CSV guardado en: {RATINGS_CSV.relative_to(BASE)}')

    items = [(k, v) for k, v in ratings.items() if v['n'] >= min_n]
    items.sort(key=lambda kv: kv[1]['factor'])

    print(f'\n  TOP 10 MÁS PERMISIVOS (n>={min_n}):')
    print(f"  {'Árbitro':<40} {'n':>4} {'avg':>5} {'liga':>5} {'fRaw':>6} {'fShr':>6}")
    print('  ' + '-' * 76)
    for k, v in items[:10]:
        print(f"  {k[:40]:<40} {v['n']:>4} {v['avg_cards']:>5.2f} "
              f"{v['liga_avg']:>5.2f} {v['factor_raw']:>6.3f} {v['factor']:>6.3f}")

    print(f'\n  TOP 10 MÁS SEVEROS (n>={min_n}):')
    print(f"  {'Árbitro':<40} {'n':>4} {'avg':>5} {'liga':>5} {'fRaw':>6} {'fShr':>6}")
    print('  ' + '-' * 76)
    for k, v in items[-10:][::-1]:
        print(f"  {k[:40]:<40} {v['n']:>4} {v['avg_cards']:>5.2f} "
              f"{v['liga_avg']:>5.2f} {v['factor_raw']:>6.3f} {v['factor']:>6.3f}")

    # Estadísticas de spread
    factors = [v['factor'] for v in ratings.values() if v['n'] >= min_n]
    if factors:
        print(f"\n  Factor (shrunk) — min: {min(factors):.3f}  "
              f"max: {max(factors):.3f}  "
              f"spread: {max(factors)/min(factors):.2f}x")


if __name__ == '__main__':
    main()
