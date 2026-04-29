"""
fix_regresion_media.py
-----------------------
Prueba distintos valores de alpha (shrinkage adicional de los ratings atk/def hacia 1)
sobre el cache de analisis_calidad_partido para encontrar el que minimiza el sesgo
de regresión a la media en goles.

mu_original del modelo v3 = liga_avg × atk × def × forma

Aplicamos shrinkage posterior:
    atk_new = 1 + alpha × (atk_raw - 1)
    def_new = 1 + alpha × (def_raw - 1)
    mu_new  = mu_original × (atk_new × def_new) / (atk_raw × def_raw)

Con alpha=1.0 no cambia nada. Con alpha=0.0 mu se colapsa a liga_avg × forma.
Recorremos alpha en [0.3, 0.4, ..., 1.0] y reportamos:
  - gap global (mean actual - mean pred)
  - gap en Q1 y Q5 (extremos)
  - MAE de quintiles
"""
import sys, csv, math
from pathlib import Path
from collections import defaultdict

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).parent))

CACHE = BASE / 'data/tmp/calidad_walkfwd.csv'


def load_cache():
    with open(CACHE, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        for k in ['atk_L','atk_V','def_L','def_V','mu_L','mu_V']:
            r[k] = float(r[k])
        r['g_L']   = int(r['g_L'])
        r['g_V']   = int(r['g_V'])
        r['total_g']  = r['g_L'] + r['g_V']
        r['total_mu_raw'] = r['mu_L'] + r['mu_V']
    return rows


def mu_ajustado(r: dict, alpha: float) -> float:
    """Recomputa mu_L + mu_V aplicando shrinkage alpha a los ratings."""
    atk_L_new = 1 + alpha * (r['atk_L'] - 1)
    atk_V_new = 1 + alpha * (r['atk_V'] - 1)
    def_L_new = 1 + alpha * (r['def_L'] - 1)
    def_V_new = 1 + alpha * (r['def_V'] - 1)
    # factor liga_avg × forma (no toca el shrinkage de ratings base)
    factor_L = r['mu_L'] / (r['atk_L'] * r['def_V']) if r['atk_L'] * r['def_V'] > 0 else 0
    factor_V = r['mu_V'] / (r['atk_V'] * r['def_L']) if r['atk_V'] * r['def_L'] > 0 else 0
    mu_L_new = factor_L * atk_L_new * def_V_new
    mu_V_new = factor_V * atk_V_new * def_L_new
    return mu_L_new + mu_V_new


def poisson_cdf(k: int, mu: float) -> float:
    if mu <= 0: return 1.0
    s = 0.0
    for i in range(k + 1):
        s += math.exp(-mu) * (mu ** i) / math.factorial(i)
    return s


def reportar_quintiles(rows, alpha, mu_func):
    srt = sorted(rows, key=lambda r: mu_func(r))
    n = len(srt)
    q_size = n // 5
    quintiles_data = []
    for q in range(5):
        lo = q * q_size
        hi = (q + 1) * q_size if q < 4 else n
        bucket = srt[lo:hi]
        bn = len(bucket)
        m_a = sum(b['total_g']  for b in bucket) / bn
        m_p = sum(mu_func(b)    for b in bucket) / bn
        gap = m_a - m_p
        u25_a = sum(1 for b in bucket if b['total_g'] <= 2) / bn
        u25_p = sum(poisson_cdf(2, mu_func(b)) for b in bucket) / bn
        quintiles_data.append({
            'q': q+1, 'N': bn,
            'act': m_a, 'pred': m_p, 'gap': gap,
            'u25_a': u25_a, 'u25_p': u25_p, 'du25': u25_a - u25_p,
            'mu_range': (mu_func(bucket[0]), mu_func(bucket[-1])),
        })
    return quintiles_data


def validar_oos(train, test):
    """Split chronologico: alpha optimo en TRAIN, reporta performance en TEST."""
    alphas = [1.00, 0.90, 0.80, 0.70, 0.60, 0.50, 0.40, 0.35, 0.30, 0.25, 0.20]

    print(f'\n{"="*95}')
    print('  OOS: MAE dU2.5 y MAE gap de cada alpha en TRAIN y TEST')
    print(f'{"="*95}')
    print(f'  {"alpha":>6}  '
          f'{"MAE gap tr":>11}  {"MAE gap te":>11}  '
          f'{"MAE dU tr":>10}  {"MAE dU te":>10}  '
          f'{"gap Q1 te":>10}  {"gap Q5 te":>10}  '
          f'{"dU Q1 te":>10}  {"dU Q5 te":>10}')
    print('  ' + '-' * 110)

    best_train_alpha = None
    best_train_mae   = 1e9

    for alpha in alphas:
        mu_f = lambda r, a=alpha: mu_ajustado(r, a)
        q_tr = reportar_quintiles(train, alpha, mu_f)
        q_te = reportar_quintiles(test,  alpha, mu_f)

        mae_gap_tr = sum(abs(q['gap']) for q in q_tr) / 5
        mae_gap_te = sum(abs(q['gap']) for q in q_te) / 5
        mae_du_tr  = sum(abs(q['du25']) for q in q_tr) / 5
        mae_du_te  = sum(abs(q['du25']) for q in q_te) / 5

        if mae_du_tr < best_train_mae:
            best_train_mae = mae_du_tr
            best_train_alpha = alpha

        marker = ''
        print(f'  {alpha:>6.2f}  '
              f'{mae_gap_tr:>+11.3f}  {mae_gap_te:>+11.3f}  '
              f'{mae_du_tr:>+10.1%}  {mae_du_te:>+10.1%}  '
              f'{q_te[0]["gap"]:>+10.3f}  {q_te[4]["gap"]:>+10.3f}  '
              f'{q_te[0]["du25"]:>+10.1%}  {q_te[4]["du25"]:>+10.1%}'
              f'{marker}')

    print(f'\n  Mejor alpha en TRAIN: {best_train_alpha:.2f}  (MAE dU2.5 train={best_train_mae:.1%})')

    # Validacion: alpha elegido en train, performance en test
    mu_f = lambda r, a=best_train_alpha: mu_ajustado(r, a)
    q_te = reportar_quintiles(test, best_train_alpha, mu_f)
    mae_du_te = sum(abs(q['du25']) for q in q_te) / 5
    mae_gap_te = sum(abs(q['gap']) for q in q_te) / 5

    # Baseline: alpha=1.0 (modelo actual) en test
    mu_f_base = lambda r: r['total_mu_raw']
    q_te_base = reportar_quintiles(test, 1.0, mu_f_base)
    mae_du_te_base = sum(abs(q['du25']) for q in q_te_base) / 5
    mae_gap_te_base = sum(abs(q['gap']) for q in q_te_base) / 5

    print(f'\n{"="*95}')
    print(f'  RESULTADO OOS — baseline alpha=1.0 vs fix alpha={best_train_alpha:.2f}')
    print(f'{"="*95}')
    print(f'  En TEST: baseline MAE dU2.5={mae_du_te_base:.1%}  fix MAE dU2.5={mae_du_te:.1%}  '
          f'reduccion={100*(mae_du_te_base-mae_du_te)/mae_du_te_base:.0f}%')
    print(f'           baseline MAE gap  ={mae_gap_te_base:+.3f}  fix MAE gap  ={mae_gap_te:+.3f}  '
          f'reduccion={100*(mae_gap_te_base-mae_gap_te)/mae_gap_te_base:.0f}%')

    print(f'\n  DETALLE en TEST con alpha={best_train_alpha:.2f}:')
    print(f'  {"Q":>2}  {"mu range":<15}  {"N":>4}  {"act":>6}  {"pred":>6}  {"gap":>8}  '
          f'{"U2.5 act":>8}  {"U2.5 pred":>9}  {"dU2.5":>7}')
    for q in q_te:
        rng = f'{q["mu_range"][0]:.2f}-{q["mu_range"][1]:.2f}'
        print(f'  Q{q["q"]}  {rng:<15}  {q["N"]:>4}  {q["act"]:>6.2f}  {q["pred"]:>6.2f}  '
              f'{q["gap"]:>+8.3f}  {q["u25_a"]:>8.1%}  {q["u25_p"]:>9.1%}  {q["du25"]:>+7.1%}')

    # Estabilidad: ¿qué pasa si elegimos alpha vecinos al optimo?
    print(f'\n  Estabilidad: alphas cercanos evaluados en TEST:')
    print(f'  {"alpha":>6}  {"MAE dU2.5":>10}  {"MAE gap":>8}')
    for alpha in [best_train_alpha - 0.10, best_train_alpha - 0.05,
                   best_train_alpha,
                   best_train_alpha + 0.05, best_train_alpha + 0.10]:
        if alpha <= 0 or alpha > 1: continue
        mu_f = lambda r, a=alpha: mu_ajustado(r, a)
        q = reportar_quintiles(test, alpha, mu_f)
        mae_du = sum(abs(x['du25']) for x in q) / 5
        mae_gap = sum(abs(x['gap']) for x in q) / 5
        print(f'  {alpha:>6.2f}  {mae_du:>+10.1%}  {mae_gap:>+8.3f}')


def main():
    import sys as _sys
    OOS = '--oos' in _sys.argv

    rows = load_cache()
    n = len(rows)
    print(f'Cache: {n} partidos')
    print(f'\nGoles actual global: {sum(r["total_g"] for r in rows)/n:.3f}')

    if OOS:
        # Ordenar por fecha (el cache ya está cronológico, pero por las dudas)
        rows.sort(key=lambda r: r.get('fecha', ''))
        split = n // 2
        train = rows[:split]
        test  = rows[split:]
        f0, f1 = train[0]['fecha'], train[-1]['fecha']
        g0, g1 = test[0]['fecha'], test[-1]['fecha']
        print(f'\n[OOS] TRAIN: {len(train)} partidos  {f0} -> {f1}')
        print(f'[OOS] TEST : {len(test)} partidos  {g0} -> {g1}')
        validar_oos(train, test)
        return

    alphas = [1.00, 0.90, 0.80, 0.70, 0.60, 0.50, 0.40, 0.30]

    # Tabla resumen: para cada alpha, mostrar gap en Q1, Q5 y MAE global
    print(f'\n{"="*85}')
    print(f'  RESUMEN: gap y MAE vs quintiles, recorrido de alpha')
    print(f'{"="*85}')
    print(f'  {"alpha":>6}  {"gap Q1":>8}  {"gap Q5":>8}  {"dU2.5 Q1":>10}  {"dU2.5 Q5":>10}  '
          f'{"MAE gap":>9}  {"MAE dU2.5":>10}')
    print('  ' + '-' * 75)
    mejores = []
    for alpha in alphas:
        mu_f = lambda r, a=alpha: mu_ajustado(r, a)
        quin = reportar_quintiles(rows, alpha, mu_f)
        gap_q1 = quin[0]['gap']
        gap_q5 = quin[4]['gap']
        du_q1 = quin[0]['du25']
        du_q5 = quin[4]['du25']
        mae_gap = sum(abs(q['gap']) for q in quin) / 5
        mae_du  = sum(abs(q['du25']) for q in quin) / 5
        marker = '  <--' if abs(mae_du) < 0.02 else ''
        print(f'  {alpha:>6.2f}  {gap_q1:>+8.3f}  {gap_q5:>+8.3f}  '
              f'{du_q1:>+9.1%}  {du_q5:>+9.1%}  '
              f'{mae_gap:>+8.3f}  {mae_du:>+9.1%}{marker}')
        mejores.append((alpha, mae_du))

    # Encontrar el óptimo
    alpha_best = min(mejores, key=lambda x: abs(x[1]))[0]
    print(f'\n  Alpha con menor MAE dU2.5: {alpha_best:.2f}')

    # Detalle del óptimo
    mu_f = lambda r, a=alpha_best: mu_ajustado(r, a)
    print(f'\n{"="*85}')
    print(f'  DETALLE con alpha={alpha_best:.2f}')
    print(f'{"="*85}')
    print(f'  {"Q":>2}  {"mu range":<15}  {"N":>4}  {"act":>6}  {"pred":>6}  {"gap":>8}  '
          f'{"U2.5 act":>8}  {"U2.5 pred":>9}  {"dU2.5":>7}')
    for q in reportar_quintiles(rows, alpha_best, mu_f):
        rng = f'{q["mu_range"][0]:.2f}-{q["mu_range"][1]:.2f}'
        print(f'  Q{q["q"]}  {rng:<15}  {q["N"]:>4}  {q["act"]:>6.2f}  {q["pred"]:>6.2f}  '
              f'{q["gap"]:>+8.3f}  {q["u25_a"]:>8.1%}  {q["u25_p"]:>9.1%}  {q["du25"]:>+7.1%}')

    # Comparar contra baseline alpha=1.0 (modelo actual)
    print(f'\n{"="*85}')
    print(f'  COMPARACIÓN alpha=1.00 (actual) vs alpha={alpha_best:.2f} (fix)')
    print(f'{"="*85}')
    q_base = reportar_quintiles(rows, 1.00, lambda r: r['total_mu_raw'])
    q_fix  = reportar_quintiles(rows, alpha_best, mu_f)
    print(f'  {"Q":>2}  {"gap base":>9}  {"gap fix":>9}  {"dU2.5 base":>11}  {"dU2.5 fix":>10}')
    for qb, qf in zip(q_base, q_fix):
        print(f'  Q{qb["q"]}  {qb["gap"]:>+9.3f}  {qf["gap"]:>+9.3f}  '
              f'{qb["du25"]:>+10.1%}  {qf["du25"]:>+9.1%}')


if __name__ == '__main__':
    main()
