"""
Entrena calibradores isotónicos por (categoria, alcance) usando pronosticos.csv.

Calibra: Goles, Corners, Tiros, Tarjetas, Arco (local/total/visitante cuando aplica).
NO calibra: 1X2, BTTS, Doble Oportunidad (quedan con prob cruda).

Salida: un .pkl por (categoria, alcance) en data/calibrador/.
Cada pickle contiene un dict con {'x': np.array, 'y_fit': np.array}
Para predecir: np.interp(p_new, x, y_fit).
"""
from pathlib import Path
import pickle
import numpy as np
import pandas as pd
from scipy.optimize import isotonic_regression

ROOT = Path(__file__).resolve().parent.parent
PRON_CSV = ROOT / 'data' / 'apuestas' / 'pronosticos.csv'
OUT_DIR  = ROOT / 'data' / 'calibrador'

# Mercados a calibrar — BTTS, 1X2, DO quedan fuera por decisión explícita.
CALIBRAR = [
    ('Goles',    'Local'),
    ('Goles',    'Total'),
    ('Goles',    'Visitante'),
    ('Corners',  'Local'),
    ('Corners',  'Total'),
    ('Corners',  'Visitante'),
    ('Tiros',    'Local'),
    ('Tiros',    'Total'),
    ('Tiros',    'Visitante'),
    ('Tarjetas', 'Total'),
    ('Arco',     'Local'),
    ('Arco',     'Total'),
    ('Arco',     'Visitante'),
]


def _fit_isotonic(p: np.ndarray, y: np.ndarray):
    """
    Fit isotonic regression y ~ p.
    Returns (x_sorted_unique, y_fitted_at_x) para usar con np.interp.
    """
    order = np.argsort(p)
    p_sorted = p[order]
    y_sorted = y[order].astype(float)
    # scipy devuelve los valores ajustados en el orden de entrada
    res = isotonic_regression(y_sorted, increasing=True)
    y_fit = np.asarray(res.x)
    # Colapsar p duplicadas promediando y_fit
    df = pd.DataFrame({'p': p_sorted, 'y': y_fit}).groupby('p', as_index=False)['y'].mean()
    return df['p'].to_numpy(), df['y'].to_numpy()


def _safe_name(cat: str, alc: str) -> str:
    return f'isotonic_{cat}_{alc}.pkl'.replace(' ', '_')


def fit_all(verbose: bool = True) -> dict:
    df = pd.read_csv(PRON_CSV)
    r  = df[df['resultado'].isin(['W', 'L'])].copy()
    r['hit'] = (r['resultado'] == 'W').astype(int)
    # modelo_prob puede venir como string si hay filas viejas — forzar numérico
    r['modelo_prob'] = pd.to_numeric(r['modelo_prob'], errors='coerce')
    r = r.dropna(subset=['modelo_prob'])

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    resumen = {}

    if verbose:
        print(f"Pronósticos resueltos totales: {len(r):,}")
        print(f"Calibradores a entrenar: {len(CALIBRAR)}\n")
        print(f"{'Cat':<10} {'Alc':<11} {'N':>6}  {'MAE pre':>8}  {'MAE post':>9}  {'mejora':>7}")
        print('-' * 60)

    for cat, alc in CALIBRAR:
        sub = r[(r['categoria'] == cat) & (r['alcance'] == alc)]
        if len(sub) < 50:
            if verbose:
                print(f'{cat:<10} {alc:<11} {len(sub):>6}  (saltado — <50 obs)')
            continue
        p = sub['modelo_prob'].to_numpy()
        y = sub['hit'].to_numpy()

        # MAE pre-calibración (modelo vs WR real por bucket de 10 puntos)
        bins = np.linspace(0, 1, 11)
        pb   = np.digitize(p, bins) - 1
        pb   = np.clip(pb, 0, 9)
        mae_pre = 0.0
        mae_post = 0.0
        n_tot = 0
        x_fit, y_fit = _fit_isotonic(p, y)
        p_cal = np.interp(p, x_fit, y_fit)
        for b in range(10):
            mask = pb == b
            if mask.sum() < 5:
                continue
            mae_pre  += abs(y[mask].mean() - p[mask].mean()) * mask.sum()
            mae_post += abs(y[mask].mean() - p_cal[mask].mean()) * mask.sum()
            n_tot += mask.sum()
        if n_tot == 0:
            continue
        mae_pre  /= n_tot
        mae_post /= n_tot

        out_path = OUT_DIR / _safe_name(cat, alc)
        with open(out_path, 'wb') as f:
            pickle.dump({'x': x_fit, 'y_fit': y_fit, 'n_train': len(sub)}, f)

        resumen[(cat, alc)] = (len(sub), mae_pre, mae_post)
        if verbose:
            print(f'{cat:<10} {alc:<11} {len(sub):>6}  {mae_pre*100:>6.2f}pp  {mae_post*100:>7.2f}pp  {(mae_pre-mae_post)*100:>+5.2f}pp')

    if verbose:
        print(f"\nCalibradores guardados en: {OUT_DIR}")
    return resumen


if __name__ == '__main__':
    fit_all()
