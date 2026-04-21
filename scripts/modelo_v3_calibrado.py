"""
Wrapper de modelo_v3 que aplica calibración isotónica a las probabilidades
post-simulación. NO reimplementa nada: sólo transforma el dict de probs.

Uso desde pipeline.py:
    from modelo_v3_calibrado import calibrar_probs
    probs_raw = compute_all_probs(sim)
    probs_cal = calibrar_probs(probs_raw)

Mercados calibrados:  Goles, Corners, Tiros, Tarjetas, Arco (por alcance).
Mercados sin tocar:   1X2, Doble Oportunidad, BTTS, AHcp, score_dist, E_*.

Los calibradores se entrenan con scripts/fit_calibrador.py y viven en
data/calibrador/isotonic_<Cat>_<Alc>.pkl.
"""
from pathlib import Path
import pickle
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
CAL_DIR = ROOT / 'data' / 'calibrador'

# Prefijo de clave → (categoria, alcance).
# Se ordenan de más específico a menos específico (gl_ antes que g_, etc.).
_PREFIX_MAP = [
    ('cards_',  ('Tarjetas', 'Total')),
    ('gl_',     ('Goles',    'Local')),
    ('gv_',     ('Goles',    'Visitante')),
    ('g_',      ('Goles',    'Total')),
    ('cl_',     ('Corners',  'Local')),
    ('cv_',     ('Corners',  'Visitante')),
    ('tc_',     ('Corners',  'Total')),
    ('sl_',     ('Tiros',    'Local')),
    ('sv_',     ('Tiros',    'Visitante')),
    ('ts_',     ('Tiros',    'Total')),
    ('sla_',    ('Arco',     'Local')),
    ('sva_',    ('Arco',     'Visitante')),
    ('ta_',     ('Arco',     'Total')),
]

_calibradores = None


def _load_calibradores() -> dict:
    """Carga todos los pickles de data/calibrador/. Cachea en memoria."""
    global _calibradores
    if _calibradores is not None:
        return _calibradores
    _calibradores = {}
    if not CAL_DIR.exists():
        return _calibradores
    for pkl in CAL_DIR.glob('isotonic_*.pkl'):
        name = pkl.stem.replace('isotonic_', '')
        parts = name.rsplit('_', 1)
        if len(parts) != 2:
            continue
        cat, alc = parts
        with open(pkl, 'rb') as f:
            _calibradores[(cat, alc)] = pickle.load(f)
    return _calibradores


def _cat_alc_from_key(key: str):
    """Devuelve (cat, alc) para una prob_key, o None si no se calibra."""
    for prefix, ca in _PREFIX_MAP:
        if key.startswith(prefix):
            return ca
    return None


def calibrar_prob(p: float, cat: str, alc: str) -> float:
    """Aplica calibrador a una sola prob. Si no hay calibrador, devuelve p."""
    cals = _load_calibradores()
    cal = cals.get((cat, alc))
    if cal is None:
        return p
    # Clipear para que interpolación no explote en los bordes
    p_clip = min(max(p, 0.0), 1.0)
    return float(np.interp(p_clip, cal['x'], cal['y_fit']))


def calibrar_probs(probs: dict) -> dict:
    """
    Devuelve un dict nuevo donde las claves calibrables están transformadas
    y el resto queda igual (1X2, BTTS, DO, AHcp, score_dist, E_*).
    """
    cals = _load_calibradores()
    out = {}
    for k, v in probs.items():
        if not isinstance(v, (int, float)):
            # score_dist es un dict, E_* son números pero los preservo igual
            out[k] = v
            continue
        ca = _cat_alc_from_key(k)
        if ca is None or ca not in cals:
            out[k] = v
        else:
            out[k] = calibrar_prob(v, ca[0], ca[1])
    return out


def calibradores_disponibles() -> list:
    """Lista los (cat, alc) con calibrador cargado. Útil para diagnóstico."""
    return sorted(_load_calibradores().keys())


if __name__ == '__main__':
    cals = _load_calibradores()
    print(f'Calibradores cargados: {len(cals)}')
    for (cat, alc), cal in sorted(cals.items()):
        print(f'  {cat:<10} {alc:<12}  n_train={cal["n_train"]:>5}  breakpoints={len(cal["x"]):>4}')
