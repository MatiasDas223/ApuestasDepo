"""
modelo_v3_ref.py
----------------
v3.3-ref: extiende modelo_v3.py aplicando un multiplicador por árbitro
sobre las medias de tarjetas (mu_tarjetas_local, mu_tarjetas_vis).

Estructura:
    mu_tarjetas_local *= ref_factor^alpha
    mu_tarjetas_vis   *= ref_factor^alpha

donde:
  ref_factor = factor shrinkado del árbitro (1.0 = neutro vs su liga)
  alpha      = control de intensidad
                 alpha=0    -> sin efecto  (igual que v3.2)
                 alpha=0.5  -> half-effect (factor 1.20 -> 1.10)
                 alpha=1.0  -> full effect

Uso:
    from modelo_v3 import compute_match_params, run_simulation
    from modelo_v3_ref import apply_referee_factor

    params = compute_match_params(local, visita, hist, liga)
    params = apply_referee_factor(params, referee='M. Oliver', alpha=0.5)
    sim    = run_simulation(params, n=100_000)
"""
from referee_ratings import load_ratings, get_factor

# Cache lazy del CSV de ratings — evita releer en cada llamada del backtest
_RATINGS_CACHE = None


def _get_ratings():
    global _RATINGS_CACHE
    if _RATINGS_CACHE is None:
        _RATINGS_CACHE = load_ratings()
    return _RATINGS_CACHE


def reload_ratings():
    """Forzar relectura del CSV (útil después de regenerar ratings)."""
    global _RATINGS_CACHE
    _RATINGS_CACHE = None


def apply_referee_factor(params, referee=None, alpha=0.5, ratings=None,
                         verbose=False):
    """
    Mutaciona y devuelve `params` aplicando el factor del árbitro a las medias
    de tarjetas. Si referee es None, vacío o desconocido → no hace nada
    (factor=1.0 efectivo).

    Parameters
    ----------
    params : dict
        Output de compute_match_params() de modelo_v3.
    referee : str | None
        Nombre del árbitro tal como viene de API Football (o partidos_historicos).
    alpha : float
        Intensidad del ajuste. Default 0.5.
    ratings : dict | None
        Si se pasa, usa ese dict en vez del CSV. Útil en backtest leave-one-out.
    verbose : bool
        Imprime el factor aplicado.
    """
    if not referee:
        if verbose:
            print('  [ref] sin árbitro asignado → sin ajuste')
        return params

    ratings_dict = ratings if ratings is not None else _get_ratings()
    factor = get_factor(referee, ratings_dict, alpha=alpha, default=1.0)

    if factor == 1.0:
        if verbose:
            print(f'  [ref] {referee!r} no en ratings → sin ajuste')
        return params

    mu_tl_old = params['mu_tarjetas_local']
    mu_tv_old = params['mu_tarjetas_vis']
    params['mu_tarjetas_local'] = mu_tl_old * factor
    params['mu_tarjetas_vis']   = mu_tv_old * factor

    # Anotar para diagnóstico
    params['_ref_factor'] = factor
    params['_ref_alpha']  = alpha
    params['_referee']    = referee

    if verbose:
        ref_meta = ratings_dict.get(referee, {})
        n = ref_meta.get('n', '?')
        print(f'  [ref] {referee} (n={n})  factor^α={factor:.3f}  '
              f'mu_tarjetas: {mu_tl_old:.2f}+{mu_tv_old:.2f} '
              f'-> {params["mu_tarjetas_local"]:.2f}+{params["mu_tarjetas_vis"]:.2f}')

    return params


# ─────────────────────────────────────────────────────────────────────────────
# CLI demo
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')

    ratings = _get_ratings()
    print(f'  Ratings cargados: {len(ratings)} árbitros')

    # Demo: comparar mu efectivo para varios árbitros con un mu base de 2.5+2.5
    base = {'mu_tarjetas_local': 2.5, 'mu_tarjetas_vis': 2.5}
    test_refs = ['Michael Oliver, England', 'Nicolas Ramirez, Argentina',
                 'NoExisteEsteRef', 'F. Brych']

    print('\n  Demo (mu base = 2.5+2.5 = 5.0 esperado):')
    for alpha in (0.0, 0.3, 0.5, 1.0):
        print(f'\n  ─── alpha={alpha} ───')
        for ref in test_refs:
            p = dict(base)
            apply_referee_factor(p, ref, alpha=alpha, verbose=False)
            total = p['mu_tarjetas_local'] + p['mu_tarjetas_vis']
            f = p.get('_ref_factor', 1.0)
            print(f'    {ref:<35}  factor={f:.3f}  total_mu={total:.2f}')
