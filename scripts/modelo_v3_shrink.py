"""
modelo_v3_shrink.py
-------------------
v3.4-shrink: extiende modelo_v3.py aplicando shrinkage adicional a los ratings
atk/def hacia 1.0, para mitigar regresión a la media en los extremos.

Descubierto en walk-forward OOS (2026-04-21, script analisis_calidad_partido.py
+ fix_regresion_media.py): el modelo v3.2 infla mu_goles en los extremos.
  - Q1 (mu predicho < 2.2):  goles real 2.29 vs pred 1.87  gap +0.43  (+23%)
  - Q5 (mu predicho > 3.25): goles real 3.10 vs pred 3.77  gap -0.68  (-18%)
  Validación OOS con split 50/50 cronológico: alpha=0.30 reduce MAE dU2.5
  de 8.5% a 1.6% (81%). Óptimo chato en [0.20, 0.35].

Fórmula:
    atk_new = 1 + alpha * (atk_raw - 1)
    def_new = 1 + alpha * (def_raw - 1)
    lambda_local_new = factor_L × atk_local_new × def_vis_new
    lambda_vis_new   = factor_V × atk_vis_new   × def_local_new
    donde factor = lambda_raw / (atk_raw × def_raw)   (= liga_avg × forma)

Solo toca las medias de GOLES. No modifica corners/tiros/tarjetas/arco porque
el análisis OOS solo validó goles. Extender a otros mercados requiere nueva
validación.

Uso (desde pipeline.py):
    from modelo_v3_shrink import apply_shrink_to_ratings
    params_shr = apply_shrink_to_ratings(params, alpha=0.30)
    sim        = run_simulation(params_shr, N_SIM)
"""


def apply_shrink_to_ratings(params: dict, alpha: float = 0.30, verbose: bool = False) -> dict:
    """
    Devuelve un dict nuevo con lambda_local/lambda_vis ajustados por shrinkage
    de los ratings atk/def hacia 1.0.

    Parameters
    ----------
    params : dict
        Output de compute_match_params() de modelo_v3. Se espera '_ratings' con
        atk_local, atk_visita, def_local, def_visita.
    alpha : float
        Intensidad del shrinkage.
          alpha=1.0 → sin efecto (modelo v3.2 tal cual)
          alpha=0.30 → valor validado OOS
          alpha=0.0 → ratings colapsan a 1.0 (solo usa liga_avg × forma)
    verbose : bool
        Imprime el lambda antes/después.

    Returns
    -------
    dict
        Copia de params con lambda_local/lambda_vis ajustados. El resto de
        campos (mu_corners, mu_shots, mu_tarjetas, etc) queda sin tocar.
    """
    ratings = params.get('_ratings')
    if ratings is None:
        if verbose:
            print('  [shrink] params sin _ratings → sin ajuste')
        return params

    atk_L = ratings.get('atk_local')
    atk_V = ratings.get('atk_visita')
    def_L = ratings.get('def_local')
    def_V = ratings.get('def_visita')

    if None in (atk_L, atk_V, def_L, def_V):
        if verbose:
            print('  [shrink] ratings incompletos → sin ajuste')
        return params

    if alpha >= 1.0:
        if verbose:
            print(f'  [shrink] alpha={alpha} → sin efecto')
        return params

    # Ratings shrinkados hacia 1.0
    atk_L_new = 1 + alpha * (atk_L - 1)
    atk_V_new = 1 + alpha * (atk_V - 1)
    def_L_new = 1 + alpha * (def_L - 1)
    def_V_new = 1 + alpha * (def_V - 1)

    # Factor que no incluye los ratings (= liga_avg × forma × otros)
    lam_L_old = params['lambda_local']
    lam_V_old = params['lambda_vis']
    denom_L = atk_L * def_V
    denom_V = atk_V * def_L
    if denom_L <= 0 or denom_V <= 0:
        return params

    factor_L = lam_L_old / denom_L
    factor_V = lam_V_old / denom_V

    lam_L_new = factor_L * atk_L_new * def_V_new
    lam_V_new = factor_V * atk_V_new * def_L_new

    out = dict(params)
    out['lambda_local'] = lam_L_new
    out['lambda_vis']   = lam_V_new
    out['_shrink_alpha'] = alpha
    out['_shrink_lambda_old'] = (lam_L_old, lam_V_old)

    if verbose:
        tot_old = lam_L_old + lam_V_old
        tot_new = lam_L_new + lam_V_new
        print(f'  [shrink alpha={alpha}]  mu: {lam_L_old:.2f}+{lam_V_old:.2f}={tot_old:.2f}  '
              f'→ {lam_L_new:.2f}+{lam_V_new:.2f}={tot_new:.2f}')

    return out


if __name__ == '__main__':
    # Demo
    mock_params = {
        'lambda_local': 0.80,   # mu_L bajo (equipo débil visitando a uno mejor)
        'lambda_vis':   1.20,
        '_ratings': {
            'atk_local':  0.65, 'def_visita': 1.00,
            'atk_visita': 0.90, 'def_local':  1.30,
            'forma_atk_local': 1.0, 'forma_atk_visita': 1.0,
        }
    }
    print('Escenario: equipo débil local (atk=0.65) vs equipo medio visitante')
    print(f'  v3.2 raw:   mu_L={mock_params["lambda_local"]:.2f}  '
          f'mu_V={mock_params["lambda_vis"]:.2f}  '
          f'total={mock_params["lambda_local"]+mock_params["lambda_vis"]:.2f}')
    for alpha in [0.90, 0.70, 0.50, 0.30, 0.10]:
        p = apply_shrink_to_ratings(mock_params, alpha=alpha)
        tot = p['lambda_local'] + p['lambda_vis']
        print(f'  alpha={alpha:.2f}: mu_L={p["lambda_local"]:.2f}  mu_V={p["lambda_vis"]:.2f}  total={tot:.2f}')
