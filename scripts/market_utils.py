"""
market_utils.py — helpers para mapear strings de mercado/lado a claves de odds/probs.

La clave devuelta es la misma que usan:
  - el dict ODDS de fetch_odds.py
  - el dict de probs de analizar_partido.compute_all_probs()

Copia extraida de backtest_v3.py para centralizar y poder reusar desde
snapshot_cierre.py y otros scripts futuros. Las copias locales en
backtest_v3.py, calibracion_full_negbin.py, etc. se conservan sin tocar
para evitar churn.
"""

import re


def mercado_to_odds_key(mercado: str, lado: str,
                         team_local: str, team_visita: str) -> str | None:
    """
    Convierte (mercado, lado) a la clave interna de odds/probs.
    Retorna None si no se puede mapear.
    """
    m = mercado.strip()
    l = lado.strip()
    is_over = l in ('Over/Si', 'Si', 'Over')

    def thr():
        match = re.search(r'O/U\s+([\d.]+)', m)
        return float(match.group(1)) if match else None

    if m.startswith('1X2'):
        if 'Empate' in m:    return 'X'
        if team_visita in m: return '2'
        if team_local  in m: return '1'
        return None

    if m.startswith('DC'):
        if '(1X)' in m: return 'dc_1x'
        if '(12)' in m: return 'dc_12'
        if '(X2)' in m: return 'dc_x2'
        return None

    if m.upper().startswith('BTTS'):
        return 'btts_si' if is_over else 'btts_no'

    if m.startswith('Goles'):
        t = thr()
        if t is None: return None
        side = 'over' if is_over else 'under'
        if 'tot.' in m:           return f'g_{side}_{t}'
        if team_local in m:       return f'gl_{side}_{t}'
        if team_visita in m:      return f'gv_{side}_{t}'
        return None

    if m.startswith('Tiros'):
        t = thr()
        if t is None: return None
        side = 'over' if is_over else 'under'
        if 'tot.' in m:           return f'ts_{side}_{t}'
        if team_local in m:       return f'sl_{side}_{t}'
        if team_visita in m:      return f'sv_{side}_{t}'
        return None

    if m.startswith('Arco'):
        t = thr()
        if t is None: return None
        side = 'over' if is_over else 'under'
        if 'tot.' in m:           return f'ta_{side}_{t}'
        if team_local in m:       return f'sla_{side}_{t}'
        if team_visita in m:      return f'sva_{side}_{t}'
        return None

    if m.startswith('Corners'):
        t = thr()
        if t is None: return None
        side = 'over' if is_over else 'under'
        if 'tot.' in m:           return f'tc_{side}_{t}'
        if team_local in m:       return f'cl_{side}_{t}'
        if team_visita in m:      return f'cv_{side}_{t}'
        return None

    if m.startswith('Tarjetas'):
        t = thr()
        if t is None: return None
        side = 'over' if is_over else 'under'
        if 'tot.' in m:           return f'cards_{side}_{t}'
        return None

    return None
