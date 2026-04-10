"""
Descarga las odds de Bet365 (o cualquier bookmaker) desde API Football
para un fixture dado y las mapea al formato interno de analizar_partido.py.

Uso standalone:
    python fetch_odds.py 1492015           # fixture_id
    python fetch_odds.py 1492015 --bk 4   # otro bookmaker (default: 8 = Bet365)

Uso como modulo:
    from fetch_odds import get_odds
    odds, resumen = get_odds(fixture_id=1492015)
"""

import json
import sys
import time
import urllib.request
import urllib.parse
from pathlib import Path

API_KEY    = '5a7d5d038454c3640c8771ce2274c18c'
BASE_URL   = 'https://v3.football.api-sports.io'
ODDS_DIR   = Path(r'C:\Users\Matt\Apuestas Deportivas\data\odds')
BK_DEFAULT = 8   # Bet365

ODDS_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def api_get(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    if params:
        url += '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={'x-apisports-key': API_KEY})
    with urllib.request.urlopen(req, timeout=15) as r:
        data = json.loads(r.read())
    if data.get('errors'):
        raise RuntimeError(f"API error: {data['errors']}")
    return data.get('response', [])


def parse_threshold(value_str):
    """Extrae el número float de strings como 'Over 2.5', 'Under 1.5'."""
    parts = value_str.strip().split()
    if len(parts) >= 2:
        try:
            return float(parts[-1])
        except ValueError:
            pass
    return None


def is_over(value_str):
    return value_str.strip().lower().startswith('over')


# ─────────────────────────────────────────────────────────────────────────────
# Mapeo de mercados API → claves internas
# ─────────────────────────────────────────────────────────────────────────────

def parse_bets(bets_list):
    """
    Convierte la lista de mercados de la API al dict de ODDS interno.
    Retorna (odds_dict, resumen_dict) donde resumen_dict muestra
    qué mercados se mapearon y cuáles no están disponibles.
    """
    by_id = {b['id']: b for b in bets_list}
    odds  = {}
    resumen = {'mapeados': [], 'no_disponibles': [], 'parciales': []}

    def add(key, val):
        odds[key] = float(val)

    def add_ou(values, prefix_over, prefix_under):
        """Agrega pares over/under con threshold al dict."""
        added = 0
        for v in values:
            thr = parse_threshold(v['value'])
            if thr is None:
                continue
            key = f"{prefix_over}_{thr}" if is_over(v['value']) else f"{prefix_under}_{thr}"
            add(key, v['odd'])
            added += 1
        return added

    # ── 1X2 ──────────────────────────────────────────────────────────────────
    if 1 in by_id:
        for v in by_id[1]['values']:
            lbl = v['value']
            if   lbl == 'Home': add('1', v['odd'])
            elif lbl == 'Draw': add('X', v['odd'])
            elif lbl == 'Away': add('2', v['odd'])
        resumen['mapeados'].append('1X2')

    # ── Double Chance ─────────────────────────────────────────────────────────
    if 12 in by_id:
        for v in by_id[12]['values']:
            lbl = v['value']
            if   lbl == 'Home/Draw': add('dc_1x', v['odd'])
            elif lbl == 'Home/Away': add('dc_12', v['odd'])
            elif lbl == 'Draw/Away': add('dc_x2', v['odd'])
        resumen['mapeados'].append('Double Chance')

    # ── Asian Handicap ────────────────────────────────────────────────────────
    if 4 in by_id:
        for v in by_id[4]['values']:
            lbl = v['value']   # e.g. "Home -0.5", "Away +1.0"
            parts = lbl.split()
            if len(parts) == 2:
                side, hcp_str = parts
                try:
                    hcp = float(hcp_str)
                    if side == 'Home':
                        add(f'ahcp_{hcp:+.2f}', v['odd'])
                    else:
                        add(f'ahcp_away_{hcp:+.2f}', v['odd'])
                except ValueError:
                    pass
        resumen['mapeados'].append('Asian Handicap')

    # ── Goles totales ─────────────────────────────────────────────────────────
    if 5 in by_id:
        n = add_ou(by_id[5]['values'], 'g_over', 'g_under')
        resumen['mapeados'].append(f'Goles totales ({n//2} lineas)')

    # ── Goles primer tiempo ───────────────────────────────────────────────────
    if 6 in by_id:
        n = add_ou(by_id[6]['values'], 'g1h_over', 'g1h_under')
        resumen['mapeados'].append(f'Goles 1T ({n//2} lineas)')

    # ── BTTS ─────────────────────────────────────────────────────────────────
    if 8 in by_id:
        for v in by_id[8]['values']:
            if   v['value'] == 'Yes': add('btts_si', v['odd'])
            elif v['value'] == 'No':  add('btts_no', v['odd'])
        resumen['mapeados'].append('BTTS')

    # ── Goles local ───────────────────────────────────────────────────────────
    if 16 in by_id:
        n = add_ou(by_id[16]['values'], 'gl_over', 'gl_under')
        resumen['mapeados'].append(f'Goles local ({n//2} lineas)')

    # ── Goles visitante ───────────────────────────────────────────────────────
    if 17 in by_id:
        n = add_ou(by_id[17]['values'], 'gv_over', 'gv_under')
        resumen['mapeados'].append(f'Goles visitante ({n//2} lineas)')

    # ── Corners totales ───────────────────────────────────────────────────────
    if 45 in by_id:
        n = add_ou(by_id[45]['values'], 'tc_over', 'tc_under')
        if n > 0:
            resumen['mapeados'].append(f'Corners totales ({n//2} lineas)')
        else:
            resumen['no_disponibles'].append('Corners totales (sin threshold)')

    # ── Corners local ─────────────────────────────────────────────────────────
    if 57 in by_id:
        n = add_ou(by_id[57]['values'], 'cl_over', 'cl_under')
        if n > 0:
            resumen['mapeados'].append(f'Corners local ({n//2} lineas)')
        else:
            resumen['no_disponibles'].append('Corners local (sin threshold)')

    # ── Corners visitante ─────────────────────────────────────────────────────
    if 58 in by_id:
        n = add_ou(by_id[58]['values'], 'cv_over', 'cv_under')
        if n > 0:
            resumen['mapeados'].append(f'Corners visitante ({n//2} lineas)')
        else:
            resumen['no_disponibles'].append('Corners visitante (sin threshold)')

    # ── Tiros totales [211] ───────────────────────────────────────────────────
    # Bet365 vía API solo devuelve "Over/Under" sin número para Liga Profesional
    if 211 in by_id:
        n = add_ou(by_id[211]['values'], 'ts_over', 'ts_under')
        if n > 0:
            resumen['mapeados'].append(f'Tiros totales ({n//2} lineas)')
        else:
            resumen['parciales'].append(
                'Tiros totales: API no devuelve threshold — completar manualmente '
                '(ts_over_17.5, ts_over_19.5, ts_over_21.5...)')

    # ── Remates al arco totales [87] ─────────────────────────────────────────
    if 87 in by_id:
        n = add_ou(by_id[87]['values'], 'ta_over', 'ta_under')
        if n > 0:
            resumen['mapeados'].append(f'Arco totales ({n//2} lineas)')
            resumen['parciales'].append(
                'Arco local/visita: no disponible en API — completar manualmente '
                '(sla_over_X.5, sva_over_X.5)')
        else:
            resumen['no_disponibles'].append('Arco totales (sin threshold)')

    # ── Tiros totales local/visita — NO disponible a nivel equipo en API ──────
    resumen['no_disponibles'].append(
        'Tiros local O/U (sl_over/under): Bet365 API no ofrece por equipo — completar manualmente')
    resumen['no_disponibles'].append(
        'Tiros visita O/U (sv_over/under): Bet365 API no ofrece por equipo — completar manualmente')

    # ── Tarjetas totales [80] ─────────────────────────────────────────────────
    if 80 in by_id:
        n = add_ou(by_id[80]['values'], 'cards_over', 'cards_under')
        if n > 0:
            resumen['mapeados'].append(f'Tarjetas totales ({n//2} lineas)')
            resumen['parciales'].append(
                'Tarjetas local/visita: no disponible en API — completar manualmente')
        else:
            resumen['no_disponibles'].append('Tarjetas totales (sin threshold)')

    # ── Win to Nil ────────────────────────────────────────────────────────────
    if 36 in by_id:
        for v in by_id[36]['values']:
            if   v['value'] == 'Home': add('wtn_home', v['odd'])
            elif v['value'] == 'Away': add('wtn_away', v['odd'])
        resumen['mapeados'].append('Win to Nil')

    # ── Clean Sheet ───────────────────────────────────────────────────────────
    if 27 in by_id:
        for v in by_id[27]['values']:
            if   v['value'] == 'Yes': add('cs_home_si', v['odd'])
            elif v['value'] == 'No':  add('cs_home_no', v['odd'])
        resumen['mapeados'].append('Clean Sheet local')
    if 28 in by_id:
        for v in by_id[28]['values']:
            if   v['value'] == 'Yes': add('cs_away_si', v['odd'])
            elif v['value'] == 'No':  add('cs_away_no', v['odd'])
        resumen['mapeados'].append('Clean Sheet visitante')

    # ── Shots 1x2 / ShotOnTarget 1x2 ─────────────────────────────────────────
    for bid, prefix in [(340, 'shots_1x2'), (176, 'arco_1x2')]:
        if bid in by_id:
            for v in by_id[bid]['values']:
                lbl = v['value']
                if   lbl == 'Home': add(f'{prefix}_home', v['odd'])
                elif lbl == 'Draw': add(f'{prefix}_draw', v['odd'])
                elif lbl == 'Away': add(f'{prefix}_away', v['odd'])
            resumen['mapeados'].append(
                'Tiros 1x2 (quien tira mas)' if bid == 340 else 'Arco 1x2 (quien remata mas al arco)')

    return odds, resumen


# ─────────────────────────────────────────────────────────────────────────────
# Función principal
# ─────────────────────────────────────────────────────────────────────────────

def get_odds(fixture_id, bookmaker_id=BK_DEFAULT, force=False):
    """
    Descarga y parsea odds para un fixture.
    Cachea en data/odds/{fixture_id}.json.
    Retorna (odds_dict, resumen_dict).
    """
    cache_path = ODDS_DIR / f"{fixture_id}.json"

    if cache_path.exists() and not force:
        with open(cache_path, encoding='utf-8') as f:
            cached = json.load(f)
        return cached['odds'], cached['resumen']

    print(f"  Descargando odds  fixture={fixture_id}  bookmaker={bookmaker_id}...")
    resp = api_get('odds', {'fixture': fixture_id, 'bookmaker': bookmaker_id})
    time.sleep(0.3)

    if not resp or not resp[0].get('bookmakers'):
        print(f"  Sin odds disponibles para fixture {fixture_id}")
        return {}, {}

    bets = resp[0]['bookmakers'][0]['bets']
    odds, resumen = parse_bets(bets)

    # Guardar cache
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump({'fixture_id': fixture_id, 'bookmaker_id': bookmaker_id,
                   'odds': odds, 'resumen': resumen}, f, indent=2)

    return odds, resumen


def print_odds_report(odds, resumen, team_local, team_visita):
    """Imprime un reporte legible de las odds obtenidas."""
    sep = '=' * 62
    print(f"\n{sep}")
    print(f"  ODDS BET365 — {team_local} vs {team_visita}")
    print(sep)

    # Mercados mapeados
    print(f"\n  DISPONIBLES ({len(resumen.get('mapeados', []))} mercados):")
    for m in resumen.get('mapeados', []):
        print(f"    + {m}")

    # Parciales (disponibles pero incompletos)
    if resumen.get('parciales'):
        print(f"\n  INCOMPLETOS — completar manualmente:")
        for m in resumen.get('parciales', []):
            print(f"    ~ {m}")

    # No disponibles
    if resumen.get('no_disponibles'):
        print(f"\n  NO DISPONIBLES en API:")
        for m in resumen.get('no_disponibles', []):
            print(f"    - {m}")

    # Odds principales
    print(f"\n  ODDS PRINCIPALES:")
    for key, label in [
        ('1',    f'  {team_local} gana'),
        ('X',    '  Empate'),
        ('2',    f'  {team_visita} gana'),
        ('btts_si', '  BTTS Si'),
        ('btts_no', '  BTTS No'),
        ('g_over_1.5',  '  Goles Over 1.5'),
        ('g_under_1.5', '  Goles Under 1.5'),
        ('g_over_2.5',  '  Goles Over 2.5'),
        ('g_under_2.5', '  Goles Under 2.5'),
        ('tc_over_8.5',  '  Corners Over 8.5'),
        ('tc_under_8.5', '  Corners Under 8.5'),
        ('ta_over_7.5',  '  Arco Over 7.5'),
        ('ta_under_7.5', '  Arco Under 7.5'),
        ('cards_over_5.5',  '  Tarjetas Over 5.5'),
        ('cards_under_5.5', '  Tarjetas Under 5.5'),
    ]:
        if key in odds:
            print(f"    {label:<28} {odds[key]}")

    print()


def build_odds_dict_str(odds, team_local, team_visita):
    """
    Construye el bloque ODDS como string Python listo para escribir en analizar_partido.py.
    Usa el valor real de la API donde está disponible, None donde no.
    """
    def v(key):
        val = odds.get(key)
        if val is None:
            return 'None'
        return str(val)

    def pair(k_over, k_under):
        return f"    '{k_over}': {v(k_over)},  '{k_under}': {v(k_under)},"

    L = team_local
    V = team_visita
    lines = ["ODDS = {"]

    lines += [
        "    # 1X2",
        f"    '1': {v('1')},  'X': {v('X')},  '2': {v('2')},",
        "",
        "    # BTTS",
        f"    'btts_si': {v('btts_si')},  'btts_no': {v('btts_no')},",
        "",
        "    # Goles totales",
    ]
    for thr in [0.5, 1.5, 2.5, 3.5, 4.5]:
        lines.append(pair(f'g_over_{thr}', f'g_under_{thr}'))

    lines += ["", f"    # Goles {L} (local)"]
    for thr in [0.5, 1.5, 2.5, 3.5]:
        lines.append(pair(f'gl_over_{thr}', f'gl_under_{thr}'))

    lines += ["", f"    # Goles {V} (visita)"]
    for thr in [0.5, 1.5, 2.5, 3.5]:
        lines.append(pair(f'gv_over_{thr}', f'gv_under_{thr}'))

    lines += ["", "    # Corners totales"]
    for thr in [7.5, 8.5, 9.5, 10.5, 11.5]:
        lines.append(pair(f'tc_over_{thr}', f'tc_under_{thr}'))

    lines += ["", f"    # Corners {L} (local)"]
    for thr in [3.5, 4.5, 5.5, 6.5]:
        lines.append(pair(f'cl_over_{thr}', f'cl_under_{thr}'))

    lines += ["", f"    # Corners {V} (visita)"]
    for thr in [2.5, 3.5, 4.5, 5.5]:
        lines.append(pair(f'cv_over_{thr}', f'cv_under_{thr}'))

    lines += ["", "    # Tiros totales — completar manualmente desde bookie"]
    for thr in [15.5, 17.5, 19.5, 21.5, 23.5, 25.5, 27.5]:
        lines.append(pair(f'ts_over_{thr}', f'ts_under_{thr}'))

    lines += ["", f"    # Tiros {L} (local) — completar manualmente"]
    for thr in [6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5]:
        lines.append(pair(f'sl_over_{thr}', f'sl_under_{thr}'))

    lines += ["", f"    # Tiros {V} (visita) — completar manualmente"]
    for thr in [5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5]:
        lines.append(pair(f'sv_over_{thr}', f'sv_under_{thr}'))

    lines += ["", "    # Remates al arco totales"]
    for thr in [4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5]:
        lines.append(pair(f'ta_over_{thr}', f'ta_under_{thr}'))

    lines += ["", f"    # Remates al arco {L} (local) — completar manualmente"]
    for thr in [1.5, 2.5, 3.5, 4.5, 5.5, 6.5]:
        lines.append(pair(f'sla_over_{thr}', f'sla_under_{thr}'))

    lines += ["", f"    # Remates al arco {V} (visita) — completar manualmente"]
    for thr in [1.5, 2.5, 3.5, 4.5, 5.5, 6.5]:
        lines.append(pair(f'sva_over_{thr}', f'sva_under_{thr}'))

    lines += ["", "    # Tarjetas totales"]
    for thr in [3.5, 4.5, 5.5, 6.5]:
        lines.append(pair(f'cards_over_{thr}', f'cards_under_{thr}'))

    lines.append("}")
    return "\n".join(lines)


def print_odds_dict(odds, team_local, team_visita):
    """Imprime el dict ODDS listo para pegar en analizar_partido.py."""
    print("\n# ── Pegar en analizar_partido.py ─────────────────────────────")
    print(f"TEAM_LOCAL  = '{team_local}'")
    print(f"TEAM_VISITA = '{team_visita}'")
    print()
    print("ODDS = {")

    # Agrupar por categoría
    grupos = [
        ('# 1X2',         ['1', 'X', '2']),
        ('# Double Chance', ['dc_1x', 'dc_12', 'dc_x2']),
        ('# Win to Nil / Clean Sheet',
         ['wtn_home', 'wtn_away', 'cs_home_si', 'cs_home_no', 'cs_away_si', 'cs_away_no']),
        ('# BTTS',        ['btts_si', 'btts_no']),
        ('# Goles totales',
         [f'g_over_{t}' for t in [0.5,1.5,2.5,3.5,4.5,5.5]] +
         [f'g_under_{t}' for t in [0.5,1.5,2.5,3.5,4.5,5.5]]),
        ('# Goles 1er tiempo',
         [f'g1h_over_{t}' for t in [0.5,1.5,2.5]] +
         [f'g1h_under_{t}' for t in [0.5,1.5,2.5]]),
        (f'# Goles {team_local}',
         [f'gl_over_{t}' for t in [0.5,1.5,2.5,3.5]] +
         [f'gl_under_{t}' for t in [0.5,1.5,2.5,3.5]]),
        (f'# Goles {team_visita}',
         [f'gv_over_{t}' for t in [0.5,1.5,2.5,3.5]] +
         [f'gv_under_{t}' for t in [0.5,1.5,2.5,3.5]]),
        ('# Asian Handicap (local)',
         [k for k in sorted(odds) if k.startswith('ahcp_') and 'away' not in k]),
        ('# Corners',
         [f'tc_over_{t}' for t in [7.5,8.5,9.5,10.5,11.5]] +
         [f'tc_under_{t}' for t in [7.5,8.5,9.5,10.5,11.5]] +
         [f'cl_over_{t}' for t in [3.5,4.5,5.5,6.5]] +
         [f'cl_under_{t}' for t in [3.5,4.5,5.5,6.5]] +
         [f'cv_over_{t}' for t in [2.5,3.5,4.5,5.5]] +
         [f'cv_under_{t}' for t in [2.5,3.5,4.5,5.5]]),
        ('# Tiros totales — COMPLETAR MANUALMENTE desde bookie',
         [f'ts_over_{t}' for t in [15.5,17.5,19.5,21.5,23.5,25.5,27.5]] +
         [f'ts_under_{t}' for t in [15.5,17.5,19.5,21.5,23.5,25.5,27.5]]),
        (f'# Tiros {team_local} — COMPLETAR MANUALMENTE',
         [f'sl_over_{t}' for t in [6.5,7.5,8.5,9.5,10.5,11.5,12.5,13.5]] +
         [f'sl_under_{t}' for t in [6.5,7.5,8.5,9.5,10.5,11.5,12.5,13.5]]),
        (f'# Tiros {team_visita} — COMPLETAR MANUALMENTE',
         [f'sv_over_{t}' for t in [5.5,6.5,7.5,8.5,9.5,10.5,11.5,12.5]] +
         [f'sv_under_{t}' for t in [5.5,6.5,7.5,8.5,9.5,10.5,11.5,12.5]]),
        ('# Arco totales',
         [f'ta_over_{t}' for t in [3.5,4.5,5.5,6.5,7.5,8.5,9.5,10.5,11.5]] +
         [f'ta_under_{t}' for t in [3.5,4.5,5.5,6.5,7.5,8.5,9.5,10.5,11.5]]),
        (f'# Arco {team_local} — COMPLETAR MANUALMENTE',
         [f'sla_over_{t}' for t in [1.5,2.5,3.5,4.5,5.5,6.5]] +
         [f'sla_under_{t}' for t in [1.5,2.5,3.5,4.5,5.5,6.5]]),
        (f'# Arco {team_visita} — COMPLETAR MANUALMENTE',
         [f'sva_over_{t}' for t in [1.5,2.5,3.5,4.5,5.5,6.5]] +
         [f'sva_under_{t}' for t in [1.5,2.5,3.5,4.5,5.5,6.5]]),
        ('# Tarjetas — COMPLETAR MANUALMENTE local/visita',
         [f'cards_over_{t}' for t in [3.5,4.5,5.5,6.5]] +
         [f'cards_under_{t}' for t in [3.5,4.5,5.5,6.5]]),
    ]

    for comentario, keys in grupos:
        printed_any = False
        for k in keys:
            val = odds.get(k)
            comment = ''
            if val is None and 'COMPLETAR' in comentario:
                comment = '  # TODO'
                val = 'None'
            elif val is None:
                continue
            if not printed_any:
                print(f"    {comentario}")
                printed_any = True
            print(f"    '{k}': {val},{comment}")

    print("}")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    args = sys.argv[1:]
    if not args:
        print("Uso: python fetch_odds.py <fixture_id> [--bk <bookmaker_id>] [--force]")
        sys.exit(1)

    fixture_id = int(args[0])
    bk_id      = BK_DEFAULT
    force      = '--force' in args
    if '--bk' in args:
        bk_id = int(args[args.index('--bk') + 1])

    odds, resumen = get_odds(fixture_id, bk_id, force=force)
    print_odds_report(odds, resumen, 'Local', 'Visitante')
    print_odds_dict(odds, 'Local', 'Visitante')
