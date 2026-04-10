"""
Análisis de partido con cuotas reales del bookmaker.
Uso: editar TEAM_LOCAL, TEAM_VISITA, COMPETITION y ODDS al final del archivo.
"""

import sys
sys.path.insert(0, r'C:\Users\Matt\Apuestas Deportivas\scripts')

from modelo_v2 import (load_csv, compute_match_params, run_simulation,
                       _remove_vig, MIN_EDGE, poisson_sample, norm as norm_text,
                       MIN_MATCHES, load_teams_db, resolve_team_id)


# ─────────────────────────────────────────────────────────────────────────────
# Parámetros para remates al arco (tiros_arco)
# ─────────────────────────────────────────────────────────────────────────────

def compute_arco_params(team_local, team_visita, rows, competition=None):
    """
    Estima mu_arco_local y mu_arco_vis mediante ratings multiplicativos
    sobre tiros_arco_local / tiros_arco_visitante del CSV histórico.

    Modelo:
        mu_local = la_arco_home * atk_local * def_vis
        mu_vis   = la_arco_away * atk_vis   * def_local

    donde:
        atk_local = avg_arco_generados_local(home) / la_arco_home
        def_vis   = avg_arco_concedidos_vis(away)  / la_arco_home
    """
    # Liga: promedios de referencia (filtrado por liga_id con fallback)
    from modelo_v2 import load_leagues_db, resolve_liga_id
    _, name_to_id_leagues = load_leagues_db()
    liga_id_filter = resolve_liga_id(competition, name_to_id_leagues) if competition else None
    comp_rows = [r for r in rows
                 if not liga_id_filter or int(r['liga_id']) == liga_id_filter]
    if len(comp_rows) < 3:
        comp_rows = rows

    la_home = sum(int(r['tiros_arco_local'])    for r in comp_rows) / len(comp_rows)
    la_away = sum(int(r['tiros_arco_visitante']) for r in comp_rows) / len(comp_rows)

    # Resolver nombres → IDs
    _, name_to_id = load_teams_db()
    local_id = resolve_team_id(team_local,  name_to_id)
    vis_id   = resolve_team_id(team_visita, name_to_id)

    def avg_home_gen(tid):
        vals = [int(r['tiros_arco_local']) for r in rows if int(r['equipo_local_id']) == tid]
        return sum(vals) / len(vals) if len(vals) >= MIN_MATCHES else None

    def avg_away_gen(tid):
        vals = [int(r['tiros_arco_visitante']) for r in rows if int(r['equipo_visitante_id']) == tid]
        return sum(vals) / len(vals) if len(vals) >= MIN_MATCHES else None

    def avg_home_con(tid):
        vals = [int(r['tiros_arco_visitante']) for r in rows if int(r['equipo_local_id']) == tid]
        return sum(vals) / len(vals) if len(vals) >= MIN_MATCHES else None

    def avg_away_con(tid):
        vals = [int(r['tiros_arco_local']) for r in rows if int(r['equipo_visitante_id']) == tid]
        return sum(vals) / len(vals) if len(vals) >= MIN_MATCHES else None

    def rating(val, base):
        return (val / base) if val is not None and base > 0 else 1.0

    # Ratings de ataque (cuántos arco genera)
    atk_local = rating(avg_home_gen(local_id), la_home)
    atk_vis   = rating(avg_away_gen(vis_id),   la_away)

    # Ratings de defensa (cuántos arco concede al rival)
    def_local = rating(avg_home_con(local_id), la_away)
    def_vis   = rating(avg_away_con(vis_id),   la_home)

    mu_local = la_home * atk_local * def_vis
    mu_vis   = la_away * atk_vis   * def_local

    n_local = len([r for r in rows if int(r['equipo_local_id'])     == local_id])
    n_vis   = len([r for r in rows if int(r['equipo_visitante_id']) == vis_id])

    return {
        'mu_arco_local': max(0.1, mu_local),
        'mu_arco_vis':   max(0.1, mu_vis),
        'la_arco_home':  la_home,
        'la_arco_away':  la_away,
        'n_arco_local':  n_local,
        'n_arco_vis':    n_vis,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Cálculo de probabilidades (thresholds libres)
# ─────────────────────────────────────────────────────────────────────────────

def compute_all_probs(sim):
    n  = sim['n']
    gl = sim['gl']; gv = sim['gv']
    sl = sim['sl']; sv = sim['sv']
    cl = sim['cl']; cv = sim['cv']

    tg = [gl[i]+gv[i] for i in range(n)]
    ts = [sl[i]+sv[i] for i in range(n)]
    tc = [cl[i]+cv[i] for i in range(n)]

    def over(data, thr):  return sum(x > thr for x in data) / n
    def under(data, thr): return 1 - over(data, thr)

    p = {}

    # 1X2
    p['1'] = sum(gl[i] > gv[i] for i in range(n)) / n
    p['X'] = sum(gl[i] == gv[i] for i in range(n)) / n
    p['2'] = sum(gv[i] > gl[i] for i in range(n)) / n

    # Goles totales
    for thr in [0.5, 1.5, 2.5, 3.5, 4.5, 5.5]:
        p[f'g_over_{thr}']  = over(tg, thr)
        p[f'g_under_{thr}'] = under(tg, thr)

    # Goles local
    for thr in [0.5, 1.5, 2.5, 3.5]:
        p[f'gl_over_{thr}']  = over(gl, thr)
        p[f'gl_under_{thr}'] = under(gl, thr)

    # Goles visita
    for thr in [0.5, 1.5, 2.5, 3.5]:
        p[f'gv_over_{thr}']  = over(gv, thr)
        p[f'gv_under_{thr}'] = under(gv, thr)

    # BTTS
    p['btts_si'] = sum(gl[i] > 0 and gv[i] > 0 for i in range(n)) / n
    p['btts_no'] = 1 - p['btts_si']

    # Tiros totales — thresholds del bookmaker
    for thr in [15.5, 17.5, 19.5, 21.5, 23.5, 25.5, 27.5]:
        p[f'ts_over_{thr}']  = over(ts, thr)
        p[f'ts_under_{thr}'] = under(ts, thr)

    # Tiros local
    for thr in [6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5]:
        p[f'sl_over_{thr}']  = over(sl, thr)
        p[f'sl_under_{thr}'] = under(sl, thr)

    # Tiros visita
    for thr in [5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5]:
        p[f'sv_over_{thr}']  = over(sv, thr)
        p[f'sv_under_{thr}'] = under(sv, thr)

    # Corners totales
    for thr in [7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5]:
        p[f'tc_over_{thr}']  = over(tc, thr)
        p[f'tc_under_{thr}'] = under(tc, thr)

    # Corners local
    for thr in [3.5, 4.5, 5.5, 6.5, 7.5]:
        p[f'cl_over_{thr}']  = over(cl, thr)
        p[f'cl_under_{thr}'] = under(cl, thr)

    # Corners visita
    for thr in [2.5, 3.5, 4.5, 5.5, 6.5]:
        p[f'cv_over_{thr}']  = over(cv, thr)
        p[f'cv_under_{thr}'] = under(cv, thr)

    # Remates al arco (solo si el sim fue enriquecido con sla_arco/sva_arco)
    if 'sla_arco' in sim:
        sla_a = sim['sla_arco']
        sva_a = sim['sva_arco']
        ta_a  = [sla_a[i] + sva_a[i] for i in range(n)]
        for thr in [3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5]:
            p[f'ta_over_{thr}']  = over(ta_a, thr)
            p[f'ta_under_{thr}'] = under(ta_a, thr)
        for thr in [1.5, 2.5, 3.5, 4.5, 5.5, 6.5]:
            p[f'sla_over_{thr}']  = over(sla_a, thr)
            p[f'sla_under_{thr}'] = under(sla_a, thr)
            p[f'sva_over_{thr}']  = over(sva_a, thr)
            p[f'sva_under_{thr}'] = under(sva_a, thr)
        p['E_sla_arco'] = sum(sla_a) / n
        p['E_sva_arco'] = sum(sva_a) / n

    # Distribución de marcadores
    from collections import Counter
    sc = Counter(zip(gl, gv))
    p['score_dist'] = {
        f"{g}-{v}": cnt/n
        for (g,v), cnt in sorted(sc.items(), key=lambda x: -x[1])[:12]
    }

    # Valores esperados
    p['E_gl'] = sum(gl)/n;  p['E_gv'] = sum(gv)/n
    p['E_sl'] = sum(sl)/n;  p['E_sv'] = sum(sv)/n
    p['E_cl'] = sum(cl)/n;  p['E_cv'] = sum(cv)/n

    return p


# ─────────────────────────────────────────────────────────────────────────────
# APUESTAS COMBINADAS (Parlays)
# ─────────────────────────────────────────────────────────────────────────────

def evaluar_condicion(sim, event_key):
    """
    Evalúa una condición sobre los datos de simulación y devuelve una lista
    de booleanos (True = evento ocurrió en esa iteración).

    Claves soportadas
    -----------------
    Resultado   : '1'  '2'  'X'
    BTTS        : 'btts_si'  'btts_no'
    Goles total : 'g_over_2.5'  'g_under_2.5'
    Goles local : 'gl_over_1.5' 'gl_under_0.5'
    Goles visita: 'gv_over_0.5' 'gv_under_1.5'
    Tiros total : 'ts_over_21.5' 'ts_under_19.5'
    Tiros local : 'sl_over_9.5'  'sl_under_11.5'
    Tiros visita: 'sv_over_7.5'  'sv_under_9.5'
    Corners tot.: 'tc_over_9.5'  'tc_under_8.5'
    Corners loc.: 'cl_over_5.5'  'cl_under_4.5'
    Corners vis.: 'cv_over_3.5'  'cv_under_4.5'
    AHcp local  : 'ahcp_-0.5'  'ahcp_+1.5'
                  (hcp aplicado al local; gana si gl-gv > -hcp)
    """
    n  = sim['n']
    gl = sim['gl']; gv = sim['gv']
    sl = sim['sl']; sv = sim['sv']
    cl = sim['cl']; cv = sim['cv']
    tg = [gl[i]+gv[i] for i in range(n)]
    ts = [sl[i]+sv[i] for i in range(n)]
    tc = [cl[i]+cv[i] for i in range(n)]
    diff = [gl[i]-gv[i] for i in range(n)]
    # Remates al arco (si disponibles; cero-array si no)
    sla_a = sim.get('sla_arco', [0]*n)
    sva_a = sim.get('sva_arco', [0]*n)
    ta_a  = [sla_a[i]+sva_a[i] for i in range(n)]

    ek = event_key.lower().strip()

    if   ek == '1':        return [gl[i] > gv[i]  for i in range(n)]
    elif ek == 'x':        return [gl[i] == gv[i] for i in range(n)]
    elif ek == '2':        return [gv[i] > gl[i]  for i in range(n)]
    elif ek == 'btts_si':  return [gl[i]>0 and gv[i]>0 for i in range(n)]
    elif ek == 'btts_no':  return [not(gl[i]>0 and gv[i]>0) for i in range(n)]

    # Genérico: prefijo_dirección_threshold
    parts = ek.rsplit('_', 1)
    if len(parts) == 2:
        prefix, thr_str = parts
        thr = float(thr_str)
        DATA_MAP = {
            'g_over':   (tg,    True),  'g_under':   (tg,    False),
            'gl_over':  (gl,    True),  'gl_under':  (gl,    False),
            'gv_over':  (gv,    True),  'gv_under':  (gv,    False),
            'ts_over':  (ts,    True),  'ts_under':  (ts,    False),
            'sl_over':  (sl,    True),  'sl_under':  (sl,    False),
            'sv_over':  (sv,    True),  'sv_under':  (sv,    False),
            'tc_over':  (tc,    True),  'tc_under':  (tc,    False),
            'cl_over':  (cl,    True),  'cl_under':  (cl,    False),
            'cv_over':  (cv,    True),  'cv_under':  (cv,    False),
            # Remates al arco
            'ta_over':  (ta_a,  True),  'ta_under':  (ta_a,  False),
            'sla_over': (sla_a, True),  'sla_under': (sla_a, False),
            'sva_over': (sva_a, True),  'sva_under': (sva_a, False),
        }
        if prefix in DATA_MAP:
            data, is_over = DATA_MAP[prefix]
            if is_over:
                return [x > thr for x in data]
            else:
                return [x <= thr for x in data]

    # Asian Handicap: ahcp_-0.5  ahcp_+1.5  etc.
    if ek.startswith('ahcp_'):
        hcp = float(ek[5:])
        threshold = -hcp   # local gana si diff > -hcp
        return [d > threshold for d in diff]

    raise ValueError(f"Condicion desconocida: '{event_key}'. "
                     f"Ejemplos: '1','X','2','g_over_2.5','sl_under_9.5','ahcp_-0.5'")


def calcular_combinada(legs, sims_dict):
    """
    Calcula la probabilidad y EV de una apuesta combinada (parlay).

    legs : lista de dicts con:
        {
          'match_id': str,    # debe existir en sims_dict
          'event':    str,    # clave del evento (ver evaluar_condicion)
          'label':    str,    # descripción legible (opcional)
          'odds':     float,  # cuota del bookmaker para ese evento (opcional)
        }

    sims_dict : { match_id: sim }

    Retorna dict con probabilidades, cuotas y análisis de value.
    """
    # ── Agrupar legs por partido ──────────────────────────────────────────────
    by_match = {}
    for leg in legs:
        mid = leg['match_id']
        by_match.setdefault(mid, []).append(leg)

    # ── Probabilidad conjunta dentro de cada partido (correlación real) ───────
    match_results = {}
    for mid, match_legs in by_match.items():
        sim = sims_dict[mid]
        n   = sim['n']

        # AND de todas las condiciones del partido en cada iteración
        conditions = [evaluar_condicion(sim, leg['event']) for leg in match_legs]
        joint = sum(all(c[i] for c in conditions) for i in range(n)) / n

        # Probabilidades individuales de cada leg en este partido
        indiv = [sum(c)/n for c in conditions]
        prod_indep = 1.0
        for p in indiv: prod_indep *= p

        match_results[mid] = {
            'legs':       match_legs,
            'joint_p':    joint,          # prob real (correlación capturada)
            'indep_p':    prod_indep,     # prod. naive (asume independencia)
            'correlation_effect': joint - prod_indep,
        }

    # ── Probabilidad total del parlay (independencia entre partidos) ──────────
    prob_total = 1.0
    for mr in match_results.values():
        prob_total *= mr['joint_p']

    fair_odds = 1 / prob_total if prob_total > 0 else float('inf')

    # ── Cuotas del bookmaker (producto de cuotas individuales) ────────────────
    book_legs_odds  = [leg.get('odds') for leg in legs]
    all_have_odds   = all(o is not None for o in book_legs_odds)
    book_combined   = 1.0
    if all_have_odds:
        for o in book_legs_odds:
            book_combined *= o
    implied_p = 1 / book_combined if all_have_odds else None
    edge      = prob_total - implied_p if implied_p else None
    ev_pct    = (prob_total * book_combined - 1) * 100 if all_have_odds else None

    return {
        'legs':          legs,
        'match_results': match_results,
        'prob_total':    prob_total,
        'fair_odds':     fair_odds,
        'book_combined': book_combined if all_have_odds else None,
        'implied_p':     implied_p,
        'edge':          edge,
        'ev_pct':        ev_pct,
    }


def print_combinada(result, sims_dict):
    """Imprime el análisis de una apuesta combinada."""
    sep = '=' * 68
    print(f"\n{sep}")
    print(f"  APUESTA COMBINADA ({len(result['legs'])} selecciones)")
    print(sep)

    # ── Desglose por partido ──────────────────────────────────────────────────
    for mid, mr in result['match_results'].items():
        sim  = sims_dict[mid]
        tl   = sim.get('team_local',  mid.split('_')[0])
        tv   = sim.get('team_visita', mid.split('_')[1] if '_' in mid else '?')
        print(f"\n  Partido: {tl} vs {tv}  (match_id='{mid}')")
        print(f"  {'-'*60}")
        for leg in mr['legs']:
            label = leg.get('label', leg['event'])
            odds  = leg.get('odds')
            # Probabilidad individual
            n  = sim['n']
            cond = evaluar_condicion(sim, leg['event'])
            p_ind = sum(cond) / n
            line  = f"    [{leg['event']}]  {label:<35}  P={p_ind:.1%}"
            if odds:
                line += f"  @{odds:.2f}"
            print(line)

        # Efecto de correlación (solo si hay 2+ legs en el mismo partido)
        if len(mr['legs']) >= 2:
            ce = mr['correlation_effect']
            sign = '+' if ce >= 0 else ''
            print(f"    {'Prob. conjunta (correlacion real):':<38} {mr['joint_p']:.1%}")
            print(f"    {'Prod. naive (si fueran independientes):':<38} {mr['indep_p']:.1%}")
            print(f"    {'Efecto correlacion:':<38} {sign}{ce:.1%}")
        else:
            print(f"    {'Prob. del evento:':<38} {mr['joint_p']:.1%}")

    # ── Resultado global del parlay ───────────────────────────────────────────
    print(f"\n  {'='*60}")
    prob  = result['prob_total']
    fodds = result['fair_odds']
    print(f"  PROBABILIDAD TOTAL DEL PARLAY : {prob:.2%}")
    print(f"  CUOTA JUSTA (1/P)             : {fodds:.2f}")

    if result['book_combined']:
        bc = result['book_combined']
        ip = result['implied_p']
        ed = result['edge']
        ev = result['ev_pct']
        print(f"  CUOTA COMBINADA BOOKMAKER     : {bc:.2f}")
        print(f"  PROBABILIDAD IMPLICITA (fair) : {ip:.2%}")
        sign = '+' if ed >= 0 else ''
        verdict = 'VALUE' if ed >= 0.04 else ('Marginal' if ed >= 0 else 'Sin valor')
        print(f"  EDGE                          : {sign}{ed:.1%}  [{verdict}]")
        print(f"  EV%                           : {sign}{ev:.1f}%")

    print(f"  {'='*60}\n")


# ─────────────────────────────────────────────────────────────────────────────
# Detección de value bets
# ─────────────────────────────────────────────────────────────────────────────

def analizar_value_bets(probs, odds, team_local, team_visita, min_edge=MIN_EDGE):
    vb = []

    def check(label, pk, bk_over, bk_under=None):
        if pk not in probs:
            return
        model_p = probs[pk]
        if bk_over and bk_under:
            fp_over, fp_under = _remove_vig(bk_over, bk_under)
            fp = fp_over
        elif bk_over:
            fp = 1 / bk_over
        else:
            return
        edge = model_p - fp
        if abs(edge) >= min_edge:
            # También registra el lado contrario si hay edge inverso
            if edge >= min_edge:
                ev = model_p * bk_over - 1
                vb.append({'market': label, 'lado': 'Over/Si',
                           'odds': bk_over, 'model_p': model_p,
                           'implied_p': fp, 'edge': edge, 'EV_%': ev*100})
            elif bk_under and (-edge) >= min_edge:
                fp_u = fp_under if bk_under else 1/bk_under
                model_under = 1 - model_p
                ev = model_under * bk_under - 1
                vb.append({'market': label, 'lado': 'Under/No',
                           'odds': bk_under, 'model_p': model_under,
                           'implied_p': fp_u, 'edge': -edge, 'EV_%': ev*100})

    def check2(label, pk_o, pk_u, bk_over, bk_under):
        """Chequea ambos lados de un mercado binario."""
        if bk_over and bk_under:
            fp_over, fp_under = _remove_vig(bk_over, bk_under)
        else:
            fp_over  = 1/bk_over  if bk_over  else None
            fp_under = 1/bk_under if bk_under else None

        for pk, fp, bk, lado in [
            (pk_o, fp_over,  bk_over,  'Over/Si'),
            (pk_u, fp_under, bk_under, 'Under/No'),
        ]:
            if pk not in probs or fp is None or bk is None:
                continue
            model_p = probs[pk]
            edge = model_p - fp
            if edge >= min_edge:
                ev = model_p * bk - 1
                vb.append({'market': label, 'lado': lado,
                           'odds': bk, 'model_p': model_p,
                           'implied_p': fp, 'edge': edge, 'EV_%': ev*100})

    o = odds

    # 1X2
    if all(k in o for k in ('1','X','2')):
        fp1, fpx, fp2 = _remove_vig(o['1'], o['X'], o['2'])
        for pk, fp, bk, lbl in [
            ('1', fp1, o['1'], f'1X2 -> {team_local} gana'),
            ('X', fpx, o['X'], '1X2 -> Empate'),
            ('2', fp2, o['2'], f'1X2 -> {team_visita} gana'),
        ]:
            edge = probs[pk] - fp
            if edge >= min_edge:
                ev = probs[pk]*bk - 1
                vb.append({'market': lbl, 'lado': '',
                           'odds': bk, 'model_p': probs[pk],
                           'implied_p': fp, 'edge': edge, 'EV_%': ev*100})

    # BTTS
    check2('BTTS', 'btts_si', 'btts_no',
           o.get('btts_si'), o.get('btts_no'))

    # Goles totales
    for thr in [0.5, 1.5, 2.5, 3.5, 4.5]:
        check2(f'Goles tot. O/U {thr}', f'g_over_{thr}', f'g_under_{thr}',
               o.get(f'g_over_{thr}'), o.get(f'g_under_{thr}'))

    # Goles local
    for thr in [0.5, 1.5, 2.5]:
        check2(f'Goles {team_local} O/U {thr}', f'gl_over_{thr}', f'gl_under_{thr}',
               o.get(f'gl_over_{thr}'), o.get(f'gl_under_{thr}'))

    # Goles visita
    for thr in [0.5, 1.5, 2.5]:
        check2(f'Goles {team_visita} O/U {thr}', f'gv_over_{thr}', f'gv_under_{thr}',
               o.get(f'gv_over_{thr}'), o.get(f'gv_under_{thr}'))

    # Tiros totales
    for thr in [15.5, 17.5, 19.5, 21.5, 23.5, 25.5, 27.5]:
        check2(f'Tiros tot. O/U {thr}', f'ts_over_{thr}', f'ts_under_{thr}',
               o.get(f'ts_over_{thr}'), o.get(f'ts_under_{thr}'))

    # Tiros local
    for thr in [6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5]:
        check2(f'Tiros {team_local} O/U {thr}', f'sl_over_{thr}', f'sl_under_{thr}',
               o.get(f'sl_over_{thr}'), o.get(f'sl_under_{thr}'))

    # Tiros visita
    for thr in [5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5]:
        check2(f'Tiros {team_visita} O/U {thr}', f'sv_over_{thr}', f'sv_under_{thr}',
               o.get(f'sv_over_{thr}'), o.get(f'sv_under_{thr}'))

    # Remates al arco totales
    for thr in [3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5]:
        check2(f'Arco tot. O/U {thr}', f'ta_over_{thr}', f'ta_under_{thr}',
               o.get(f'ta_over_{thr}'), o.get(f'ta_under_{thr}'))

    # Remates al arco local
    for thr in [1.5, 2.5, 3.5, 4.5, 5.5, 6.5]:
        check2(f'Arco {team_local} O/U {thr}', f'sla_over_{thr}', f'sla_under_{thr}',
               o.get(f'sla_over_{thr}'), o.get(f'sla_under_{thr}'))

    # Remates al arco visita
    for thr in [1.5, 2.5, 3.5, 4.5, 5.5, 6.5]:
        check2(f'Arco {team_visita} O/U {thr}', f'sva_over_{thr}', f'sva_under_{thr}',
               o.get(f'sva_over_{thr}'), o.get(f'sva_under_{thr}'))

    vb.sort(key=lambda x: -x['edge'])
    return vb


# ─────────────────────────────────────────────────────────────────────────────
# Reporte
# ─────────────────────────────────────────────────────────────────────────────

def print_analisis(team_local, team_visita, competition, params, probs, value_bets):
    sep = '=' * 68
    print(f"\n{sep}")
    print(f"  {team_local} vs {team_visita}  |  {competition}")
    print(sep)

    arco_info = sim.get('arco_params', {})
    print(f"\n[PARAMETROS]  (local n={params['n_local_home']}  visita n={params['n_vis_away']})")
    print(f"   lambda goles: local={params['lambda_local']:.3f}  visita={params['lambda_vis']:.3f}")
    print(f"   mu corners : local={params['mu_corners_local']:.2f}  visita={params['mu_corners_vis']:.2f}")
    print(f"   mu tiros   : local={params['mu_shots_local']:.1f}+/-{params['sigma_shots_local']:.1f}  "
          f"visita={params['mu_shots_vis']:.1f}+/-{params['sigma_shots_vis']:.1f}")
    if arco_info:
        print(f"   mu arco    : local={arco_info['mu_arco_local']:.2f}  "
              f"visita={arco_info['mu_arco_vis']:.2f}")
    print(f"   Posesion local: {params['poss_local']:.1f}%")

    E_g = probs['E_gl']+probs['E_gv']
    E_s = probs['E_sl']+probs['E_sv']
    E_c = probs['E_cl']+probs['E_cv']
    print(f"\n[VALORES ESPERADOS]")
    print(f"   Goles  : {E_g:.2f}  ({team_local}:{probs['E_gl']:.2f}  {team_visita}:{probs['E_gv']:.2f})")
    print(f"   Tiros  : {E_s:.2f}  ({team_local}:{probs['E_sl']:.2f}  {team_visita}:{probs['E_sv']:.2f})")
    print(f"   Corners: {E_c:.2f}  ({team_local}:{probs['E_cl']:.2f}  {team_visita}:{probs['E_cv']:.2f})")

    def j(p): return f"{1/p:.2f}" if p > 0.001 else ">999"

    print(f"\n[RESULTADO 1X2]")
    print(f"   {team_local:<22} {probs['1']:6.1%}  odds justas: {j(probs['1'])}")
    print(f"   {'Empate':<22} {probs['X']:6.1%}  odds justas: {j(probs['X'])}")
    print(f"   {team_visita:<22} {probs['2']:6.1%}  odds justas: {j(probs['2'])}")

    print(f"\n[GOLES TOTALES]")
    for thr in [0.5, 1.5, 2.5, 3.5, 4.5]:
        po = probs.get(f'g_over_{thr}', 0)
        pu = probs.get(f'g_under_{thr}', 0)
        print(f"   Over  {thr}: {po:5.1%}  ({j(po)})   Under {thr}: {pu:5.1%}  ({j(pu)})")

    print(f"\n[GOLES POR EQUIPO]")
    print(f"   {team_local} anota 1+: {probs.get('gl_over_0.5',0):5.1%}  "
          f"2+: {probs.get('gl_over_1.5',0):5.1%}  3+: {probs.get('gl_over_2.5',0):5.1%}")
    print(f"   {team_visita} anota 1+: {probs.get('gv_over_0.5',0):5.1%}  "
          f"2+: {probs.get('gv_over_1.5',0):5.1%}  3+: {probs.get('gv_over_2.5',0):5.1%}")
    print(f"   BTTS Si: {probs.get('btts_si',0):5.1%}    BTTS No: {probs.get('btts_no',0):5.1%}")

    print(f"\n[TIROS TOTALES]")
    for thr in [17.5, 19.5, 21.5, 23.5, 25.5]:
        po = probs.get(f'ts_over_{thr}', 0)
        pu = probs.get(f'ts_under_{thr}', 0)
        print(f"   Over  {thr}: {po:5.1%}  ({j(po)})   Under {thr}: {pu:5.1%}  ({j(pu)})")

    print(f"\n[TIROS {team_local.upper()}]")
    for thr in [7.5, 9.5, 11.5, 13.5, 15.5]:
        po = probs.get(f'sl_over_{thr}', 0)
        pu = probs.get(f'sl_under_{thr}', 0)
        print(f"   Over  {thr}: {po:5.1%}  ({j(po)})   Under {thr}: {pu:5.1%}  ({j(pu)})")

    print(f"\n[TIROS {team_visita.upper()}]")
    for thr in [6.5, 7.5, 9.5, 11.5, 13.5]:
        po = probs.get(f'sv_over_{thr}', 0)
        pu = probs.get(f'sv_under_{thr}', 0)
        print(f"   Over  {thr}: {po:5.1%}  ({j(po)})   Under {thr}: {pu:5.1%}  ({j(pu)})")

    # Remates al arco (solo si fueron simulados)
    if 'E_sla_arco' in probs:
        E_ta = probs['E_sla_arco'] + probs['E_sva_arco']
        print(f"\n[REMATES AL ARCO]")
        print(f"   E. arco: {E_ta:.2f}  ({team_local}:{probs['E_sla_arco']:.2f}  "
              f"{team_visita}:{probs['E_sva_arco']:.2f})")
        print(f"   Totales:")
        for thr in [4.5, 5.5, 6.5, 7.5, 8.5, 9.5]:
            po = probs.get(f'ta_over_{thr}', 0)
            pu = probs.get(f'ta_under_{thr}', 0)
            print(f"     Over  {thr}: {po:5.1%}  ({j(po)})   Under {thr}: {pu:5.1%}  ({j(pu)})")
        print(f"   {team_local}:")
        for thr in [1.5, 2.5, 3.5, 4.5, 5.5]:
            po = probs.get(f'sla_over_{thr}', 0)
            pu = probs.get(f'sla_under_{thr}', 0)
            print(f"     Over  {thr}: {po:5.1%}  ({j(po)})   Under {thr}: {pu:5.1%}  ({j(pu)})")
        print(f"   {team_visita}:")
        for thr in [1.5, 2.5, 3.5, 4.5, 5.5]:
            po = probs.get(f'sva_over_{thr}', 0)
            pu = probs.get(f'sva_under_{thr}', 0)
            print(f"     Over  {thr}: {po:5.1%}  ({j(po)})   Under {thr}: {pu:5.1%}  ({j(pu)})")

    print(f"\n[CORNERS]")
    for thr in [8.5, 9.5, 10.5, 11.5]:
        po = probs.get(f'tc_over_{thr}', 0)
        print(f"   Totales Over {thr}: {po:5.1%}  ({j(po)})")
    print(f"   {team_local} Over 5.5: {probs.get('cl_over_5.5',0):5.1%}   "
          f"Over 6.5: {probs.get('cl_over_6.5',0):5.1%}")
    print(f"   {team_visita} Over 3.5: {probs.get('cv_over_3.5',0):5.1%}   "
          f"Over 4.5: {probs.get('cv_over_4.5',0):5.1%}")

    print(f"\n[MARCADORES MAS PROBABLES]")
    for score, prob in sorted(probs['score_dist'].items(), key=lambda x: -x[1])[:10]:
        bar = '#' * int(prob * 200)
        print(f"   {score:>5}: {prob:5.1%}  {bar}")

    if value_bets:
        print(f"\n{'*'*68}")
        print(f"  VALUE BETS (edge >= {MIN_EDGE:.0%})")
        print(f"{'*'*68}")
        hdr = f"  {'Mercado':<34} {'Lado':<8} {'Odds':>5}  {'Modelo':>6}  {'Impl.':>6}  {'Edge':>5}  {'EV%':>5}"
        print(hdr)
        print('  ' + '-' * 70)
        for vb in value_bets:
            print(f"  {vb['market']:<34} {vb['lado']:<8} {vb['odds']:>5.2f}  "
                  f"{vb['model_p']:>6.1%}  {vb['implied_p']:>6.1%}  "
                  f"{vb['edge']:>+4.1%}  {vb['EV_%']:>+4.1f}%")
    else:
        print(f"\n  Sin value bets detectadas con edge >= {MIN_EDGE:.0%}")

    print()


# ─────────────────────────────────────────────────────────────────────────────
# SEGUIMIENTO DE VALUE BETS
# ─────────────────────────────────────────────────────────────────────────────

import csv as _csv
from datetime import datetime as _dt
from pathlib import Path as _Path

_VB_CSV  = _Path(r'C:\Users\Matt\Apuestas Deportivas\data\apuestas\value_bets.csv')
_VB_COLS = [
    'fecha_analisis', 'fixture_id', 'partido', 'competicion',
    'mercado', 'lado', 'odds', 'modelo_prob', 'implied_prob',
    'edge', 'ev_pct', 'metodo', 'resultado',
]

def guardar_value_bets(value_bets, team_local, team_visita, competition,
                       fixture_id=None, metodo='v2'):
    """
    Agrega las value bets detectadas al CSV de seguimiento.
    Deduplica por (fixture_id, mercado, lado) — correr el analisis varias
    veces no genera filas duplicadas.
    La columna 'resultado' queda vacía para completar manualmente: W / L / V
    """
    _VB_CSV.parent.mkdir(parents=True, exist_ok=True)

    # Cargar existentes
    existing, seen = [], set()
    if _VB_CSV.exists():
        with open(_VB_CSV, newline='', encoding='utf-8') as f:
            for r in _csv.DictReader(f):
                existing.append(r)
                seen.add((r['fixture_id'], r['mercado'], r['lado']))

    partido = f"{team_local} vs {team_visita}"
    ahora   = _dt.now().strftime('%Y-%m-%d %H:%M')
    fid_str = str(fixture_id) if fixture_id else ''

    nuevas = []
    for vb in value_bets:
        key = (fid_str, vb['market'], vb['lado'])
        if key in seen:
            continue
        nuevas.append({
            'fecha_analisis': ahora,
            'fixture_id':     fid_str,
            'partido':        partido,
            'competicion':    competition,
            'mercado':        vb['market'],
            'lado':           vb['lado'],
            'odds':           f"{vb['odds']:.2f}",
            'modelo_prob':    f"{vb['model_p']:.1%}",
            'implied_prob':   f"{vb['implied_p']:.1%}",
            'edge':           f"{vb['edge']:+.1%}",
            'ev_pct':         f"{vb['EV_%']:+.1f}",
            'metodo':         metodo,
            'resultado':      '',
        })
        seen.add(key)

    if not nuevas:
        print(f"\n  [tracking] Sin value bets nuevas (ya registradas o ninguna detectada)")
        return

    all_rows = existing + nuevas
    with open(_VB_CSV, 'w', newline='', encoding='utf-8') as f:
        w = _csv.DictWriter(f, fieldnames=_VB_COLS)
        w.writeheader()
        w.writerows(all_rows)

    print(f"\n  [tracking] {len(nuevas)} value bet(s) guardadas -> {_VB_CSV.name}")
    print(f"  [tracking] Completar columna 'resultado' con:  W (ganada)  L (perdida)  V (void)")


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN DEL PARTIDO  [AUTO — generado por preparar_partido.py]
# ─────────────────────────────────────────────────────────────────────────────
# ── BEGIN PARTIDO CONFIG ─
TEAM_LOCAL  = 'Belgrano Cordoba'
TEAM_VISITA = 'Aldosivi'
COMPETITION = 'Liga Profesional'
FIXTURE_ID  = 1492014
N_SIM = 200_000

ODDS = {
    # 1X2
    '1': 1.62,  'X': 3.5,  '2': 6.5,

    # BTTS
    'btts_si': 2.5,  'btts_no': 1.5,

    # Goles totales
    'g_over_0.5': 1.1,  'g_under_0.5': 7.0,
    'g_over_1.5': 1.5,  'g_under_1.5': 2.5,
    'g_over_2.5': 2.5,  'g_under_2.5': 1.5,
    'g_over_3.5': 5.0,  'g_under_3.5': 1.17,
    'g_over_4.5': 11.0,  'g_under_4.5': 1.05,

    # Goles Belgrano Cordoba (local)
    'gl_over_0.5': 1.22,  'gl_under_0.5': 4.0,
    'gl_over_1.5': 2.0,  'gl_under_1.5': 1.73,
    'gl_over_2.5': 4.33,  'gl_under_2.5': 1.2,
    'gl_over_3.5': 11.0,  'gl_under_3.5': 1.05,

    # Goles Aldosivi (visita)
    'gv_over_0.5': 2.0,  'gv_under_0.5': 1.73,
    'gv_over_1.5': 7.0,  'gv_under_1.5': 1.1,
    'gv_over_2.5': 26.0,  'gv_under_2.5': 1.01,
    'gv_over_3.5': None,  'gv_under_3.5': None,

    # Corners totales
    'tc_over_7.5': None,  'tc_under_7.5': None,
    'tc_over_8.5': 1.67,  'tc_under_8.5': 2.1,
    'tc_over_9.5': None,  'tc_under_9.5': None,
    'tc_over_10.5': None,  'tc_under_10.5': None,
    'tc_over_11.5': None,  'tc_under_11.5': None,

    # Corners Belgrano Cordoba (local)
    'cl_over_3.5': None,  'cl_under_3.5': None,
    'cl_over_4.5': None,  'cl_under_4.5': None,
    'cl_over_5.5': 2.0,  'cl_under_5.5': 1.73,
    'cl_over_6.5': None,  'cl_under_6.5': None,

    # Corners Aldosivi (visita)
    'cv_over_2.5': None,  'cv_under_2.5': None,
    'cv_over_3.5': 2.0,  'cv_under_3.5': 1.73,
    'cv_over_4.5': None,  'cv_under_4.5': None,
    'cv_over_5.5': None,  'cv_under_5.5': None,

    # Tiros totales — completar manualmente desde bookie
    'ts_over_15.5': None,  'ts_under_15.5': None,
    'ts_over_17.5': None,  'ts_under_17.5': None,
    'ts_over_19.5': None,  'ts_under_19.5': None,
    'ts_over_20.5': 1.25,  'ts_under_20.5': 3.75,
    'ts_over_21.5': None,  'ts_under_21.5': None,
    'ts_over_22.5': 1.53,  'ts_under_22.5': 2.37,
    'ts_over_26.5': 2.75,  'ts_under_26.5': 1.40,
    'ts_over_28.5': 4.33,  'ts_under_28.5': 1.20,

    # Tiros Belgrano Cordoba (local) — completar manualmente
    'sl_over_6.5': None,  'sl_under_6.5': None,
    'sl_over_7.5': None,  'sl_under_7.5': None,
    'sl_over_8.5': None,  'sl_under_8.5': None,
    'sl_over_9.5': None,  'sl_under_9.5': None,
    'sl_over_10.5': 1.25,  'sl_under_10.5': 3.75,
    'sl_over_11.5': 1.36,  'sl_under_11.5': 3.00,
    'sl_over_12.5': 1.53,  'sl_under_12.5': 2.37,
    'sl_over_13.5': 1.80,  'sl_under_13.5': 1.90,

    # Tiros Aldosivi (visita) — completar manualmente
    'sv_over_5.5': None,  'sv_under_5.5': None,
    'sv_over_6.5': None,  'sv_under_6.5': None,
    'sv_over_7.5': 1.28,  'sv_under_7.5': 3.50,
    'sv_over_8.5': 1.44,  'sv_under_8.5': 2.62,
    'sv_over_9.5': 1.72,  'sv_under_9.5': 2.00,
    'sv_over_10.5': 2.10,  'sv_under_10.5': 1.66,
    'sv_over_11.5': 2.62,  'sv_under_11.5': 1.44,
    'sv_over_12.5': 3.50,  'sv_under_12.5': 1.28,

    # Remates al arco totales
    'ta_over_4.5': None,  'ta_under_4.5': None,
    'ta_over_5.5': 1.33,  'ta_under_5.5': 3.25,
    'ta_over_6.5': 1.66,  'ta_under_6.5': 2.10,
    'ta_over_7.5': 2.2,  'ta_under_7.5': 1.61,
    'ta_over_8.5': 3.25,  'ta_under_8.5': 1.61,
    'ta_over_9.5': 4.5,  'ta_under_9.5': 1.33,
    'ta_over_10.5': None,  'ta_under_10.5': None,
    'ta_over_11.5': None,  'ta_under_11.5': None,

    # Remates al arco Belgrano Cordoba (local) — completar manualmente
    'sla_over_1.5': None,  'sla_under_1.5': None,
    'sla_over_2.5': 1.22,  'sla_under_2.5': 4.00,
    'sla_over_3.5': 1.53,  'sla_under_3.5': 2.37,
    'sla_over_4.5': 2.10,  'sla_under_4.5': 1.66,
    'sla_over_5.5': 3.25,  'sla_under_5.5': 1.33,
    'sla_over_6.5': None,  'sla_under_6.5': None,

    # Remates al arco Aldosivi (visita) — completar manualmente
    'sva_over_1.5': 1.30,  'sva_under_1.5': 3.40,
    'sva_over_2.5': 1.83,  'sva_under_2.5': 1.83,
    'sva_over_3.5': 3.00,  'sva_under_3.5': 1.36,
    'sva_over_4.5': 5.00,  'sva_under_4.5': 1.14,
    'sva_over_5.5': None,  'sva_under_5.5': None,
    'sva_over_6.5': None,  'sva_under_6.5': None,

    # Tarjetas totales
    'cards_over_3.5': None,  'cards_under_3.5': None,
    'cards_over_4.5': None,  'cards_under_4.5': None,
    'cards_over_5.5': 2.1,  'cards_under_5.5': 1.67,
    'cards_over_6.5': None,  'cards_under_6.5': None,
}
# ── END PARTIDO CONFIG ─

# ─────────────────────────────────────────────────────────────────────────────
# EJECUCIÓN
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print(f"Cargando datos y simulando {N_SIM:,} iteraciones...")
    rows   = load_csv()
    params = compute_match_params(TEAM_LOCAL, TEAM_VISITA, rows, COMPETITION)
    sim    = run_simulation(params, N_SIM)

    # Guardar metadatos en el sim para que print_combinada pueda usar los nombres
    sim['team_local']  = TEAM_LOCAL
    sim['team_visita'] = TEAM_VISITA

    # ── Simular remates al arco ──────────────────────────────────────────────
    arco_p = compute_arco_params(TEAM_LOCAL, TEAM_VISITA, rows, COMPETITION)
    sim['sla_arco'] = [poisson_sample(arco_p['mu_arco_local']) for _ in range(N_SIM)]
    sim['sva_arco'] = [poisson_sample(arco_p['mu_arco_vis'])   for _ in range(N_SIM)]
    sim['arco_params'] = arco_p
    print(f"   mu_arco: {TEAM_LOCAL}={arco_p['mu_arco_local']:.2f}  "
          f"{TEAM_VISITA}={arco_p['mu_arco_vis']:.2f}  "
          f"(liga home={arco_p['la_arco_home']:.2f} away={arco_p['la_arco_away']:.2f})")

    probs = compute_all_probs(sim)
    vb    = analizar_value_bets(probs, ODDS, TEAM_LOCAL, TEAM_VISITA)
    print_analisis(TEAM_LOCAL, TEAM_VISITA, COMPETITION, params, probs, vb)
    guardar_value_bets(vb, TEAM_LOCAL, TEAM_VISITA, COMPETITION, FIXTURE_ID)

    # ─────────────────────────────────────────────────────────────────────────
    # APUESTAS COMBINADAS — editá esta sección con tus selecciones
    # ─────────────────────────────────────────────────────────────────────────
    # El dict sims_dict mapea match_id -> sim.
    # Si querés combinar con otro partido, corré compute_match_params /
    # run_simulation para ese partido y agregalo con su propio match_id.
    # ─────────────────────────────────────────────────────────────────────────

    MATCH_ID = 'boca_indep'
    sims_dict = {MATCH_ID: sim}

    # ── Ejemplos de combinadas (editar a gusto) ───────────────────────────────

    combinadas = [

        # 1) Boca gana + Independiente no marca (mismo partido, correlacionadas)
        [
            {'match_id': MATCH_ID, 'event': '1',
             'label': 'Boca Juniors gana', 'odds': 2.30},
            {'match_id': MATCH_ID, 'event': 'gv_under_0.5',
             'label': 'Independiente no marca', 'odds': 2.37},
        ],

        # 2) Boca gana + Under 2.5 goles (correlacionadas)
        [
            {'match_id': MATCH_ID, 'event': '1',
             'label': 'Boca Juniors gana', 'odds': 2.30},
            {'match_id': MATCH_ID, 'event': 'g_under_2.5',
             'label': 'Total Under 2.5 goles', 'odds': 1.50},
        ],

        # 3) Triple: Boca gana + Indep no marca + Total Under 2.5
        [
            {'match_id': MATCH_ID, 'event': '1',
             'label': 'Boca Juniors gana', 'odds': 2.30},
            {'match_id': MATCH_ID, 'event': 'gv_under_0.5',
             'label': 'Independiente no marca', 'odds': 2.37},
            {'match_id': MATCH_ID, 'event': 'g_under_2.5',
             'label': 'Total Under 2.5 goles', 'odds': 1.50},
        ],

        # 4) Boca gana + Tiros Indep Under 9.5
        [
            {'match_id': MATCH_ID, 'event': '1',
             'label': 'Boca Juniors gana', 'odds': 2.30},
            {'match_id': MATCH_ID, 'event': 'sv_under_9.5',
             'label': 'Tiros Independiente Under 9.5', 'odds': 2.10},
        ],

    ]

    print("\n" + "="*68)
    print("  ANALISIS DE APUESTAS COMBINADAS")
    print("="*68)

    for legs in combinadas:
        result = calcular_combinada(legs, sims_dict)
        print_combinada(result, sims_dict)
