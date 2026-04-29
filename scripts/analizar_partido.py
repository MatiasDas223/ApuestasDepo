"""
Análisis de partido con cuotas reales del bookmaker.
Uso: editar TEAM_LOCAL, TEAM_VISITA, COMPETITION y ODDS al final del archivo.
"""

import sys
sys.path.insert(0, r'C:\Users\Matt\Apuestas Deportivas\scripts')

from modelo_v3 import (load_csv, compute_match_params, run_simulation,
                       _remove_vig, MIN_EDGE, poisson_sample, norm as norm_text,
                       MIN_MATCHES, load_teams_db, resolve_team_id)


# ─────────────────────────────────────────────────────────────────────────────
# Parámetros para remates al arco (tiros_arco)
# ─────────────────────────────────────────────────────────────────────────────

K_PREC_SHRINK = 8   # partidos de shrinkage para precisión SOT

def compute_arco_params(team_local, team_visita, rows, competition=None):
    """
    Estima la tasa de precisión (SOT / tiros totales) por equipo con
    shrinkage bayesiano hacia la media de la liga.

    En la simulación, SOT se genera como Binomial(tiros_simulados, precision).
    """
    from modelo_v3 import load_leagues_db, resolve_liga_id

    _, name_to_id_leagues = load_leagues_db()
    liga_id_filter = resolve_liga_id(competition, name_to_id_leagues) if competition else None
    comp_rows = [r for r in rows
                 if not liga_id_filter or int(r['liga_id']) == liga_id_filter]
    if len(comp_rows) < 3:
        comp_rows = rows

    # Precisión media de la liga
    total_shots_league = 0
    total_sot_league = 0
    for r in comp_rows:
        sl = int(r.get('tiros_local', 0) or 0)
        sv = int(r.get('tiros_visitante', 0) or 0)
        al = int(r.get('tiros_arco_local', 0) or 0)
        av = int(r.get('tiros_arco_visitante', 0) or 0)
        if sl > 0:
            total_shots_league += sl
            total_sot_league += al
        if sv > 0:
            total_shots_league += sv
            total_sot_league += av
    prec_liga = total_sot_league / total_shots_league if total_shots_league > 0 else 0.34

    # Resolver nombres → IDs
    _, name_to_id = load_teams_db()
    local_id = resolve_team_id(team_local,  name_to_id)
    vis_id   = resolve_team_id(team_visita, name_to_id)

    def team_precision(tid):
        """Precisión del equipo (SOT/shots) con shrinkage hacia liga."""
        team_rows = [r for r in rows
                     if int(r['equipo_local_id']) == tid or int(r['equipo_visitante_id']) == tid]
        shots, sot = 0, 0
        for r in team_rows:
            if int(r['equipo_local_id']) == tid:
                s = int(r.get('tiros_local', 0) or 0)
                a = int(r.get('tiros_arco_local', 0) or 0)
            else:
                s = int(r.get('tiros_visitante', 0) or 0)
                a = int(r.get('tiros_arco_visitante', 0) or 0)
            if s > 0:
                shots += s
                sot += a
        n = len(team_rows)
        if n < MIN_MATCHES or shots == 0:
            return prec_liga, 0
        raw_prec = sot / shots
        # Shrinkage: (n * raw + K * liga) / (n + K)
        prec = (n * raw_prec + K_PREC_SHRINK * prec_liga) / (n + K_PREC_SHRINK)
        return prec, n

    prec_local, n_local = team_precision(local_id)
    prec_vis,   n_vis   = team_precision(vis_id)

    return {
        'prec_local':    prec_local,
        'prec_vis':      prec_vis,
        'prec_liga':     prec_liga,
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

    # Doble oportunidad
    p['1X'] = p['1'] + p['X']
    p['X2'] = p['X'] + p['2']
    p['12'] = p['1'] + p['2']

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

    # Tiros totales — cobertura completa half-integer
    for thr in [i + 0.5 for i in range(11, 32)]:   # 11.5 .. 31.5
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
    for thr in [3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5]:
        p[f'tc_over_{thr}']  = over(tc, thr)
        p[f'tc_under_{thr}'] = under(tc, thr)

    # Corners local
    for thr in [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5]:
        p[f'cl_over_{thr}']  = over(cl, thr)
        p[f'cl_under_{thr}'] = under(cl, thr)

    # Corners visita
    for thr in [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5]:
        p[f'cv_over_{thr}']  = over(cv, thr)
        p[f'cv_under_{thr}'] = under(cv, thr)

    # Tarjetas totales (vienen de run_simulation via mu_tarjetas_local/vis)
    if 'tl' in sim:
        tl_s = sim['tl']
        tv_s = sim['tv']
        tt_s = [tl_s[i] + tv_s[i] for i in range(n)]
        for thr in [2.5, 3.5, 4.5, 5.5, 6.5, 7.5]:
            p[f'cards_over_{thr}']  = over(tt_s, thr)
            p[f'cards_under_{thr}'] = under(tt_s, thr)
        p['E_tl'] = sum(tl_s) / n
        p['E_tv'] = sum(tv_s) / n

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
            ev = model_p * bk - 1
            if edge >= min_edge and ev >= 0:
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
            ev = probs[pk]*bk - 1
            if edge >= min_edge and ev >= 0:
                vb.append({'market': lbl, 'lado': '',
                           'odds': bk, 'model_p': probs[pk],
                           'implied_p': fp, 'edge': edge, 'EV_%': ev*100})

    # Doble oportunidad
    if all(k in o for k in ('dc_1x', 'dc_12', 'dc_x2')):
        fp_1x, fp_12, fp_x2 = _remove_vig(o['dc_1x'], o['dc_12'], o['dc_x2'], expected_sum=2.0)
        for pk, fp, bk, lbl in [
            ('1X', fp_1x, o['dc_1x'], f'DC -> {team_local} o Empate (1X)'),
            ('12', fp_12, o['dc_12'], f'DC -> {team_local} o {team_visita} (12)'),
            ('X2', fp_x2, o['dc_x2'], f'DC -> Empate o {team_visita} (X2)'),
        ]:
            if pk not in probs:
                continue
            edge = probs[pk] - fp
            ev = probs[pk]*bk - 1
            if edge >= min_edge and ev >= 0:
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
    for thr in [i + 0.5 for i in range(11, 32)]:
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

    # Corners totales
    for thr in [3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5]:
        check2(f'Corners tot. O/U {thr}', f'tc_over_{thr}', f'tc_under_{thr}',
               o.get(f'tc_over_{thr}'), o.get(f'tc_under_{thr}'))

    # Corners local
    for thr in [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5]:
        check2(f'Corners {team_local} O/U {thr}', f'cl_over_{thr}', f'cl_under_{thr}',
               o.get(f'cl_over_{thr}'), o.get(f'cl_under_{thr}'))

    # Corners visita
    for thr in [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5]:
        check2(f'Corners {team_visita} O/U {thr}', f'cv_over_{thr}', f'cv_under_{thr}',
               o.get(f'cv_over_{thr}'), o.get(f'cv_under_{thr}'))

    # Remates al arco totales
    for thr in [3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5]:
        check2(f'Arco tot. O/U {thr}', f'ta_over_{thr}', f'ta_under_{thr}',
               o.get(f'ta_over_{thr}'), o.get(f'ta_under_{thr}'))

    # Remates al arco local
    for thr in [1.5, 2.5, 3.5, 4.5, 5.5, 6.5]:
        check2(f'Arco {team_local} O/U {thr}', f'sla_over_{thr}', f'sla_under_{thr}',
               o.get(f'sla_over_{thr}'), o.get(f'sla_under_{thr}'))

    # Remates al arco visita
    for thr in [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5]:
        check2(f'Arco {team_visita} O/U {thr}', f'sva_over_{thr}', f'sva_under_{thr}',
               o.get(f'sva_over_{thr}'), o.get(f'sva_under_{thr}'))

    # Tarjetas totales
    for thr in [2.5, 3.5, 4.5, 5.5, 6.5, 7.5]:
        check2(f'Tarjetas tot. O/U {thr}', f'cards_over_{thr}', f'cards_under_{thr}',
               o.get(f'cards_over_{thr}'), o.get(f'cards_under_{thr}'))

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

    print(f"\n[PARAMETROS]  (local n={params['n_local_home']}  visita n={params['n_vis_away']})")
    print(f"   lambda goles: local={params['lambda_local']:.3f}  visita={params['lambda_vis']:.3f}")
    print(f"   mu corners : total={params['mu_corners_total']:.2f}  "
          f"(local={params['mu_corners_local']:.2f}  visita={params['mu_corners_vis']:.2f}  "
          f"share={params['share_corners_loc']:.1%}  k={params['k_corners']:.1f})")
    print(f"   mu tarjetas: local={params['mu_tarjetas_local']:.2f}  visita={params['mu_tarjetas_vis']:.2f}")
    print(f"   mu tiros   : local={params['mu_shots_local']:.1f} (k={params['k_shots_local']:.1f})  "
          f"visita={params['mu_shots_vis']:.1f} (k={params['k_shots_vis']:.1f})")
    print(f"   prec arco  : local={params['prec_local']:.3f}  visita={params['prec_vis']:.3f}")
    print(f"   Posesion local: {params['poss_local']:.1f}%")

    E_g = probs['E_gl']+probs['E_gv']
    E_s = probs['E_sl']+probs['E_sv']
    E_c = probs['E_cl']+probs['E_cv']
    print(f"\n[VALORES ESPERADOS]")
    print(f"   Goles    : {E_g:.2f}  ({team_local}:{probs['E_gl']:.2f}  {team_visita}:{probs['E_gv']:.2f})")
    print(f"   Tiros    : {E_s:.2f}  ({team_local}:{probs['E_sl']:.2f}  {team_visita}:{probs['E_sv']:.2f})")
    print(f"   Corners  : {E_c:.2f}  ({team_local}:{probs['E_cl']:.2f}  {team_visita}:{probs['E_cv']:.2f})")
    if 'E_tl' in probs:
        E_t = probs['E_tl'] + probs['E_tv']
        print(f"   Tarjetas : {E_t:.2f}  ({team_local}:{probs['E_tl']:.2f}  {team_visita}:{probs['E_tv']:.2f})")

    def j(p): return f"{1/p:.2f}" if p > 0.001 else ">999"

    print(f"\n[RESULTADO 1X2]")
    print(f"   {team_local:<22} {probs['1']:6.1%}  odds justas: {j(probs['1'])}")
    print(f"   {'Empate':<22} {probs['X']:6.1%}  odds justas: {j(probs['X'])}")
    print(f"   {team_visita:<22} {probs['2']:6.1%}  odds justas: {j(probs['2'])}")

    print(f"\n[DOBLE OPORTUNIDAD]")
    print(f"   {team_local+' o Empate (1X)':<30} {probs['1X']:6.1%}  odds justas: {j(probs['1X'])}")
    print(f"   {team_local+' o '+team_visita+' (12)':<30} {probs['12']:6.1%}  odds justas: {j(probs['12'])}")
    print(f"   {'Empate o '+team_visita+' (X2)':<30} {probs['X2']:6.1%}  odds justas: {j(probs['X2'])}")

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
    'edge', 'ev_pct', 'metodo', 'categoria', 'alcance', 'resultado',
    'odds_close', 'clv_pct', 'fecha_cierre',
    'odds_close_est', 'clv_pct_est', 'clv_method',
]


def _clasificar_mercado(mercado, partido):
    """Clasifica un mercado en (categoria, alcance) para filtrar en BI."""
    m = mercado.lower()
    partes = partido.split(' vs ')
    local  = partes[0].strip() if len(partes) > 1 else ''
    visita = partes[1].strip() if len(partes) > 1 else ''

    # Categoria
    if mercado.startswith('1X2'):
        cat = '1X2'
    elif mercado.startswith('DC'):
        cat = 'Doble Oportunidad'
    elif 'btts' in m:
        cat = 'BTTS'
    elif 'tarjetas' in m:
        cat = 'Tarjetas'
    elif 'arco' in m:
        cat = 'Arco'
    elif 'corners' in m:
        cat = 'Corners'
    elif 'tiros' in m:
        cat = 'Tiros'
    elif 'goles' in m:
        cat = 'Goles'
    else:
        cat = 'Otros'

    # Alcance
    if 'tot.' in m or cat in ('1X2', 'Doble Oportunidad', 'BTTS'):
        alcance = 'Total'
    elif local and local in mercado:
        alcance = 'Local'
    elif visita and visita in mercado:
        alcance = 'Visitante'
    else:
        alcance = 'Total'

    return cat, alcance

def guardar_value_bets(value_bets, team_local, team_visita, competition,
                       fixture_id=None, metodo='v3.2', csv_path=None):
    """
    Agrega las value bets detectadas al CSV de seguimiento.
    Deduplica por (fixture_id, mercado, lado) — correr el analisis varias
    veces no genera filas duplicadas.
    La columna 'resultado' queda vacía para completar manualmente: W / L / V
    `csv_path` permite sobreescribir el destino (útil para modelo calibrado).
    """
    csv_file = _Path(csv_path) if csv_path else _VB_CSV
    csv_file.parent.mkdir(parents=True, exist_ok=True)

    # Cargar existentes (backfill categoria/alcance si faltan)
    existing, seen = [], set()
    if csv_file.exists():
        with open(csv_file, newline='', encoding='utf-8') as f:
            for r in _csv.DictReader(f):
                if not r.get('categoria') or not r.get('alcance'):
                    c, a = _clasificar_mercado(r['mercado'], r.get('partido', ''))
                    r['categoria'] = c
                    r['alcance'] = a
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
        cat, alc = _clasificar_mercado(vb['market'], partido)
        nuevas.append({
            'fecha_analisis': ahora,
            'fixture_id':     fid_str,
            'partido':        partido,
            'competicion':    competition,
            'mercado':        vb['market'],
            'lado':           vb['lado'],
            'odds':           f"{vb['odds']:.2f}",
            'modelo_prob':    f"{vb['model_p']:.4f}",
            'implied_prob':   f"{vb['implied_p']:.4f}",
            'edge':           f"{vb['edge']:.4f}",
            'ev_pct':         f"{vb['EV_%']/100:.4f}",
            'metodo':         metodo,
            'categoria':      cat,
            'alcance':        alc,
            'resultado':      '',
        })
        seen.add(key)

    if not nuevas:
        print(f"\n  [tracking] Sin value bets nuevas (ya registradas o ninguna detectada)")
        return

    all_rows = existing + nuevas
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        w = _csv.DictWriter(f, fieldnames=_VB_COLS)
        w.writeheader()
        w.writerows(all_rows)

    print(f"\n  [tracking] {len(nuevas)} value bet(s) guardadas -> {csv_file.name}")
    print(f"  [tracking] Completar columna 'resultado' con:  W (ganada)  L (perdida)  V (void)")


# ─────────────────────────────────────────────────────────────────────────────
# ESTRATEGIA FILTRADA (descubierta por backtest sobre value_bets.csv)
#   BTTS     + Over/Si  + edge >= 0.06  -> +30.2% ROI historico
#   Corners  + Over/Si  + edge >= 0.10  -> +12.3% ROI historico
#   Goles    + Over/Si  + edge >= 0.15  -> +40.4% ROI historico
#   Tarjetas + Under/No + edge >= 0.06  -> +20.1% ROI historico
# Resto de categorias (1X2, Arco, Tiros, Doble Oportunidad) descartadas.
# Cap de cuota aplicado globalmente: cuotas >= 4.00 tienen -45.9% ROI y son
# el leak principal del modelo; 3.0-4.0 tambien pierde en Goles Over.
# ─────────────────────────────────────────────────────────────────────────────

ESTRATEGIA_FILTRADA = {
    ('BTTS',     'Over/Si'):  0.06,
    ('Corners',  'Over/Si'):  0.10,
    ('Goles',    'Over/Si'):  0.15,
    ('Tarjetas', 'Under/No'): 0.06,
}

ODDS_MAX = 3.50


def filtrar_estrategia(value_bets, team_local, team_visita):
    """Aplica la estrategia filtrada: cat+lado+min_edge especificos por categoria,
    mas cap de cuota global (ODDS_MAX)."""
    partido = f"{team_local} vs {team_visita}"
    out = []
    for vb in value_bets:
        if vb['odds'] >= ODDS_MAX:
            continue
        cat, _ = _clasificar_mercado(vb['market'], partido)
        min_e = ESTRATEGIA_FILTRADA.get((cat, vb['lado']))
        if min_e is not None and vb['edge'] >= min_e:
            out.append(vb)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# SEGUIMIENTO DE PRONÓSTICOS (todos los mercados, con o sin valor)
# ─────────────────────────────────────────────────────────────────────────────

_PRON_CSV  = _Path(r'C:\Users\Matt\Apuestas Deportivas\data\apuestas\pronosticos.csv')
_PRON_COLS = [
    'fecha_analisis', 'fixture_id', 'partido', 'competicion',
    'mercado', 'lado', 'odds', 'modelo_prob', 'implied_prob',
    'edge', 'ev_pct', 'metodo', 'categoria', 'alcance', 'resultado',
]


def _collect_all_market_entries(probs, odds, team_local, team_visita):
    """
    Genera una lista con TODAS las entradas de mercado, sin filtro de edge.
    Cada entrada: dict con market, lado, model_p, odds, implied_p, edge, ev_pct.
    Si no hay cuota para ese lado: odds/implied_p/edge/ev_pct = None.
    """
    entries = []
    o = odds

    def _vig2(bk_o, bk_u):
        if bk_o and bk_u:
            return _remove_vig(bk_o, bk_u)
        return (1/bk_o if bk_o else None), (1/bk_u if bk_u else None)

    def add2(label, pk_o, pk_u, bk_over, bk_under):
        fp_over, fp_under = _vig2(bk_over, bk_under)
        for pk, fp, bk, lado in [
            (pk_o, fp_over,  bk_over,  'Over/Si'),
            (pk_u, fp_under, bk_under, 'Under/No'),
        ]:
            if pk not in probs:
                continue
            model_p = probs[pk]
            if fp is not None and bk is not None:
                edge   = model_p - fp
                ev_pct = (model_p * bk - 1) * 100
            else:
                edge = ev_pct = None
            entries.append({
                'market': label, 'lado': lado,
                'model_p': model_p,
                'odds': bk, 'implied_p': fp,
                'edge': edge, 'ev_pct': ev_pct,
            })

    # 1X2
    if all(k in o for k in ('1', 'X', '2')):
        fp1, fpx, fp2 = _remove_vig(o['1'], o['X'], o['2'])
        for pk, fp, bk, lbl, lado in [
            ('1', fp1, o['1'], f'1X2 -> {team_local} gana',   ''),
            ('X', fpx, o['X'], '1X2 -> Empate',               ''),
            ('2', fp2, o['2'], f'1X2 -> {team_visita} gana',  ''),
        ]:
            if pk not in probs:
                continue
            model_p = probs[pk]
            edge   = model_p - fp
            ev_pct = (model_p * bk - 1) * 100
            entries.append({
                'market': lbl, 'lado': lado,
                'model_p': model_p,
                'odds': bk, 'implied_p': fp,
                'edge': edge, 'ev_pct': ev_pct,
            })
    else:
        # Sin cuotas: guardar igual con probabilidad del modelo
        for pk, lbl, lado in [
            ('1', f'1X2 -> {team_local} gana',   ''),
            ('X', '1X2 -> Empate',               ''),
            ('2', f'1X2 -> {team_visita} gana',  ''),
        ]:
            if pk in probs:
                entries.append({'market': lbl, 'lado': lado,
                                'model_p': probs[pk],
                                'odds': None, 'implied_p': None,
                                'edge': None, 'ev_pct': None})

    # Doble oportunidad
    if all(k in o for k in ('dc_1x', 'dc_12', 'dc_x2')):
        fp_1x, fp_12, fp_x2 = _remove_vig(o['dc_1x'], o['dc_12'], o['dc_x2'], expected_sum=2.0)
        for pk, fp, bk, lbl, lado in [
            ('1X', fp_1x, o['dc_1x'], f'DC -> {team_local} o Empate (1X)',       ''),
            ('12', fp_12, o['dc_12'], f'DC -> {team_local} o {team_visita} (12)', ''),
            ('X2', fp_x2, o['dc_x2'], f'DC -> Empate o {team_visita} (X2)',       ''),
        ]:
            if pk not in probs:
                continue
            model_p = probs[pk]
            edge   = model_p - fp
            ev_pct = (model_p * bk - 1) * 100
            entries.append({
                'market': lbl, 'lado': lado,
                'model_p': model_p,
                'odds': bk, 'implied_p': fp,
                'edge': edge, 'ev_pct': ev_pct,
            })
    else:
        for pk, lbl, lado in [
            ('1X', f'DC -> {team_local} o Empate (1X)',       ''),
            ('12', f'DC -> {team_local} o {team_visita} (12)', ''),
            ('X2', f'DC -> Empate o {team_visita} (X2)',       ''),
        ]:
            if pk in probs:
                entries.append({'market': lbl, 'lado': lado,
                                'model_p': probs[pk],
                                'odds': None, 'implied_p': None,
                                'edge': None, 'ev_pct': None})

    # BTTS
    add2('BTTS', 'btts_si', 'btts_no', o.get('btts_si'), o.get('btts_no'))

    # Goles totales
    for thr in [0.5, 1.5, 2.5, 3.5, 4.5]:
        add2(f'Goles tot. O/U {thr}', f'g_over_{thr}', f'g_under_{thr}',
             o.get(f'g_over_{thr}'), o.get(f'g_under_{thr}'))

    # Goles local
    for thr in [0.5, 1.5, 2.5]:
        add2(f'Goles {team_local} O/U {thr}', f'gl_over_{thr}', f'gl_under_{thr}',
             o.get(f'gl_over_{thr}'), o.get(f'gl_under_{thr}'))

    # Goles visita
    for thr in [0.5, 1.5, 2.5]:
        add2(f'Goles {team_visita} O/U {thr}', f'gv_over_{thr}', f'gv_under_{thr}',
             o.get(f'gv_over_{thr}'), o.get(f'gv_under_{thr}'))

    # Tiros totales
    for thr in [i + 0.5 for i in range(11, 32)]:
        add2(f'Tiros tot. O/U {thr}', f'ts_over_{thr}', f'ts_under_{thr}',
             o.get(f'ts_over_{thr}'), o.get(f'ts_under_{thr}'))

    # Tiros local
    for thr in [6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5]:
        add2(f'Tiros {team_local} O/U {thr}', f'sl_over_{thr}', f'sl_under_{thr}',
             o.get(f'sl_over_{thr}'), o.get(f'sl_under_{thr}'))

    # Tiros visita
    for thr in [5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5]:
        add2(f'Tiros {team_visita} O/U {thr}', f'sv_over_{thr}', f'sv_under_{thr}',
             o.get(f'sv_over_{thr}'), o.get(f'sv_under_{thr}'))

    # Corners totales
    for thr in [3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5]:
        add2(f'Corners tot. O/U {thr}', f'tc_over_{thr}', f'tc_under_{thr}',
             o.get(f'tc_over_{thr}'), o.get(f'tc_under_{thr}'))

    # Corners local
    for thr in [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5]:
        add2(f'Corners {team_local} O/U {thr}', f'cl_over_{thr}', f'cl_under_{thr}',
             o.get(f'cl_over_{thr}'), o.get(f'cl_under_{thr}'))

    # Corners visita
    for thr in [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5]:
        add2(f'Corners {team_visita} O/U {thr}', f'cv_over_{thr}', f'cv_under_{thr}',
             o.get(f'cv_over_{thr}'), o.get(f'cv_under_{thr}'))

    # Remates al arco totales
    for thr in [3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5]:
        add2(f'Arco tot. O/U {thr}', f'ta_over_{thr}', f'ta_under_{thr}',
             o.get(f'ta_over_{thr}'), o.get(f'ta_under_{thr}'))

    # Remates al arco local
    for thr in [1.5, 2.5, 3.5, 4.5, 5.5, 6.5]:
        add2(f'Arco {team_local} O/U {thr}', f'sla_over_{thr}', f'sla_under_{thr}',
             o.get(f'sla_over_{thr}'), o.get(f'sla_under_{thr}'))

    # Remates al arco visita
    for thr in [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5]:
        add2(f'Arco {team_visita} O/U {thr}', f'sva_over_{thr}', f'sva_under_{thr}',
             o.get(f'sva_over_{thr}'), o.get(f'sva_under_{thr}'))

    # Tarjetas totales
    for thr in [2.5, 3.5, 4.5, 5.5, 6.5, 7.5]:
        add2(f'Tarjetas tot. O/U {thr}', f'cards_over_{thr}', f'cards_under_{thr}',
             o.get(f'cards_over_{thr}'), o.get(f'cards_under_{thr}'))

    return entries


def guardar_pronosticos(probs, odds, team_local, team_visita, competition,
                        fixture_id=None, metodo='v3.2', csv_path=None):
    """
    Guarda TODOS los pronósticos (todos los mercados) en pronosticos.csv,
    tengan valor o no. Útil para calibración posterior del modelo.
    Deduplica por (fixture_id, mercado, lado).
    La columna 'resultado' queda vacía para completar manualmente: W / L / V
    `csv_path` permite sobreescribir el destino (útil para modelo calibrado).
    """
    csv_file = _Path(csv_path) if csv_path else _PRON_CSV
    csv_file.parent.mkdir(parents=True, exist_ok=True)

    # Cargar existentes (backfill categoria/alcance si faltan)
    existing, seen = [], set()
    if csv_file.exists():
        with open(csv_file, newline='', encoding='utf-8') as f:
            for r in _csv.DictReader(f):
                if not r.get('categoria') or not r.get('alcance'):
                    c, a = _clasificar_mercado(r['mercado'], r.get('partido', ''))
                    r['categoria'] = c
                    r['alcance'] = a
                existing.append(r)
                seen.add((r['fixture_id'], r['mercado'], r['lado']))

    partido = f"{team_local} vs {team_visita}"
    ahora   = _dt.now().strftime('%Y-%m-%d %H:%M')
    fid_str = str(fixture_id) if fixture_id else ''

    entries = _collect_all_market_entries(probs, odds, team_local, team_visita)

    nuevas = []
    for e in entries:
        key = (fid_str, e['market'], e['lado'])
        if key in seen:
            continue
        cat, alc = _clasificar_mercado(e['market'], partido)
        nuevas.append({
            'fecha_analisis': ahora,
            'fixture_id':     fid_str,
            'partido':        partido,
            'competicion':    competition,
            'mercado':        e['market'],
            'lado':           e['lado'],
            'odds':           f"{e['odds']:.2f}"    if e['odds']     is not None else '',
            'modelo_prob':    f"{e['model_p']:.4f}" if e['model_p']  is not None else '',
            'implied_prob':   f"{e['implied_p']:.4f}" if e['implied_p'] is not None else '',
            'edge':           f"{e['edge']:.4f}"    if e['edge']     is not None else '',
            'ev_pct':         f"{e['ev_pct']/100:.4f}" if e['ev_pct'] is not None else '',
            'metodo':         metodo,
            'categoria':      cat,
            'alcance':        alc,
            'resultado':      '',
        })
        seen.add(key)

    if not nuevas:
        print(f"  [pronosticos] Sin entradas nuevas (ya registradas o fixture sin datos)")
        return

    all_rows = existing + nuevas
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        w = _csv.DictWriter(f, fieldnames=_PRON_COLS)
        w.writeheader()
        w.writerows(all_rows)

    print(f"  [pronosticos] {len(nuevas)} entrada(s) guardadas -> {csv_file.name}")


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN DEL PARTIDO  [AUTO — generado por preparar_partido.py]
# ─────────────────────────────────────────────────────────────────────────────
# ── BEGIN PARTIDO CONFIG ─
TEAM_LOCAL  = 'Racing Club'
TEAM_VISITA = 'River Plate'
COMPETITION = 'Liga Profesional'
FIXTURE_ID  = 1492025
N_SIM = 200_000

ODDS = {
    # 1X2
    '1': 2.8,  'X': 3.0,  '2': 2.8,

    # BTTS
    'btts_si': 2.05,  'btts_no': 1.7,

    # Goles totales
    'g_over_0.5': 1.11,  'g_under_0.5': 6.5,
    'g_over_1.5': 1.5,  'g_under_1.5': 2.5,
    'g_over_2.5': 2.5,  'g_under_2.5': 1.5,
    'g_over_3.5': 5.0,  'g_under_3.5': 1.17,
    'g_over_4.5': 11.0,  'g_under_4.5': 1.05,

    # Goles Racing Club (local)
    'gl_over_0.5': 1.44,  'gl_under_0.5': 2.62,
    'gl_over_1.5': 3.25,  'gl_under_1.5': 1.33,
    'gl_over_2.5': 9.0,  'gl_under_2.5': 1.07,
    'gl_over_3.5': 26.0,  'gl_under_3.5': 1.01,

    # Goles River Plate (visita)
    'gv_over_0.5': 1.44,  'gv_under_0.5': 2.62,
    'gv_over_1.5': 3.25,  'gv_under_1.5': 1.33,
    'gv_over_2.5': 9.0,  'gv_under_2.5': 1.07,
    'gv_over_3.5': 26.0,  'gv_under_3.5': 1.01,

    # Corners totales
    'tc_over_3.5': None,  'tc_under_3.5': 23.0,
    'tc_over_4.5': 1.04,  'tc_under_4.5': 13.0,
    'tc_over_5.5': 1.111,  'tc_under_5.5': 6.5,
    'tc_over_6.5': 1.25,  'tc_under_6.5': 3.75,
    'tc_over_7.5': 1.444,  'tc_under_7.5': 2.625,
    'tc_over_8.5': 1.8,  'tc_under_8.5': 2.0,
    'tc_over_9.5': 2.1,  'tc_under_9.5': 1.67,
    'tc_over_10.5': 3.25,  'tc_under_10.5': 1.333,
    'tc_over_11.5': 4.333,  'tc_under_11.5': 1.2,
    'tc_over_12.5': 6.5,  'tc_under_12.5': 1.111,
    'tc_over_13.5': 11.0,  'tc_under_13.5': 1.05,
    'tc_over_14.5': 17.0,  'tc_under_14.5': 1.025,

    # Corners Racing Club (local)
    'cl_over_0.5': None,  'cl_under_0.5': None,
    'cl_over_1.5': None,  'cl_under_1.5': None,
    'cl_over_2.5': None,  'cl_under_2.5': None,
    'cl_over_3.5': None,  'cl_under_3.5': None,
    'cl_over_4.5': 1.8,  'cl_under_4.5': 1.91,
    'cl_over_5.5': None,  'cl_under_5.5': None,
    'cl_over_6.5': None,  'cl_under_6.5': None,
    'cl_over_7.5': None,  'cl_under_7.5': None,
    'cl_over_8.5': None,  'cl_under_8.5': None,
    'cl_over_9.5': None,  'cl_under_9.5': None,

    # Corners River Plate (visita)
    'cv_over_0.5': None,  'cv_under_0.5': None,
    'cv_over_1.5': None,  'cv_under_1.5': None,
    'cv_over_2.5': None,  'cv_under_2.5': None,
    'cv_over_3.5': 1.67,  'cv_under_3.5': 2.1,
    'cv_over_4.5': 2.2,  'cv_under_4.5': 1.62,
    'cv_over_5.5': None,  'cv_under_5.5': None,
    'cv_over_6.5': None,  'cv_under_6.5': None,
    'cv_over_7.5': None,  'cv_under_7.5': None,
    'cv_over_8.5': None,  'cv_under_8.5': None,
    'cv_over_9.5': None,  'cv_under_9.5': None,
    'cv_over_10.5': None,  'cv_under_10.5': None,

    # Tiros totales
    'ts_over_13.5': None,  'ts_under_13.5': None,
    'ts_over_15.5': None,  'ts_under_15.5': None,
    'ts_over_17.5': None,  'ts_under_17.5': None,
    'ts_over_19.5': None,  'ts_under_19.5': None,
    'ts_over_21.5': None,  'ts_under_21.5': None,
    'ts_over_23.5': None,  'ts_under_23.5': None,
    'ts_over_25.5': None,  'ts_under_25.5': None,
    'ts_over_27.5': None,  'ts_under_27.5': None,
    'ts_over_29.5': None,  'ts_under_29.5': None,

    # Tiros Racing Club (local)
    'sl_over_4.5': None,  'sl_under_4.5': None,
    'sl_over_5.5': None,  'sl_under_5.5': None,
    'sl_over_6.5': None,  'sl_under_6.5': None,
    'sl_over_7.5': None,  'sl_under_7.5': None,
    'sl_over_8.5': None,  'sl_under_8.5': None,
    'sl_over_9.5': None,  'sl_under_9.5': None,
    'sl_over_10.5': None,  'sl_under_10.5': None,
    'sl_over_11.5': 1.833,  'sl_under_11.5': 1.833,
    'sl_over_12.5': None,  'sl_under_12.5': None,
    'sl_over_13.5': None,  'sl_under_13.5': None,

    # Tiros River Plate (visita)
    'sv_over_4.5': None,  'sv_under_4.5': None,
    'sv_over_5.5': None,  'sv_under_5.5': None,
    'sv_over_6.5': None,  'sv_under_6.5': None,
    'sv_over_7.5': None,  'sv_under_7.5': None,
    'sv_over_8.5': None,  'sv_under_8.5': None,
    'sv_over_9.5': None,  'sv_under_9.5': None,
    'sv_over_10.5': None,  'sv_under_10.5': None,
    'sv_over_11.5': None,  'sv_under_11.5': None,
    'sv_over_12.5': 1.909,  'sv_under_12.5': 1.8,
    'sv_over_13.5': None,  'sv_under_13.5': None,

    # Remates al arco totales
    'ta_over_3.5': None,  'ta_under_3.5': None,
    'ta_over_4.5': None,  'ta_under_4.5': None,
    'ta_over_5.5': None,  'ta_under_5.5': None,
    'ta_over_6.5': None,  'ta_under_6.5': None,
    'ta_over_7.5': 1.73,  'ta_under_7.5': 2.0,
    'ta_over_8.5': None,  'ta_under_8.5': None,
    'ta_over_9.5': None,  'ta_under_9.5': None,
    'ta_over_10.5': None,  'ta_under_10.5': None,
    'ta_over_11.5': None,  'ta_under_11.5': None,

    # Remates al arco Racing Club (local)
    'sla_over_0.5': None,  'sla_under_0.5': None,
    'sla_over_1.5': None,  'sla_under_1.5': None,
    'sla_over_2.5': None,  'sla_under_2.5': None,
    'sla_over_3.5': 1.727,  'sla_under_3.5': 2.0,
    'sla_over_4.5': None,  'sla_under_4.5': None,
    'sla_over_5.5': None,  'sla_under_5.5': None,
    'sla_over_6.5': None,  'sla_under_6.5': None,
    'sla_over_7.5': None,  'sla_under_7.5': None,

    # Remates al arco River Plate (visita)
    'sva_over_0.5': None,  'sva_under_0.5': None,
    'sva_over_1.5': None,  'sva_under_1.5': None,
    'sva_over_2.5': None,  'sva_under_2.5': None,
    'sva_over_3.5': 1.727,  'sva_under_3.5': 2.0,
    'sva_over_4.5': None,  'sva_under_4.5': None,
    'sva_over_5.5': None,  'sva_under_5.5': None,
    'sva_over_6.5': None,  'sva_under_6.5': None,
    'sva_over_7.5': None,  'sva_under_7.5': None,

    # Tarjetas totales
    'cards_over_3.5': None,  'cards_under_3.5': None,
    'cards_over_4.5': None,  'cards_under_4.5': None,
    'cards_over_5.5': 1.8,  'cards_under_5.5': 1.91,
    'cards_over_6.5': 2.1,  'cards_under_6.5': 1.67,
    'cards_over_7.5': None,  'cards_under_7.5': None,
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

    # arco ya se simula dentro de run_simulation como Binomial(tiros, precision)
    sim['arco_params'] = {
        'prec_local': params['prec_local'],
        'prec_vis':   params['prec_vis'],
    }
    print(f"   precision arco: {TEAM_LOCAL}={params['prec_local']:.3f}  "
          f"{TEAM_VISITA}={params['prec_vis']:.3f}")

    probs = compute_all_probs(sim)
    vb    = analizar_value_bets(probs, ODDS, TEAM_LOCAL, TEAM_VISITA)
    print_analisis(TEAM_LOCAL, TEAM_VISITA, COMPETITION, params, probs, vb)
    guardar_value_bets(vb, TEAM_LOCAL, TEAM_VISITA, COMPETITION, FIXTURE_ID)
    guardar_pronosticos(probs, ODDS, TEAM_LOCAL, TEAM_VISITA, COMPETITION, FIXTURE_ID)

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
