"""
analisis_rendimiento.py
-----------------------
Reporte completo de rendimiento del modelo de value bets.

Secciones:
  1. Resumen global por categoria de mercado
  2. ROI por rango de cuota
  3. Calibracion del modelo (win rate real vs prob predicha)
  4. Calibracion del EV — ranking de oportunidades por EV predicho
  5. Calibracion del edge — diferencia P_modelo vs P_mercado
  6. Analisis por version del modelo (v2, v3, ...)
  7. Analisis por tipo de torneo (ligas locales vs copas)
  8. Analisis por competicion individual

Uso:
    python analisis_rendimiento.py
    python analisis_rendimiento.py --csv path/al/archivo.csv
    python analisis_rendimiento.py --min-apuestas 5   # filtro minimo por grupo
"""

import csv
import sys
import re
from pathlib import Path
from collections import defaultdict

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE   = Path(__file__).resolve().parent.parent
VB_CSV = BASE / 'data/apuestas/value_bets.csv'
STAKE  = 1.0

SEP  = '=' * 76
SEP2 = '-' * 76

# ---------------------------------------------------------------------------
# Helpers de parseo
# ---------------------------------------------------------------------------

def _parse_float(s):
    if s is None:
        return None
    s = str(s).strip().replace('%', '').replace('+', '').replace(' ', '')
    if s in ('', '-'):
        return None
    try:
        val = float(s)
        if abs(val) > 1.5:
            val = val / 100.0
        return val
    except ValueError:
        return None


def _categorize_mercado(mercado):
    m = mercado.lower()
    if 'corner' in m:                                    return 'Corners'
    if 'arco' in m or 'on target' in m:                 return 'Arco'
    if 'tiro' in m or 'shot' in m:                      return 'Tiros'
    if 'tarjeta' in m or 'card' in m or 'booking' in m: return 'Tarjetas'
    if 'btts' in m or 'ambos' in m:                     return 'BTTS'
    if '1x2' in m or 'resultado' in m:                  return '1X2'
    if 'gol' in m or 'goal' in m:                       return 'Goles'
    return 'Otros'


_LIGAS_LOCALES = {
    'liga profesional', 'la liga', 'premier league', 'bundesliga',
    'serie a', 'brasileirao', 'brasileirao serie a', 'ligue 1',
}
_COPAS_EUR = {'champions league', 'europa league', 'conference league'}
_COPAS_SUD = {'copa libertadores', 'copa sudamericana'}
_COPAS_DOM = {'copa del rey', 'fa cup', 'dfb pokal', 'coppa italia', 'copa argentina'}


def _tipo_torneo(competicion):
    c = competicion.lower().strip()
    if any(l in c for l in _LIGAS_LOCALES):   return 'Ligas locales'
    if any(l in c for l in _COPAS_EUR):        return 'Copas europeas'
    if any(l in c for l in _COPAS_SUD):        return 'Copas sudamericanas'
    if any(l in c for l in _COPAS_DOM):        return 'Copas domesticas'
    return 'Otros'


def _rango_cuota(odds):
    if odds < 1.50:   return '1.20-1.50'
    if odds < 2.00:   return '1.50-2.00'
    if odds < 3.00:   return '2.00-3.00'
    return            '>3.00'

_ORDEN_CUOTAS = ['1.20-1.50', '1.50-2.00', '2.00-3.00', '>3.00']


def _rango_prob(p):
    if p is None:     return None
    if p < 0.40:      return '<0.40'
    if p < 0.45:      return '0.40-0.45'
    if p < 0.50:      return '0.45-0.50'
    if p < 0.55:      return '0.50-0.55'
    if p < 0.60:      return '0.55-0.60'
    if p < 0.65:      return '0.60-0.65'
    if p < 0.70:      return '0.65-0.70'
    return            '>=0.70'

_ORDEN_PROBS = ['<0.40','0.40-0.45','0.45-0.50','0.50-0.55',
                '0.55-0.60','0.60-0.65','0.65-0.70','>=0.70']


def _rango_ev(ev):
    """Bins de EV% predicho (como decimal: 0.10 = 10%)."""
    if ev is None:  return None
    if ev < 0.02:   return '0-2%'
    if ev < 0.05:   return '2-5%'
    if ev < 0.10:   return '5-10%'
    if ev < 0.15:   return '10-15%'
    return                 '>15%'

_ORDEN_EV = ['0-2%', '2-5%', '5-10%', '10-15%', '>15%']


def _rango_edge_prob(edge):
    """Bins de edge como diferencia de probabilidades (P_modelo - P_implied)."""
    if edge is None: return None
    if edge < 0.02:  return '0-2%'
    if edge < 0.04:  return '2-4%'
    if edge < 0.06:  return '4-6%'
    if edge < 0.10:  return '6-10%'
    return                  '>10%'

_ORDEN_EDGE = ['0-2%', '2-4%', '4-6%', '6-10%', '>10%']


def _pnl(resultado, odds):
    r = str(resultado).strip().upper()
    if r == 'W': return odds - STAKE
    if r == 'L': return -STAKE
    if r == 'V': return 0.0
    return None


# ---------------------------------------------------------------------------
# Carga
# ---------------------------------------------------------------------------

def cargar_apuestas(csv_path):
    apuestas = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            res = row.get('resultado', '').strip().upper()
            if res not in ('W', 'L', 'V'):
                continue
            try:
                odds = float(row['odds'])
            except (ValueError, KeyError):
                continue

            mp  = _parse_float(row.get('modelo_prob'))
            ip  = _parse_float(row.get('implied_prob'))
            ev  = _parse_float(row.get('ev_pct'))
            edg = _parse_float(row.get('edge'))

            apuestas.append({
                'partido':     row.get('partido', ''),
                'competicion': row.get('competicion', '').strip(),
                'mercado':     row.get('mercado', ''),
                'lado':        row.get('lado', ''),
                'metodo':      row.get('metodo', 'v2').strip() or 'v2',
                'odds':        odds,
                'modelo_prob': mp,
                'implied_prob':ip,
                'edge':        edg,
                'ev':          ev,
                'ev_esp':      ev * STAKE if ev is not None else None,
                'resultado':   res,
                'pnl':         _pnl(res, odds),
                'categoria':    _categorize_mercado(row.get('mercado', '')),
                'tipo_torneo':  _tipo_torneo(row.get('competicion', '')),
                'rango_cuota':  _rango_cuota(odds),
                'rango_prob':   _rango_prob(mp),
                'rango_ev':     _rango_ev(ev),
                'rango_edge':   _rango_edge_prob(edg),
            })
    return apuestas


# ---------------------------------------------------------------------------
# Estadisticas
# ---------------------------------------------------------------------------

def _stats(bets):
    if not bets:
        return None
    total  = len(bets)
    wins   = sum(1 for a in bets if a['resultado'] == 'W')
    losses = sum(1 for a in bets if a['resultado'] == 'L')
    voids  = sum(1 for a in bets if a['resultado'] == 'V')
    pnl    = sum(a['pnl'] for a in bets)
    ev_esp = sum(a['ev_esp'] for a in bets if a['ev_esp'] is not None)
    edges  = [a['edge'] for a in bets if a['edge'] is not None]
    edge_m = sum(edges) / len(edges) if edges else 0.0
    odds_m = sum(a['odds'] for a in bets) / total
    base   = wins + losses
    return {
        'total': total, 'wins': wins, 'losses': losses, 'voids': voids,
        'pnl': pnl, 'ev_esp': ev_esp,
        'hit_rate': wins / base if base else 0.0,
        'roi_real': pnl / (total * STAKE) if total else 0.0,
        'roi_ev':   ev_esp / (total * STAKE) if total else 0.0,
        'edge_m': edge_m, 'odds_m': odds_m,
    }


# ---------------------------------------------------------------------------
# Impresion de tablas
# ---------------------------------------------------------------------------

def _fmt(v, pct=False, sign=True):
    if v is None: return '  -  '
    if pct:
        return f"{'+' if sign and v>=0 else ''}{v:.1%}"
    return f"{'+' if sign and v>=0 else ''}{v:.3f}u"


def _titulo(txt):
    print()
    print(SEP)
    print(f"  {txt}")
    print(SEP)


def _tabla_por_grupos(grupos, orden=None, min_apuestas=1,
                      col_label='Grupo', col_width=22):
    """Imprime tabla de rendimiento para un dict {nombre: [apuestas]}."""
    keys = orden if orden else sorted(grupos.keys())

    hdr = (f"  {col_label:<{col_width}}  {'N':>4}  {'W':>4}  {'L':>4}  "
           f"{'Hit%':>6}  {'Odds':>5}  {'Edge%':>6}  "
           f"{'P&L':>8}  {'EV':>8}  {'ROI%':>6}  {'dROI':>6}")
    print(hdr)
    print(f"  {SEP2}")

    for k in keys:
        bets = grupos.get(k, [])
        if len(bets) < min_apuestas:
            continue
        s = _stats(bets)
        delta = s['roi_real'] - s['roi_ev']
        print(
            f"  {str(k):<{col_width}}  {s['total']:>4}  {s['wins']:>4}  {s['losses']:>4}  "
            f"{s['hit_rate']:>5.1%}  {s['odds_m']:>5.2f}  {s['edge_m']:>5.1%}  "
            f"{_fmt(s['pnl']):>8}  {_fmt(s['ev_esp']):>8}  "
            f"{s['roi_real']:>+5.1%}  {delta:>+5.1%}"
        )


def _tabla_calibracion(bets, min_apuestas=5):
    """Tabla win rate real vs prob predicha por rango de prob."""
    grupos = defaultdict(list)
    for a in bets:
        rp = a['rango_prob']
        if rp:
            grupos[rp].append(a)

    print(f"  {'Rango prob':<12}  {'N':>4}  {'Win% real':>10}  "
          f"{'Prob media':>10}  {'Delta':>7}  {'ROI%':>6}")
    print(f"  {SEP2}")

    for rango in _ORDEN_PROBS:
        g = grupos.get(rango, [])
        if len(g) < min_apuestas:
            continue
        wins  = sum(1 for a in g if a['resultado'] == 'W')
        base  = sum(1 for a in g if a['resultado'] in ('W', 'L'))
        wr    = wins / base if base else 0.0
        probs = [a['modelo_prob'] for a in g if a['modelo_prob'] is not None]
        pm    = sum(probs) / len(probs) if probs else 0.0
        delta = wr - pm
        roi   = _stats(g)['roi_real']
        flag  = '  <<' if abs(delta) > 0.05 else ''
        print(f"  {rango:<12}  {len(g):>4}  {wr:>9.1%}  "
              f"{pm:>9.1%}  {delta:>+6.1%}  {roi:>+5.1%}{flag}")


def _tabla_bins_ev(bets, campo_rango, orden, titulo_col, min_apuestas=3):
    """
    Tabla de calibracion por bins de EV o edge.
    Muestra N, Win%, ROI real, EV medio y tendencia.
    La idea: a mayor bin → mayor ROI real. Si no ocurre, el modelo rankea mal.
    """
    grupos = defaultdict(list)
    for a in bets:
        k = a.get(campo_rango)
        if k:
            grupos[k].append(a)

    print(f"  {titulo_col:<10}  {'N':>4}  {'Win%':>7}  {'ROI real':>9}  "
          f"{'EV medio':>9}  {'dROI':>7}  Tendencia")
    print(f"  {SEP2}")

    prev_roi = None
    for rango in orden:
        g = grupos.get(rango, [])
        if len(g) < min_apuestas:
            continue
        s   = _stats(g)
        evs = [a['ev'] for a in g if a['ev'] is not None]
        ev_m = sum(evs) / len(evs) if evs else 0.0
        delta = s['roi_real'] - s['roi_ev']

        # Tendencia: sube/baja/igual respecto al bin anterior
        if prev_roi is None:
            tendencia = '  --'
        elif s['roi_real'] > prev_roi + 0.005:
            tendencia = '  up'
        elif s['roi_real'] < prev_roi - 0.005:
            tendencia = '  DOWN  <<'
        else:
            tendencia = '  ~'
        prev_roi = s['roi_real']

        print(f"  {rango:<10}  {s['total']:>4}  {s['hit_rate']:>6.1%}  "
              f"{s['roi_real']:>+8.1%}  {ev_m:>+8.1%}  {delta:>+6.1%}{tendencia}")

    print()
    print("  Esperado: ROI sube al subir el bin.")
    print("  'DOWN <<' indica que el modelo NO rankea bien ese tramo.")


def _barra(val, max_val=0.40, width=18):
    filled = int(min(abs(val) / max_val, 1.0) * width)
    bar = '#' * filled + '.' * (width - filled)
    return f"[{bar}]" if val >= 0 else f"[{bar}](-)"


# ---------------------------------------------------------------------------
# Reporte por variable individual
# ---------------------------------------------------------------------------

_SEP_VAR = '█' * 76

_ORDEN_CATS = ['Goles', 'Tiros', 'Arco', 'Corners', 'Tarjetas', 'BTTS', '1X2', 'Otros']
_ORDEN_TIPO = ['Ligas locales', 'Copas europeas', 'Copas sudamericanas',
               'Copas domesticas', 'Otros']


def _reporte_una_variable(cat_nombre, bets, min_apuestas=3):
    """
    Ejecuta el analisis completo de rendimiento para una sola categoria
    de mercado: cuota, calibracion prob, EV, edge, modelo, torneo, competicion.
    """
    s = _stats(bets)
    if s is None:
        return

    # ── Encabezado de variable ──────────────────────────────────────────────
    print()
    print(_SEP_VAR)
    print(f"  VARIABLE: {cat_nombre.upper()}")
    print(f"  {s['total']} apuestas  ({s['wins']}W / {s['losses']}L / {s['voids']}V)  "
          f"|  Odds medias: {s['odds_m']:.2f}  |  Edge medio: {s['edge_m']:+.1%}")
    print(f"  ROI real: {s['roi_real']:+.1%}   EV esperado: {s['roi_ev']:+.1%}   "
          f"dROI: {(s['roi_real'] - s['roi_ev']):+.1%}   P&L: {_fmt(s['pnl'])}")
    print(_SEP_VAR)

    # Para sub-tablas con pocos datos usamos min_apuestas reducido
    min_sub = max(1, min_apuestas // 2)

    # ── A. Por rango de cuota ───────────────────────────────────────────────
    print()
    print(f"  A. ROI POR RANGO DE CUOTA — {cat_nombre}")
    print(f"  {SEP2}")
    by_cuota = defaultdict(list)
    for a in bets:
        by_cuota[a['rango_cuota']].append(a)
    _tabla_por_grupos(by_cuota, orden=_ORDEN_CUOTAS, min_apuestas=min_sub,
                      col_label='Rango cuota', col_width=12)
    # Grafico
    print()
    for rango in _ORDEN_CUOTAS:
        g = by_cuota.get(rango)
        if not g or len(g) < min_sub:
            continue
        st = _stats(g)
        print(f"  {rango:<12}  {_barra(st['roi_real'])}  {st['roi_real']:>+.1%}  (n={st['total']})")

    # ── B. Calibracion de probabilidades ────────────────────────────────────
    print()
    print(f"  B. CALIBRACION DE PROBABILIDADES — {cat_nombre}")
    print(f"  {'Rango prob':<12}  {'N':>4}  {'Win% real':>10}  "
          f"{'Prob media':>10}  {'Delta':>7}  {'ROI%':>6}")
    print(f"  {SEP2}")
    grp_prob = defaultdict(list)
    for a in bets:
        rp = a['rango_prob']
        if rp:
            grp_prob[rp].append(a)
    for rango in _ORDEN_PROBS:
        g = grp_prob.get(rango, [])
        if len(g) < min_sub:
            continue
        wins  = sum(1 for a in g if a['resultado'] == 'W')
        base  = sum(1 for a in g if a['resultado'] in ('W', 'L'))
        wr    = wins / base if base else 0.0
        probs = [a['modelo_prob'] for a in g if a['modelo_prob'] is not None]
        pm    = sum(probs) / len(probs) if probs else 0.0
        delta = wr - pm
        roi   = _stats(g)['roi_real']
        flag  = '  <<' if abs(delta) > 0.05 else ''
        print(f"  {rango:<12}  {len(g):>4}  {wr:>9.1%}  "
              f"{pm:>9.1%}  {delta:>+6.1%}  {roi:>+5.1%}{flag}")

    # ── C. Calibracion del EV ───────────────────────────────────────────────
    print()
    print(f"  C. CALIBRACION DEL EV  (mayor EV predicho -> mayor ROI real?) — {cat_nombre}")
    print(f"  {SEP2}")
    _tabla_bins_ev(bets, 'rango_ev', _ORDEN_EV,
                   'EV predicho', min_apuestas=min_sub)

    # ── D. Calibracion del edge ─────────────────────────────────────────────
    print()
    print(f"  D. CALIBRACION DEL EDGE  (P_modelo - P_implied) — {cat_nombre}")
    print(f"  {SEP2}")
    _tabla_bins_ev(bets, 'rango_edge', _ORDEN_EDGE,
                   'Edge prob', min_apuestas=min_sub)

    # ── E. Por version del modelo ────────────────────────────────────────────
    print()
    print(f"  E. POR VERSION DEL MODELO — {cat_nombre}")
    print(f"  {SEP2}")
    by_met = defaultdict(list)
    for a in bets:
        by_met[a['metodo']].append(a)
    _tabla_por_grupos(by_met, orden=sorted(by_met.keys()), min_apuestas=1,
                      col_label='Modelo', col_width=10)

    # ── F. Por tipo de torneo ────────────────────────────────────────────────
    print()
    print(f"  F. POR TIPO DE TORNEO — {cat_nombre}")
    print(f"  {SEP2}")
    by_tipo = defaultdict(list)
    for a in bets:
        by_tipo[a['tipo_torneo']].append(a)
    _tabla_por_grupos(by_tipo, orden=_ORDEN_TIPO, min_apuestas=1,
                      col_label='Tipo torneo', col_width=22)

    # ── G. Por competicion ───────────────────────────────────────────────────
    print()
    print(f"  G. POR COMPETICION — {cat_nombre}")
    print(f"  {SEP2}")
    by_comp = defaultdict(list)
    for a in bets:
        comp = a['competicion'] or 'Sin datos'
        by_comp[comp].append(a)
    orden_comp = sorted(by_comp.keys(), key=lambda k: -len(by_comp[k]))
    _tabla_por_grupos(by_comp, orden=orden_comp, min_apuestas=1,
                      col_label='Competicion', col_width=26)

    print()


def reporte_por_variable(apuestas, min_apuestas=3):
    """
    Corre el analisis completo (A-G) para cada categoria de mercado por separado.
    Solo muestra categorias con al menos `min_apuestas` apuestas resueltas.
    """
    print()
    print(_SEP_VAR)
    print("  ANALISIS DETALLADO POR VARIABLE DE MERCADO")
    print("  Cada bloque repite las 7 sub-secciones del reporte global")
    print("  pero filtrado exclusivamente a esa categoria.")
    print(_SEP_VAR)

    by_cat = defaultdict(list)
    for a in apuestas:
        by_cat[a['categoria']].append(a)

    for cat in _ORDEN_CATS:
        bets = by_cat.get(cat, [])
        if len(bets) < min_apuestas:
            print(f"\n  [{cat}]  solo {len(bets)} apuesta(s) resuelta(s) — se omite "
                  f"(umbral: {min_apuestas})")
            continue
        _reporte_una_variable(cat, bets, min_apuestas=min_apuestas)

    print(_SEP_VAR)
    print()


# ---------------------------------------------------------------------------
# Reporte principal
# ---------------------------------------------------------------------------

def reporte(apuestas, csv_path, min_apuestas=3):

    if not apuestas:
        print("No hay apuestas resueltas todavia.")
        return

    s_total = _stats(apuestas)

    # ──────────────────────────────────────────────────────────────────────
    # ENCABEZADO
    # ──────────────────────────────────────────────────────────────────────
    print()
    print(SEP)
    print(f"  ANALISIS DE RENDIMIENTO  |  stake={STAKE:.0f}u  |  {csv_path.name}")
    print(f"  Total apuestas resueltas : {s_total['total']}  "
          f"({s_total['wins']}W / {s_total['losses']}L / {s_total['voids']}V)")
    print(f"  P&L total : {_fmt(s_total['pnl'])}   "
          f"ROI real: {s_total['roi_real']:+.1%}   "
          f"EV esp.: {s_total['roi_ev']:+.1%}")
    print(SEP)

    # ──────────────────────────────────────────────────────────────────────
    # 1. POR CATEGORIA DE MERCADO
    # ──────────────────────────────────────────────────────────────────────
    _titulo("1. RENDIMIENTO POR CATEGORIA DE MERCADO")
    orden_cats = ['Goles','Tiros','Arco','Corners','Tarjetas','BTTS','1X2','Otros']
    by_cat = defaultdict(list)
    for a in apuestas:
        by_cat[a['categoria']].append(a)
    _tabla_por_grupos(by_cat, orden=orden_cats, min_apuestas=min_apuestas,
                      col_label='Categoria', col_width=12)
    print(f"  {SEP2}")
    s = s_total
    print(f"  {'TOTAL':<12}  {s['total']:>4}  {s['wins']:>4}  {s['losses']:>4}  "
          f"{s['hit_rate']:>5.1%}  {s['odds_m']:>5.2f}  {s['edge_m']:>5.1%}  "
          f"{_fmt(s['pnl']):>8}  {_fmt(s['ev_esp']):>8}  "
          f"{s['roi_real']:>+5.1%}  {(s['roi_real']-s['roi_ev']):>+5.1%}")

    # Grafico barras
    print()
    print("  ROI real vs EV por categoria:")
    for cat in orden_cats:
        g = by_cat.get(cat)
        if not g or len(g) < min_apuestas:
            continue
        st = _stats(g)
        print(f"  {cat:<12}  Real {_barra(st['roi_real'])}  {st['roi_real']:>+.1%}")
        print(f"  {'':12}  EV   {_barra(st['roi_ev'])}  {st['roi_ev']:>+.1%}")

    # ──────────────────────────────────────────────────────────────────────
    # 2. POR RANGO DE CUOTA
    # ──────────────────────────────────────────────────────────────────────
    _titulo("2. RENDIMIENTO POR RANGO DE CUOTA")
    by_cuota = defaultdict(list)
    for a in apuestas:
        by_cuota[a['rango_cuota']].append(a)
    _tabla_por_grupos(by_cuota, orden=_ORDEN_CUOTAS, min_apuestas=min_apuestas,
                      col_label='Rango cuota', col_width=12)

    print()
    print("  ROI real por rango de cuota:")
    for rango in _ORDEN_CUOTAS:
        g = by_cuota.get(rango)
        if not g or len(g) < min_apuestas:
            continue
        st = _stats(g)
        print(f"  {rango:<12}  {_barra(st['roi_real'])}  {st['roi_real']:>+.1%}  "
              f"(n={st['total']})")

    # ──────────────────────────────────────────────────────────────────────
    # 3. CALIBRACION DE PROBABILIDADES
    # ──────────────────────────────────────────────────────────────────────
    _titulo("3. CALIBRACION DEL MODELO (win rate real vs prob predicha)")
    _tabla_calibracion(apuestas, min_apuestas=min_apuestas)
    print()
    print("  '<<' indica descalibracion > 5pp (el modelo sobre/subestima esa franja)")

    # ──────────────────────────────────────────────────────────────────────
    # 4. CALIBRACION DEL EV — ranking de oportunidades
    # ──────────────────────────────────────────────────────────────────────
    _titulo("4. CALIBRACION DEL EV  (mayor EV predicho -> mayor ROI real?)")
    print("  EV% = (P_modelo * odds - 1).  Mide si el modelo rankea bien")
    print("  las oportunidades: apuestas con mas EV deben dar mas ROI real.")
    print()
    _tabla_bins_ev(apuestas, 'rango_ev', _ORDEN_EV,
                  'EV predicho', min_apuestas=min_apuestas)

    # Por modelo
    by_metodo_ev = defaultdict(list)
    for a in apuestas:
        by_metodo_ev[a['metodo']].append(a)
    for metodo in sorted(by_metodo_ev.keys()):
        g = by_metodo_ev[metodo]
        if len(g) < min_apuestas * 2:
            continue
        print(f"  -- {metodo} --")
        _tabla_bins_ev(g, 'rango_ev', _ORDEN_EV,
                      'EV predicho', min_apuestas=min_apuestas)

    # ──────────────────────────────────────────────────────────────────────
    # 5. CALIBRACION DEL EDGE (P_modelo - P_mercado)
    # ──────────────────────────────────────────────────────────────────────
    _titulo("5. CALIBRACION DEL EDGE  (P_modelo - P_implied)")
    print("  Edge = P_modelo - 1/odds.  A mayor edge -> mayor ventaja sobre")
    print("  el mercado.  Si ROI no sube con el edge, el modelo sobreestima.")
    print()
    _tabla_bins_ev(apuestas, 'rango_edge', _ORDEN_EDGE,
                  'Edge prob', min_apuestas=min_apuestas)

    # ──────────────────────────────────────────────────────────────────────
    # 6. POR VERSION DEL MODELO
    # ──────────────────────────────────────────────────────────────────────
    _titulo("6. RENDIMIENTO POR VERSION DEL MODELO")
    by_metodo = defaultdict(list)
    for a in apuestas:
        by_metodo[a['metodo']].append(a)

    orden_met = sorted(by_metodo.keys())
    _tabla_por_grupos(by_metodo, orden=orden_met, min_apuestas=1,
                      col_label='Modelo', col_width=10)

    # Calibracion de prob por modelo
    for metodo in orden_met:
        g = by_metodo[metodo]
        if len(g) < min_apuestas:
            continue
        print()
        print(f"  Calibracion prob {metodo}:")
        _tabla_calibracion(g, min_apuestas=min_apuestas)

    # ──────────────────────────────────────────────────────────────────────
    # 7. POR TIPO DE TORNEO
    # ──────────────────────────────────────────────────────────────────────
    _titulo("7. RENDIMIENTO POR TIPO DE TORNEO")
    by_tipo = defaultdict(list)
    for a in apuestas:
        by_tipo[a['tipo_torneo']].append(a)

    orden_tipo = ['Ligas locales','Copas europeas','Copas sudamericanas',
                  'Copas domesticas','Otros']
    _tabla_por_grupos(by_tipo, orden=orden_tipo, min_apuestas=min_apuestas,
                      col_label='Tipo torneo', col_width=22)

    # ──────────────────────────────────────────────────────────────────────
    # 8. POR COMPETICION INDIVIDUAL
    # ──────────────────────────────────────────────────────────────────────
    _titulo("8. RENDIMIENTO POR COMPETICION")
    by_comp = defaultdict(list)
    for a in apuestas:
        comp = a['competicion'] or 'Sin datos'
        by_comp[comp].append(a)

    orden_comp = sorted(by_comp.keys(), key=lambda k: -len(by_comp[k]))
    _tabla_por_grupos(by_comp, orden=orden_comp, min_apuestas=min_apuestas,
                      col_label='Competicion', col_width=26)

    print()
    print(SEP)
    print()

    # ── 9. ANALISIS POR VARIABLE ─────────────────────────────────────────────
    reporte_por_variable(apuestas, min_apuestas=min_apuestas)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    csv_path     = VB_CSV
    min_apuestas = 3

    args = sys.argv[1:]
    if '--csv' in args:
        idx = args.index('--csv')
        csv_path = Path(args[idx + 1])
    if '--min-apuestas' in args:
        idx = args.index('--min-apuestas')
        min_apuestas = int(args[idx + 1])

    if not csv_path.exists():
        print(f"Archivo no encontrado: {csv_path}")
        sys.exit(1)

    apuestas = cargar_apuestas(csv_path)

    if not apuestas:
        print("No hay apuestas resueltas (W/L/V) en el CSV todavia.")
        sys.exit(0)

    reporte(apuestas, csv_path, min_apuestas=min_apuestas)
