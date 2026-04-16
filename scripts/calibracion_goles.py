"""
calibracion_goles.py
--------------------
Análisis de calibración profundo del modelo de goles.
Cruza pronosticos.csv + value_bets.csv con resultados reales del histórico
para maximizar la muestra de datos.

Para cada pronóstico con fixture_id en el histórico, determina W/L real
y compara probabilidad predicha vs win rate real.

Separa por: Goles Totales, Goles Local, Goles Visita, BTTS, 1X2

Uso:
    python scripts/calibracion_goles.py
"""

import csv
import sys
import re
import math
from pathlib import Path
from collections import defaultdict, Counter

sys.path.insert(0, str(Path(__file__).parent))
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

PRONOSTICOS_CSV = Path(r'C:\Users\Matt\Apuestas Deportivas\data\apuestas\pronosticos.csv')
VB_CSV          = Path(r'C:\Users\Matt\Apuestas Deportivas\data\apuestas\value_bets.csv')
HIST_CSV        = Path(r'C:\Users\Matt\Apuestas Deportivas\data\historico\partidos_historicos.csv')

SEP  = '=' * 80
SEP2 = '-' * 80

# ─────────────────────────────────────────────────────────────────────────────
# Cargar resultados reales
# ─────────────────────────────────────────────────────────────────────────────

def cargar_resultados():
    """Carga partidos_historicos y devuelve dict fixture_id → resultado."""
    with open(HIST_CSV, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    resultados = {}
    for r in rows:
        fid = r['fixture_id']
        try:
            resultados[fid] = {
                'goles_local':     int(r['goles_local']),
                'goles_visitante': int(r['goles_visitante']),
                'goles_total':     int(r['goles_local']) + int(r['goles_visitante']),
            }
        except (ValueError, KeyError):
            continue
    return resultados


# ─────────────────────────────────────────────────────────────────────────────
# Clasificar mercado y determinar resultado
# ─────────────────────────────────────────────────────────────────────────────

def clasificar_mercado(mercado, lado, partido=''):
    """
    Devuelve (categoria, sub_tipo) donde:
    - categoria: 'Goles Totales', 'Goles Local', 'Goles Visita', 'BTTS', '1X2'
    - sub_tipo: descripción detallada (e.g., 'Over 2.5', 'Under 1.5', 'Si', '1', 'X', '2')
    """
    m = mercado.lower().strip()
    l = lado.strip()

    # 1X2
    if '1x2' in m:
        if 'empate' in m:
            return '1X2', 'Empate (X)'
        else:
            # Determinar si es local o visitante
            partes = partido.split(' vs ', 1)
            if len(partes) == 2:
                team_local = partes[0].strip().lower()
                # Check if local team name is in mercado
                for word in team_local.split():
                    if len(word) > 3 and word in m:
                        return '1X2', 'Local gana (1)'
            return '1X2', 'Visita gana (2)'

    # BTTS
    if 'btts' in m:
        side = 'Si' if 'Over' in l or 'Si' in l else 'No'
        return 'BTTS', f'BTTS {side}'

    # Goles O/U
    thr_match = re.search(r'(\d+\.5)', mercado)
    thr = thr_match.group(1) if thr_match else '?'
    side = 'Over' if 'Over' in l or 'Si' in l else 'Under'

    if 'tot.' in m:
        return 'Goles Totales', f'{side} {thr}'

    # Goles equipo individual - determinar si es local o visita
    partes = partido.split(' vs ', 1)
    if len(partes) == 2:
        team_local = partes[0].strip().lower()
        # Buscar si alguna palabra del local aparece en el mercado
        for word in team_local.split():
            if len(word) > 3 and word in m:
                return 'Goles Local', f'{side} {thr}'
    return 'Goles Visita', f'{side} {thr}'


def determinar_resultado(mercado, lado, partido, resultado_real):
    """Dado un mercado y el resultado real del partido, determina W/L."""
    gl = resultado_real['goles_local']
    gv = resultado_real['goles_visitante']
    gt = resultado_real['goles_total']

    m = mercado.lower().strip()
    l = lado.strip()

    # 1X2
    if '1x2' in m:
        if 'empate' in m:
            return 'W' if gl == gv else 'L'
        partes = partido.split(' vs ', 1)
        if len(partes) == 2:
            team_local = partes[0].strip().lower()
            for word in team_local.split():
                if len(word) > 3 and word in m:
                    return 'W' if gl > gv else 'L'
        return 'W' if gv > gl else 'L'

    # BTTS
    if 'btts' in m:
        btts = gl > 0 and gv > 0
        if 'Over' in l or 'Si' in l:
            return 'W' if btts else 'L'
        return 'W' if not btts else 'L'

    # Goles O/U
    thr_match = re.search(r'(\d+\.5)', mercado)
    if not thr_match:
        return None
    thr = float(thr_match.group(1))
    is_over = 'Over' in l or 'Si' in l

    if 'tot.' in m:
        val = gt
    else:
        partes = partido.split(' vs ', 1)
        is_local = False
        if len(partes) == 2:
            team_local = partes[0].strip().lower()
            for word in team_local.split():
                if len(word) > 3 and word in m:
                    is_local = True
                    break
        val = gl if is_local else gv

    if is_over:
        return 'W' if val > thr else 'L'
    else:
        return 'W' if val < thr else 'L'


# ─────────────────────────────────────────────────────────────────────────────
# Cargar todas las predicciones y resolver resultados
# ─────────────────────────────────────────────────────────────────────────────

def cargar_predicciones(resultados):
    """
    Combina pronosticos.csv y value_bets.csv.
    Prioriza resultado de value_bets si existe, sino calcula desde histórico.
    """
    predicciones = []
    seen = set()  # (fixture_id, mercado, lado) para evitar duplicados

    # 1) value_bets.csv - tienen resultado directo
    with open(VB_CSV, newline='', encoding='utf-8') as f:
        vb_rows = list(csv.DictReader(f))

    for r in vb_rows:
        mercado = r.get('mercado', '')
        lado = r.get('lado', '')

        # Solo mercados de goles
        if not ('gol' in mercado.lower() or 'btts' in mercado.lower() or '1x2' in mercado.lower()):
            continue

        resultado = r.get('resultado', '').strip().upper()
        prob = float(r.get('modelo_prob', 0)) if r.get('modelo_prob') else None
        if prob is None or prob <= 0:
            continue

        fid = r['fixture_id']
        key = (fid, mercado, lado)

        # Si ya fue resuelto, usar ese resultado
        if resultado in ('W', 'L', 'V'):
            res = resultado
        elif fid in resultados:
            res = determinar_resultado(mercado, lado, r.get('partido', ''), resultados[fid])
        else:
            continue

        if res is None or res == 'V':
            continue

        cat, sub = clasificar_mercado(mercado, lado, r.get('partido', ''))
        odds = float(r.get('odds', 0)) if r.get('odds') else None
        ip = float(r.get('implied_prob', 0)) if r.get('implied_prob') else (1.0/odds if odds else None)

        predicciones.append({
            'fixture_id': fid,
            'partido':    r.get('partido', ''),
            'mercado':    mercado,
            'lado':       lado,
            'prob':       prob,
            'odds':       odds,
            'implied_prob': ip,
            'resultado':  res,
            'categoria':  cat,
            'sub_tipo':   sub,
            'fuente':     'value_bets',
        })
        seen.add(key)

    # 2) pronosticos.csv - resolver resultado desde histórico
    with open(PRONOSTICOS_CSV, newline='', encoding='utf-8') as f:
        pro_rows = list(csv.DictReader(f))

    for r in pro_rows:
        mercado = r.get('mercado', '')
        lado = r.get('lado', '')

        if not ('gol' in mercado.lower() or 'btts' in mercado.lower() or '1x2' in mercado.lower()):
            continue

        fid = r['fixture_id']
        key = (fid, mercado, lado)
        if key in seen:
            continue

        prob = float(r.get('modelo_prob', 0)) if r.get('modelo_prob') else None
        if prob is None or prob <= 0:
            continue

        if fid not in resultados:
            continue

        res = determinar_resultado(mercado, lado, r.get('partido', ''), resultados[fid])
        if res is None:
            continue

        cat, sub = clasificar_mercado(mercado, lado, r.get('partido', ''))
        odds = float(r.get('odds', 0)) if r.get('odds') else None
        ip = float(r.get('implied_prob', 0)) if r.get('implied_prob') else (1.0/odds if odds else None)

        predicciones.append({
            'fixture_id': fid,
            'partido':    r.get('partido', ''),
            'mercado':    mercado,
            'lado':       lado,
            'prob':       prob,
            'odds':       odds,
            'implied_prob': ip,
            'resultado':  res,
            'categoria':  cat,
            'sub_tipo':   sub,
            'fuente':     'pronosticos',
        })
        seen.add(key)

    return predicciones


# ─────────────────────────────────────────────────────────────────────────────
# Análisis de calibración
# ─────────────────────────────────────────────────────────────────────────────

def calibracion(preds, titulo, rangos=None, step=0.05):
    """Calibración: agrupa por prob predicha y compara con win rate real."""
    if not preds:
        print(f"\n  {titulo}: sin datos")
        return

    if rangos is None:
        # Rangos finos de 5%
        rangos = []
        p = 0.0
        while p < 1.0:
            rangos.append((p, p + step))
            p += step
            p = round(p, 2)

    wins = sum(1 for p in preds if p['resultado'] == 'W')
    total = len(preds)
    probs = [p['prob'] for p in preds]
    avg_prob = sum(probs) / len(probs)

    print(f"\n  {titulo}")
    print(f"  N={total}  W={wins}  L={total-wins}  Win%={wins/total:.1%}  Prob media={avg_prob:.1%}")
    print(f"  {SEP2}")
    print(f"  {'Rango prob':<14}  {'N':>5}  {'Win':>4}  {'Win% real':>10}  "
          f"{'Prob media':>11}  {'Delta':>7}  {'Brier':>7}  Calibración")
    print(f"  {SEP2}")

    brier_total = 0
    n_brier = 0
    all_deltas = []

    for lo, hi in rangos:
        bucket = [p for p in preds if lo <= p['prob'] < hi]
        if not bucket:
            continue

        n = len(bucket)
        w = sum(1 for p in bucket if p['resultado'] == 'W')
        wr = w / n
        pm = sum(p['prob'] for p in bucket) / n
        delta = wr - pm

        # Brier score contribution
        for p in bucket:
            outcome = 1 if p['resultado'] == 'W' else 0
            brier_total += (p['prob'] - outcome) ** 2
            n_brier += 1

        all_deltas.append((pm, wr, n, delta))

        # Visual calibration
        if abs(delta) < 0.03:
            cal = '  ✓ bien calibrado'
        elif delta > 0.08:
            cal = f'  ↑ SUBESTIMA  (+{delta:.0%})'
        elif delta < -0.08:
            cal = f'  ↓ SOBRESTIMA ({delta:.0%})'
        elif delta > 0:
            cal = f'  ↑ subestima leve'
        else:
            cal = f'  ↓ sobrestima leve'

        label = f'{lo:.0%}-{hi:.0%}'
        print(f"  {label:<14}  {n:>5}  {w:>4}  {wr:>9.1%}  "
              f"{pm:>10.1%}  {delta:>+6.1%}  {sum((p['prob'] - (1 if p['resultado']=='W' else 0))**2 for p in bucket)/n:>6.4f}{cal}")

    brier = brier_total / n_brier if n_brier else 0
    print(f"\n  Brier Score: {brier:.4f}  (0=perfecto, 0.25=random)")

    # Resumen de calibración
    if all_deltas:
        mae = sum(abs(d[3]) * d[2] for d in all_deltas) / sum(d[2] for d in all_deltas)
        bias = sum(d[3] * d[2] for d in all_deltas) / sum(d[2] for d in all_deltas)
        print(f"  MAE calibración: {mae:.3f}  (error absoluto medio ponderado)")
        print(f"  Bias: {bias:+.3f}  (positivo=subestima, negativo=sobrestima)")

    return brier


def calibracion_por_subtipo(preds, categoria, titulo):
    """Calibración separada por sub-tipo dentro de una categoría."""
    subset = [p for p in preds if p['categoria'] == categoria]
    if not subset:
        return

    print(f"\n{'█' * 80}")
    print(f"  {titulo}")
    print(f"  Total: {len(subset)} predicciones")
    print(f"{'█' * 80}")

    # Calibración global de la categoría
    rangos_amplios = [
        (0.00, 0.10), (0.10, 0.20), (0.20, 0.30), (0.30, 0.40),
        (0.40, 0.50), (0.50, 0.60), (0.60, 0.70), (0.70, 0.80),
        (0.80, 0.90), (0.90, 1.00),
    ]
    calibracion(subset, f'{titulo} — GLOBAL', rangos_amplios)

    # Por sub-tipo
    by_sub = defaultdict(list)
    for p in subset:
        by_sub[p['sub_tipo']].append(p)

    # Ordenar sub-tipos
    orden = sorted(by_sub.keys())
    for sub in orden:
        sub_preds = by_sub[sub]
        if len(sub_preds) < 5:
            continue
        calibracion(sub_preds, f'{titulo} — {sub}', rangos_amplios)

    # Tabla resumen por sub-tipo
    print(f"\n  RESUMEN POR SUB-TIPO")
    print(f"  {SEP2}")
    print(f"  {'Sub-tipo':<20}  {'N':>5}  {'Win%':>7}  {'Prob med':>8}  "
          f"{'Delta':>7}  {'Brier':>7}  {'MAE':>6}")
    print(f"  {SEP2}")

    for sub in orden:
        sub_preds = by_sub[sub]
        if len(sub_preds) < 3:
            continue
        n = len(sub_preds)
        w = sum(1 for p in sub_preds if p['resultado'] == 'W')
        wr = w / n
        pm = sum(p['prob'] for p in sub_preds) / n
        delta = wr - pm
        brier = sum((p['prob'] - (1 if p['resultado']=='W' else 0))**2 for p in sub_preds) / n
        print(f"  {sub:<20}  {n:>5}  {wr:>6.1%}  {pm:>7.1%}  "
              f"{delta:>+6.1%}  {brier:>6.4f}  {abs(delta):>5.3f}")


def diagrama_calibracion(preds, titulo):
    """Diagrama visual de calibración (pred vs real)."""
    print(f"\n  DIAGRAMA DE CALIBRACIÓN — {titulo}")
    print(f"  {SEP2}")
    print(f"  (Línea diagonal = calibración perfecta)")
    print()

    rangos = [(i/10, (i+1)/10) for i in range(10)]
    WIDTH = 50

    print(f"  {'Pred':>8}  {'Real':>6}  {'N':>5}  Diagrama")
    print(f"  {SEP2}")

    for lo, hi in rangos:
        bucket = [p for p in preds if lo <= p['prob'] < hi]
        if not bucket:
            continue

        n = len(bucket)
        w = sum(1 for p in bucket if p['resultado'] == 'W')
        wr = w / n
        pm = sum(p['prob'] for p in bucket) / n

        # Bar: ideal position vs actual
        ideal_pos = int(pm * WIDTH)
        actual_pos = int(wr * WIDTH)

        bar = list('.' * WIDTH)
        bar[ideal_pos] = '|'  # ideal
        bar[actual_pos] = '#'  # actual
        if ideal_pos == actual_pos:
            bar[ideal_pos] = '@'  # perfect

        label = f'{lo:.0%}-{hi:.0%}'
        print(f"  {pm:>7.1%}  {wr:>5.1%}  {n:>5}  [{''.join(bar)}]  "
              f"{'<<' if abs(wr-pm) > 0.08 else ''}")

    print(f"\n  Leyenda: | = predicho  # = real  @ = coinciden")


def analisis_over_under_por_linea(preds, titulo):
    """Análisis detallado: calibración por línea (0.5, 1.5, 2.5, etc.)."""
    print(f"\n  {titulo} — OVER vs UNDER POR LÍNEA")
    print(f"  {SEP2}")
    print(f"  {'Línea':<8}  {'Lado':<7}  {'N':>5}  {'Win%':>7}  {'Prob med':>8}  "
          f"{'Delta':>7}  {'Brier':>7}  Calibración")
    print(f"  {SEP2}")

    by_line = defaultdict(list)
    for p in preds:
        thr_match = re.search(r'(\d+\.5)', p['mercado'])
        if not thr_match:
            continue
        thr = thr_match.group(1)
        side = 'Over' if 'Over' in p['lado'] or 'Si' in p['lado'] else 'Under'
        by_line[(thr, side)].append(p)

    for (thr, side) in sorted(by_line.keys(), key=lambda x: (float(x[0]), x[1])):
        bucket = by_line[(thr, side)]
        if len(bucket) < 3:
            continue
        n = len(bucket)
        w = sum(1 for p in bucket if p['resultado'] == 'W')
        wr = w / n
        pm = sum(p['prob'] for p in bucket) / n
        delta = wr - pm
        brier = sum((p['prob'] - (1 if p['resultado']=='W' else 0))**2 for p in bucket) / n

        if abs(delta) < 0.03:
            cal = '✓'
        elif delta > 0.08:
            cal = f'↑ SUBESTIMA'
        elif delta < -0.08:
            cal = f'↓ SOBRESTIMA'
        elif delta > 0:
            cal = '↑ leve'
        else:
            cal = '↓ leve'

        print(f"  {thr:<8}  {side:<7}  {n:>5}  {wr:>6.1%}  {pm:>7.1%}  "
              f"{delta:>+6.1%}  {brier:>6.4f}  {cal}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print("Cargando datos...")
    resultados = cargar_resultados()
    print(f"  {len(resultados)} partidos con resultado en histórico")

    preds = cargar_predicciones(resultados)
    print(f"  {len(preds)} predicciones de goles con resultado resuelto")

    # Conteo por fuente
    by_fuente = Counter(p['fuente'] for p in preds)
    print(f"  Fuentes: {dict(by_fuente)}")

    by_cat = Counter(p['categoria'] for p in preds)
    print(f"\n  Por categoría:")
    for cat, n in by_cat.most_common():
        w = sum(1 for p in preds if p['categoria'] == cat and p['resultado'] == 'W')
        print(f"    {cat:<20} {n:>5} preds  ({w}W / {n-w}L)")

    # ══════════════════════════════════════════════════════════════════════════
    # RESUMEN GLOBAL
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print(f"  CALIBRACIÓN DEL MODELO DE GOLES — ANÁLISIS COMPLETO")
    print(f"  Datos: pronosticos.csv + value_bets.csv cruzados con histórico")
    print(SEP)

    rangos_10 = [(i/10, (i+1)/10) for i in range(10)]

    # Global todas las predicciones
    calibracion(preds, 'TODAS LAS PREDICCIONES DE GOLES', rangos_10)
    diagrama_calibracion(preds, 'GLOBAL')

    # ══════════════════════════════════════════════════════════════════════════
    # POR CATEGORÍA
    # ══════════════════════════════════════════════════════════════════════════
    categorias = ['Goles Totales', 'Goles Local', 'Goles Visita', 'BTTS', '1X2']

    for cat in categorias:
        calibracion_por_subtipo(preds, cat, cat.upper())

    # ══════════════════════════════════════════════════════════════════════════
    # OVER vs UNDER POR LÍNEA (solo O/U markets)
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print(f"  ANÁLISIS POR LÍNEA (O/U)")
    print(SEP)

    for cat in ['Goles Totales', 'Goles Local', 'Goles Visita']:
        subset = [p for p in preds if p['categoria'] == cat]
        if subset:
            analisis_over_under_por_linea(subset, cat.upper())

    # ══════════════════════════════════════════════════════════════════════════
    # BRIER SCORE COMPARATIVO
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print(f"  BRIER SCORE COMPARATIVO")
    print(SEP)
    print(f"\n  {'Categoría':<20}  {'N':>5}  {'Brier':>7}  {'Win%':>7}  {'Prob med':>8}  Calidad")
    print(f"  {SEP2}")

    for cat in categorias:
        subset = [p for p in preds if p['categoria'] == cat]
        if not subset:
            continue
        n = len(subset)
        brier = sum((p['prob'] - (1 if p['resultado']=='W' else 0))**2 for p in subset) / n
        wr = sum(1 for p in subset if p['resultado'] == 'W') / n
        pm = sum(p['prob'] for p in subset) / n

        # Brier de un modelo naive (predict base rate siempre)
        brier_naive = wr * (1 - wr)
        # Skill score
        skill = 1 - brier / brier_naive if brier_naive > 0 else 0

        if brier < 0.18:
            cal = 'BUENO'
        elif brier < 0.22:
            cal = 'ACEPTABLE'
        else:
            cal = 'MEJORABLE'

        print(f"  {cat:<20}  {n:>5}  {brier:>6.4f}  {wr:>6.1%}  {pm:>7.1%}  "
              f"{cal}  (skill={skill:+.3f})")

    # Global
    n_all = len(preds)
    brier_all = sum((p['prob'] - (1 if p['resultado']=='W' else 0))**2 for p in preds) / n_all
    wr_all = sum(1 for p in preds if p['resultado'] == 'W') / n_all
    brier_naive = wr_all * (1 - wr_all)
    skill_all = 1 - brier_all / brier_naive if brier_naive > 0 else 0
    print(f"  {'GLOBAL':<20}  {n_all:>5}  {brier_all:>6.4f}  {wr_all:>6.1%}  "
          f"{sum(p['prob'] for p in preds)/n_all:>7.1%}  (skill={skill_all:+.3f})")

    # ══════════════════════════════════════════════════════════════════════════
    # ANÁLISIS DE SESGO POR PROBABILIDAD
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{SEP}")
    print(f"  ANÁLISIS DE SESGO SISTEMÁTICO")
    print(SEP)
    print(f"\n  ¿El modelo tiende a sobrestimar o subestimar en ciertos rangos?")
    print(f"  Delta = Win% real - Prob predicha (positivo = subestima)")
    print()

    for cat in categorias:
        subset = [p for p in preds if p['categoria'] == cat]
        if len(subset) < 10:
            continue

        print(f"\n  {cat}:")
        terciles = sorted(subset, key=lambda p: p['prob'])
        n = len(terciles)
        tercios = [
            ('Bajo (prob <33%)',  [p for p in terciles if p['prob'] < 0.33]),
            ('Medio (33-66%)',    [p for p in terciles if 0.33 <= p['prob'] < 0.66]),
            ('Alto (prob >66%)',  [p for p in terciles if p['prob'] >= 0.66]),
        ]

        print(f"    {'Tercil':<22}  {'N':>5}  {'Win%':>7}  {'Prob med':>8}  {'Delta':>7}  Sesgo")
        print(f"    {'-'*72}")
        for label, bucket in tercios:
            if not bucket:
                continue
            nn = len(bucket)
            w = sum(1 for p in bucket if p['resultado'] == 'W')
            wr = w / nn
            pm = sum(p['prob'] for p in bucket) / nn
            delta = wr - pm
            sesgo = 'SUBESTIMA' if delta > 0.05 else 'SOBRESTIMA' if delta < -0.05 else 'OK'
            print(f"    {label:<22}  {nn:>5}  {wr:>6.1%}  {pm:>7.1%}  {delta:>+6.1%}  {sesgo}")

    print()
