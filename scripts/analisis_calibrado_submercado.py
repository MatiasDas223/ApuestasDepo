"""
Análisis del modelo calibrado (v3.2-cal) por mercado y submercado completo.
Submercado = categoria × alcance × lado.
Solo lectura, no modifica nada.
"""
import csv
from pathlib import Path
from collections import defaultdict

CSV = Path(__file__).resolve().parent.parent / 'data/apuestas/value_bets_calibrado.csv'
STAKE = 1.0


def pnl(res, odds):
    if res == 'W': return odds - STAKE
    if res == 'L': return -STAKE
    if res == 'V': return 0.0
    return 0.0


def parse_pct(s):
    if not s: return None
    s = str(s).strip().replace('%', '').replace('+', '')
    if not s or s == '-': return None
    try:
        v = float(s)
        return v / 100.0 if abs(v) > 1.5 else v
    except ValueError:
        return None


def cargar():
    rows = []
    with open(CSV, encoding='utf-8') as f:
        for r in csv.DictReader(f):
            res = r.get('resultado', '').strip().upper()
            if res not in ('W', 'L', 'V'):
                continue
            try:
                odds = float(r['odds'])
            except (ValueError, KeyError):
                continue
            rows.append({
                'cat':   r.get('categoria', '').strip() or 'Sin',
                'alc':   r.get('alcance', '').strip() or 'Sin',
                'lado':  r.get('lado', '').strip() or 'Sin',
                'mer':   r.get('mercado', '').strip(),
                'odds':  odds,
                'edge':  parse_pct(r.get('edge')),
                'ev':    parse_pct(r.get('ev_pct')),
                'res':   res,
                'pnl':   pnl(res, odds),
            })
    return rows


def stats(bets):
    if not bets: return None
    n = len(bets)
    w = sum(1 for b in bets if b['res'] == 'W')
    l = sum(1 for b in bets if b['res'] == 'L')
    v = sum(1 for b in bets if b['res'] == 'V')
    p = sum(b['pnl'] for b in bets)
    evs = [b['ev'] for b in bets if b['ev'] is not None]
    edges = [b['edge'] for b in bets if b['edge'] is not None]
    odds = [b['odds'] for b in bets]
    base = w + l
    return dict(
        n=n, w=w, l=l, v=v, pnl=p,
        hit=w/base if base else 0,
        roi=p/n if n else 0,
        ev_m=sum(evs)/len(evs) if evs else 0,
        ev_total=sum(evs) if evs else 0,
        edge_m=sum(edges)/len(edges) if edges else 0,
        odds_m=sum(odds)/len(odds) if odds else 0,
    )


def fmt(s, label, w_label=30):
    flag = '  **' if s['n'] >= 20 and s['roi'] >= 0.05 else (
           '  XX' if s['n'] >= 20 and s['roi'] <= -0.20 else '')
    return (f"  {label:<{w_label}}  {s['n']:>4}  {s['w']:>4}/{s['l']:>3}  "
            f"{s['hit']:>5.1%}  {s['odds_m']:>5.2f}  {s['edge_m']:>+5.1%}  "
            f"{s['ev_m']:>+6.1%}  {s['roi']:>+6.1%}  "
            f"{(s['roi']-s['ev_m']):>+6.1%}{flag}")


def header(w_label=30):
    return (f"  {'Grupo':<{w_label}}  {'N':>4}  {'W':>4}/{'L':>3}  "
            f"{'Hit%':>5}  {'Odds':>5}  {'Edge%':>5}  "
            f"{'EVm%':>6}  {'ROI%':>6}  {'dROI':>6}")


def seccion(titulo):
    print()
    print('=' * 95)
    print(f'  {titulo}')
    print('=' * 95)


def bloque(grupos, orden=None, min_n=1, label_w=30):
    keys = orden if orden else sorted(grupos.keys(), key=lambda k: -len(grupos[k]))
    print(header(label_w))
    print('  ' + '-' * 90)
    for k in keys:
        g = grupos.get(k, [])
        if len(g) < min_n: continue
        s = stats(g)
        print(fmt(s, str(k), label_w))


def rango_cuota(o):
    if o < 1.50: return '1.20-1.50'
    if o < 2.00: return '1.50-2.00'
    if o < 3.00: return '2.00-3.00'
    if o < 5.00: return '3.00-5.00'
    return         '>5.00'

ORDEN_CUOTAS = ['1.20-1.50','1.50-2.00','2.00-3.00','3.00-5.00','>5.00']


def main():
    apuestas = cargar()
    if not apuestas:
        print('No hay apuestas resueltas')
        return

    s_total = stats(apuestas)
    print('=' * 95)
    print(f"  ANALISIS DEL MODELO CALIBRADO (v3.2-cal)")
    print(f"  Total: {s_total['n']} bets resueltas ({s_total['w']}W / {s_total['l']}L)")
    print(f"  P&L: {s_total['pnl']:+.2f}u   ROI: {s_total['roi']:+.1%}   "
          f"EV esp: {s_total['ev_m']:+.1%}   dROI: {(s_total['roi']-s_total['ev_m']):+.1%}")
    print(f"  ** = subgrupo positivo (n>=20, ROI>=5%)   "
          f"XX = subgrupo perdedor (n>=20, ROI<=-20%)")
    print('=' * 95)

    # ─────────────────────────────────────────────────────────────
    # 1. Por categoría
    # ─────────────────────────────────────────────────────────────
    seccion('1. POR CATEGORIA DE MERCADO')
    by_cat = defaultdict(list)
    for a in apuestas:
        by_cat[a['cat']].append(a)
    bloque(by_cat, label_w=20)

    # ─────────────────────────────────────────────────────────────
    # 2. Por categoría × alcance
    # ─────────────────────────────────────────────────────────────
    seccion('2. POR CATEGORIA × ALCANCE (Total / Local / Visitante)')
    by_ca = defaultdict(list)
    for a in apuestas:
        by_ca[f"{a['cat']} | {a['alc']}"].append(a)
    bloque(by_ca, label_w=30)

    # ─────────────────────────────────────────────────────────────
    # 3. Por categoría × alcance × lado (SUBMERCADO COMPLETO)
    # ─────────────────────────────────────────────────────────────
    seccion('3. SUBMERCADO COMPLETO (categoria × alcance × lado)')
    by_cal = defaultdict(list)
    for a in apuestas:
        by_cal[f"{a['cat']} | {a['alc']} | {a['lado']}"].append(a)
    bloque(by_cal, label_w=42)

    # ─────────────────────────────────────────────────────────────
    # 4. Por categoría × cuota — donde valen los precios
    # ─────────────────────────────────────────────────────────────
    seccion('4. POR CATEGORIA × RANGO DE CUOTA')
    cats = sorted({a['cat'] for a in apuestas})
    for cat in cats:
        bets_cat = [a for a in apuestas if a['cat'] == cat]
        if len(bets_cat) < 5: continue
        s = stats(bets_cat)
        print()
        print(f"  --- {cat} ({s['n']} bets, ROI {s['roi']:+.1%}) ---")
        by_c = defaultdict(list)
        for a in bets_cat:
            by_c[rango_cuota(a['odds'])].append(a)
        bloque(by_c, orden=ORDEN_CUOTAS, label_w=14)

    # ─────────────────────────────────────────────────────────────
    # 5. Top mercados específicos (mercado string completo)
    # ─────────────────────────────────────────────────────────────
    seccion('5. TOP-25 MERCADOS ESPECIFICOS POR VOLUMEN (n>=10)')
    by_mer = defaultdict(list)
    for a in apuestas:
        by_mer[a['mer']].append(a)
    items = sorted(by_mer.items(), key=lambda kv: -len(kv[1]))[:25]
    print(header(45))
    print('  ' + '-' * 90)
    for k, g in items:
        if len(g) < 10: continue
        s = stats(g)
        label = (k[:42] + '...') if len(k) > 45 else k
        print(fmt(s, label, 45))

    # ─────────────────────────────────────────────────────────────
    # 6. Mejores y peores submercados (n>=10) ranking final
    # ─────────────────────────────────────────────────────────────
    seccion('6. RANKING SUBMERCADOS (n>=10)  — top y bottom')
    candidatos = []
    for k, g in by_cal.items():
        if len(g) < 10: continue
        s = stats(g)
        candidatos.append((k, s))
    candidatos.sort(key=lambda kv: -kv[1]['roi'])

    print()
    print('  TOP 10 (mejor ROI):')
    print(header(42))
    print('  ' + '-' * 90)
    for k, s in candidatos[:10]:
        print(fmt(s, k, 42))

    print()
    print('  BOTTOM 10 (peor ROI):')
    print(header(42))
    print('  ' + '-' * 90)
    for k, s in candidatos[-10:][::-1]:
        print(fmt(s, k, 42))

    print()


if __name__ == '__main__':
    main()
