"""
Backfill correcto de implied_prob y edge para filas DC en CSVs historicos.

Bug: _remove_vig asumia probabilidades que suman 1 (mutuamente excluyentes).
Para Doble Oportunidad las 3 facetas (1X, 12, X2) suman 2 — el codigo guardo
implied_prob a la mitad y edge inflado al doble.

Fix matematico: implied_prob_correcto = 2 * implied_prob_guardado
                edge_correcto         = modelo_prob - implied_prob_correcto

Solo se modifican filas cuyo 'mercado' arranca con "DC ->". Las demas columnas
se preservan tal cual estaban.
"""
import csv
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
APU  = BASE / 'data' / 'apuestas'

CSVS = [
    'value_bets.csv',
    'value_bets_calibrado.csv',
    'value_bets_filtrados.csv',
    'value_bets_v33ref.csv',
    'value_bets_v34shrink.csv',
    'value_bets_v35dedup.csv',
    'value_bets_v36.csv',
    'pronosticos.csv',
    'pronosticos_calibrado.csv',
    'pronosticos_v33ref.csv',
    'pronosticos_v34shrink.csv',
]


def fix_csv(path: Path, dry_run: bool = False) -> dict:
    if not path.exists():
        return {'file': path.name, 'status': 'missing'}

    with open(path, newline='', encoding='utf-8') as f:
        reader  = csv.DictReader(f)
        cols    = reader.fieldnames
        rows    = list(reader)

    if 'mercado' not in cols or 'implied_prob' not in cols or 'edge' not in cols or 'modelo_prob' not in cols:
        return {'file': path.name, 'status': 'cols_missing', 'cols': cols}

    n_total   = len(rows)
    n_dc      = 0
    n_changed = 0
    n_skipped_no_ip       = 0
    n_skipped_already_fixed = 0

    for row in rows:
        mkt = row.get('mercado', '')
        if not mkt.startswith('DC ->'):
            continue
        n_dc += 1

        ip_str = row.get('implied_prob', '').strip()
        mp_str = row.get('modelo_prob',  '').strip()
        odds_str = row.get('odds', '').strip()
        if not ip_str or not mp_str or not odds_str:
            n_skipped_no_ip += 1
            continue

        try:
            ip_old = float(ip_str)
            mp     = float(mp_str)
            odds   = float(odds_str)
        except ValueError:
            n_skipped_no_ip += 1
            continue

        # Guard contra doble-aplicacion: pre-fix ip_old * odds ~ 0.5 (rango .45-.50);
        # post-fix ip_old * odds ~ 1.0 (rango .90-.99). Si ya esta cerca de raw, skip.
        if ip_old * odds > 0.70:
            n_skipped_already_fixed += 1
            continue

        ip_new   = min(2 * ip_old, 1.0)
        edge_new = mp - ip_new

        row['implied_prob'] = f'{ip_new:.4f}'
        row['edge']         = f'{edge_new:.4f}'
        n_changed += 1

    if not dry_run and n_changed > 0:
        with open(path, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)

    return {
        'file': path.name,
        'status': 'ok',
        'total_rows': n_total,
        'dc_rows': n_dc,
        'changed': n_changed,
        'skipped_no_ip': n_skipped_no_ip,
        'skipped_already_fixed': n_skipped_already_fixed,
    }


def main(dry_run: bool = False):
    print(f"\nBackfill DC implied_prob/edge — modo: {'DRY-RUN' if dry_run else 'WRITE'}")
    print('=' * 80)
    totals = {'total_rows': 0, 'dc_rows': 0, 'changed': 0, 'skipped_no_ip': 0, 'skipped_already_fixed': 0}
    for fname in CSVS:
        r = fix_csv(APU / fname, dry_run=dry_run)
        if r['status'] == 'missing':
            print(f"  {fname:<35}  [MISSING]")
            continue
        if r['status'] != 'ok':
            print(f"  {fname:<35}  [SKIP: {r['status']}]")
            continue
        print(f"  {fname:<35}  total={r['total_rows']:>5d}  DC={r['dc_rows']:>5d}  modificadas={r['changed']:>5d}  sin_ip={r['skipped_no_ip']:>3d}  ya_arregladas={r['skipped_already_fixed']:>3d}")
        for k in totals:
            totals[k] += r[k]

    print('-' * 80)
    print(f"  {'TOTAL':<35}  total={totals['total_rows']:>5d}  DC={totals['dc_rows']:>5d}  modificadas={totals['changed']:>5d}  sin_ip={totals['skipped_no_ip']:>3d}  ya_arregladas={totals['skipped_already_fixed']:>3d}")


if __name__ == '__main__':
    import sys
    main(dry_run=('--dry-run' in sys.argv))
