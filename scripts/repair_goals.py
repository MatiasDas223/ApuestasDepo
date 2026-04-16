"""
repair_goals.py
---------------
Repara los goles incorrectos en partidos_historicos.csv.

El bug: fetch_historia.py usaba stat_value(stats, 'Goals') del endpoint
de estadisticas, pero los goles NO estan ahi — vienen de fix['goals']
en el endpoint principal de fixtures.

Estrategia eficiente:
  Por cada (liga_id, season) unico en el CSV, llama a:
    GET /fixtures?league=X&season=Y
  y actualiza goles_local / goles_visitante de todos los fixtures
  de esa combinacion usando fix['goals']['home'] y fix['goals']['away'].

  Costo: ~8 llamadas API en total (vs 2400+ si fuera fixture por fixture).
"""

import csv
import json
import sys
import time
import urllib.request
import urllib.parse
from pathlib import Path
from collections import defaultdict

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE     = Path(r'C:\Users\Matt\Apuestas Deportivas')
CSV_PATH = BASE / 'data/historico/partidos_historicos.csv'
API_KEY  = '5a7d5d038454c3640c8771ce2274c18c'
BASE_URL = 'https://v3.football.api-sports.io'

CSV_FIELDS = [
    'fixture_id', 'fecha', 'liga_id',
    'equipo_local_id', 'equipo_visitante_id',
    'goles_local', 'goles_visitante',
    'tiros_local', 'tiros_visitante',
    'tiros_arco_local', 'tiros_arco_visitante',
    'corners_local', 'corners_visitante',
    'posesion_local', 'posesion_visitante',
    'tarjetas_local', 'tarjetas_visitante',
]

def api_get(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    if params:
        url += '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={'x-apisports-key': API_KEY})
    with urllib.request.urlopen(req, timeout=20) as r:
        data = json.loads(r.read())
    if data.get('errors'):
        raise RuntimeError(f"API error: {data['errors']}")
    return data.get('response', [])


def detect_season(fecha: str, liga_id: int) -> int:
    """
    Infiere la season a partir de la fecha y la liga.
    Ligas europeas: season = año en que EMPIEZA (ej. 2023 = 2023/24).
    Ligas argentinas: season = el año del partido.
    """
    year = int(fecha[:4])
    month = int(fecha[5:7])
    european_leagues = {2, 3, 140, 143, 848}
    if liga_id in european_leagues:
        # Si el partido es de jul-dic, pertenece a season=year
        # Si es ene-jun, pertenece a season=year-1
        return year if month >= 7 else year - 1
    else:
        return year


def main():
    # Cargar CSV
    rows = []
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    print(f"Cargados {len(rows)} partidos del CSV.")

    # Identificar cuales tienen goles probablemente incorrectos
    # Los incorrectos son 0-0 pero eso incluye partidos reales 0-0.
    # Mejor estrategia: actualizar TODOS — si son correctos los sobreescribe igual.

    # Agrupar por (liga_id, season)
    grupos = defaultdict(list)
    for i, r in enumerate(rows):
        lid    = int(r['liga_id'])
        season = detect_season(r['fecha'], lid)
        grupos[(lid, season)].append(i)

    print(f"Grupos (liga_id, season): {len(grupos)}")
    for (lid, season), idxs in sorted(grupos.items()):
        print(f"  liga_id={lid:<4}  season={season}  ->  {len(idxs)} partidos")

    # Para cada grupo, bajar la lista de fixtures y actualizar goles
    total_updated = 0
    total_api_calls = 0

    for (lid, season), idxs in sorted(grupos.items()):
        print(f"\nActualizando liga_id={lid} season={season} ({len(idxs)} partidos)...", end=' ')

        try:
            fixtures = api_get('fixtures', {'league': lid, 'season': season})
            total_api_calls += 1
            time.sleep(0.4)
        except Exception as e:
            print(f"ERROR: {e}")
            continue

        # Construir mapa fixture_id -> goals
        goals_map = {}
        for fix in fixtures:
            fid = str(fix['fixture']['id'])
            gh  = fix.get('goals', {}).get('home')
            ga  = fix.get('goals', {}).get('away')
            if gh is not None and ga is not None:
                goals_map[fid] = (int(gh), int(ga))

        # Actualizar filas del CSV
        updated = 0
        for i in idxs:
            fid = rows[i]['fixture_id']
            if fid in goals_map:
                gh, ga = goals_map[fid]
                old_h = rows[i]['goles_local']
                old_a = rows[i]['goles_visitante']
                rows[i]['goles_local']     = gh
                rows[i]['goles_visitante'] = ga
                if str(old_h) != str(gh) or str(old_a) != str(ga):
                    updated += 1

        print(f"OK — {updated} partidos actualizados  ({len(goals_map)} fixtures en API)")
        total_updated += updated

    # Guardar CSV (preservar columnas existentes)
    print(f"\nGuardando CSV con {total_updated} goles corregidos...")
    rows.sort(key=lambda r: (r['fecha'], int(r.get('fixture_id', 0))))
    file_fields = None
    if CSV_PATH.exists():
        with open(CSV_PATH, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            file_fields = list(reader.fieldnames) if reader.fieldnames else None
    if file_fields:
        merged = list(file_fields)
        for col in CSV_FIELDS:
            if col not in merged:
                merged.append(col)
    else:
        merged = list(CSV_FIELDS)
    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=merged, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)

    print(f"Listo. {total_updated} goles corregidos con {total_api_calls} llamadas a la API.")

    # Verificacion rapida
    rows_check = list(csv.DictReader(open(CSV_PATH, encoding='utf-8')))
    cero = sum(1 for r in rows_check if r['goles_local'] == '0' and r['goles_visitante'] == '0')
    print(f"Verificacion: {cero}/{len(rows_check)} partidos con 0-0 ({cero/len(rows_check):.1%})")


if __name__ == '__main__':
    main()
