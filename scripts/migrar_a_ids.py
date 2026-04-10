"""
Migración one-time: convierte partidos_historicos.csv de nombres a IDs.
Limpia duplicados y normaliza todo usando data/db/equipos.csv y ligas.csv.

Uso:
    python migrar_a_ids.py --dry-run   # ver resultados sin guardar
    python migrar_a_ids.py             # ejecutar migracion
"""

import csv
import sys
import unicodedata
from pathlib import Path
from collections import defaultdict

BASE = Path(r'C:\Users\Matt\Apuestas Deportivas')
CSV_OLD  = BASE / 'data/historico/partidos_historicos.csv'
CSV_NEW  = BASE / 'data/historico/partidos_historicos.csv'
EQUIPOS  = BASE / 'data/db/equipos.csv'
LIGAS    = BASE / 'data/db/ligas.csv'

# ─────────────────────────────────────────────────────────────────────────────
# Mapeo manual: variantes de nombre encontradas en el CSV → ID de equipo
# ─────────────────────────────────────────────────────────────────────────────
NAME_TO_ID = {
    # Boca Juniors
    'boca juniors': 451,
    # Independiente
    'independiente': 453,
    # Racing Club
    'racing club': 436,
    # River Plate
    'river plate': 435,
    # Estudiantes (con todas sus variantes)
    'estudiantes la plata': 450,
    'estudiantes l.p.': 450,
    'estudiantes lp': 450,
    # Talleres
    'talleres cordoba': 456,
    'talleres córdoba': 456,
    'talleres c\u00f3rdoba': 456,
    # Newells
    "newell's old boys": 457,
    'newells old boys': 457,
    # Lanus
    'lanus': 446,
    'lan\u00fas': 446,
    # Velez
    'velez sarsfield': 438,
    'v\u00e9lez sarsfield': 438,
    # Union Santa Fe
    'union santa fe': 441,
    'uni\u00f3n santa fe': 441,
    # Instituto
    'instituto cordoba': 478,
    'instituto c\u00f3rdoba': 478,
    # Central Cordoba
    'central c\u00f3rdoba': 1065,
    'central cordoba': 1065,
    # Gimnasia Mendoza
    'gimnasia m.': 1066,
    'gimnasia mendoza': 1066,
    # Independiente Rivadavia
    'independiente rivadavia': 473,
    'independ. rivadavia': 473,
    # Barcelona
    'barcelona': 529,
    # Atletico Madrid
    'atletico madrid': 530,
    'atl\u00e9tico madrid': 530,
    'atl\xe9tico madrid': 530,
    # Real Madrid
    'real madrid': 541,
    # Sevilla
    'sevilla': 536,
    # Villarreal
    'villarreal': 533,
    # Real Betis
    'real betis': 543,
    # Getafe
    'getafe': 546,
    # Girona
    'girona': 547,
    # Real Sociedad
    'real sociedad': 548,
    # Rayo Vallecano
    'rayo vallecano': 728,
    # Athletic Club
    'athletic club': 531,
    # Osasuna
    'osasuna': 727,
    # Mallorca
    'mallorca': 798,
    # Espanyol
    'espanyol': 540,
    # Levante
    'levante': 539,
    # Elche
    'elche': 797,
    # Albacete
    'albacete': 722,
    # Oviedo / Real Oviedo
    'real oviedo': 718,
    'oviedo': 718,
    # Argentinos JRS
    'argentinos jrs': 458,
    # San Lorenzo
    'san lorenzo': 460,
    # Platense
    'platense': 1064,
    # Tigre
    'tigre': 452,
    # Huracan
    'huracan': 445,
    # Deportivo Riestra
    'deportivo riestra': 476,
    # Banfield
    'banfield': 449,
    # Belgrano
    'belgrano cordoba': 440,
    'belgrano': 440,
    # Godoy Cruz
    'godoy cruz': 439,
    # Atletico Tucuman
    'atletico tucuman': 455,
    'atl\u00e9tico tucum\u00e1n': 455,
    # Sarmiento
    'sarmiento junin': 474,
    # Defensa y Justicia
    'defensa y justicia': 442,
    # Barracas Central
    'barracas central': 2432,
    # Estudiantes RC
    'estudiantes rc': 2424,
    # Club Brugge
    'club brugge': 569,
    'club brugge kv': 569,
    # Newcastle
    'newcastle united': 34,
    'newcastle': 34,
    # Tottenham
    'tottenham hotspur': 47,
    'tottenham': 47,
    # Universidad Católica (Chile)
    'universidad cat\u00f3lica': 2994,
    'u. cat\u00f3lica': 2994,
    'u. catolica': 2994,
    # Sportivo Atenas
    'sportivo atenas': 21043,
    'atenas': 21043,
    # Gimnasia Chivilcoy
    'gimnasia chivilcoy': 25758,
    # Gimnasia Mendoza (abbrev)
    'gimnasia m.': 1066,
}

# ─────────────────────────────────────────────────────────────────────────────
# Mapeo de competicion (nombre viejo) → liga_id
# ─────────────────────────────────────────────────────────────────────────────
COMP_TO_ID = {
    'liga profesional': 128,
    'copa argentina': 130,
    'copa libertadores': 13,
    'champions league': 2,
    'la liga': 140,
    'copa del rey': 848,
}


def norm_str(s):
    """Normaliza a minúsculas sin acentos para matching."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', s.lower().strip())
        if unicodedata.category(c) != 'Mn'
    )


def resolve_team(raw_name):
    key = raw_name.lower().strip()
    if key in NAME_TO_ID:
        return NAME_TO_ID[key]
    # Fallback: intenta con version sin acentos
    key_norm = norm_str(raw_name)
    for k, v in NAME_TO_ID.items():
        if norm_str(k) == key_norm:
            return v
    return None


def resolve_comp(raw_comp):
    key = norm_str(raw_comp)
    for k, v in COMP_TO_ID.items():
        if norm_str(k) == key:
            return v
    return None


def load_team_names():
    """Devuelve {id: nombre} desde equipos.csv."""
    names = {}
    with open(EQUIPOS, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            names[int(row['id'])] = row['nombre']
    return names


def migrate(dry_run=False):
    team_names = load_team_names()

    with open(CSV_OLD, encoding='utf-8') as f:
        old_rows = list(csv.DictReader(f))

    print(f"Filas originales: {len(old_rows)}")

    new_rows = []
    skipped_dup = 0
    skipped_unknown = 0
    seen_keys = set()

    for row in old_rows:
        # Resolver equipos → IDs
        lid = resolve_team(row['equipo_local'])
        vid = resolve_team(row['equipo_visitante'])
        cid = resolve_comp(row['competicion'])

        if lid is None:
            print(f"  [WARN] Equipo local desconocido: '{row['equipo_local']}'  "
                  f"({row['fecha']})")
            skipped_unknown += 1
            continue
        if vid is None:
            print(f"  [WARN] Equipo visitante desconocido: '{row['equipo_visitante']}'  "
                  f"({row['fecha']})")
            skipped_unknown += 1
            continue
        if cid is None:
            print(f"  [WARN] Competicion desconocida: '{row['competicion']}'  "
                  f"({row['fecha']})")
            skipped_unknown += 1
            continue

        # Clave única para detectar duplicados
        key = (row['fecha'], lid, vid, cid)
        if key in seen_keys:
            print(f"  [DUP]  {row['fecha']}  {team_names.get(lid, lid)} vs "
                  f"{team_names.get(vid, vid)}  (eliminado)")
            skipped_dup += 1
            continue
        seen_keys.add(key)

        new_rows.append({
            'fecha':                row['fecha'],
            'liga_id':              cid,
            'equipo_local_id':      lid,
            'equipo_visitante_id':  vid,
            'goles_local':          row['goles_local'],
            'goles_visitante':      row['goles_visitante'],
            'tiros_local':          row['tiros_local'],
            'tiros_visitante':      row['tiros_visitante'],
            'tiros_arco_local':     row['tiros_arco_local'],
            'tiros_arco_visitante': row['tiros_arco_visitante'],
            'corners_local':        row['corners_local'],
            'corners_visitante':    row['corners_visitante'],
            'posesion_local':       row['posesion_local'],
            'posesion_visitante':   row['posesion_visitante'],
            'tarjetas_local':       row['tarjetas_local'],
            'tarjetas_visitante':   row['tarjetas_visitante'],
        })

    new_rows.sort(key=lambda r: r['fecha'])

    print(f"\nResultado:")
    print(f"  Filas validas    : {len(new_rows)}")
    print(f"  Duplicados elim. : {skipped_dup}")
    print(f"  Desconocidos     : {skipped_unknown}")

    if dry_run:
        print("\n[DRY RUN] Primeras 5 filas migridas:")
        for r in new_rows[:5]:
            ln = team_names.get(r['equipo_local_id'],    r['equipo_local_id'])
            vn = team_names.get(r['equipo_visitante_id'], r['equipo_visitante_id'])
            print(f"  {r['fecha']}  liga={r['liga_id']}  "
                  f"{ln} ({r['equipo_local_id']}) {r['goles_local']}-"
                  f"{r['goles_visitante']} {vn} ({r['equipo_visitante_id']})")
        return

    fields = [
        'fecha', 'liga_id', 'equipo_local_id', 'equipo_visitante_id',
        'goles_local', 'goles_visitante',
        'tiros_local', 'tiros_visitante',
        'tiros_arco_local', 'tiros_arco_visitante',
        'corners_local', 'corners_visitante',
        'posesion_local', 'posesion_visitante',
        'tarjetas_local', 'tarjetas_visitante',
    ]
    with open(CSV_NEW, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(new_rows)

    print(f"\nCSV guardado: {CSV_NEW}")
    print("Ahora actualiza modelo_v2.py y fetch_api_football.py para usar IDs.")


if __name__ == '__main__':
    dry_run = '--dry-run' in sys.argv
    migrate(dry_run=dry_run)
