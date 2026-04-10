import csv
import unicodedata
from pathlib import Path

CSV_PATH = Path(r'C:\Users\Matt\Apuestas Deportivas\data\historico\partidos_historicos.csv')

def norm(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn').lower().strip()

# ============================================================
# Data format per match:
# equipo_local / equipo_visitante = REAL home/away (FootyStats confirmed)
# stats_local / stats_vis = (tiros, tiros_arco, corners, posesion, tarjetas)
# swap_goals = True if CSV has teams reversed AND we know real score direction
#              In that case, goles_local/goles_visitante are also corrected
# ============================================================

MATCHES = {
    # ========== BARCELONA LA LIGA ==========
    '2026-01-31': {
        'local': 'Elche', 'visitante': 'Barcelona',
        'sl': (9,3,2,39,2), 'sv': (30,8,8,61,1),
        'swap_goals': False,
    },
    '2026-02-07': {  # CSV had Mallorca as local — WRONG, Barça was home
        'local': 'Barcelona', 'visitante': 'Mallorca',
        'sl': (24,7,12,76,0), 'sv': (9,4,3,24,0),
        'swap_goals': True,
    },
    '2026-02-16': {
        'local': 'Girona', 'visitante': 'Barcelona',
        'sl': (13,9,3,29,3), 'sv': (27,4,7,71,3),
        'swap_goals': False,
    },
    '2026-02-22': {  # CSV had Levante as local — WRONG
        'local': 'Barcelona', 'visitante': 'Levante',
        'sl': (22,9,13,73,2), 'sv': (5,2,6,27,1),
        'swap_goals': True,
    },
    '2026-02-28-barca': {  # Villarreal vs Barça — CSV had Villarreal as local — WRONG
        'local': 'Barcelona', 'visitante': 'Villarreal',
        'sl': (19,8,3,74,2), 'sv': (6,1,2,26,1),
        'swap_goals': True,
        'fecha': '2026-02-28',
        'competicion': 'La Liga',
    },
    '2026-03-07-barca': {
        'local': 'Athletic Club', 'visitante': 'Barcelona',
        'sl': (10,3,2,30,2), 'sv': (7,2,4,70,1),
        'swap_goals': False,
        'fecha': '2026-03-07',
        'competicion': 'La Liga',
    },
    '2026-03-15': {  # CSV had Sevilla as local — WRONG
        'local': 'Barcelona', 'visitante': 'Sevilla',
        'sl': (13,8,3,61,0), 'sv': (8,4,4,39,1),
        'swap_goals': True,
    },
    '2026-03-22-barca': {  # CSV had Rayo as local — WRONG
        'local': 'Barcelona', 'visitante': 'Rayo Vallecano',
        'sl': (15,4,6,61,3), 'sv': (8,4,9,39,4),
        'swap_goals': True,
        'fecha': '2026-03-22',
        'competicion': 'La Liga',
    },
    # Barça vs Atlético La Liga (Apr 4) already updated in Independiente script
    # (it was in the Atlético section, updating via Atlético data)

    # ========== BARCELONA COPA DEL REY ==========
    # 2026-02-03 Albacete: NOT FOUND, skip
    '2026-02-12': {  # Copa del Rey: Atlético HOME — CSV match correct
        'local': 'Atletico Madrid', 'visitante': 'Barcelona',
        'sl': (12,8,5,35,5), 'sv': (14,4,8,65,5),
        'swap_goals': False,
    },
    '2026-03-03': {  # Copa del Rey: Barça HOME — CSV had Atlético as local — WRONG
        'local': 'Barcelona', 'visitante': 'Atletico Madrid',
        'sl': (21,9,15,71,3), 'sv': (7,2,0,29,1),
        'swap_goals': True,
        'goles_local': 3, 'goles_visitante': 0,  # Known real score
    },

    # ========== BARCELONA CHAMPIONS LEAGUE ==========
    '2026-03-10-barca': {  # Newcastle HOME — CSV match correct
        'local': 'Newcastle United', 'visitante': 'Barcelona',
        'sl': (16,4,9,46,2), 'sv': (9,2,4,54,1),
        'swap_goals': False,
        'fecha': '2026-03-10',
        'competicion': 'Champions League',
    },
    '2026-03-18-barca': {  # Barça HOME — CSV had Newcastle as local — WRONG
        'local': 'Barcelona', 'visitante': 'Newcastle United',
        'sl': (18,13,6,63,1), 'sv': (8,5,2,37,3),
        'swap_goals': True,
        'fecha': '2026-03-18',
        'competicion': 'Champions League',
    },
    '2026-04-08': {  # UCL: Barça HOME — CSV had Atlético as local — WRONG
        'local': 'Barcelona', 'visitante': 'Atletico Madrid',
        'sl': (18,7,7,58,3), 'sv': (5,3,1,42,2),
        'swap_goals': True,
        'goles_local': 0, 'goles_visitante': 2,  # Known real score (Atlético won 2-0)
    },

    # ========== ATLÉTICO MADRID LA LIGA ==========
    '2026-02-08-atletico': {  # CSV had Betis as local — WRONG, Atlético HOME
        'local': 'Atletico Madrid', 'visitante': 'Real Betis',
        'sl': (10,5,7,65,3), 'sv': (8,4,3,35,1),
        'swap_goals': True,
        'fecha': '2026-02-08',
        'competicion': 'La Liga',
    },
    '2026-02-15': {  # CSV had Atlético as local — WRONG, Rayo HOME
        'local': 'Rayo Vallecano', 'visitante': 'Atletico Madrid',
        'sl': (13,9,4,41,1), 'sv': (9,3,8,59,3),
        'swap_goals': True,
    },
    '2026-02-21-atletico': {  # Atlético HOME — CSV match correct
        'local': 'Atletico Madrid', 'visitante': 'Espanyol',
        'sl': (18,9,5,61,0), 'sv': (8,2,3,39,1),
        'swap_goals': False,
        'fecha': '2026-02-21',
        'competicion': 'La Liga',
    },
    '2026-02-28-atletico': {  # CSV had Atlético as local — WRONG, Oviedo HOME
        'local': 'Real Oviedo', 'visitante': 'Atletico Madrid',
        'sl': (16,6,8,38,2), 'sv': (12,1,3,62,2),
        'swap_goals': True,
        'fecha': '2026-02-28',
        'competicion': 'La Liga',
    },
    '2026-03-07-atletico': {  # CSV had Sociedad as local — WRONG, Atlético HOME
        'local': 'Atletico Madrid', 'visitante': 'Real Sociedad',
        'sl': (24,9,8,52,1), 'sv': (7,3,1,48,1),
        'swap_goals': True,
        'fecha': '2026-03-07',
        'competicion': 'La Liga',
    },
    '2026-03-14': {  # Atlético HOME — CSV match correct
        'local': 'Atletico Madrid', 'visitante': 'Getafe',
        'sl': (16,5,10,66,3), 'sv': (7,3,3,34,5),
        'swap_goals': False,
    },
    '2026-03-22-atletico': {  # Real Madrid HOME — CSV match correct
        'local': 'Real Madrid', 'visitante': 'Atletico Madrid',
        'sl': (17,10,4,52,3), 'sv': (13,7,1,48,4),
        'swap_goals': False,
        'fecha': '2026-03-22',
        'competicion': 'La Liga',
    },
    '2026-04-04-atletico': {  # Atlético vs Barça La Liga — CSV match correct
        'local': 'Atletico Madrid', 'visitante': 'Barcelona',
        'sl': (6,2,1,33,7), 'sv': (22,8,9,67,2),
        'swap_goals': False,
        'fecha': '2026-04-04',
        'competicion': 'La Liga',
    },

    # ========== ATLÉTICO CHAMPIONS LEAGUE ==========
    '2026-02-18': {  # CSV had Atlético as local — WRONG, Brugge HOME
        'local': 'Club Brugge', 'visitante': 'Atletico Madrid',
        'sl': (17,10,4,58,1), 'sv': (13,4,6,42,2),
        'swap_goals': True,
    },
    '2026-02-24': {  # CSV had Brugge as local — WRONG, Atlético HOME
        'local': 'Atletico Madrid', 'visitante': 'Club Brugge',
        'sl': (14,5,2,45,1), 'sv': (11,6,7,55,2),
        'swap_goals': True,
    },
    '2026-03-10-atletico': {  # CSV had Tottenham as local — WRONG, Atlético HOME
        'local': 'Atletico Madrid', 'visitante': 'Tottenham Hotspur',
        'sl': (11,7,4,58,0), 'sv': (11,5,2,42,5),
        'swap_goals': True,
        'fecha': '2026-03-10',
        'competicion': 'Champions League',
    },
    '2026-03-18-atletico': {  # Tottenham HOME — CSV match correct
        'local': 'Tottenham Hotspur', 'visitante': 'Atletico Madrid',
        'sl': (18,11,7,51,4), 'sv': (18,6,7,49,3),
        'swap_goals': False,
        'fecha': '2026-03-18',
        'competicion': 'Champions League',
    },
}

STAT_FIELDS = ['tiros_local','tiros_visitante','tiros_arco_local','tiros_arco_visitante',
               'corners_local','corners_visitante','posesion_local','posesion_visitante',
               'tarjetas_local','tarjetas_visitante']

def teams_fuzzy_match(csv_t, data_t):
    cn = norm(csv_t)
    dn = norm(data_t)
    # Match if one contains the other or share significant substring
    return cn in dn or dn in cn or (len(cn)>4 and cn[:5] in dn) or (len(dn)>4 and dn[:5] in cn)

rows = []
with open(CSV_PATH, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    rows = list(reader)

updated = 0
team_corrections = 0

for row in rows:
    fecha_csv = row['fecha']
    comp_csv = norm(row['competicion'])
    loc_csv = row['equipo_local']
    vis_csv = row['equipo_visitante']

    # Try to find a matching entry in MATCHES
    matched_key = None
    matched_data = None

    for key, data in MATCHES.items():
        # Determine the actual fecha to compare
        actual_fecha = data.get('fecha', key[:10] if len(key) >= 10 else key)
        if actual_fecha != fecha_csv:
            continue

        # Check competition filter if specified
        if 'competicion' in data:
            if norm(data['competicion']) not in comp_csv and comp_csv not in norm(data['competicion']):
                continue

        data_local = data['local']
        data_vis = data['visitante']

        # Check if teams match (either same order or swapped)
        local_match = teams_fuzzy_match(loc_csv, data_local)
        vis_match = teams_fuzzy_match(vis_csv, data_vis)
        local_swap = teams_fuzzy_match(loc_csv, data_vis)
        vis_swap = teams_fuzzy_match(vis_csv, data_local)

        if local_match and vis_match:
            matched_key = key
            matched_data = data
            break
        elif local_swap and vis_swap:
            matched_key = key
            matched_data = data
            break

    if not matched_key:
        continue

    data = matched_data
    sl = data['sl']  # (tiros, tiros_arco, corners, posesion, tarjetas) for real LOCAL
    sv = data['sv']  # same for real VISITANTE
    data_local = data['local']
    data_vis = data['visitante']

    # Determine if CSV teams match real or are swapped
    local_match = teams_fuzzy_match(loc_csv, data_local) and teams_fuzzy_match(vis_csv, data_vis)

    if local_match:
        # Teams match — apply stats directly
        row['tiros_local'] = str(sl[0])
        row['tiros_visitante'] = str(sv[0])
        row['tiros_arco_local'] = str(sl[1])
        row['tiros_arco_visitante'] = str(sv[1])
        row['corners_local'] = str(sl[2])
        row['corners_visitante'] = str(sv[2])
        row['posesion_local'] = str(sl[3])
        row['posesion_visitante'] = str(sv[3])
        row['tarjetas_local'] = str(sl[4])
        row['tarjetas_visitante'] = str(sv[4])
        updated += 1
        print(f"OK         {fecha_csv} {comp_csv[:3]}: {loc_csv} vs {vis_csv}")
    else:
        # Teams are swapped — correct equipo, stats, and optionally goals
        row['equipo_local'] = data_local
        row['equipo_visitante'] = data_vis
        row['tiros_local'] = str(sl[0])
        row['tiros_visitante'] = str(sv[0])
        row['tiros_arco_local'] = str(sl[1])
        row['tiros_arco_visitante'] = str(sv[1])
        row['corners_local'] = str(sl[2])
        row['corners_visitante'] = str(sv[2])
        row['posesion_local'] = str(sl[3])
        row['posesion_visitante'] = str(sv[3])
        row['tarjetas_local'] = str(sl[4])
        row['tarjetas_visitante'] = str(sv[4])

        if data.get('swap_goals'):
            if 'goles_local' in data:
                # Known real score
                row['goles_local'] = str(data['goles_local'])
                row['goles_visitante'] = str(data['goles_visitante'])
            else:
                # Swap goals (old local → new visitante)
                old_local_g = row['goles_local']
                old_vis_g = row['goles_visitante']
                row['goles_local'] = old_vis_g
                row['goles_visitante'] = old_local_g

        updated += 1
        team_corrections += 1
        print(f"CORRECTED  {fecha_csv} {comp_csv[:3]}: {loc_csv} vs {vis_csv} => {data_local} vs {data_vis}")

# Write updated CSV
with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"\nDone: {updated} rows updated ({team_corrections} team corrections). CSV saved.")
