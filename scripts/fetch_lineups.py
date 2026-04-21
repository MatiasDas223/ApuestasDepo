"""
Descarga lineups (XI titular + suplentes + formación + DT) desde API Football.
Cachea en data/contexto/lineups/{fixture_id}.json.

Lineups confirmados aparecen ~20-40min antes del KO. Antes del anuncio
oficial el endpoint devuelve [] o array vacío.

Uso standalone:
    python fetch_lineups.py 1492015              # bajar y mostrar
    python fetch_lineups.py 1492015 --force      # ignorar cache

Uso como modulo:
    from fetch_lineups import get_lineups
    lineups, resumen = get_lineups(fixture_id=1492015)
    # lineups = {
    #     'home': {'team_id', 'formation', 'coach', 'startXI': [...], 'subs': [...]},
    #     'away': {...},
    #     'confirmed': bool,   # True si ambos equipos publicaron XI
    # }
"""
import json
import sys
import time
import urllib.request
import urllib.parse
from pathlib import Path

API_KEY    = '5a7d5d038454c3640c8771ce2274c18c'
BASE_URL   = 'https://v3.football.api-sports.io'
CACHE_DIR  = Path(r'C:\Users\Matt\Apuestas Deportivas\data\contexto\lineups')
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Cuánto tiempo confiar en el caché. Si el lineup está confirmado (ambos XI),
# es definitivo y se puede cachear largo. Si no está confirmado, refrescar pronto.
CACHE_TTL_CONFIRMED = 24 * 3600   # 24h
CACHE_TTL_PENDING   = 5 * 60      # 5min


def api_get(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    if params:
        url += '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={'x-apisports-key': API_KEY})
    with urllib.request.urlopen(req, timeout=20) as r:
        data = json.loads(r.read())
    if data.get('errors'):
        if isinstance(data['errors'], (list, dict)) and not data['errors']:
            pass
        else:
            raise RuntimeError(f"API error: {data['errors']}")
    return data.get('response', [])


def _player(item):
    p = (item or {}).get('player', {}) or {}
    return {
        'id':     p.get('id'),
        'name':   p.get('name'),
        'number': p.get('number'),
        'pos':    p.get('pos'),
        'grid':   p.get('grid'),
    }


def _normalize_team(item):
    """Convierte el lineup de un team raw a forma compacta."""
    coach = item.get('coach') or {}
    return {
        'team_id':   (item.get('team') or {}).get('id'),
        'team_name': (item.get('team') or {}).get('name'),
        'formation': item.get('formation'),
        'coach_id':   coach.get('id'),
        'coach_name': coach.get('name'),
        'startXI':   [_player(x) for x in (item.get('startXI') or [])],
        'subs':      [_player(x) for x in (item.get('substitutes') or [])],
    }


def get_lineups(fixture_id, force=False, verbose=False):
    """
    Devuelve (data, resumen).
    data = {
        'fixture_id', 'home', 'away', 'confirmed',
    }
    """
    cache_path = CACHE_DIR / f'{fixture_id}.json'

    # ── Cache ─────────────────────────────────────────────────────────────
    if cache_path.exists() and not force:
        with open(cache_path, encoding='utf-8') as f:
            cached = json.load(f)
        ttl = (CACHE_TTL_CONFIRMED if cached['data'].get('confirmed')
               else CACHE_TTL_PENDING)
        age = time.time() - cache_path.stat().st_mtime
        if age < ttl:
            if verbose:
                print(f'  [cache hit] {fixture_id} '
                      f'confirmed={cached["data"].get("confirmed")} '
                      f'age={age/60:.0f}min')
            return cached['data'], cached['resumen']

    # ── Bajar de API ───────────────────────────────────────────────────────
    items = api_get('fixtures/lineups', {'fixture': fixture_id})

    # API devuelve hasta 2 entries (1 por equipo). Si el lineup no fue publicado
    # aún, devuelve [] vacío.
    teams = [_normalize_team(t) for t in items]
    home = teams[0] if len(teams) >= 1 else None
    away = teams[1] if len(teams) >= 2 else None

    confirmed = bool(home and away
                     and len(home['startXI']) == 11
                     and len(away['startXI']) == 11)

    data = {
        'fixture_id':  fixture_id,
        'home':        home,
        'away':        away,
        'confirmed':   confirmed,
    }
    resumen = {
        'fixture_id':         fixture_id,
        'confirmed':          confirmed,
        'home_xi_size':       len(home['startXI']) if home else 0,
        'away_xi_size':       len(away['startXI']) if away else 0,
        'home_formation':     home['formation'] if home else None,
        'away_formation':     away['formation'] if away else None,
        'home_coach':         home['coach_name'] if home else None,
        'away_coach':         away['coach_name'] if away else None,
    }

    cache_payload = {
        'fetched_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'data':       data,
        'resumen':    resumen,
    }
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(cache_payload, f, ensure_ascii=False, indent=2)

    if verbose:
        if confirmed:
            print(f'  [cached confirmed] {fixture_id}')
        else:
            print(f'  [cached pending]  {fixture_id} — XI no publicado todavía')

    return data, resumen


def diff_with_history(lineup_data, historical_starters, threshold=0.7):
    """
    Compara el XI confirmado contra una lista de jugadores que SUELEN ser
    titulares (ej. los que jugaron >threshold de los últimos N partidos).

    historical_starters = {team_id: set(player_id, ...)}
    Devuelve por equipo: ausentes_titulares (player_ids que estaban en
    histórico y NO están en startXI).

    Útil para penalizar el rating cuando faltan piezas clave.
    """
    out = {}
    for side in ('home', 'away'):
        team = lineup_data.get(side) or {}
        tid  = team.get('team_id')
        if not tid or tid not in historical_starters:
            out[side] = {'team_id': tid, 'ausentes': [],
                         'n_ausentes': 0, 'pct_ausentes': 0.0}
            continue
        xi_ids = {p['id'] for p in team.get('startXI', []) if p.get('id')}
        hist   = historical_starters[tid]
        ausentes = sorted(hist - xi_ids)
        out[side] = {
            'team_id':      tid,
            'ausentes':     ausentes,
            'n_ausentes':   len(ausentes),
            'pct_ausentes': (len(ausentes) / len(hist)) if hist else 0.0,
        }
    return out


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]
    force = '--force' in args
    if force: args.remove('--force')

    if not args:
        print(__doc__)
        sys.exit(1)

    fixture_id = int(args[0])
    data, resumen = get_lineups(fixture_id, force=force, verbose=True)
    print(json.dumps(resumen, indent=2, ensure_ascii=False))

    if not data['confirmed']:
        print('\n  ⚠ XI todavía NO confirmado para ambos equipos')

    for side in ('home', 'away'):
        team = data.get(side)
        if not team:
            print(f'\n  {side.upper()}: (sin datos)')
            continue
        print(f"\n  {side.upper()}: {team['team_name']} "
              f"({team['formation']})  DT: {team['coach_name']}")
        for p in team['startXI']:
            print(f"    {p.get('number') or '?':>2}  {p.get('pos') or '-':<3}  "
                  f"{p['name']}")
        if team['subs']:
            print(f"    -- Suplentes ({len(team['subs'])}) --")


if __name__ == '__main__':
    main()
