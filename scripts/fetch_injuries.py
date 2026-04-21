"""
Descarga injuries desde API Football para un fixture o equipo.
Cachea en data/contexto/injuries/{fixture_id}.json.

Uso standalone:
    python fetch_injuries.py 1492015              # por fixture
    python fetch_injuries.py 1492015 --force      # ignora cache
    python fetch_injuries.py --team 451           # por equipo
    python fetch_injuries.py --team 451 --season 2025

Uso como modulo:
    from fetch_injuries import get_injuries
    injuries, resumen = get_injuries(fixture_id=1492015)
    # injuries = {
    #     'home': [{'player_id', 'player_name', 'type', 'reason'}, ...],
    #     'away': [...],
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
CACHE_DIR  = Path(r'C:\Users\Matt\Apuestas Deportivas\data\contexto\injuries')
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Cuánto tiempo confiar en el caché (segundos). Injuries cambian rápido pre-KO.
CACHE_TTL_SEC = 3 * 3600   # 3h


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


def _normalize_injury(item):
    """Convierte un item raw de API Football a nuestra forma compacta."""
    p = item.get('player', {}) or {}
    t = item.get('team', {}) or {}
    fix = item.get('fixture', {}) or {}
    return {
        'player_id':   p.get('id'),
        'player_name': p.get('name'),
        'photo':       p.get('photo'),
        'type':        p.get('type'),     # 'Missing Fixture' | 'Questionable'
        'reason':      p.get('reason'),
        'team_id':     t.get('id'),
        'fixture_id':  fix.get('id'),
    }


def get_injuries(fixture_id=None, team_id=None, season=None,
                 force=False, verbose=False):
    """
    Devuelve (data, resumen).

    Si fixture_id: data = {'home': [...], 'away': [...]} con ambos equipos.
    Si team_id:    data = lista plana de injuries del equipo (requiere season).
    """
    if fixture_id is None and team_id is None:
        raise ValueError('Debe pasar fixture_id o team_id')

    # ── Cache ─────────────────────────────────────────────────────────────
    if fixture_id is not None:
        cache_path = CACHE_DIR / f'{fixture_id}.json'
    else:
        cache_path = CACHE_DIR / f'team_{team_id}_{season}.json'

    if cache_path.exists() and not force:
        age = time.time() - cache_path.stat().st_mtime
        if age < CACHE_TTL_SEC:
            with open(cache_path, encoding='utf-8') as f:
                cached = json.load(f)
            if verbose:
                print(f'  [cache hit] {cache_path.name} (age={age/60:.0f}min)')
            return cached['data'], cached['resumen']

    # ── Bajar de API ───────────────────────────────────────────────────────
    if fixture_id is not None:
        items = api_get('injuries', {'fixture': fixture_id})
        # Determinar quién es home/away mirando las teams del fixture
        # Para no agregar otra call: usamos el primer team que aparezca como
        # 'home' por convención del orden de la respuesta.
        # Mejor: pedir el fixture y obtener el team_id local explícito.
        try:
            fix_resp = api_get('fixtures', {'id': fixture_id})
            time.sleep(0.3)
            if fix_resp:
                home_id = fix_resp[0]['teams']['home']['id']
                away_id = fix_resp[0]['teams']['away']['id']
            else:
                home_id = away_id = None
        except Exception:
            home_id = away_id = None

        # API Football devuelve a veces el mismo player_id duplicado en la
        # misma respuesta — dedupe por (player_id, type, reason).
        def _dedupe(injs):
            seen = set()
            out = []
            for inj in injs:
                key = (inj['player_id'], inj['type'], inj['reason'])
                if key not in seen:
                    seen.add(key)
                    out.append(inj)
            return out

        home_inj = _dedupe([_normalize_injury(i) for i in items
                            if (i.get('team') or {}).get('id') == home_id])
        away_inj = _dedupe([_normalize_injury(i) for i in items
                            if (i.get('team') or {}).get('id') == away_id])

        data = {
            'home_team_id':  home_id,
            'away_team_id':  away_id,
            'home':          home_inj,
            'away':          away_inj,
        }
        resumen = {
            'fixture_id':       fixture_id,
            'home_total':       len(home_inj),
            'home_missing':     sum(1 for i in home_inj if i['type'] == 'Missing Fixture'),
            'home_questionable':sum(1 for i in home_inj if i['type'] == 'Questionable'),
            'away_total':       len(away_inj),
            'away_missing':     sum(1 for i in away_inj if i['type'] == 'Missing Fixture'),
            'away_questionable':sum(1 for i in away_inj if i['type'] == 'Questionable'),
        }
    else:
        if season is None:
            raise ValueError('team_id requiere season')
        items = api_get('injuries', {'team': team_id, 'season': season})
        data = [_normalize_injury(i) for i in items]
        resumen = {
            'team_id':       team_id,
            'season':        season,
            'total':         len(data),
            'missing':       sum(1 for i in data if i['type'] == 'Missing Fixture'),
            'questionable':  sum(1 for i in data if i['type'] == 'Questionable'),
        }

    # ── Guardar cache ─────────────────────────────────────────────────────
    cache_payload = {
        'fetched_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'data':       data,
        'resumen':    resumen,
    }
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(cache_payload, f, ensure_ascii=False, indent=2)

    if verbose:
        print(f'  [cached] {cache_path.name}')

    return data, resumen


def get_sidelined(team_id, force=False, verbose=False):
    """
    /sidelined?team=X devuelve bajas largas (lesiones de meses, suspensiones FIFA, etc).
    Cache por team con TTL más largo.
    """
    cache_path = CACHE_DIR / f'sidelined_{team_id}.json'
    if cache_path.exists() and not force:
        age = time.time() - cache_path.stat().st_mtime
        if age < 24 * 3600:   # 24h
            with open(cache_path, encoding='utf-8') as f:
                cached = json.load(f)
            if verbose:
                print(f'  [cache hit] sidelined {team_id} (age={age/3600:.1f}h)')
            return cached['data']

    items = api_get('sidelined', {'team': team_id})
    data = []
    for item in items:
        p = item.get('player', {}) or {}
        data.append({
            'player_id':   p.get('id'),
            'player_name': p.get('name'),
            'type':        item.get('type'),
            'start':       item.get('start'),
            'end':         item.get('end'),
        })

    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump({'fetched_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                   'data': data}, f, ensure_ascii=False, indent=2)
    return data


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _print_resumen(data, resumen):
    print(json.dumps(resumen, indent=2, ensure_ascii=False))
    if isinstance(data, dict) and 'home' in data:
        for side in ('home', 'away'):
            print(f'\n  {side.upper()} ({len(data[side])}):')
            for inj in data[side]:
                print(f"    [{inj['type']:<18}] {inj['player_name']:<25} "
                      f"({inj['reason']})")
    elif isinstance(data, list):
        for inj in data[:20]:
            print(f"  [{inj['type']:<18}] {inj['player_name']:<25} "
                  f"({inj['reason']})")
        if len(data) > 20:
            print(f"  ...({len(data)-20} más)")


def main():
    args = sys.argv[1:]
    force = '--force' in args
    if force: args.remove('--force')

    if '--team' in args:
        idx = args.index('--team')
        team_id = int(args[idx + 1])
        season = None
        if '--season' in args:
            season = int(args[args.index('--season') + 1])
        else:
            season = time.localtime().tm_year
        print(f'  /injuries team={team_id} season={season}')
        data, resumen = get_injuries(team_id=team_id, season=season,
                                     force=force, verbose=True)
        _print_resumen(data, resumen)
    elif args:
        fixture_id = int(args[0])
        print(f'  /injuries fixture={fixture_id}')
        data, resumen = get_injuries(fixture_id=fixture_id,
                                     force=force, verbose=True)
        _print_resumen(data, resumen)
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == '__main__':
    main()
