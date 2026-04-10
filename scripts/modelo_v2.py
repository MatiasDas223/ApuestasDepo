"""
MODELO DE PREDICCIÓN V2 — Apuestas Deportivas
================================================
Pipeline:
  1. Carga el CSV histórico
  2. Estima parámetros por equipo (ataque / defensa) con ratings multiplicativos
  3. Simulación Monte Carlo (100 000 iteraciones) con Poisson para goles y corners,
     Normal para tiros
  4. Calcula probabilidades para todos los mercados principales
  5. Detecta value bets comparando con cuotas del bookmaker (opción)

Uso básico:
    python modelo_v2.py

Uso como librería:
    from modelo_v2 import predict
    probs, params = predict('Independiente', 'Racing Club',
                            competition='Liga Profesional',
                            odds={'1': 2.20, 'X': 3.10, '2': 3.40,
                                  'odds_over_2.5': 1.90, 'odds_under_2.5': 1.90})
"""

import csv
import math
import random
import unicodedata
from collections import defaultdict, Counter
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

CSV_PATH     = Path(r'C:\Users\Matt\Apuestas Deportivas\data\historico\partidos_historicos.csv')
EQUIPOS_PATH = Path(r'C:\Users\Matt\Apuestas Deportivas\data\db\equipos.csv')
LIGAS_PATH   = Path(r'C:\Users\Matt\Apuestas Deportivas\data\db\ligas.csv')
N_SIM_DEFAULT = 100_000
MIN_EDGE = 0.04          # 4 % de ventaja mínima para declarar value bet
MIN_MATCHES = 2          # mínimo de partidos para usar stats propias del equipo

# ─────────────────────────────────────────────────────────────────────────────
# Utilidades
# ─────────────────────────────────────────────────────────────────────────────

def norm(s: str) -> str:
    """Normaliza texto: sin acentos, minúsculas, sin espacios extra."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    ).lower().strip()


def poisson_sample(lam: float) -> int:
    """Muestrea de distribución Poisson (Knuth)."""
    if lam <= 0:
        return 0
    L = math.exp(-min(lam, 700))
    k, p = 0, 1.0
    while p > L:
        k += 1
        p *= random.random()
    return k - 1


def normal_sample_pos(mu: float, sigma: float) -> int:
    """Muestrea de Normal, forzado a entero no negativo."""
    return max(0, round(random.gauss(mu, sigma)))


def safe_mean(vals: list) -> float | None:
    return sum(vals) / len(vals) if vals else None


def safe_std(vals: list, mean: float) -> float:
    if len(vals) < 2:
        return 0.0
    var = sum((x - mean) ** 2 for x in vals) / (len(vals) - 1)
    return math.sqrt(var)


# ─────────────────────────────────────────────────────────────────────────────
# Carga de datos
# ─────────────────────────────────────────────────────────────────────────────

def load_csv(path: Path = CSV_PATH) -> list[dict]:
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def load_teams_db(path: Path = EQUIPOS_PATH) -> tuple[dict, dict]:
    """
    Carga equipos.csv.
    Devuelve ({id_int: nombre}, {nombre_norm: id_int}).
    """
    id_to_name, name_to_id = {}, {}
    with open(path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            tid  = int(row['id'])
            name = row['nombre']
            id_to_name[tid] = name
            name_to_id[norm(name)] = tid
    return id_to_name, name_to_id


def load_leagues_db(path: Path = LIGAS_PATH) -> tuple[dict, dict]:
    """
    Carga ligas.csv.
    Devuelve ({id_int: nombre}, {nombre_norm: id_int}).
    """
    id_to_name, name_to_id = {}, {}
    with open(path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            lid  = int(row['id'])
            name = row['nombre']
            id_to_name[lid] = name
            name_to_id[norm(name)] = lid
    return id_to_name, name_to_id


def resolve_team_id(team: str | int, name_to_id: dict) -> int | None:
    """Acepta nombre (str) o ID directo (int/str numérico). Devuelve ID o None."""
    if isinstance(team, int):
        return team
    try:
        return int(team)
    except (ValueError, TypeError):
        pass
    return name_to_id.get(norm(str(team)))


def resolve_liga_id(liga: str | int, name_to_id: dict) -> int | None:
    """Acepta nombre de liga o ID. Devuelve ID o None."""
    if isinstance(liga, int):
        return liga
    try:
        return int(liga)
    except (ValueError, TypeError):
        pass
    return name_to_id.get(norm(str(liga)))


# ─────────────────────────────────────────────────────────────────────────────
# Estadísticas por equipo
# ─────────────────────────────────────────────────────────────────────────────

class TeamRecord:
    """Acumula estadísticas de un equipo como local y como visitante."""

    FIELDS = ['goals', 'goals_conceded', 'shots', 'shots_conceded',
              'corners', 'corners_conceded', 'possession', 'cards']

    def __init__(self):
        self.home: dict[str, list] = defaultdict(list)
        self.away: dict[str, list] = defaultdict(list)

    def _add(self, store: dict, row: dict, prefix_local: bool):
        """prefix_local=True ->este equipo es local en la fila."""
        l, v = 'local', 'visitante'
        if prefix_local:
            store['goals'].append(int(row['goles_local']))
            store['goals_conceded'].append(int(row['goles_visitante']))
            store['shots'].append(int(row['tiros_local']))
            store['shots_conceded'].append(int(row['tiros_visitante']))
            store['corners'].append(int(row['corners_local']))
            store['corners_conceded'].append(int(row['corners_visitante']))
            store['possession'].append(int(row['posesion_local']))
            store['cards'].append(int(row['tarjetas_local']))
        else:
            store['goals'].append(int(row['goles_visitante']))
            store['goals_conceded'].append(int(row['goles_local']))
            store['shots'].append(int(row['tiros_visitante']))
            store['shots_conceded'].append(int(row['tiros_local']))
            store['corners'].append(int(row['corners_visitante']))
            store['corners_conceded'].append(int(row['corners_local']))
            store['possession'].append(int(row['posesion_visitante']))
            store['cards'].append(int(row['tarjetas_visitante']))

    def add_home_row(self, row: dict):
        self._add(self.home, row, prefix_local=True)

    def add_away_row(self, row: dict):
        self._add(self.away, row, prefix_local=False)

    def n(self, ctx: str) -> int:
        store = self.home if ctx == 'home' else self.away
        return len(store['goals'])

    def avg(self, ctx: str, field: str) -> float | None:
        store = self.home if ctx == 'home' else self.away
        return safe_mean(store[field])

    def std(self, ctx: str, field: str) -> float:
        store = self.home if ctx == 'home' else self.away
        m = safe_mean(store[field])
        if m is None:
            return 0.0
        return safe_std(store[field], m)


def build_records(rows: list[dict],
                  liga_id_filter: int | None = None) -> dict[int, TeamRecord]:
    """
    Construye TeamRecord para todos los equipos indexado por team_id (int).
    Filtra opcionalmente por liga_id.
    """
    records: dict[int, TeamRecord] = defaultdict(TeamRecord)
    for row in rows:
        if liga_id_filter and int(row['liga_id']) != liga_id_filter:
            continue
        local_id = int(row['equipo_local_id'])
        vis_id   = int(row['equipo_visitante_id'])
        records[local_id].add_home_row(row)
        records[vis_id].add_away_row(row)
    return records


def league_avgs(rows: list[dict],
                liga_id_filter: int | None = None) -> dict[str, float]:
    """Calcula promedios de liga para usar como baseline."""
    filtered = [r for r in rows if not liga_id_filter
                or int(r['liga_id']) == liga_id_filter]
    if len(filtered) < 3:
        filtered = rows          # fallback a todo el histórico

    n = len(filtered)

    def avg_field(fn):
        return sum(fn(r) for r in filtered) / n

    return {
        'home_goals':    avg_field(lambda r: int(r['goles_local'])),
        'away_goals':    avg_field(lambda r: int(r['goles_visitante'])),
        'home_corners':  avg_field(lambda r: int(r['corners_local'])),
        'away_corners':  avg_field(lambda r: int(r['corners_visitante'])),
        'home_shots':    avg_field(lambda r: int(r['tiros_local'])),
        'away_shots':    avg_field(lambda r: int(r['tiros_visitante'])),
        'home_shots_std': 4.0,   # default σ tiros
        'away_shots_std': 4.0,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Estimación de parámetros del partido
# ─────────────────────────────────────────────────────────────────────────────

def _rating(team_avg: float | None, league_avg: float) -> float:
    """Rating multiplicativo: avg_equipo / avg_liga. Default 1.0 si sin datos."""
    if team_avg is None or league_avg == 0:
        return 1.0
    return team_avg / league_avg


def _best_record(records_comp: dict, records_all: dict,
                 team: str, ctx: str, field: str) -> float | None:
    """Devuelve el promedio disponible: filtra por competición primero, luego global."""
    for recs in (records_comp, records_all):
        rec = recs.get(team)
        if rec and rec.n(ctx) >= MIN_MATCHES:
            v = rec.avg(ctx, field)
            if v is not None:
                return v
    # Último recurso: contexto opuesto
    for recs in (records_comp, records_all):
        rec = recs.get(team)
        if rec:
            opp = 'away' if ctx == 'home' else 'home'
            if rec.n(opp) >= MIN_MATCHES:
                v = rec.avg(opp, field)
                if v is not None:
                    return v
    return None


def compute_match_params(team_local: str | int, team_visitante: str | int,
                         rows: list[dict],
                         competition: str | int | None = None) -> dict:
    """
    Estima todos los parámetros necesarios para la simulación Monte Carlo.
    Acepta nombres de equipo (str) o IDs directos (int).
    Acepta nombre de competición (str) o liga_id (int).

    Modelo de goles (Poisson):
        λ_local   = avg_goles_local_liga × atk_rating_local × def_rating_visitante
        λ_visit   = avg_goles_visit_liga × atk_rating_visit × def_rating_local

    Modelo de corners (Poisson) — ídem estructura
    Modelo de tiros   (Normal)  — ídem estructura
    """
    # Resolver nombres → IDs usando la DB
    _, name_to_id_teams   = load_teams_db()
    _, name_to_id_leagues = load_leagues_db()

    local_id = resolve_team_id(team_local,    name_to_id_teams)
    vis_id   = resolve_team_id(team_visitante, name_to_id_teams)
    liga_id  = resolve_liga_id(competition, name_to_id_leagues) if competition else None

    if local_id is None:
        raise ValueError(f"Equipo local no encontrado en DB: '{team_local}'")
    if vis_id is None:
        raise ValueError(f"Equipo visitante no encontrado en DB: '{team_visitante}'")

    recs_comp = build_records(rows, liga_id) if liga_id else {}
    recs_all  = build_records(rows)
    la        = league_avgs(rows, liga_id)

    def ga(team_id, ctx, field):
        return _best_record(recs_comp, recs_all, team_id, ctx, field)

    # ── Conteo de partidos disponibles ────────────────────────────────────────
    def n_ctx(team_id, ctx):
        for recs in (recs_comp, recs_all):
            rec = recs.get(team_id)
            if rec and rec.n(ctx) >= 1:
                return rec.n(ctx)
        return 0

    n_local_home = n_ctx(local_id, 'home')
    n_vis_away   = n_ctx(vis_id,   'away')

    # ── Ratings ───────────────────────────────────────────────────────────────
    # Goles
    atk_l = _rating(ga(local_id, 'home', 'goals'),           la['home_goals'])
    def_v = _rating(ga(vis_id,   'away', 'goals_conceded'),  la['home_goals'])
    atk_v = _rating(ga(vis_id,   'away', 'goals'),           la['away_goals'])
    def_l = _rating(ga(local_id, 'home', 'goals_conceded'),  la['away_goals'])

    lambda_local = max(0.15, la['home_goals'] * atk_l * def_v)
    lambda_vis   = max(0.15, la['away_goals'] * atk_v * def_l)

    # Corners
    atk_cl = _rating(ga(local_id, 'home', 'corners'),           la['home_corners'])
    def_cv = _rating(ga(vis_id,   'away', 'corners_conceded'),  la['home_corners'])
    atk_cv = _rating(ga(vis_id,   'away', 'corners'),           la['away_corners'])
    def_cl = _rating(ga(local_id, 'home', 'corners_conceded'),  la['away_corners'])

    mu_corners_local = max(0.5, la['home_corners'] * atk_cl * def_cv)
    mu_corners_vis   = max(0.5, la['away_corners'] * atk_cv * def_cl)

    # Tiros
    atk_sl = _rating(ga(local_id, 'home', 'shots'),           la['home_shots'])
    def_sv = _rating(ga(vis_id,   'away', 'shots_conceded'),  la['home_shots'])
    atk_sv = _rating(ga(vis_id,   'away', 'shots'),           la['away_shots'])
    def_sl = _rating(ga(local_id, 'home', 'shots_conceded'),  la['away_shots'])

    mu_shots_local = max(1.0, la['home_shots'] * atk_sl * def_sv)
    mu_shots_vis   = max(1.0, la['away_shots'] * atk_sv * def_sl)

    # σ tiros — usa el std histórico propio si disponible
    def shot_std(team_id, ctx, mu):
        for recs in (recs_comp, recs_all):
            rec = recs.get(team_id)
            if rec and rec.n(ctx) >= MIN_MATCHES:
                s = rec.std(ctx, 'shots')
                if s > 0:
                    return s
        return mu * 0.30   # 30 % del promedio como fallback

    sigma_shots_local = shot_std(local_id, 'home', mu_shots_local)
    sigma_shots_vis   = shot_std(vis_id,   'away', mu_shots_vis)

    # ── Posesión esperada ─────────────────────────────────────────────────────
    raw_poss_l = ga(local_id, 'home', 'possession') or 50.0
    raw_poss_v = ga(vis_id,   'away', 'possession') or 50.0
    total_poss  = raw_poss_l + raw_poss_v
    poss_local  = 100.0 * raw_poss_l / total_poss if total_poss > 0 else 50.0

    return {
        'lambda_local':    lambda_local,
        'lambda_vis':      lambda_vis,
        'mu_corners_local': mu_corners_local,
        'mu_corners_vis':   mu_corners_vis,
        'mu_shots_local':  mu_shots_local,
        'mu_shots_vis':    mu_shots_vis,
        'sigma_shots_local': max(0.5, sigma_shots_local),
        'sigma_shots_vis':   max(0.5, sigma_shots_vis),
        'poss_local':      poss_local,
        'n_local_home':    n_local_home,
        'n_vis_away':      n_vis_away,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Simulación Monte Carlo
# ─────────────────────────────────────────────────────────────────────────────

def run_simulation(params: dict, n: int = N_SIM_DEFAULT) -> dict:
    """Ejecuta n iteraciones de Monte Carlo. Devuelve listas de resultados simulados."""
    lam_l  = params['lambda_local']
    lam_v  = params['lambda_vis']
    mu_cl  = params['mu_corners_local']
    mu_cv  = params['mu_corners_vis']
    mu_sl  = params['mu_shots_local']
    mu_sv  = params['mu_shots_vis']
    sig_sl = params['sigma_shots_local']
    sig_sv = params['sigma_shots_vis']

    gl, gv, cl, cv, sl, sv = [], [], [], [], [], []

    for _ in range(n):
        gl.append(poisson_sample(lam_l))
        gv.append(poisson_sample(lam_v))
        cl.append(poisson_sample(mu_cl))
        cv.append(poisson_sample(mu_cv))
        sl.append(normal_sample_pos(mu_sl, sig_sl))
        sv.append(normal_sample_pos(mu_sv, sig_sv))

    return {
        'gl': gl, 'gv': gv,
        'cl': cl, 'cv': cv,
        'sl': sl, 'sv': sv,
        'n': n,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Cálculo de probabilidades
# ─────────────────────────────────────────────────────────────────────────────

def compute_probabilities(sim: dict) -> dict:
    """Convierte los resultados de simulación en probabilidades por mercado."""
    n  = sim['n']
    gl = sim['gl']; gv = sim['gv']
    cl = sim['cl']; cv = sim['cv']
    sl = sim['sl']; sv = sim['sv']

    tg = [gl[i] + gv[i] for i in range(n)]
    tc = [cl[i] + cv[i] for i in range(n)]
    ts = [sl[i] + sv[i] for i in range(n)]

    p = {}

    # 1X2
    p['1'] = sum(gl[i] > gv[i] for i in range(n)) / n
    p['X'] = sum(gl[i] == gv[i] for i in range(n)) / n
    p['2'] = sum(gv[i] > gl[i] for i in range(n)) / n

    # Doble oportunidad
    p['1X'] = p['1'] + p['X']
    p['X2'] = p['X'] + p['2']
    p['12'] = p['1'] + p['2']

    # BTTS
    p['btts_si'] = sum(gl[i] > 0 and gv[i] > 0 for i in range(n)) / n
    p['btts_no'] = 1 - p['btts_si']

    # Goles O/U
    for thr in [0.5, 1.5, 2.5, 3.5, 4.5, 5.5]:
        p[f'over_{thr}']  = sum(g > thr for g in tg) / n
        p[f'under_{thr}'] = 1 - p[f'over_{thr}']

    # Asian Handicap goles (linea local)
    # hcp es el handicap APLICADO al local:
    #   hcp=-1.5 -> local da 1.5 goles, gana si gl-gv > 1.5 (gana por 2+)
    #   hcp=+1.5 -> local recibe 1.5 goles, gana si gl-gv > -1.5
    # Formula: local gana si gl - gv + hcp > 0  <=>  d > -hcp
    diff = [gl[i] - gv[i] for i in range(n)]
    for hcp in [-2.5, -2.0, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5]:
        threshold = -hcp
        if hcp == int(hcp):
            # linea entera -> push posible
            p[f'ahcp_{hcp:+.1f}_win']  = sum(d > threshold for d in diff) / n
            p[f'ahcp_{hcp:+.1f}_push'] = sum(d == threshold for d in diff) / n
            p[f'ahcp_{hcp:+.1f}_loss'] = sum(d < threshold for d in diff) / n
        else:
            p[f'ahcp_{hcp:+.1f}'] = sum(d > threshold for d in diff) / n

    # Goles individuales por equipo
    for thr in [0.5, 1.5, 2.5]:
        p[f'goles_local_over_{thr}']   = sum(g > thr for g in gl) / n
        p[f'goles_local_under_{thr}']  = 1 - p[f'goles_local_over_{thr}']
        p[f'goles_visita_over_{thr}']  = sum(g > thr for g in gv) / n
        p[f'goles_visita_under_{thr}'] = 1 - p[f'goles_visita_over_{thr}']

    # Corners O/U totales
    for thr in [7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5]:
        p[f'corners_over_{thr}']  = sum(c > thr for c in tc) / n
        p[f'corners_under_{thr}'] = 1 - p[f'corners_over_{thr}']

    # Corners individuales por equipo
    for thr in [3.5, 4.5, 5.5, 6.5, 7.5]:
        p[f'corners_local_over_{thr}']   = sum(c > thr for c in cl) / n
        p[f'corners_local_under_{thr}']  = 1 - p[f'corners_local_over_{thr}']
        p[f'corners_visita_over_{thr}']  = sum(c > thr for c in cv) / n
        p[f'corners_visita_under_{thr}'] = 1 - p[f'corners_visita_over_{thr}']

    # Tiros O/U totales
    for thr in [16.5, 18.5, 20.5, 22.5, 24.5, 26.5, 28.5]:
        p[f'shots_over_{thr}']  = sum(s > thr for s in ts) / n
        p[f'shots_under_{thr}'] = 1 - p[f'shots_over_{thr}']

    # Tiros individuales por equipo
    for thr in [7.5, 9.5, 11.5, 13.5, 15.5]:
        p[f'shots_local_over_{thr}']   = sum(s > thr for s in sl) / n
        p[f'shots_local_under_{thr}']  = 1 - p[f'shots_local_over_{thr}']
        p[f'shots_visita_over_{thr}']  = sum(s > thr for s in sv) / n
        p[f'shots_visita_under_{thr}'] = 1 - p[f'shots_visita_over_{thr}']

    # Distribución de marcadores (top 15)
    sc = Counter(zip(gl, gv))
    p['score_dist'] = {
        f"{g}-{v}": cnt / n
        for (g, v), cnt in sorted(sc.items(), key=lambda x: -x[1])[:15]
    }

    # Valores esperados
    p['E_gl'] = sum(gl) / n
    p['E_gv'] = sum(gv) / n
    p['E_cl'] = sum(cl) / n
    p['E_cv'] = sum(cv) / n
    p['E_sl'] = sum(sl) / n
    p['E_sv'] = sum(sv) / n

    return p


# ─────────────────────────────────────────────────────────────────────────────
# Detección de value bets
# ─────────────────────────────────────────────────────────────────────────────

def _remove_vig(*odds_list: float) -> list[float]:
    """Quita el margen del bookmaker. Devuelve probabilidades justas."""
    implied = [1 / o for o in odds_list]
    total   = sum(implied)
    return [p / total for p in implied]


def find_value_bets(probs: dict, odds: dict, min_edge: float = MIN_EDGE) -> list[dict]:
    """
    Detecta value bets comparando probabilidades del modelo con cuotas del bookmaker.

    Formato esperado del dict odds:
        '1', 'X', '2'                        ->cuotas 1X2
        'odds_over_2.5', 'odds_under_2.5'    ->O/U goles
        'odds_btts_si',  'odds_btts_no'      ->BTTS
        'odds_corners_over_9.5', etc.        ->corners
        'odds_ahcp_-0.5'                     ->Asian Handicap local -0.5 goles
    """
    vb = []

    def check(label, model_p, book_odds, fair_p):
        edge = model_p - fair_p
        if edge >= min_edge:
            ev = model_p * book_odds - 1
            vb.append({
                'market':    label,
                'odds':      book_odds,
                'model_p':   model_p,
                'implied_p': fair_p,
                'edge':      edge,
                'EV_%':      ev * 100,
            })

    # ── 1X2 ──────────────────────────────────────────────────────────────────
    if all(k in odds for k in ('1', 'X', '2')):
        fp1, fpx, fp2 = _remove_vig(odds['1'], odds['X'], odds['2'])
        check('1X2 ->Local (1)',  probs['1'], odds['1'], fp1)
        check('1X2 ->Empate (X)', probs['X'], odds['X'], fpx)
        check('1X2 ->Visita (2)', probs['2'], odds['2'], fp2)

    # ── Mercados binarios ─────────────────────────────────────────────────────
    BINARY_MARKETS = [
        # Goles totales
        ('odds_over_1.5',  'odds_under_1.5',  'over_1.5',  'under_1.5',  'Goles tot. O/U 1.5'),
        ('odds_over_2.5',  'odds_under_2.5',  'over_2.5',  'under_2.5',  'Goles tot. O/U 2.5'),
        ('odds_over_3.5',  'odds_under_3.5',  'over_3.5',  'under_3.5',  'Goles tot. O/U 3.5'),
        ('odds_over_4.5',  'odds_under_4.5',  'over_4.5',  'under_4.5',  'Goles tot. O/U 4.5'),
        # BTTS
        ('odds_btts_si',   'odds_btts_no',    'btts_si',   'btts_no',    'BTTS'),
        # Goles local
        ('odds_goles_local_over_0.5',  'odds_goles_local_under_0.5',  'goles_local_over_0.5',  'goles_local_under_0.5',  'Goles local O/U 0.5'),
        ('odds_goles_local_over_1.5',  'odds_goles_local_under_1.5',  'goles_local_over_1.5',  'goles_local_under_1.5',  'Goles local O/U 1.5'),
        ('odds_goles_local_over_2.5',  'odds_goles_local_under_2.5',  'goles_local_over_2.5',  'goles_local_under_2.5',  'Goles local O/U 2.5'),
        # Goles visitante
        ('odds_goles_visita_over_0.5', 'odds_goles_visita_under_0.5', 'goles_visita_over_0.5', 'goles_visita_under_0.5', 'Goles visita O/U 0.5'),
        ('odds_goles_visita_over_1.5', 'odds_goles_visita_under_1.5', 'goles_visita_over_1.5', 'goles_visita_under_1.5', 'Goles visita O/U 1.5'),
        ('odds_goles_visita_over_2.5', 'odds_goles_visita_under_2.5', 'goles_visita_over_2.5', 'goles_visita_under_2.5', 'Goles visita O/U 2.5'),
        # Corners totales
        ('odds_corners_over_8.5',  'odds_corners_under_8.5',  'corners_over_8.5',  'corners_under_8.5',  'Corners tot. O/U 8.5'),
        ('odds_corners_over_9.5',  'odds_corners_under_9.5',  'corners_over_9.5',  'corners_under_9.5',  'Corners tot. O/U 9.5'),
        ('odds_corners_over_10.5', 'odds_corners_under_10.5', 'corners_over_10.5', 'corners_under_10.5', 'Corners tot. O/U 10.5'),
        ('odds_corners_over_11.5', 'odds_corners_under_11.5', 'corners_over_11.5', 'corners_under_11.5', 'Corners tot. O/U 11.5'),
        # Corners local
        ('odds_corners_local_over_3.5', 'odds_corners_local_under_3.5', 'corners_local_over_3.5', 'corners_local_under_3.5', 'Corners local O/U 3.5'),
        ('odds_corners_local_over_4.5', 'odds_corners_local_under_4.5', 'corners_local_over_4.5', 'corners_local_under_4.5', 'Corners local O/U 4.5'),
        ('odds_corners_local_over_5.5', 'odds_corners_local_under_5.5', 'corners_local_over_5.5', 'corners_local_under_5.5', 'Corners local O/U 5.5'),
        ('odds_corners_local_over_6.5', 'odds_corners_local_under_6.5', 'corners_local_over_6.5', 'corners_local_under_6.5', 'Corners local O/U 6.5'),
        # Corners visitante
        ('odds_corners_visita_over_3.5', 'odds_corners_visita_under_3.5', 'corners_visita_over_3.5', 'corners_visita_under_3.5', 'Corners visita O/U 3.5'),
        ('odds_corners_visita_over_4.5', 'odds_corners_visita_under_4.5', 'corners_visita_over_4.5', 'corners_visita_under_4.5', 'Corners visita O/U 4.5'),
        ('odds_corners_visita_over_5.5', 'odds_corners_visita_under_5.5', 'corners_visita_over_5.5', 'corners_visita_under_5.5', 'Corners visita O/U 5.5'),
        # Tiros totales
        ('odds_shots_over_20.5',   'odds_shots_under_20.5',   'shots_over_20.5',   'shots_under_20.5',   'Tiros tot. O/U 20.5'),
        ('odds_shots_over_22.5',   'odds_shots_under_22.5',   'shots_over_22.5',   'shots_under_22.5',   'Tiros tot. O/U 22.5'),
        ('odds_shots_over_24.5',   'odds_shots_under_24.5',   'shots_over_24.5',   'shots_under_24.5',   'Tiros tot. O/U 24.5'),
        # Tiros local
        ('odds_shots_local_over_7.5',  'odds_shots_local_under_7.5',  'shots_local_over_7.5',  'shots_local_under_7.5',  'Tiros local O/U 7.5'),
        ('odds_shots_local_over_9.5',  'odds_shots_local_under_9.5',  'shots_local_over_9.5',  'shots_local_under_9.5',  'Tiros local O/U 9.5'),
        ('odds_shots_local_over_11.5', 'odds_shots_local_under_11.5', 'shots_local_over_11.5', 'shots_local_under_11.5', 'Tiros local O/U 11.5'),
        ('odds_shots_local_over_13.5', 'odds_shots_local_under_13.5', 'shots_local_over_13.5', 'shots_local_under_13.5', 'Tiros local O/U 13.5'),
        # Tiros visitante
        ('odds_shots_visita_over_7.5',  'odds_shots_visita_under_7.5',  'shots_visita_over_7.5',  'shots_visita_under_7.5',  'Tiros visita O/U 7.5'),
        ('odds_shots_visita_over_9.5',  'odds_shots_visita_under_9.5',  'shots_visita_over_9.5',  'shots_visita_under_9.5',  'Tiros visita O/U 9.5'),
        ('odds_shots_visita_over_11.5', 'odds_shots_visita_under_11.5', 'shots_visita_over_11.5', 'shots_visita_under_11.5', 'Tiros visita O/U 11.5'),
    ]

    for ok_over, ok_under, pk_over, pk_under, label in BINARY_MARKETS:
        has_over  = ok_over  in odds
        has_under = ok_under in odds

        if has_over and has_under:
            fp_over, fp_under = _remove_vig(odds[ok_over], odds[ok_under])
            if pk_over  in probs: check(f'{label} ->Over',  probs[pk_over],  odds[ok_over],  fp_over)
            if pk_under in probs: check(f'{label} ->Under', probs[pk_under], odds[ok_under], fp_under)
        elif has_over and pk_over in probs:
            check(f'{label} ->Over',  probs[pk_over],  odds[ok_over],  1 / odds[ok_over])
        elif has_under and pk_under in probs:
            check(f'{label} ->Under', probs[pk_under], odds[ok_under], 1 / odds[ok_under])

    # ── Asian Handicap ────────────────────────────────────────────────────────
    for k, o in odds.items():
        if k.startswith('odds_ahcp_'):
            hcp_str = k[len('odds_ahcp_'):]
            pk = f'ahcp_{hcp_str}'
            if pk in probs:
                check(f'AHcp Local {hcp_str}', probs[pk], o, 1 / o)

    vb.sort(key=lambda x: -x['edge'])
    return vb


# ─────────────────────────────────────────────────────────────────────────────
# Reporte
# ─────────────────────────────────────────────────────────────────────────────

def print_report(team_local: str, team_visitante: str,
                 competition: str | None,
                 params: dict, probs: dict,
                 value_bets: list[dict] | None = None):

    sep = '=' * 66
    print(f"\n{sep}")
    print(f"  PREDICCION: {team_local} vs {team_visitante}")
    if competition:
        print(f"  Competicion: {competition}")
    print(sep)

    print(f"\n[PARAMETROS ESTIMADOS]  (partidos: local={params['n_local_home']}, visita={params['n_vis_away']})")
    print(f"   lambda goles local    : {params['lambda_local']:.3f}")
    print(f"   lambda goles visita   : {params['lambda_vis']:.3f}")
    print(f"   mu corners local      : {params['mu_corners_local']:.2f}")
    print(f"   mu corners visita     : {params['mu_corners_vis']:.2f}")
    print(f"   mu tiros local        : {params['mu_shots_local']:.1f} +/- {params['sigma_shots_local']:.1f}")
    print(f"   mu tiros visita       : {params['mu_shots_vis']:.1f} +/- {params['sigma_shots_vis']:.1f}")
    print(f"   Posesion local        : {params['poss_local']:.1f}%")

    E_gt = probs['E_gl'] + probs['E_gv']
    E_ct = probs['E_cl'] + probs['E_cv']
    E_st = probs['E_sl'] + probs['E_sv']

    print(f"\n[VALORES ESPERADOS]")
    print(f"   Goles totales    : {E_gt:.2f}  (L:{probs['E_gl']:.2f}  V:{probs['E_gv']:.2f})")
    print(f"   Corners totales  : {E_ct:.2f}  (L:{probs['E_cl']:.2f}  V:{probs['E_cv']:.2f})")
    print(f"   Tiros totales    : {E_st:.2f}  (L:{probs['E_sl']:.2f}  V:{probs['E_sv']:.2f})")

    def j(p):
        """Odds justas a partir de probabilidad."""
        return f"{1/p:.2f}" if p > 0 else "inf"

    print(f"\n[RESULTADO 1X2]")
    print(f"   Local  (1): {probs['1']:6.1%}   odds justas: {j(probs['1'])}")
    print(f"   Empate (X): {probs['X']:6.1%}   odds justas: {j(probs['X'])}")
    print(f"   Visita (2): {probs['2']:6.1%}   odds justas: {j(probs['2'])}")

    def show_market_block(title, rows_mkts):
        print(f"\n{title}")
        for label, key in rows_mkts:
            p_val = probs.get(key, 0)
            bar   = '#' * int(p_val * 20)
            print(f"   {label:<28} {p_val:6.1%}  {bar}")

    show_market_block("[GOLES TOTALES]", [
        ('BTTS Si',              'btts_si'),
        ('BTTS No',              'btts_no'),
        ('Over 0.5 goles',       'over_0.5'),
        ('Over 1.5 goles',       'over_1.5'),
        ('Over 2.5 goles',       'over_2.5'),
        ('Over 3.5 goles',       'over_3.5'),
        ('Over 4.5 goles',       'over_4.5'),
    ])

    show_market_block(f"[GOLES {team_local.upper()} (LOCAL)]", [
        ('Anota 1+ goles',       'goles_local_over_0.5'),
        ('Anota 2+ goles',       'goles_local_over_1.5'),
        ('Anota 3+ goles',       'goles_local_over_2.5'),
    ])

    show_market_block(f"[GOLES {team_visitante.upper()} (VISITA)]", [
        ('Anota 1+ goles',       'goles_visita_over_0.5'),
        ('Anota 2+ goles',       'goles_visita_over_1.5'),
        ('Anota 3+ goles',       'goles_visita_over_2.5'),
    ])

    show_market_block("[CORNERS TOTALES]", [
        ('Over  8.5 corners',    'corners_over_8.5'),
        ('Over  9.5 corners',    'corners_over_9.5'),
        ('Over 10.5 corners',    'corners_over_10.5'),
        ('Over 11.5 corners',    'corners_over_11.5'),
        ('Over 12.5 corners',    'corners_over_12.5'),
    ])

    show_market_block(f"[CORNERS {team_local.upper()} (LOCAL)]", [
        ('Over 3.5 corners',     'corners_local_over_3.5'),
        ('Over 4.5 corners',     'corners_local_over_4.5'),
        ('Over 5.5 corners',     'corners_local_over_5.5'),
        ('Over 6.5 corners',     'corners_local_over_6.5'),
        ('Over 7.5 corners',     'corners_local_over_7.5'),
    ])

    show_market_block(f"[CORNERS {team_visitante.upper()} (VISITA)]", [
        ('Over 3.5 corners',     'corners_visita_over_3.5'),
        ('Over 4.5 corners',     'corners_visita_over_4.5'),
        ('Over 5.5 corners',     'corners_visita_over_5.5'),
        ('Over 6.5 corners',     'corners_visita_over_6.5'),
        ('Over 7.5 corners',     'corners_visita_over_7.5'),
    ])

    show_market_block("[TIROS TOTALES]", [
        ('Over 16.5 tiros',      'shots_over_16.5'),
        ('Over 18.5 tiros',      'shots_over_18.5'),
        ('Over 20.5 tiros',      'shots_over_20.5'),
        ('Over 22.5 tiros',      'shots_over_22.5'),
        ('Over 24.5 tiros',      'shots_over_24.5'),
    ])

    show_market_block(f"[TIROS {team_local.upper()} (LOCAL)]", [
        ('Over  7.5 tiros',      'shots_local_over_7.5'),
        ('Over  9.5 tiros',      'shots_local_over_9.5'),
        ('Over 11.5 tiros',      'shots_local_over_11.5'),
        ('Over 13.5 tiros',      'shots_local_over_13.5'),
        ('Over 15.5 tiros',      'shots_local_over_15.5'),
    ])

    show_market_block(f"[TIROS {team_visitante.upper()} (VISITA)]", [
        ('Over  7.5 tiros',      'shots_visita_over_7.5'),
        ('Over  9.5 tiros',      'shots_visita_over_9.5'),
        ('Over 11.5 tiros',      'shots_visita_over_11.5'),
        ('Over 13.5 tiros',      'shots_visita_over_13.5'),
        ('Over 15.5 tiros',      'shots_visita_over_15.5'),
    ])

    print(f"\n[MARCADORES MAS PROBABLES]")
    for score, prob in sorted(probs['score_dist'].items(), key=lambda x: -x[1])[:10]:
        bar = '#' * int(prob * 200)
        print(f"   {score:>5}: {prob:5.1%}  {bar}")

    print(f"\n[ASIAN HANDICAP LOCAL]")
    for hcp in [-1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5]:
        key = f'ahcp_{hcp:+.1f}'
        if key in probs:
            print(f"   {hcp:+.1f}: {probs[key]:.1%}   odds justas: {j(probs[key])}")

    if value_bets:
        print(f"\n*** VALUE BETS detectadas (edge >= {MIN_EDGE:.0%}) ***")
        hdr = f"   {'Mercado':<32} {'Odds':>6}  {'Modelo':>7}  {'Impl.':>7}  {'Edge':>6}  {'EV%':>6}"
        print(hdr)
        print('   ' + '-' * 68)
        for vb in value_bets:
            print(f"   {vb['market']:<32} {vb['odds']:>6.2f}  "
                  f"{vb['model_p']:>7.1%}  {vb['implied_p']:>7.1%}  "
                  f"{vb['edge']:>+5.1%}  {vb['EV_%']:>+5.1f}%")
    else:
        print(f"\n   (Proporciona cuotas de bookmaker para detectar value bets)")

    print()


# ─────────────────────────────────────────────────────────────────────────────
# API pública
# ─────────────────────────────────────────────────────────────────────────────

def predict(team_local: str,
            team_visitante: str,
            competition: str | None = None,
            odds: dict | None = None,
            n_sim: int = N_SIM_DEFAULT,
            verbose: bool = True) -> tuple[dict, dict]:
    """
    Función principal del modelo.

    Parámetros
    ----------
    team_local      : Nombre del equipo local (debe coincidir con el CSV)
    team_visitante  : Nombre del equipo visitante
    competition     : Filtro de competición ('La Liga', 'Liga Profesional', ...)
    odds            : Dict de cuotas del bookmaker para detectar value bets
    n_sim           : Número de simulaciones Monte Carlo (default 100 000)
    verbose         : Imprime el reporte completo

    Retorna
    -------
    (probs, params) : dicts con probabilidades y parámetros estimados
    """
    rows   = load_csv()
    params = compute_match_params(team_local, team_visitante, rows, competition)
    sim    = run_simulation(params, n_sim)
    probs  = compute_probabilities(sim)
    vb     = find_value_bets(probs, odds) if odds else None

    if verbose:
        print_report(team_local, team_visitante, competition, params, probs, vb)

    return probs, params


# ─────────────────────────────────────────────────────────────────────────────
# Demo / ejecución directa
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print("=" * 66)
    print(" MODELO V2 - DEMO CON 3 PARTIDOS DE EJEMPLO")
    print("=" * 66)

    # ── Ejemplo 1: Independiente como local en Liga Profesional ───────────────
    predict(
        team_local      = 'Independiente',
        team_visitante  = 'Racing Club',
        competition     = 'Liga Profesional',
        odds = {
            '1': 2.30, 'X': 3.20, '2': 3.00,
            'odds_over_2.5': 1.95, 'odds_under_2.5': 1.85,
            'odds_btts_si':  1.90, 'odds_btts_no':   1.90,
            'odds_corners_over_9.5': 1.80, 'odds_corners_under_9.5': 2.00,
        }
    )

    # ── Ejemplo 2: Barcelona en La Liga ──────────────────────────────────────
    predict(
        team_local      = 'Barcelona',
        team_visitante  = 'Real Madrid',
        competition     = 'La Liga',
        odds = {
            '1': 2.10, 'X': 3.50, '2': 3.30,
            'odds_over_2.5': 1.75, 'odds_under_2.5': 2.05,
            'odds_btts_si':  1.70, 'odds_btts_no':   2.10,
        }
    )

    # ── Ejemplo 3: Atlético Madrid en Champions League ────────────────────────
    predict(
        team_local      = 'Atlético Madrid',
        team_visitante  = 'Inter Milan',
        competition     = 'Champions League',
    )
