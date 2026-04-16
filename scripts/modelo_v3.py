"""
MODELO DE PREDICCIÓN V3 — Apuestas Deportivas
================================================
Mejoras respecto a V2:

  [V3-1] SHRINKAGE BAYESIANO
         Los ratings de equipo se mezclan con el promedio de liga (1.0)
         según el tamaño de muestra. Con pocos partidos el modelo confía
         más en la liga; con muchos, en el equipo.
         Formula: rating_shrunk = (n × raw + K) / (n + K),  K = K_SHRINK

  [V3-2] MIN_MATCHES elevado de 2 → 5
         Con sólo 2 partidos los ratings eran muy ruidosos. Ahora se
         necesitan al menos 5 para considerar los datos "confiables"
         (el shrinkage sigue operando con n < 5, pero el reporte avisa).

  [V3-3] ELIMINACIÓN DEL FALLBACK AL CONTEXTO OPUESTO
         En V2, si un equipo no tenía suficientes datos como local usaba
         sus estadísticas de visitante. Eso introducía sesgo sistemático.
         En V3, si no hay datos en el contexto correcto se usa el promedio
         de liga (rating = 1.0 vía shrinkage), que es neutral.

  [V3-4] PONDERACIÓN POR RECENCIA (decaimiento exponencial)
         Los partidos recientes pesan más que los lejanos.
         Peso: w_i = exp(-ln(2) × días_atrás / HALF_LIFE_DAYS)
         Con HALF_LIFE_DAYS = 90 un partido de hace 3 meses pesa 0.5,
         uno de hace 6 meses pesa 0.25, uno de hace 1 año pesa ~0.06.

  [V3-5] FACTOR FORMA (últimos N_FORM partidos)
         Se calcula un multiplicador basado en los últimos N_FORM partidos
         en el contexto correcto vs el promedio ponderado de temporada.
         Se mezcla con el rating global: FORM_WEIGHT × forma + (1-FORM_WEIGHT) × 1.0
         Esto captura rachas sin dominar el modelo con muestras pequeñas.

  [V3-6] SHOTS ON TARGET incluido en TeamRecord
         Se agrega tiros_arco_local/visitante al registro de equipo,
         disponible para compute_arco_params y análisis futuros.

Uso básico:
    python modelo_v3.py

Uso como librería:
    from modelo_v3 import predict
    probs, params = predict('Independiente', 'Racing Club',
                            competition='Liga Profesional')
"""

import csv
import math
import random
import datetime
import unicodedata
from collections import defaultdict, Counter
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

CSV_PATH      = Path(r'C:\Users\Matt\Apuestas Deportivas\data\historico\partidos_historicos.csv')
EQUIPOS_PATH  = Path(r'C:\Users\Matt\Apuestas Deportivas\data\db\equipos.csv')
LIGAS_PATH    = Path(r'C:\Users\Matt\Apuestas Deportivas\data\db\ligas.csv')

N_SIM_DEFAULT = 100_000
MIN_EDGE      = 0.04     # 4 % de ventaja mínima para declarar value bet

# ── [V3-1] Shrinkage ────────────────────────────────────────────────────────
K_SHRINK      = 8        # partidos "virtuales" al promedio de liga para shrinkage

# ── [V3-2] Mínimo de partidos para rating "confiable" (sólo para avisos) ────
MIN_MATCHES   = 5        # era 2 en V2

# ── [V3-4] Recencia ─────────────────────────────────────────────────────────
HALF_LIFE_DAYS = 90      # días en que el peso de un partido se reduce a la mitad

# ── [V3-5] Forma ────────────────────────────────────────────────────────────
N_FORM        = 5        # últimos N partidos para el factor forma
FORM_WEIGHT   = 0.20     # 20% forma reciente, 80% promedio ponderado de temporada


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


def negbinom_sample(mu: float, k: float) -> int:
    """
    Muestrea de Binomial Negativa parameterizada por media (mu) y
    parámetro de dispersión (k).

    Método: mezcla Gamma-Poisson.
        λ ~ Gamma(shape=k, scale=mu/k)
        X | λ ~ Poisson(λ)
        => X ~ NegBin(mu, k)

    Propiedades:
        E[X]   = mu
        Var[X] = mu + mu²/k   (siempre > mu, es overdispersed)
        k→∞   => NegBin → Poisson(mu)

    Valores típicos para corners totales: k ≈ 5–15
    """
    if mu <= 0:
        return 0
    if k <= 0 or k >= 1000:      # k muy grande → aproximar con Poisson
        return poisson_sample(mu)
    lam = random.gammavariate(k, mu / k)
    return poisson_sample(lam)


def binomial_sample(n: int, p: float) -> int:
    """
    Muestrea de Binomial(n, p) por método directo (trial por trial).

    Apropiado para n pequeños (corners por partido: n ≤ ~25).
    Para n grandes considerar aproximación Normal.
    """
    if n <= 0:
        return 0
    p = max(0.0, min(1.0, p))
    return sum(1 for _ in range(n) if random.random() < p)


def safe_mean(vals: list) -> float | None:
    return sum(vals) / len(vals) if vals else None


def safe_std(vals: list, mean: float) -> float:
    if len(vals) < 2:
        return 0.0
    var = sum((x - mean) ** 2 for x in vals) / (len(vals) - 1)
    return math.sqrt(var)


def _today_ord() -> int:
    return datetime.date.today().toordinal()


def _parse_date(fecha_str: str) -> int:
    """Convierte 'YYYY-MM-DD' a número ordinal. Devuelve 0 si falla."""
    try:
        return datetime.date.fromisoformat(fecha_str).toordinal()
    except (ValueError, TypeError):
        return 0


# ─────────────────────────────────────────────────────────────────────────────
# Carga de datos
# ─────────────────────────────────────────────────────────────────────────────

def load_csv(path: Path = CSV_PATH) -> list[dict]:
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def load_teams_db(path: Path = EQUIPOS_PATH) -> tuple[dict, dict]:
    id_to_name, name_to_id = {}, {}
    with open(path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            tid  = int(row['id'])
            name = row['nombre']
            id_to_name[tid] = name
            name_to_id[norm(name)] = tid
    return id_to_name, name_to_id


def load_leagues_db(path: Path = LIGAS_PATH) -> tuple[dict, dict]:
    id_to_name, name_to_id = {}, {}
    with open(path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            lid  = int(row['id'])
            name = row['nombre']
            id_to_name[lid] = name
            name_to_id[norm(name)] = lid
    return id_to_name, name_to_id


def resolve_team_id(team: str | int, name_to_id: dict) -> int | None:
    if isinstance(team, int):
        return team
    try:
        return int(team)
    except (ValueError, TypeError):
        pass
    return name_to_id.get(norm(str(team)))


def resolve_liga_id(liga: str | int, name_to_id: dict) -> int | None:
    if isinstance(liga, int):
        return liga
    try:
        return int(liga)
    except (ValueError, TypeError):
        pass
    return name_to_id.get(norm(str(liga)))


# ─────────────────────────────────────────────────────────────────────────────
# [V3-4][V3-5][V3-6] TeamRecord con fechas y shots_on_target
# ─────────────────────────────────────────────────────────────────────────────

class TeamRecord:
    """
    Acumula estadísticas de un equipo separadas por contexto (home/away).

    V3: cada campo almacena lista de (valor, fecha_ordinal) en lugar de sólo
    valores, lo que permite la ponderación por recencia [V3-4] y el cálculo
    de forma [V3-5]. Se agrega shots_on_target [V3-6].
    """

    FIELDS = [
        'goals', 'goals_conceded',
        'shots', 'shots_conceded',
        'shots_on_target', 'shots_on_target_conceded',  # [V3-6]
        'corners', 'corners_conceded',
        'possession', 'cards',
    ]

    def __init__(self):
        # Cada campo: lista de (valor: int, fecha_ordinal: int)
        self.home: dict[str, list[tuple[int, int]]] = defaultdict(list)
        self.away: dict[str, list[tuple[int, int]]] = defaultdict(list)

    def _add(self, store: dict, row: dict, is_home: bool):
        date_ord = _parse_date(row.get('fecha', ''))

        if is_home:
            store['goals'].append(              (int(row['goles_local']),               date_ord))
            store['goals_conceded'].append(     (int(row['goles_visitante']),            date_ord))
            store['shots'].append(              (int(row['tiros_local']),                date_ord))
            store['shots_conceded'].append(     (int(row['tiros_visitante']),            date_ord))
            store['shots_on_target'].append(    (int(row.get('tiros_arco_local',  0)),   date_ord))
            store['shots_on_target_conceded'].append((int(row.get('tiros_arco_visitante', 0)), date_ord))
            store['corners'].append(            (int(row['corners_local']),              date_ord))
            store['corners_conceded'].append(   (int(row['corners_visitante']),          date_ord))
            store['possession'].append(         (int(row['posesion_local']),             date_ord))
            store['cards'].append(              (int(row['tarjetas_local']),             date_ord))
        else:
            store['goals'].append(              (int(row['goles_visitante']),            date_ord))
            store['goals_conceded'].append(     (int(row['goles_local']),                date_ord))
            store['shots'].append(              (int(row['tiros_visitante']),            date_ord))
            store['shots_conceded'].append(     (int(row['tiros_local']),                date_ord))
            store['shots_on_target'].append(    (int(row.get('tiros_arco_visitante', 0)), date_ord))
            store['shots_on_target_conceded'].append((int(row.get('tiros_arco_local', 0)), date_ord))
            store['corners'].append(            (int(row['corners_visitante']),          date_ord))
            store['corners_conceded'].append(   (int(row['corners_local']),              date_ord))
            store['possession'].append(         (int(row['posesion_visitante']),         date_ord))
            store['cards'].append(              (int(row['tarjetas_visitante']),         date_ord))

    def add_home_row(self, row: dict): self._add(self.home, row, True)
    def add_away_row(self, row: dict): self._add(self.away, row, False)

    def n(self, ctx: str) -> int:
        store = self.home if ctx == 'home' else self.away
        return len(store['goals'])

    # [V3-4] Promedio ponderado por recencia
    def weighted_avg(self, ctx: str, field: str,
                     today_ord: int,
                     half_life: int = HALF_LIFE_DAYS) -> float | None:
        store  = self.home if ctx == 'home' else self.away
        pairs  = store.get(field, [])
        if not pairs:
            return None
        lam = math.log(2) / half_life if half_life > 0 else 0.0
        total_w = total_wv = 0.0
        for val, date_ord in pairs:
            days_ago = max(0, today_ord - date_ord)
            w = math.exp(-lam * days_ago)
            total_w  += w
            total_wv += w * val
        return total_wv / total_w if total_w > 0 else None

    # [V3-5] Promedio simple de los últimos N partidos (por fecha)
    def form_avg(self, ctx: str, field: str, n_form: int = N_FORM) -> float | None:
        store = self.home if ctx == 'home' else self.away
        pairs = store.get(field, [])
        if not pairs:
            return None
        recent = sorted(pairs, key=lambda x: x[1])[-n_form:]
        vals   = [v for v, _ in recent]
        return sum(vals) / len(vals) if vals else None

    def std(self, ctx: str, field: str) -> float:
        store = self.home if ctx == 'home' else self.away
        vals  = [v for v, _ in store.get(field, [])]
        if len(vals) < 2:
            return 0.0
        m   = sum(vals) / len(vals)
        var = sum((x - m) ** 2 for x in vals) / (len(vals) - 1)
        return math.sqrt(var)


def build_records(rows: list[dict],
                  liga_id_filter: int | None = None) -> dict[int, TeamRecord]:
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
    """Promedios de liga (no ponderados por recencia — sirven como prior estable)."""
    filtered = [r for r in rows if not liga_id_filter
                or int(r['liga_id']) == liga_id_filter]
    if len(filtered) < 3:
        filtered = rows

    n = len(filtered)

    def avg_field(fn):
        return sum(fn(r) for r in filtered) / n

    return {
        'home_goals':   avg_field(lambda r: int(r['goles_local'])),
        'away_goals':   avg_field(lambda r: int(r['goles_visitante'])),
        'home_corners': avg_field(lambda r: int(r['corners_local'])),
        'away_corners': avg_field(lambda r: int(r['corners_visitante'])),
        'home_shots':   avg_field(lambda r: int(r['tiros_local'])),
        'away_shots':   avg_field(lambda r: int(r['tiros_visitante'])),
        'home_shots_std': 4.0,
        'away_shots_std': 4.0,
        'home_cards':   avg_field(lambda r: int(r['tarjetas_local'])),
        'away_cards':   avg_field(lambda r: int(r['tarjetas_visitante'])),
    }


# ─────────────────────────────────────────────────────────────────────────────
# [V3-1] Shrinkage bayesiano
# ─────────────────────────────────────────────────────────────────────────────

def _rating_shrunk(team_avg: float | None, league_avg: float,
                   n: int, k: int = K_SHRINK) -> float:
    """
    Mezcla el rating real del equipo con 1.0 (promedio de liga) según muestra.

        rating_shrunk = (n × raw_rating + k × 1.0) / (n + k)

    Ejemplos con k=8:
        n=0  → rating=1.00  (puro promedio de liga)
        n=4  → rating=0.33*raw + 0.67*1.0
        n=8  → rating=0.50*raw + 0.50*1.0
        n=16 → rating=0.67*raw + 0.33*1.0
        n=40 → rating=0.83*raw + 0.17*1.0
    """
    if team_avg is None or league_avg == 0:
        return 1.0
    raw = team_avg / league_avg
    return (n * raw + k * 1.0) / (n + k)


# ─────────────────────────────────────────────────────────────────────────────
# [V3-3][V3-4] Obtención de estadística ponderada (sin fallback a contexto opuesto)
# ─────────────────────────────────────────────────────────────────────────────

def _get_team_stat(records_comp: dict, records_all: dict,
                   team: int, ctx: str, field: str,
                   today_ord: int) -> tuple[float | None, int]:
    """
    Retorna (promedio_ponderado_por_recencia, n_partidos) para el campo dado.
    Busca primero en registros de la competición, luego en el histórico global.

    [V3-3] NO hace fallback al contexto opuesto (home↔away).
           Si no hay datos en el contexto correcto devuelve (None, 0)
           y el shrinkage se encarga de usar el promedio de liga.
    """
    for recs in (records_comp, records_all):
        rec = recs.get(team)
        if rec and rec.n(ctx) > 0:
            v = rec.weighted_avg(ctx, field, today_ord)
            if v is not None:
                return v, rec.n(ctx)
    return None, 0


# ─────────────────────────────────────────────────────────────────────────────
# [V3-5] Factor forma
# ─────────────────────────────────────────────────────────────────────────────

def _form_multiplier(records_comp: dict, records_all: dict,
                     team: int, ctx: str, field: str) -> float:
    """
    Ratio entre el promedio simple de los últimos N_FORM partidos
    y el promedio histórico simple del equipo en ese contexto.

    Resultado: 1.0 si sin datos o sin variación, >1 si en forma, <1 si en baja.
    Se mezcla con FORM_WEIGHT para no dominar el modelo.
    """
    for recs in (records_comp, records_all):
        rec = recs.get(team)
        if rec and rec.n(ctx) >= N_FORM:
            form    = rec.form_avg(ctx, field, N_FORM)
            # Usamos el promedio simple de toda la temporada como baseline de forma
            all_vals = [v for v, _ in (rec.home if ctx == 'home' else rec.away).get(field, [])]
            season   = sum(all_vals) / len(all_vals) if all_vals else None
            if form is not None and season and season > 0:
                raw_mult = form / season
                # Mezcla: FORM_WEIGHT hacia el multiplicador real, resto neutral (1.0)
                return 1.0 + FORM_WEIGHT * (raw_mult - 1.0)
    return 1.0


# ─────────────────────────────────────────────────────────────────────────────
# Estimación de parámetros del partido
# ─────────────────────────────────────────────────────────────────────────────

def compute_match_params(team_local: str | int, team_visitante: str | int,
                         rows: list[dict],
                         competition: str | int | None = None) -> dict:
    """
    Estima todos los parámetros necesarios para la simulación Monte Carlo.

    V3: aplica shrinkage [V3-1], recencia [V3-4], forma [V3-5],
        sin fallback al contexto opuesto [V3-3].

    Modelo de goles (Poisson):
        λ_local = avg_liga_home × rating_atk_local_shrunk
                                × rating_def_visita_shrunk
                                × forma_atk_local
        λ_vis   = avg_liga_away × rating_atk_visita_shrunk
                                × rating_def_local_shrunk
                                × forma_atk_visita

    Modelo de corners (Poisson) — ídem estructura
    Modelo de tiros   (Normal)  — ídem estructura
    """
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
    today_ord = _today_ord()

    # Helper: obtiene stat + n, luego aplica shrinkage
    def rating(team, ctx, field, league_avg):
        avg, n = _get_team_stat(recs_comp, recs_all, team, ctx, field, today_ord)
        return _rating_shrunk(avg, league_avg, n)

    # Helper: forma multiplicativa
    def forma(team, ctx, field):
        return _form_multiplier(recs_comp, recs_all, team, ctx, field)

    # ── Conteo de partidos (para el reporte) ──────────────────────────────────
    _, n_local_home = _get_team_stat(recs_comp, recs_all, local_id, 'home', 'goals', today_ord)
    _, n_vis_away   = _get_team_stat(recs_comp, recs_all, vis_id,   'away', 'goals', today_ord)

    # ── Goles ─────────────────────────────────────────────────────────────────
    r_atk_l = rating(local_id, 'home', 'goals',          la['home_goals'])
    r_def_v = rating(vis_id,   'away', 'goals_conceded',  la['home_goals'])
    r_atk_v = rating(vis_id,   'away', 'goals',           la['away_goals'])
    r_def_l = rating(local_id, 'home', 'goals_conceded',  la['away_goals'])

    f_atk_l = forma(local_id, 'home', 'goals')
    f_atk_v = forma(vis_id,   'away', 'goals')

    lambda_local = max(0.15, la['home_goals'] * r_atk_l * r_def_v * f_atk_l)
    lambda_vis   = max(0.15, la['away_goals'] * r_atk_v * r_def_l * f_atk_v)

    # ── Corners — MODELO V4: NegBin total + Binomial por equipo ─────────────
    # El total del partido se ancla al pace; el reparto usa ratings atk/def.
    _cp = compute_corners_pace(team_local, team_visitante, rows, competition)
    mu_corners_total  = _cp['mu_total']
    share_corners_loc = _cp['share_local']
    k_corners         = estimate_corners_k(rows, liga_id)
    # Medias individuales (para diagnóstico / compatibilidad de prints)
    mu_corners_local  = _cp['mu_local']
    mu_corners_vis    = _cp['mu_visita']

    # ── MODELO ANTERIOR (Poisson independiente) — COMENTADO ──────────────────
    # r_atk_cl = rating(local_id, 'home', 'corners',           la['home_corners'])
    # r_def_cv = rating(vis_id,   'away', 'corners_conceded',   la['home_corners'])
    # r_atk_cv = rating(vis_id,   'away', 'corners',            la['away_corners'])
    # r_def_cl = rating(local_id, 'home', 'corners_conceded',   la['away_corners'])
    # f_c_l = forma(local_id, 'home', 'corners')
    # f_c_v = forma(vis_id,   'away', 'corners')
    # mu_corners_local = max(0.5, la['home_corners'] * r_atk_cl * r_def_cv * f_c_l)
    # mu_corners_vis   = max(0.5, la['away_corners'] * r_atk_cv * r_def_cl * f_c_v)
    # ─────────────────────────────────────────────────────────────────────────

    # ── Tiros ─────────────────────────────────────────────────────────────────
    r_atk_sl = rating(local_id, 'home', 'shots',           la['home_shots'])
    r_def_sv = rating(vis_id,   'away', 'shots_conceded',   la['home_shots'])
    r_atk_sv = rating(vis_id,   'away', 'shots',            la['away_shots'])
    r_def_sl = rating(local_id, 'home', 'shots_conceded',   la['away_shots'])

    f_s_l = forma(local_id, 'home', 'shots')
    f_s_v = forma(vis_id,   'away', 'shots')

    mu_shots_local = max(1.0, la['home_shots'] * r_atk_sl * r_def_sv * f_s_l)
    mu_shots_vis   = max(1.0, la['away_shots'] * r_atk_sv * r_def_sl * f_s_v)

    # σ tiros — usa std histórico propio si disponible
    def shot_std(team, ctx, mu):
        for recs in (recs_comp, recs_all):
            rec = recs.get(team)
            if rec and rec.n(ctx) >= MIN_MATCHES:
                s = rec.std(ctx, 'shots')
                if s > 0:
                    return s
        return mu * 0.30

    sigma_shots_local = shot_std(local_id, 'home', mu_shots_local)
    sigma_shots_vis   = shot_std(vis_id,   'away', mu_shots_vis)

    # ── Tarjetas ─────────────────────────────────────────────────────────────
    # Modelo simple: las tarjetas que un equipo recibe dependen de su propio
    # estilo (no del rival). Se usa solo el rating de ataque (generación de cards).
    r_cards_l = rating(local_id, 'home', 'cards', la['home_cards'])
    r_cards_v = rating(vis_id,   'away', 'cards', la['away_cards'])

    f_cards_l = forma(local_id, 'home', 'cards')
    f_cards_v = forma(vis_id,   'away', 'cards')

    mu_tarjetas_local = max(0.1, la['home_cards'] * r_cards_l * f_cards_l)
    mu_tarjetas_vis   = max(0.1, la['away_cards'] * r_cards_v * f_cards_v)

    # ── Posesión ──────────────────────────────────────────────────────────────
    poss_avg_l, _ = _get_team_stat(recs_comp, recs_all, local_id, 'home', 'possession', today_ord)
    poss_avg_v, _ = _get_team_stat(recs_comp, recs_all, vis_id,   'away', 'possession', today_ord)
    raw_l   = poss_avg_l or 50.0
    raw_v   = poss_avg_v or 50.0
    total_p = raw_l + raw_v
    poss_local = 100.0 * raw_l / total_p if total_p > 0 else 50.0

    # ── Ratings intermedios (para diagnóstico en reporte) ─────────────────────
    return {
        'lambda_local':      lambda_local,
        'lambda_vis':        lambda_vis,
        'mu_corners_total':    mu_corners_total,
        'share_corners_loc':   share_corners_loc,
        'k_corners':           k_corners,
        'mu_corners_local':    mu_corners_local,   # = mu_total * share  (diagnóstico)
        'mu_corners_vis':      mu_corners_vis,     # = mu_total * (1-share)
        'mu_tarjetas_local':   mu_tarjetas_local,
        'mu_tarjetas_vis':     mu_tarjetas_vis,
        'mu_shots_local':      mu_shots_local,
        'mu_shots_vis':      mu_shots_vis,
        'sigma_shots_local': max(0.5, sigma_shots_local),
        'sigma_shots_vis':   max(0.5, sigma_shots_vis),
        'poss_local':        poss_local,
        'n_local_home':      n_local_home,
        'n_vis_away':        n_vis_away,
        # Ratings desagregados para depuración
        '_ratings': {
            'atk_local': r_atk_l, 'def_visita': r_def_v,
            'atk_visita': r_atk_v, 'def_local': r_def_l,
            'forma_atk_local': f_atk_l, 'forma_atk_visita': f_atk_v,
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Simulación Monte Carlo  (sin cambios respecto a V2)
# ─────────────────────────────────────────────────────────────────────────────

def run_simulation(params: dict, n: int = N_SIM_DEFAULT) -> dict:
    """Ejecuta n iteraciones de Monte Carlo. Devuelve listas de resultados simulados."""
    lam_l  = params['lambda_local']
    lam_v  = params['lambda_vis']
    mu_tl  = params['mu_tarjetas_local']
    mu_tv  = params['mu_tarjetas_vis']
    mu_sl  = params['mu_shots_local']
    mu_sv  = params['mu_shots_vis']
    sig_sl = params['sigma_shots_local']
    sig_sv = params['sigma_shots_vis']

    # Corners — MODELO V4: NegBin total + Binomial por equipo
    mu_ct      = params['mu_corners_total']
    share_loc  = params['share_corners_loc']
    k_c        = params['k_corners']

    # ── MODELO ANTERIOR corners (Poisson independiente) — COMENTADO ──────────
    # mu_cl = params['mu_corners_local']
    # mu_cv = params['mu_corners_vis']
    # ─────────────────────────────────────────────────────────────────────────

    gl, gv, cl, cv, tl, tv, sl, sv = [], [], [], [], [], [], [], []

    for _ in range(n):
        gl.append(poisson_sample(lam_l))
        gv.append(poisson_sample(lam_v))
        tl.append(poisson_sample(mu_tl))
        tv.append(poisson_sample(mu_tv))
        sl.append(normal_sample_pos(mu_sl, sig_sl))
        sv.append(normal_sample_pos(mu_sv, sig_sv))

        # Corners V4: NegBin total → Binomial reparto
        T = negbinom_sample(mu_ct, k_c)
        c_local = binomial_sample(T, share_loc)
        cl.append(c_local)
        cv.append(T - c_local)

        # ── MODELO ANTERIOR corners — COMENTADO ──────────────────────────────
        # cl.append(poisson_sample(mu_cl))
        # cv.append(poisson_sample(mu_cv))
        # ─────────────────────────────────────────────────────────────────────

    return {
        'gl': gl, 'gv': gv,
        'cl': cl, 'cv': cv,
        'tl': tl, 'tv': tv,
        'sl': sl, 'sv': sv,
        'n': n,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Cálculo de probabilidades  (sin cambios respecto a V2)
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

    # Asian Handicap goles
    diff = [gl[i] - gv[i] for i in range(n)]
    for hcp in [-2.5, -2.0, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5]:
        threshold = -hcp
        if hcp == int(hcp):
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

    # Corners individuales
    for thr in [3.5, 4.5, 5.5, 6.5, 7.5]:
        p[f'corners_local_over_{thr}']   = sum(c > thr for c in cl) / n
        p[f'corners_local_under_{thr}']  = 1 - p[f'corners_local_over_{thr}']
        p[f'corners_visita_over_{thr}']  = sum(c > thr for c in cv) / n
        p[f'corners_visita_under_{thr}'] = 1 - p[f'corners_visita_over_{thr}']

    # Tiros O/U totales
    for thr in [16.5, 18.5, 20.5, 22.5, 24.5, 26.5, 28.5]:
        p[f'shots_over_{thr}']  = sum(s > thr for s in ts) / n
        p[f'shots_under_{thr}'] = 1 - p[f'shots_over_{thr}']

    # Tiros individuales
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
# Detección de value bets  (sin cambios respecto a V2)
# ─────────────────────────────────────────────────────────────────────────────

def _remove_vig(*odds_list: float) -> list[float]:
    implied = [1 / o for o in odds_list]
    total   = sum(implied)
    return [p / total for p in implied]


def find_value_bets(probs: dict, odds: dict, min_edge: float = MIN_EDGE) -> list[dict]:
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

    if all(k in odds for k in ('1', 'X', '2')):
        fp1, fpx, fp2 = _remove_vig(odds['1'], odds['X'], odds['2'])
        check('1X2 ->Local (1)',   probs['1'], odds['1'], fp1)
        check('1X2 ->Empate (X)',  probs['X'], odds['X'], fpx)
        check('1X2 ->Visita (2)',  probs['2'], odds['2'], fp2)

    BINARY_MARKETS = [
        ('odds_over_1.5',  'odds_under_1.5',  'over_1.5',  'under_1.5',  'Goles tot. O/U 1.5'),
        ('odds_over_2.5',  'odds_under_2.5',  'over_2.5',  'under_2.5',  'Goles tot. O/U 2.5'),
        ('odds_over_3.5',  'odds_under_3.5',  'over_3.5',  'under_3.5',  'Goles tot. O/U 3.5'),
        ('odds_over_4.5',  'odds_under_4.5',  'over_4.5',  'under_4.5',  'Goles tot. O/U 4.5'),
        ('odds_btts_si',   'odds_btts_no',    'btts_si',   'btts_no',    'BTTS'),
        ('odds_goles_local_over_0.5',  'odds_goles_local_under_0.5',  'goles_local_over_0.5',  'goles_local_under_0.5',  'Goles local O/U 0.5'),
        ('odds_goles_local_over_1.5',  'odds_goles_local_under_1.5',  'goles_local_over_1.5',  'goles_local_under_1.5',  'Goles local O/U 1.5'),
        ('odds_goles_local_over_2.5',  'odds_goles_local_under_2.5',  'goles_local_over_2.5',  'goles_local_under_2.5',  'Goles local O/U 2.5'),
        ('odds_goles_visita_over_0.5', 'odds_goles_visita_under_0.5', 'goles_visita_over_0.5', 'goles_visita_under_0.5', 'Goles visita O/U 0.5'),
        ('odds_goles_visita_over_1.5', 'odds_goles_visita_under_1.5', 'goles_visita_over_1.5', 'goles_visita_under_1.5', 'Goles visita O/U 1.5'),
        ('odds_goles_visita_over_2.5', 'odds_goles_visita_under_2.5', 'goles_visita_over_2.5', 'goles_visita_under_2.5', 'Goles visita O/U 2.5'),
        # ── Corners totales DESACTIVADOS — backtest v3.1: ROI -31.4% (38 bets) ──
        # Los corners individuales (local/visita) tienen ROI +74% y +54%.
        # Los totales pierden en todos los rangos de edge, cuota y threshold.
        # ('odds_corners_over_8.5',   'odds_corners_under_8.5',   'corners_over_8.5',   'corners_under_8.5',   'Corners tot. O/U 8.5'),
        # ('odds_corners_over_9.5',   'odds_corners_under_9.5',   'corners_over_9.5',   'corners_under_9.5',   'Corners tot. O/U 9.5'),
        # ('odds_corners_over_10.5',  'odds_corners_under_10.5',  'corners_over_10.5',  'corners_under_10.5',  'Corners tot. O/U 10.5'),
        # ('odds_corners_over_11.5',  'odds_corners_under_11.5',  'corners_over_11.5',  'corners_under_11.5',  'Corners tot. O/U 11.5'),
        ('odds_corners_local_over_3.5',  'odds_corners_local_under_3.5',  'corners_local_over_3.5',  'corners_local_under_3.5',  'Corners local O/U 3.5'),
        ('odds_corners_local_over_4.5',  'odds_corners_local_under_4.5',  'corners_local_over_4.5',  'corners_local_under_4.5',  'Corners local O/U 4.5'),
        ('odds_corners_local_over_5.5',  'odds_corners_local_under_5.5',  'corners_local_over_5.5',  'corners_local_under_5.5',  'Corners local O/U 5.5'),
        ('odds_corners_local_over_6.5',  'odds_corners_local_under_6.5',  'corners_local_over_6.5',  'corners_local_under_6.5',  'Corners local O/U 6.5'),
        ('odds_corners_visita_over_3.5', 'odds_corners_visita_under_3.5', 'corners_visita_over_3.5', 'corners_visita_under_3.5', 'Corners visita O/U 3.5'),
        ('odds_corners_visita_over_4.5', 'odds_corners_visita_under_4.5', 'corners_visita_over_4.5', 'corners_visita_under_4.5', 'Corners visita O/U 4.5'),
        ('odds_corners_visita_over_5.5', 'odds_corners_visita_under_5.5', 'corners_visita_over_5.5', 'corners_visita_under_5.5', 'Corners visita O/U 5.5'),
        ('odds_shots_over_20.5',   'odds_shots_under_20.5',   'shots_over_20.5',   'shots_under_20.5',   'Tiros tot. O/U 20.5'),
        ('odds_shots_over_22.5',   'odds_shots_under_22.5',   'shots_over_22.5',   'shots_under_22.5',   'Tiros tot. O/U 22.5'),
        ('odds_shots_over_24.5',   'odds_shots_under_24.5',   'shots_over_24.5',   'shots_under_24.5',   'Tiros tot. O/U 24.5'),
        ('odds_shots_local_over_7.5',   'odds_shots_local_under_7.5',   'shots_local_over_7.5',   'shots_local_under_7.5',   'Tiros local O/U 7.5'),
        ('odds_shots_local_over_9.5',   'odds_shots_local_under_9.5',   'shots_local_over_9.5',   'shots_local_under_9.5',   'Tiros local O/U 9.5'),
        ('odds_shots_local_over_11.5',  'odds_shots_local_under_11.5',  'shots_local_over_11.5',  'shots_local_under_11.5',  'Tiros local O/U 11.5'),
        ('odds_shots_local_over_13.5',  'odds_shots_local_under_13.5',  'shots_local_over_13.5',  'shots_local_under_13.5',  'Tiros local O/U 13.5'),
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

    sep = '=' * 68
    print(f"\n{sep}")
    print(f"  PREDICCION V3: {team_local} vs {team_visitante}")
    if competition:
        print(f"  Competicion: {competition}")
    print(sep)

    n_l = params['n_local_home']
    n_v = params['n_vis_away']
    warn_l = ' (!)' if n_l < MIN_MATCHES else ''
    warn_v = ' (!)' if n_v < MIN_MATCHES else ''

    print(f"\n[PARAMETROS V3]  local={n_l} partidos{warn_l}  visita={n_v} partidos{warn_v}")
    print(f"   lambda goles local    : {params['lambda_local']:.3f}")
    print(f"   lambda goles visita   : {params['lambda_vis']:.3f}")
    print(f"   mu corners local      : {params['mu_corners_local']:.2f}")
    print(f"   mu corners visita     : {params['mu_corners_vis']:.2f}")
    print(f"   mu tiros local        : {params['mu_shots_local']:.1f} +/- {params['sigma_shots_local']:.1f}")
    print(f"   mu tiros visita       : {params['mu_shots_vis']:.1f} +/- {params['sigma_shots_vis']:.1f}")
    print(f"   Posesion local        : {params['poss_local']:.1f}%")

    if '_ratings' in params:
        r = params['_ratings']
        print(f"\n[RATINGS V3 (shrinkage aplicado)]")
        print(f"   atk local   : {r['atk_local']:.3f}   def visita  : {r['def_visita']:.3f}  "
              f"  forma atk local  : {r['forma_atk_local']:.3f}")
        print(f"   atk visita  : {r['atk_visita']:.3f}   def local   : {r['def_local']:.3f}  "
              f"  forma atk visita : {r['forma_atk_visita']:.3f}")

    E_gt = probs['E_gl'] + probs['E_gv']
    E_ct = probs['E_cl'] + probs['E_cv']
    E_st = probs['E_sl'] + probs['E_sv']

    print(f"\n[VALORES ESPERADOS]")
    print(f"   Goles totales    : {E_gt:.2f}  (L:{probs['E_gl']:.2f}  V:{probs['E_gv']:.2f})")
    print(f"   Corners totales  : {E_ct:.2f}  (L:{probs['E_cl']:.2f}  V:{probs['E_cv']:.2f})")
    print(f"   Tiros totales    : {E_st:.2f}  (L:{probs['E_sl']:.2f}  V:{probs['E_sv']:.2f})")

    def j(p):
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
        ('BTTS Si',         'btts_si'),
        ('BTTS No',         'btts_no'),
        ('Over 1.5 goles',  'over_1.5'),
        ('Over 2.5 goles',  'over_2.5'),
        ('Over 3.5 goles',  'over_3.5'),
        ('Over 4.5 goles',  'over_4.5'),
    ])

    # Corners totales desactivados para value bets (ROI -31.4%)
    # Se muestran solo como referencia, no se buscan value bets
    show_market_block("[CORNERS TOTALES] (solo referencia, no se apuesta)", [
        ('Over  8.5', 'corners_over_8.5'),
        ('Over  9.5', 'corners_over_9.5'),
        ('Over 10.5', 'corners_over_10.5'),
        ('Over 11.5', 'corners_over_11.5'),
        ('Over 12.5', 'corners_over_12.5'),
    ])

    show_market_block("[TIROS TOTALES]", [
        ('Over 16.5', 'shots_over_16.5'),
        ('Over 18.5', 'shots_over_18.5'),
        ('Over 20.5', 'shots_over_20.5'),
        ('Over 22.5', 'shots_over_22.5'),
        ('Over 24.5', 'shots_over_24.5'),
    ])

    print(f"\n[MARCADORES MAS PROBABLES]")
    for score, prob in sorted(probs['score_dist'].items(), key=lambda x: -x[1])[:10]:
        bar = '#' * int(prob * 200)
        print(f"   {score:>5}: {prob:5.1%}  {bar}")

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
        print(f"\n   (Sin cuotas del bookmaker — no se detectan value bets)")


# ─────────────────────────────────────────────────────────────────────────────
# CORNERS POR PACE  [preparado para V4 — no integrado al pipeline actual]
# ─────────────────────────────────────────────────────────────────────────────
#
# Enfoque alternativo al modelo actual de corners:
#
#   Paso 1 — avg_liga_corners (total por partido, ponderado por recencia)
#   Paso 2 — mu_total = avg_liga_corners × pace_partido
#   Paso 3 — share_local via ratings de ataque/defensa actuales
#             s_local  = r_atk_cl × r_def_cv × f_c_l
#             s_visita = r_atk_cv × r_def_cl × f_c_v
#             share_local = s_local / (s_local + s_visita)
#   Paso 4 — mu_local  = mu_total × share_local
#             mu_visita = mu_total × (1 - share_local)
#
# Ventaja vs modelo actual: el total del partido queda anclado al ritmo real
# del partido (pace), en lugar de sumar dos medias independientes que pueden
# combinarse de forma inconsistente.
# ─────────────────────────────────────────────────────────────────────────────

K_CORNERS_CRED   = 4.0   # prior strength: partidos virtuales al promedio de liga
                         # Reducido de 8 a 4: con K=8 el prior dominaba (76% peso)
                         # y el mu_total quedaba sistemáticamente subestimado.
SHOTS_MU_WEIGHT  = 0.0   # v3.2: DESACTIVADO — backtest mostró que cualquier
                         # peso > 0 empeora MAE y ROI. La correlación r=0.44
                         # es intra-match (tiros↔corners del mismo partido),
                         # pero el promedio histórico de tiros no predice corners
                         # futuros mejor que el promedio de corners directo.
                         # Se dejan las funciones por si sirven en otra feature.


def league_corners_avg_weighted(rows: list[dict],
                                 liga_id_filter: int | None = None) -> float:
    """
    Promedio de corners TOTALES por partido en la liga, ponderado por recencia.

    A diferencia de league_avgs() (que usa media simple como prior estable),
    esta función aplica exponential decay weighting para que los partidos
    recientes pesen más — útil cuando el estilo de la liga cambia con el tiempo.

    Devuelve: avg de (corners_local + corners_visitante) por partido, ponderado.
    """
    filtered = [r for r in rows
                if not liga_id_filter or int(r['liga_id']) == liga_id_filter]
    if len(filtered) < 3:
        filtered = rows   # fallback al histórico global

    today_ord = _today_ord()
    lam       = math.log(2) / HALF_LIFE_DAYS

    total_w  = 0.0
    total_wc = 0.0

    for row in filtered:
        corners_total = int(row['corners_local']) + int(row['corners_visitante'])
        date_ord      = _parse_date(row.get('fecha', ''))
        days_ago      = max(0, today_ord - date_ord)
        w             = math.exp(-lam * days_ago)
        total_w  += w
        total_wc += w * corners_total

    return total_wc / total_w if total_w > 0 else 9.5   # fallback razonable


def _corners_avg_venue(team_id: int,
                       venue: str,
                       rows: list[dict],
                       liga_id_filter: int | None = None) -> tuple[float, float]:
    """
    Promedio ponderado por recencia de corners TOTALES (local+visitante) por partido,
    filtrando por equipo y tipo de cancha (home/away).

    Retorna: (w_sum, weighted_avg)
        w_sum        — suma de pesos decay (≈ tamaño efectivo de muestra)
        weighted_avg — promedio ponderado de corners totales del partido
    """
    lam     = math.log(2) / HALF_LIFE_DAYS
    today_o = _today_ord()
    total_w = total_wct = 0.0

    for row in rows:
        if liga_id_filter:
            try:
                if int(row['liga_id']) != liga_id_filter:
                    continue
            except (ValueError, KeyError):
                continue
        try:
            rid = int(row['equipo_local_id' if venue == 'home' else 'equipo_visitante_id'])
            if rid != team_id:
                continue
        except (ValueError, KeyError):
            continue
        try:
            ct = int(row['corners_local']) + int(row['corners_visitante'])
        except (ValueError, TypeError, KeyError):
            continue
        date_ord = _parse_date(row.get('fecha', ''))
        days_ago = max(0, today_o - date_ord)
        w        = math.exp(-lam * days_ago)
        total_w   += w
        total_wct += w * ct

    if total_w == 0.0:
        return 0.0, 0.0
    return total_w, total_wct / total_w


def compute_mu_total_credibility(local_id: int,
                                  vis_id: int,
                                  rows: list[dict],
                                  liga_id: int | None,
                                  liga_avg: float,
                                  K: float = K_CORNERS_CRED) -> tuple[float, dict]:
    """
    Estima mu_total de corners con credibility weighting dinámico.

    Formula:
        mu_total = (w_L × raw_L + w_V × raw_V + 2K × liga_avg) / (w_L + w_V + 2K)

    Los pesos α = w_L/D, β = w_V/D, γ = 2K/D  (con D = w_L + w_V + 2K) suman 1.

    - raw_L: promedio decay de corners totales en partidos HOME del equipo local
    - raw_V: promedio decay de corners totales en partidos AWAY del equipo visitante
    - K:     prior strength — cuántos partidos virtuales al promedio de liga aporta
             cada componente. Con K=8 y w_L=8 → peso 50% local, 50% liga.

    Cuando un equipo tiene pocos datos recientes → w pequeño → más peso al prior.
    Cuando tiene muchos datos recientes → w grande → más peso a su promedio propio.

    Retorna: (mu_total, diagnostico_dict)
    """
    w_L, raw_L = _corners_avg_venue(local_id, 'home', rows, liga_id)
    w_V, raw_V = _corners_avg_venue(vis_id,   'away', rows, liga_id)

    # Fallback si un equipo no tiene datos: reemplaza por liga_avg
    raw_L = raw_L if raw_L > 0 else liga_avg
    raw_V = raw_V if raw_V > 0 else liga_avg

    denom    = w_L + w_V + 2.0 * K
    mu_total = (w_L * raw_L + w_V * raw_V + 2.0 * K * liga_avg) / denom

    return mu_total, {
        'alpha':    w_L / denom,    # peso del equipo local
        'beta':     w_V / denom,    # peso del equipo visitante
        'gamma':    2.0 * K / denom,  # peso del prior de liga
        'raw_L':    raw_L,
        'raw_V':    raw_V,
        'w_L':      w_L,
        'w_V':      w_V,
        'liga_avg': liga_avg,
    }


def league_corners_per_shot_weighted(rows: list[dict],
                                      liga_id_filter: int | None = None) -> float:
    """
    Ratio ponderado por recencia corners/tiro a nivel de partido en la liga.
    Sirve como prior para estimar corners dado el volumen de tiros esperado.

    Excluye partidos donde tiros_total == 0 (datos faltantes).
    Fallback: 0.42 ≈ 9.5 corners / 22.6 tiros (promedios globales históricos).
    """
    filtered = [r for r in rows
                if not liga_id_filter or int(r.get('liga_id', 0)) == liga_id_filter]
    if len(filtered) < 3:
        filtered = rows

    today_ord = _today_ord()
    lam       = math.log(2) / HALF_LIFE_DAYS
    total_w   = total_wr = 0.0

    for row in filtered:
        try:
            shots   = int(row['tiros_local']) + int(row['tiros_visitante'])
            corners = int(row['corners_local']) + int(row['corners_visitante'])
        except (ValueError, TypeError, KeyError):
            continue
        if shots == 0:
            continue
        date_ord = _parse_date(row.get('fecha', ''))
        days_ago = max(0, today_ord - date_ord)
        w        = math.exp(-lam * days_ago)
        total_w  += w
        total_wr += w * (corners / shots)

    return total_wr / total_w if total_w > 0 else 0.42


def _total_shots_avg_venue(team_id: int,
                            venue: str,
                            rows: list[dict],
                            liga_id_filter: int | None = None) -> tuple[float, float]:
    """
    Promedio ponderado por recencia de tiros TOTALES (local+visitante) por partido,
    filtrando por equipo y tipo de cancha (home/away).

    Misma estructura que _corners_avg_venue pero para tiros.
    Retorna: (w_sum, weighted_avg_shots_total)
    """
    lam     = math.log(2) / HALF_LIFE_DAYS
    today_o = _today_ord()
    total_w = total_ws = 0.0

    for row in rows:
        if liga_id_filter:
            try:
                if int(row['liga_id']) != liga_id_filter:
                    continue
            except (ValueError, KeyError):
                continue
        try:
            rid = int(row['equipo_local_id' if venue == 'home' else 'equipo_visitante_id'])
            if rid != team_id:
                continue
        except (ValueError, KeyError):
            continue
        try:
            st = int(row['tiros_local']) + int(row['tiros_visitante'])
        except (ValueError, TypeError, KeyError):
            continue
        if st == 0:
            continue
        date_ord = _parse_date(row.get('fecha', ''))
        days_ago = max(0, today_o - date_ord)
        w        = math.exp(-lam * days_ago)
        total_w  += w
        total_ws += w * st

    if total_w == 0.0:
        return 0.0, 0.0
    return total_w, total_ws / total_w


def _league_shots_avg_weighted(rows: list[dict],
                                liga_id_filter: int | None = None) -> float:
    """Promedio ponderado por recencia de tiros totales por partido en la liga."""
    filtered = [r for r in rows
                if not liga_id_filter or int(r.get('liga_id', 0)) == liga_id_filter]
    if len(filtered) < 3:
        filtered = rows

    today_ord = _today_ord()
    lam       = math.log(2) / HALF_LIFE_DAYS
    total_w   = total_ws = 0.0

    for row in filtered:
        try:
            st = int(row['tiros_local']) + int(row['tiros_visitante'])
        except (ValueError, TypeError, KeyError):
            continue
        if st == 0:
            continue
        date_ord = _parse_date(row.get('fecha', ''))
        days_ago = max(0, today_ord - date_ord)
        w        = math.exp(-lam * days_ago)
        total_w  += w
        total_ws += w * st

    return total_ws / total_w if total_w > 0 else 22.0   # fallback global


def compute_mu_total_via_shots(local_id: int,
                                vis_id: int,
                                rows: list[dict],
                                liga_id: int | None,
                                liga_cps: float,
                                K: float = K_CORNERS_CRED) -> float:
    """
    Estima mu_total de corners vía tiros esperados × ratio corners/tiro de liga.

    Metodología:
        shots_blended = credibility_blend(shots_avg_local_home, shots_avg_vis_away)
        mu_shots_based = shots_blended × liga_cps

    Usa el mismo credibility weighting que compute_mu_total_credibility pero
    sobre tiros totales. El resultado es independiente de los corners históricos
    → complementa la estimación de credibilidad directa.

    Args:
        liga_cps: ratio corners/tiro de la liga (de league_corners_per_shot_weighted)
    """
    liga_shots_avg = _league_shots_avg_weighted(rows, liga_id)

    w_L, shots_L = _total_shots_avg_venue(local_id, 'home', rows, liga_id)
    w_V, shots_V = _total_shots_avg_venue(vis_id,   'away', rows, liga_id)

    # fallback cross-liga si equipo sin datos suficientes en esta liga
    if shots_L == 0:
        _, shots_L = _total_shots_avg_venue(local_id, 'home', rows, None)
    if shots_V == 0:
        _, shots_V = _total_shots_avg_venue(vis_id,   'away', rows, None)

    shots_L = shots_L if shots_L > 0 else liga_shots_avg
    shots_V = shots_V if shots_V > 0 else liga_shots_avg

    denom         = w_L + w_V + 2.0 * K
    shots_blended = (w_L * shots_L + w_V * shots_V + 2.0 * K * liga_shots_avg) / denom

    return shots_blended * liga_cps


def compute_corners_pace(team_local: str | int,
                         team_visita: str | int,
                         rows: list[dict],
                         competition: str | int | None = None) -> dict:
    """
    Estima mu_corners_local y mu_corners_visita usando el enfoque pace:

        mu_total  = avg_liga_corners_weighted × pace_partido
        share_loc = s_local / (s_local + s_visita)
        mu_local  = mu_total × share_local
        mu_visita = mu_total × (1 - share_local)

    donde s_local y s_visita se calculan con los mismos ratings de
    ataque/defensa/forma del modelo actual (V3), garantizando consistencia.

    Retorna dict con:
        'mu_local'      : float  — media corners equipo local
        'mu_visita'     : float  — media corners equipo visitante
        'mu_total'      : float  — total esperado del partido
        'avg_liga'      : float  — referencia de liga (ponderada por recencia)
        'pace_partido'  : float  — pace normalizado del partido
        'pace_detail'   : dict   — resultado completo de compute_match_pace()
        'share_local'   : float  — proporción corners que corresponden al local
        's_local'       : float  — score de corners del local (pre-normalización)
        's_visita'      : float  — score de corners del visitante

    PENDIENTE: una vez completado el backfill de xG/tiros/blocked_shots (datos
    desde 2024), comparar esta estimación de mu_total contra
    compute_mu_total_credibility() para ver cuál calibra mejor TOTALES.
    """
    _, name_to_id_teams   = load_teams_db()
    _, name_to_id_leagues = load_leagues_db()

    local_id = resolve_team_id(team_local,  name_to_id_teams)
    vis_id   = resolve_team_id(team_visita, name_to_id_teams)
    liga_id  = resolve_liga_id(competition, name_to_id_leagues) if competition else None

    if local_id is None:
        raise ValueError(f"Equipo local no encontrado en DB: '{team_local}'")
    if vis_id is None:
        raise ValueError(f"Equipo visitante no encontrado en DB: '{team_visita}'")

    # ── Paso 1 — avg de liga ponderado por recencia ───────────────────────────
    avg_liga = league_corners_avg_weighted(rows, liga_id)

    # ── Paso 2 — mu_total: blend credibility-corners + shots-based (v3.2) ─────
    # Credibility sobre corners históricos (v3.1):
    #   mu = (w_L*raw_L + w_V*raw_V + 2K*liga_avg) / (w_L + w_V + 2K)
    # Shots-based (v3.2 nuevo):
    #   mu = shots_blended × liga_corners_per_shot
    # Blend final:
    #   mu_total = (1-SHOTS_MU_WEIGHT)*mu_cred + SHOTS_MU_WEIGHT*mu_shots_based
    # Pearson r(tiros_totales, corners_totales) ≈ 0.44 justifica el 35% de peso.
    mu_cred, _cred_detail = compute_mu_total_credibility(
        local_id, vis_id, rows, liga_id, avg_liga)
    liga_cps       = league_corners_per_shot_weighted(rows, liga_id)
    mu_shots_based = compute_mu_total_via_shots(local_id, vis_id, rows, liga_id, liga_cps)
    mu_total       = (1.0 - SHOTS_MU_WEIGHT) * mu_cred + SHOTS_MU_WEIGHT * mu_shots_based

    # ── Paso 3 — ratings de ataque/defensa para el reparto ────────────────────
    recs_comp = build_records(rows, liga_id) if liga_id else {}
    recs_all  = build_records(rows)
    la        = league_avgs(rows, liga_id)
    today_ord = _today_ord()

    def rating(team, ctx, field, league_avg):
        avg, n = _get_team_stat(recs_comp, recs_all, team, ctx, field, today_ord)
        return _rating_shrunk(avg, league_avg, n)

    def forma(team, ctx, field):
        return _form_multiplier(recs_comp, recs_all, team, ctx, field)

    r_atk_cl = rating(local_id, 'home', 'corners',          la['home_corners'])
    r_def_cv = rating(vis_id,   'away', 'corners_conceded',  la['home_corners'])
    r_atk_cv = rating(vis_id,   'away', 'corners',           la['away_corners'])
    r_def_cl = rating(local_id, 'home', 'corners_conceded',  la['away_corners'])

    f_c_l = forma(local_id, 'home', 'corners')
    f_c_v = forma(vis_id,   'away', 'corners')

    s_local  = r_atk_cl * r_def_cv * f_c_l
    s_visita = r_atk_cv * r_def_cl * f_c_v

    # ── Paso 4 — reparto anclado al home advantage real de la liga ────────────
    # Multiplica los scores por la['home_corners'] / la['away_corners'] antes de
    # normalizar. Cuando ambos equipos son promedio (ratings=1, forma=1):
    #   share_local = home_corners / (home_corners + away_corners) ≈ 0.558
    # Los ratings siguen diferenciando entre equipos pero parten de la base
    # correcta en vez de 50/50.
    mu_share_local  = la['home_corners'] * s_local
    mu_share_visita = la['away_corners'] * s_visita
    total_mu_share  = mu_share_local + mu_share_visita
    if total_mu_share <= 0:
        share_local = la['home_corners'] / (la['home_corners'] + la['away_corners'])
    else:
        share_local = mu_share_local / total_mu_share

    mu_local  = max(0.5, mu_total * share_local)
    mu_visita = max(0.5, mu_total * (1.0 - share_local))

    return {
        'mu_local':     mu_local,
        'mu_visita':    mu_visita,
        'mu_total':     mu_total,
        'avg_liga':     avg_liga,
        'share_local':  share_local,
        's_local':      s_local,
        's_visita':     s_visita,
    }


def estimate_corners_k(rows: list[dict],
                       liga_id_filter: int | None = None,
                       min_matches: int = 20) -> float:
    """
    Estima el parámetro de dispersión k de la Binomial Negativa para
    corners totales por partido, usando método de momentos.

        k = mu² / (var - mu)

    Si var ≤ mu (datos underdispersed), la NegBin no aplica y se retorna
    un k muy grande (~inf) para que negbinom_sample degrade a Poisson.

    Parámetros:
        rows            : histórico completo
        liga_id_filter  : filtra por liga; si hay < min_matches usa todo
        min_matches     : mínimo de partidos para estimar con la liga sola

    Retorna k (float). Valores típicos: 30–75 para corners de fútbol.
    """
    filtered = [r for r in rows
                if not liga_id_filter or int(r['liga_id']) == liga_id_filter]
    if len(filtered) < min_matches:
        filtered = rows   # fallback al histórico global

    totals = [int(r['corners_local']) + int(r['corners_visitante'])
              for r in filtered]
    n = len(totals)
    if n < 2:
        return 30.0   # prior razonable si no hay datos

    mu  = sum(totals) / n
    var = sum((x - mu) ** 2 for x in totals) / (n - 1)

    if var <= mu:
        # Underdispersed o igual a Poisson → k grande para degradar a Poisson
        return 999.0

    k = mu ** 2 / (var - mu)
    return max(1.0, min(k, 999.0))   # clamp: [1, 999]


def simulate_corners_nb(mu_total: float,
                        share_local: float,
                        k: float,
                        n: int = 100_000) -> dict:
    """
    Simulación Monte Carlo de corners usando el modelo NegBin + Binomial.

    Por cada iteración:
        T        ~ NegBin(mu_total, k)          ← total del partido
        C_local  ~ Binomial(T, share_local)     ← corners del local dado T
        C_visita = T - C_local                  ← determinístico

    Ventajas vs Poisson independiente:
        1. El total T siempre es consistente (local + visita = T).
        2. NegBin captura la overdispersion real de los corners (partidos muy
           distintos entre sí, más de lo que Poisson predice).
        3. La correlación entre C_local y C_visita es negativa dado T, lo
           que refleja que si el local tiene muchos corners, el visitante
           tiende a tener menos (partidos dominados).

    Retorna dict con:
        'cl'      : list[int]  — corners locales por iteración
        'cv'      : list[int]  — corners visitante por iteración
        'ct'      : list[int]  — totales por iteración
        'n'       : int        — número de simulaciones
        'E_cl'    : float      — media simulada local
        'E_cv'    : float      — media simulada visitante
        'E_ct'    : float      — media simulada total
        'var_ct'  : float      — varianza simulada total
    """
    cl_list: list[int] = []
    cv_list: list[int] = []
    ct_list: list[int] = []

    for _ in range(n):
        T  = negbinom_sample(mu_total, k)
        cl = binomial_sample(T, share_local)
        cv = T - cl
        cl_list.append(cl)
        cv_list.append(cv)
        ct_list.append(T)

    e_cl = sum(cl_list) / n
    e_cv = sum(cv_list) / n
    e_ct = sum(ct_list) / n
    var_ct = sum((x - e_ct) ** 2 for x in ct_list) / (n - 1)

    return {
        'cl': cl_list,
        'cv': cv_list,
        'ct': ct_list,
        'n':  n,
        'E_cl':   e_cl,
        'E_cv':   e_cv,
        'E_ct':   e_ct,
        'var_ct': var_ct,
    }


def corners_probs_nb(team_local: str | int,
                     team_visita: str | int,
                     rows: list[dict],
                     competition: str | int | None = None,
                     n_sim: int = 100_000) -> dict:
    """
    Pipeline completo: estima parámetros → simula → calcula probabilidades.

    Flujo:
        1. compute_corners_pace()  → mu_total, share_local
        2. estimate_corners_k()    → k (dispersión NegBin)
        3. simulate_corners_nb()   → distribuciones de C_local, C_visita, T
        4. Calcula P(over/under X.5) para los umbrales habituales

    Retorna dict con:
        'params'  : dict de compute_corners_pace + k estimado
        'sim'     : dict de simulate_corners_nb
        'probs'   : dict con todas las probabilidades calculadas
    """
    _, name_to_id_leagues = load_leagues_db()
    liga_id = resolve_liga_id(competition, name_to_id_leagues) if competition else None

    # ── Parámetros ────────────────────────────────────────────────────────────
    cp = compute_corners_pace(team_local, team_visita, rows, competition)
    k  = estimate_corners_k(rows, liga_id)

    # ── Simulación ────────────────────────────────────────────────────────────
    sim = simulate_corners_nb(cp['mu_total'], cp['share_local'], k, n_sim)

    cl = sim['cl']
    cv = sim['cv']
    ct = sim['ct']
    n  = sim['n']

    def over(data, thr):  return sum(x > thr for x in data) / n
    def under(data, thr): return 1.0 - over(data, thr)

    # ── Probabilidades ────────────────────────────────────────────────────────
    probs: dict[str, float] = {}

    # Corners totales O/U
    for thr in [3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5]:
        probs[f'tc_over_{thr}']  = over(ct, thr)
        probs[f'tc_under_{thr}'] = under(ct, thr)

    # Corners local O/U
    for thr in [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5]:
        probs[f'cl_over_{thr}']  = over(cl, thr)
        probs[f'cl_under_{thr}'] = under(cl, thr)

    # Corners visita O/U
    for thr in [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5]:
        probs[f'cv_over_{thr}']  = over(cv, thr)
        probs[f'cv_under_{thr}'] = under(cv, thr)

    # Valores esperados
    probs['E_cl'] = sim['E_cl']
    probs['E_cv'] = sim['E_cv']
    probs['E_ct'] = sim['E_ct']

    return {
        'params': {**cp, 'k': k},
        'sim':    sim,
        'probs':  probs,
    }


# ─────────────────────────────────────────────────────────────────────────────
# PACE RATING  [preparado para V4 — no integrado al pipeline actual]
# ─────────────────────────────────────────────────────────────────────────────
#
# Mide qué tan "abiertos" suelen ser los partidos de un equipo.
# Usa actividad TOTAL del partido (for + against) porque el ritmo es bilateral:
# un equipo de alto ritmo también arrastra al rival a partidos abiertos.
#
# Fórmula por partido:
#   pace_match = 0.35*(shots_for + shots_against)
#              + 0.25*(xg_for   + xg_against)      ← solo si xG disponible
#              + 0.20*(corners_for + corners_against)
#              + 0.10*(blocked_for + blocked_against)
#              + 0.10*(insidebox_for + insidebox_against)
#
# Normalización: pace_norm = pace_equipo / pace_liga_avg
#   < 0.85  → partidos cerrados
#   ~ 1.00  → promedio de liga
#   > 1.15  → ritmo alto
#
# Aplica:
#   ✔ Ponderación temporal (decaimiento exponencial, HALF_LIFE_DAYS)
#   ✔ Shrinkage bayesiano (K_SHRINK partidos virtuales al promedio de liga)
#   ✔ Factor forma (últimos N_FORM partidos, FORM_WEIGHT)
#   ✔ Separación home/away: un equipo puede tener ritmo distinto en cada contexto
# ─────────────────────────────────────────────────────────────────────────────

# Pesos de cada componente en la fórmula de pace
PACE_WEIGHTS = {
    'shots':      0.35,
    'xg':         0.25,
    'corners':    0.20,
    'blocked':    0.10,
    'insidebox':  0.10,
}


def _safe_int(v, default: int = 0) -> int:
    """Convierte valor de CSV a int, con fallback si es vacío o '-'."""
    if v in (None, '', '-'):
        return default
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return default


def _safe_float(v, default: float = 0.0) -> float:
    """Convierte valor de CSV a float, con fallback si es vacío o '-'."""
    if v in (None, '', '-'):
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def _pace_from_row(row: dict, is_home: bool) -> tuple[float, bool]:
    """
    Calcula el valor de pace para un equipo en un partido dado.
    Devuelve (pace_value, xg_disponible).

    El equipo actúa como local (is_home=True) o visitante (is_home=False).
    Como pace usa FOR + AGAINST, el valor es el mismo para ambos equipos
    del mismo partido, pero lo separamos por contexto para shrinkage/forma.
    """
    if is_home:
        shots_f = _safe_int(row.get('tiros_local'))
        shots_a = _safe_int(row.get('tiros_visitante'))
        xg_f    = _safe_float(row.get('xg_local'))
        xg_a    = _safe_float(row.get('xg_visitante'))
        corners_f = _safe_int(row.get('corners_local'))
        corners_a = _safe_int(row.get('corners_visitante'))
        blocked_f = _safe_int(row.get('tiros_bloqueados_local'))
        blocked_a = _safe_int(row.get('tiros_bloqueados_visitante'))
        inside_f  = _safe_int(row.get('tiros_dentro_local'))
        inside_a  = _safe_int(row.get('tiros_dentro_visitante'))
    else:
        shots_f = _safe_int(row.get('tiros_visitante'))
        shots_a = _safe_int(row.get('tiros_local'))
        xg_f    = _safe_float(row.get('xg_visitante'))
        xg_a    = _safe_float(row.get('xg_local'))
        corners_f = _safe_int(row.get('corners_visitante'))
        corners_a = _safe_int(row.get('corners_local'))
        blocked_f = _safe_int(row.get('tiros_bloqueados_visitante'))
        blocked_a = _safe_int(row.get('tiros_bloqueados_local'))
        inside_f  = _safe_int(row.get('tiros_dentro_visitante'))
        inside_a  = _safe_int(row.get('tiros_dentro_local'))

    xg_disponible = row.get('xg_local', '') not in ('', '-')

    shots_total   = shots_f   + shots_a
    corners_total = corners_f + corners_a
    blocked_total = blocked_f + blocked_a
    inside_total  = inside_f  + inside_a

    if xg_disponible:
        xg_total = xg_f + xg_a
        pace = (PACE_WEIGHTS['shots']     * shots_total
              + PACE_WEIGHTS['xg']        * xg_total
              + PACE_WEIGHTS['corners']   * corners_total
              + PACE_WEIGHTS['blocked']   * blocked_total
              + PACE_WEIGHTS['insidebox'] * inside_total)
    else:
        # Sin xG: redistribuir su peso proporcionalmente entre los demás
        w_sin_xg = 1.0 - PACE_WEIGHTS['xg']
        pace = ((PACE_WEIGHTS['shots']     / w_sin_xg) * shots_total
              + (PACE_WEIGHTS['corners']   / w_sin_xg) * corners_total
              + (PACE_WEIGHTS['blocked']   / w_sin_xg) * blocked_total
              + (PACE_WEIGHTS['insidebox'] / w_sin_xg) * inside_total)

    return pace, xg_disponible


def _league_pace_avg(rows: list[dict], liga_id_filter: int | None = None) -> float:
    """
    Promedio de pace a nivel de liga (prior estable, no ponderado por recencia).
    Se calcula desde la perspectiva del equipo local (simétrico al away).
    """
    filtered = [r for r in rows
                if not liga_id_filter or int(r['liga_id']) == liga_id_filter]
    if len(filtered) < 3:
        filtered = rows

    paces = []
    for row in filtered:
        pace, _ = _pace_from_row(row, is_home=True)
        if pace > 0:
            paces.append(pace)

    return sum(paces) / len(paces) if paces else 1.0


def compute_pace_rating(team: str | int,
                        rows: list[dict],
                        competition: str | int | None = None,
                        ctx: str = 'home') -> dict:
    """
    Calcula el pace rating normalizado de un equipo en un contexto (home/away).

    Retorna:
        {
          'pace_raw':     float,  # pace medio ponderado del equipo (sin normalizar)
          'pace_norm':    float,  # pace / avg_liga  (1.0 = promedio)
          'pace_form':    float,  # pace de los últimos N_FORM partidos (sin normalizar)
          'pace_final':   float,  # rating con shrinkage + forma aplicados
          'league_avg':   float,  # referencia de liga
          'n':            int,    # partidos usados
          'n_con_xg':     int,    # de ellos, cuántos tienen xG disponible
          'ctx':          str,    # contexto consultado
        }

    Interpretación de pace_final:
        < 0.85  partidos cerrados
        ~ 1.00  ritmo promedio de liga
        > 1.15  ritmo alto
    """
    _, name_to_id_teams   = load_teams_db()
    _, name_to_id_leagues = load_leagues_db()

    team_id = resolve_team_id(team, name_to_id_teams)
    liga_id = resolve_liga_id(competition, name_to_id_leagues) if competition else None

    if team_id is None:
        raise ValueError(f"Equipo no encontrado en DB: '{team}'")

    # ── Filtrar partidos de la competición y del historial global ─────────────
    rows_comp = [r for r in rows
                 if liga_id and int(r['liga_id']) == liga_id]
    rows_all  = rows

    is_home = (ctx == 'home')
    id_col  = 'equipo_local_id' if is_home else 'equipo_visitante_id'

    def get_team_rows(source):
        return [r for r in source if int(r[id_col]) == team_id]

    team_rows = get_team_rows(rows_comp) or get_team_rows(rows_all)

    if not team_rows:
        league_avg = _league_pace_avg(rows_all, liga_id)
        return {
            'pace_raw': league_avg, 'pace_norm': 1.0,
            'pace_form': league_avg, 'pace_final': 1.0,
            'league_avg': league_avg, 'n': 0, 'n_con_xg': 0, 'ctx': ctx,
        }

    today_ord = _today_ord()
    lam       = math.log(2) / HALF_LIFE_DAYS

    # ── Pace ponderado por recencia ───────────────────────────────────────────
    total_w   = 0.0
    total_wp  = 0.0
    n_con_xg  = 0

    pace_dated = []   # [(pace, date_ord)] para forma

    for row in team_rows:
        pace, xg_ok = _pace_from_row(row, is_home)
        if xg_ok:
            n_con_xg += 1
        date_ord = _parse_date(row.get('fecha', ''))
        days_ago = max(0, today_ord - date_ord)
        w        = math.exp(-lam * days_ago)
        total_w  += w
        total_wp += w * pace
        pace_dated.append((pace, date_ord))

    pace_raw = total_wp / total_w if total_w > 0 else 0.0
    n        = len(team_rows)

    # ── Liga avg (prior para shrinkage) ──────────────────────────────────────
    # Usamos liga de la comp si hay, si no el histórico global
    league_avg = _league_pace_avg(
        rows_comp if rows_comp else rows_all,
        liga_id if rows_comp else None
    )

    # ── Shrinkage bayesiano ───────────────────────────────────────────────────
    # raw_rating = pace_raw / league_avg; shrinkage lleva hacia 1.0
    raw_rating  = pace_raw / league_avg if league_avg > 0 else 1.0
    pace_shrunk = (n * raw_rating + K_SHRINK * 1.0) / (n + K_SHRINK)

    # ── Factor forma (últimos N_FORM partidos) ────────────────────────────────
    recent_paces = [p for p, _ in sorted(pace_dated, key=lambda x: x[1])[-N_FORM:]]
    if len(recent_paces) >= N_FORM:
        form_raw  = sum(recent_paces) / len(recent_paces)
        form_mult = form_raw / league_avg if league_avg > 0 else 1.0
        # Mezcla: FORM_WEIGHT hacia el multiplicador real, resto neutral (1.0)
        form_adj  = 1.0 + FORM_WEIGHT * (form_mult - 1.0)
    else:
        form_adj = 1.0

    pace_final = pace_shrunk * form_adj
    pace_form  = (sum(recent_paces) / len(recent_paces)) if recent_paces else pace_raw

    return {
        'pace_raw':   pace_raw,
        'pace_norm':  raw_rating,           # pace_raw / league_avg (sin shrinkage)
        'pace_form':  pace_form,
        'pace_final': pace_final,           # con shrinkage + forma — usar este
        'league_avg': league_avg,
        'n':          n,
        'n_con_xg':   n_con_xg,
        'ctx':        ctx,
    }


def compute_match_pace(team_local: str | int,
                       team_visita: str | int,
                       rows: list[dict],
                       competition: str | int | None = None) -> dict:
    """
    Calcula el pace combinado del partido como producto de los pace finales
    de cada equipo en su contexto respectivo (home/away).

    pace_partido = sqrt(pace_local_final * pace_visita_final)
    (media geométrica — más robusta ante valores extremos)

    Retorna dict con:
        'pace_local'   : dict de compute_pace_rating para el local
        'pace_visita'  : dict de compute_pace_rating para el visitante
        'pace_partido' : float, media geométrica normalizada
        'interpretacion': str descriptiva
    """
    local  = compute_pace_rating(team_local,  rows, competition, ctx='home')
    visita = compute_pace_rating(team_visita, rows, competition, ctx='away')

    pace_partido = math.sqrt(local['pace_final'] * visita['pace_final'])

    if pace_partido < 0.88:
        interp = 'partido cerrado (ritmo bajo)'
    elif pace_partido < 0.95:
        interp = 'partido algo contenido'
    elif pace_partido < 1.06:
        interp = 'ritmo promedio de liga'
    elif pace_partido < 1.14:
        interp = 'partido abierto'
    else:
        interp = 'partido muy abierto (ritmo alto)'

    return {
        'pace_local':      local,
        'pace_visita':     visita,
        'pace_partido':    pace_partido,
        'interpretacion':  interp,
    }

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
    Función principal del modelo V3.

    Parámetros
    ----------
    team_local      : Nombre del equipo local (debe coincidir con la DB)
    team_visitante  : Nombre del equipo visitante
    competition     : Filtro de competición ('La Liga', 'Liga Profesional', ...)
    odds            : Dict de cuotas del bookmaker para detectar value bets
    n_sim           : Número de simulaciones Monte Carlo (default 100 000)
    verbose         : Imprime el reporte completo

    Retorna
    -------
    (probs, params)
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
# Demo
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print("=" * 68)
    print(" MODELO V3 — DEMO")
    print(f" Mejoras: shrinkage K={K_SHRINK} | MIN_MATCHES={MIN_MATCHES} |"
          f" recencia half-life={HALF_LIFE_DAYS}d | forma N={N_FORM} w={FORM_WEIGHT}")
    print("=" * 68)

    predict(
        team_local     = 'Independiente',
        team_visitante = 'Racing Club',
        competition    = 'Liga Profesional',
    )
