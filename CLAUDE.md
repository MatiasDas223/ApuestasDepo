# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project does

Sistema de value bets para fútbol: descarga histórico de partidos via API Football, entrena un modelo estadístico (distribución de Poisson + Monte Carlo), descarga odds de Bet365 (API Football + odds-api.io), y detecta apuestas con ventaja matemática (edge > 4%).

## Common commands

```bash
# Pipeline completo (actualiza W/L + busca partidos en las próximas 24h + analiza)
python scripts/pipeline.py

# Solo actualizar resultados W/L en value_bets.csv
python scripts/pipeline.py --solo-wl

# Ampliar ventana de búsqueda a 48h
python scripts/pipeline.py --horas 48

# Forzar re-descarga de odds (ignorar caché)
python scripts/pipeline.py --force

# Capturar closing line (cuota de cierre) y calcular CLV
# Pensado para correr via Windows Task Scheduler cada 5 min con ventana 20 min
python scripts/snapshot_cierre.py --window 20
# El pipeline.py corre automaticamente snapshot_cierre con window=60 como fallback al final

# Preparar un partido específico (descarga histórico + odds → escribe config en analizar_partido.py)
python scripts/preparar_partido.py "Boca Juniors" "Independiente"
python scripts/preparar_partido.py "Real Madrid" "Barcelona" --fixture 1492015

# Analizar el partido actualmente configurado en analizar_partido.py
python scripts/analizar_partido.py

# Descargar histórico masivo (reanudable)
python scripts/fetch_historia.py
python scripts/fetch_historia.py --max 50
python scripts/fetch_historia.py --status

# Reporte de rendimiento del modelo
python scripts/analisis_rendimiento.py

# Backtest walk-forward del modelo V3
python scripts/backtest_v3.py
python scripts/backtest_v3.py --dry-run

# Análisis de corners v3.1 (Totales/Local/Visita separados)
python scripts/analisis_corners_v31.py
python scripts/analisis_corners_v31.py --n-sim 50000

# Comparar corners v3 vs v3.1
python scripts/comparar_corners_v3_v31.py
python scripts/comparar_corners_v3_v31.py --n-sim 100000

# Análisis de tiros NegBin v3.2
python scripts/correlacion_tiros.py
python scripts/calibracion_full_negbin.py
python scripts/roi_tiros_negbin.py

# Gestión de aliases para odds-api.io
python scripts/manage_aliases.py --status
python scripts/manage_aliases.py --auto-ligas 128
python scripts/manage_aliases.py --confirmar-auto 128
python scripts/manage_aliases.py --set-equipo TEAM_ID "Nombre en oddsapi"
python scripts/manage_aliases.py --set-liga LIGA_ID "slug-en-oddsapi"
```

## Architecture

### Data flow

```
API Football (v3.football.api-sports.io)
  └─ preparar_partido.py / fetch_historia.py / pipeline.py
       └─ data/historico/partidos_historicos.csv   ← fuente de verdad del histórico

modelo_v3.py   ← lee el CSV, computa ratings y simulación Monte Carlo
analizar_partido.py   ← usa modelo_v3 + odds → detecta value bets → guarda en data/apuestas/

odds-api.io (corners, tiros, arco, tarjetas)
API Football  (goles, BTTS, 1X2, hándicaps)
  └─ fetch_odds.py   ← caché en data/odds/*.json y data/odds/oddsapi/*.json
```

### Key scripts

| Script | Rol |
|---|---|
| `pipeline.py` | Orquestador completo: pasos 0–5 (historia → odds → simulación → value bets) |
| `preparar_partido.py` | Modo interactivo para un partido específico; escribe el bloque de config en `analizar_partido.py` |
| `analizar_partido.py` | Lee el bloque `# ── BEGIN PARTIDO CONFIG ─` y ejecuta el análisis completo; también exportable como módulo |
| `modelo_v3.py` | Motor estadístico: ratings Bayesianos con shrinkage + decaimiento exponencial por recencia + factor forma; goles=Poisson, corners=NegBin+Binomial, tiros=NegBin(k por equipo) |
| `fetch_odds.py` | Descarga y normaliza odds de dos fuentes; caché en disco por `fixture_id` |
| `fetch_historia.py` | Descarga masiva reanudable; progreso en `data/historico/fetch_historia_progress.json` |
| `manage_aliases.py` | Mapea nombres de equipos/ligas entre API Football y odds-api.io |
| `analisis_rendimiento.py` | ROI, calibración, comparativa por versión de modelo |
| `analisis_corners_v31.py` | Análisis de rendimiento exclusivo para corners con probs v3.1 re-computadas. Separa Totales/Local/Visita |
| `comparar_corners_v3_v31.py` | Compara corners v3 (Poisson) vs v3.1 (NegBin+Binomial): ROI, calibración, mu precision |
| `backtest_v3.py` | Walk-forward: re-calcula apuestas históricas dejando fuera el fixture evaluado |
| `correlacion_tiros.py` | Análisis exploratorio de predictores de tiros (intra-match, predictivo, pace, variabilidad) |
| `calibracion_full_negbin.py` | Calibración walk-forward NegBin sobre todos los partidos del histórico (54k+ predicciones) |
| `roi_tiros_negbin.py` | ROI de tiros NegBin por cuota, EV, probabilidad, edge — separado por Total/Local/Visita |

### Data files

| Archivo | Contenido |
|---|---|
| `data/historico/partidos_historicos.csv` | 29 columnas: IDs, goles, tiros, tiros al arco, corners, posesión, tarjetas + 12 columnas extendidas (xg, tiros_dentro/fuera/bloqueados, atajadas, goles_prevenidos) |
| `data/db/equipos.csv` | `id, nombre, pais, liga_id_principal` — IDs son los de API Football |
| `data/db/ligas.csv` | `id, nombre, pais` |
| `data/db/team_aliases.csv` | Mapeo `team_id` → nombre en odds-api.io |
| `data/db/league_aliases.csv` | Mapeo `liga_id` → slug en odds-api.io |
| `data/apuestas/value_bets.csv` | Apuestas detectadas con edge, EV, cuota, resultado W/L + `odds_close` / `clv_pct` / `fecha_cierre` (CLV capturado por snapshot_cierre.py) |
| `data/apuestas/pronosticos.csv` | Todas las probabilidades calculadas (para calibración) |
| `data/odds/*.json` | Caché de odds de API Football por `fixture_id` |
| `data/odds/oddsapi/*.json` | Caché de odds de odds-api.io por `event_id` |

### Modelo estadístico (modelo_v3.py)

**Goles**: Poisson doble con ratings multiplicativos:
- `mu_local = liga_avg_home × atk_local × def_vis`
- Shrinkage Bayesiano: `rating = (n × raw + K_SHRINK) / (n + K_SHRINK)` con `K_SHRINK=8`
- Ponderación por recencia: `w = exp(-ln(2) × días / HALF_LIFE_DAYS)` con `HALF_LIFE_DAYS=90`
- Factor forma: últimos `N_FORM=5` partidos, mezclado con peso `FORM_WEIGHT=0.20`

**Corners (v3.1)**: NegBin + Binomial:
- `mu_total` via credibility weighting sobre corners totales históricos por equipo+cancha (`K_CORNERS_CRED=4`)
- `k` (dispersión NegBin) estimado por método de momentos por liga
- `share_local` via ratings atk/def de corners + home advantage real de la liga
- Reparto: `Binomial(total_NegBin, share_local)`
- Solo se buscan value bets en corners individuales (local/visita), no en totales

**Tiros (v3.2)**: NegBin independiente por equipo, k por equipo con shrinkage:
- `mu_local = liga_avg_home × atk_local × def_vis × forma` (misma estructura que goles)
- `k_local = (n_team * k_team_raw + K_SHOTS_SHRINK * k_league) / (n_team + K_SHOTS_SHRINK)` con `K_SHOTS_SHRINK=6`
- k estimado por método de momentos sobre tiros del equipo en su venue (home/away)
- Valores típicos: k=5 (Villarreal, variable) a k=700+ (Barcelona, estable)
- Antes era Normal(mu, sigma) — NegBin modela mejor la sobredispersión real (var/media=2.16)
- Calibración walk-forward sobre 54,468 predicciones: delta max 1.2pp (excelente)
- Tiros local Over en cuotas 1.50-2.10 es el sub-mercado más prometedor

**General**: `MIN_EDGE=0.04` (4%) para declarar una apuesta como value bet

### Config de partido en analizar_partido.py

`preparar_partido.py` sobreescribe automáticamente el bloque delimitado por:
```python
# ── BEGIN PARTIDO CONFIG ─
TEAM_LOCAL  = '...'
TEAM_VISITA = '...'
COMPETITION = '...'
FIXTURE_ID  = ...
ODDS = {...}
# ── END PARTIDO CONFIG ─
```

### APIs externas

- **API Football**: `https://v3.football.api-sports.io` — key en `preparar_partido.py` y `fetch_odds.py` (`API_KEY`)
- **odds-api.io**: `https://api.odds-api.io/v3` — key en `fetch_odds.py` (`ODDSAPI_KEY`)
- Bookmaker por defecto: Bet365 (`BK_DEFAULT = 8`)
- Rate limiting manual: `time.sleep(0.3)` entre llamadas

### Estado del CSV histórico (al 2026-04-16)

- **1576 partidos** actualmente en `partidos_historicos.csv` (29 columnas completas, stats extendidas integradas)
- **~5251 partidos pendientes de descarga** — cupo API se agota diariamente, se reinicia a las 00 UTC
- **PENDIENTE**: correr `fetch_historia.py` diariamente hasta completar ~6800 partidos. El cupo de API Football es ~7500 calls/día. El script para automáticamente al recibir 429.
- El progreso se guarda en `data/historico/fetch_historia_progress.json` (reanudable). Dedup: el script chequea contra el CSV (`existing_fids`) y contra `prog['fetched']` antes de descargar.
- `fetch_historia.py` descarga las 29 columnas en una sola consulta (antes requería backfill_stats.py por separado)
- Orden de descarga: temporadas actuales primero, luego hacia atrás
- Bug corregido: 6 scripts que reescribían el CSV preservan columnas existentes (extrasaction='ignore') para no truncar datos

### Estado del modelo por mercado

- **Goles (1X2, O/U, BTTS)**: modelo más acertado, en producción
- **Corners individuales (local/visita)**: EN PRODUCCION — NegBin total + Binomial reparto (v3.1). ROI +74% local, +54% visita sobre 38 bets confirmadas
- **Corners totales**: DESACTIVADO — backtest demostró ROI -31.4% sobre 38 bets. Comentados en BINARY_MARKETS de modelo_v3.py. Se siguen calculando probabilidades como referencia pero no se buscan value bets. Se eliminaron 313 entradas históricas de value_bets.csv
- **Tiros**: NegBin con k por equipo (v3.2). Calibración excelente (delta <2pp). ROI histórico: local -4.7%, total -31.8%, visita -65.0%. Recolectando datos para validar mejora.
- **Tiros al arco (SOT)**: `Binomial(tiros_simulados, precision)` acoplado al NegBin de tiros. Precisión por equipo (`SOT/tiros`) con shrinkage bayesiano hacia media de liga (`K_PREC_SHRINK`). `compute_arco_params()` en `analizar_partido.py`, invocada desde `modelo_v3.py` dentro del Monte Carlo. Reemplaza Poisson simple anterior (ROI -30.8%). Pendiente validar ROI con datos nuevos.

### Modelo de corners v3.1 — decisiones y hallazgos

**Distribución**: NegBin(mu_total, k) para el total del partido + Binomial(total, share_local) para el reparto.
- k estimado por método de momentos por liga (valores típicos 13–102)
- share_local via ratings atk/def de corners + home advantage real de la liga
- mu_total via credibility weighting: `(w_L*raw_L + w_V*raw_V + 2K*liga_avg) / (w_L + w_V + 2K)` con `K_CORNERS_CRED=4`

**Experiment: tiros como predictor de mu_corners (DESCARTADO)**
- Pearson r(tiros_totales, corners_totales) = 0.44 (intra-match), r(corners_local, tiros_local) = 0.57
- Se implementó blend `mu = (1-λ)*mu_cred + λ*mu_shots_based` con funciones `league_corners_per_shot_weighted`, `_total_shots_avg_venue`, `compute_mu_total_via_shots`
- Backtest: λ=0.35 empeoró MAE (1.52→1.60) y ROI (+17.9%→+9.5%); λ=0.15 también empeoró
- Conclusión: la correlación r=0.44 es intra-match (mismo partido). El promedio histórico de tiros no predice corners futuros mejor que el promedio de corners directo. `SHOTS_MU_WEIGHT=0.0` (desactivado, funciones conservadas)

**Análisis detallado corners v3.1 (script: `analisis_corners_v31.py`)**:
- Corners individuales: sweet spot en cuotas 1.50–2.50, edge >10%, thresholds O/U 3.5–6.5
- Liga Profesional es el mercado más rentable (+47% ROI corners, +121% local, +86% visita)
- Calibración: el modelo subestima probabilidades en rango 50-80% (delta +9-12pp) — conservador, las bets que detecta son reales
- Corners totales: pierde en todos los rangos de edge, cuota y threshold. Over totales 0W/12L en cuotas >3.00

### Modelo de tiros v3.2 — decisiones y hallazgos

**Distribución**: NegBin(mu, k) independiente por equipo (local y visita simulados por separado).
- Antes: Normal(mu, sigma) truncada a >=0. Problema: sobredispersión real var/media=2.16 no capturada.
- Ahora: NegBin con k estimado POR EQUIPO con shrinkage bayesiano hacia k de liga (`K_SHOTS_SHRINK=6`).
- k por equipo captura que Barcelona (k=700+) es muy estable en tiros mientras Villarreal (k=5) es variable.

**Análisis exploratorio (`correlacion_tiros.py`)**:
- Mejor predictor individual: `atk_home * def_away` (r=0.468 con tiros locales)
- Posesión correlaciona con tiros individuales (r=0.315) — NO integrado al modelo aún
- Pace (métrica compuesta): r=-0.03 después de controlar por hist_shots — NO agrega info
- CV intra-equipo: tiros=0.37 (más predecible), SOT=0.51 (ruidoso), goles=0.88
- Liga Profesional es la más difícil de predecir (r=0.13 vs r=0.40 en Brasileirao)

**Calibración walk-forward (`calibracion_full_negbin.py`, 54,468 predicciones)**:
- Global: delta max 1.2pp en todos los rangos de probabilidad (excelente)
- Local: delta max 1.4pp — casi perfecta
- Visita: sesgo leve en colas (+3.9pp en <10%, -3.9pp en >90%)
- Totales: sesgo leve 2-4pp, subestima overs, sobreestima unders

**ROI sobre 101 value bets reales (`roi_tiros_negbin.py`)**:
- Tiros local: ROI -4.7% (35 bets). Over local ROI +13.3%. Edge 4-8% ROI +83.3%. Prob 70-80% ROI +35.9%.
- Tiros total: ROI -31.8% (29 bets). Pierde en casi todos los rangos.
- Tiros visita: ROI -65.0% (37 bets). Sobreestima 30-50pp en todas las franjas. Irrecuperable.
- Cuotas >3.50: 0W en 10 bets — evitar completamente.

**Conclusión**: el modelo está bien calibrado pero el mercado de tiros es eficiente. Tiros local Over en cuotas 1.50-2.10 es el único sub-mercado con potencial. Se necesitan más datos para confirmar.

### Ligas configuradas en el pipeline

Las ligas activas están en el dict `LIGAS` de `pipeline.py`. Las ligas con `solo_equipos_en_db=True` solo se analizan si al menos uno de los equipos ya está en `data/db/equipos.csv`. La temporada se determina automáticamente: ligas europeas usan año de inicio (cambio en julio), el resto usan año calendario.
