"""ROI y CLV por mercado / sub-mercado / competición sobre los 7 CSVs de value bets."""
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

ROOT = Path(__file__).resolve().parent.parent
APU = ROOT / "data" / "apuestas"

CSVS = {
    "raw":       APU / "value_bets.csv",
    "cal":       APU / "value_bets_calibrado.csv",
    "fil":       APU / "value_bets_filtrados.csv",
    "v33ref":    APU / "value_bets_v33ref.csv",
    "v34shrink": APU / "value_bets_v34shrink.csv",
    "v35dedup":  APU / "value_bets_v35dedup.csv",
    "v36":       APU / "value_bets_v36.csv",
}

def parse_resultado(r):
    if pd.isna(r):
        return None
    s = str(r).strip().upper()
    if s in ("W", "WIN", "GANO", "GANADA"):
        return 1.0
    if s in ("L", "LOSS", "PERDIDA"):
        return 0.0
    if s in ("PUSH", "VOID", "REINTEGRO"):
        return 0.5  # devuelve stake
    try:
        f = float(s)
        if 0.0 <= f <= 1.0:
            return f
    except Exception:
        pass
    return None

def roi_summary(df):
    df = df.copy()
    df["res"] = df["resultado"].apply(parse_resultado)
    closed = df[df["res"].notna()].copy()
    n = len(closed)
    if n == 0:
        return dict(n=0, wins=0, roi=np.nan, profit=0, hit=np.nan, avg_odds=np.nan,
                    clv=np.nan, n_clv=0)
    closed["profit"] = np.where(
        closed["res"] == 1.0, closed["odds"] - 1.0,
        np.where(closed["res"] == 0.5, 0.0, -1.0)
    )
    profit = closed["profit"].sum()
    roi = profit / n * 100
    wins = (closed["res"] == 1.0).sum()
    hit = wins / n * 100
    avg_odds = closed["odds"].mean()
    # CLV: usar clv_pct (real) y rellenar con clv_pct_est (estimado) cuando no hay closing
    clv_real = pd.to_numeric(closed.get("clv_pct"), errors="coerce")
    clv_est = pd.to_numeric(closed.get("clv_pct_est"), errors="coerce")
    clv = clv_real.combine_first(clv_est)
    return dict(
        n=n, wins=int(wins), roi=roi, profit=profit, hit=hit, avg_odds=avg_odds,
        clv=clv.mean() * 100 if clv.notna().any() else np.nan,
        n_clv=int(clv.notna().sum()),
    )

def fmt(d):
    if d["n"] == 0:
        return f"  N=0"
    parts = [
        f"N={d['n']:>4}",
        f"W={d['wins']:>3}",
        f"hit={d['hit']:5.1f}%",
        f"avgO={d['avg_odds']:.2f}",
        f"ROI={d['roi']:+6.1f}%",
        f"profit={d['profit']:+7.2f}u",
    ]
    if not np.isnan(d["clv"]):
        parts.append(f"CLV={d['clv']:+5.2f}% (n={d['n_clv']})")
    else:
        parts.append("CLV=n/a")
    return "  " + "  ".join(parts)

def section(title, ch="="):
    print()
    print(ch * 90)
    print(f"  {title}")
    print(ch * 90)

def breakdown(df, by, label, min_n=15, top=None):
    print(f"\n>> {label}  (min N={min_n})")
    print(f"  {'group':<40} {'N':>4} {'W':>3} {'hit':>6} {'avgO':>5} {'ROI':>8} {'profit':>9} {'CLV':>10}")
    print(f"  {'-'*40} {'-'*4} {'-'*3} {'-'*6} {'-'*5} {'-'*8} {'-'*9} {'-'*10}")
    rows = []
    for key, g in df.groupby(by):
        s = roi_summary(g)
        if s["n"] >= min_n:
            rows.append((key, s))
    rows.sort(key=lambda r: r[1]["roi"] if not np.isnan(r[1]["roi"]) else -999, reverse=True)
    if top:
        rows = rows[:top]
    for key, s in rows:
        clv_str = f"{s['clv']:+.2f}%" if not np.isnan(s["clv"]) else "n/a"
        key_str = str(key)[:40]
        print(f"  {key_str:<40} {s['n']:>4} {s['wins']:>3} {s['hit']:>5.1f}% {s['avg_odds']:>5.2f} {s['roi']:>+7.1f}% {s['profit']:>+8.2f}u {clv_str:>10}")

# ─────────────────────────────────────────────────────────────────────────────
# Cargar todo
print(f"\nCargando {len(CSVS)} CSVs...")
dfs = {}
for name, path in CSVS.items():
    if path.exists():
        df = pd.read_csv(path)
        df["modelo"] = name
        dfs[name] = df
        print(f"  {name:<10} {len(df):>5} filas  ({path.name})")

# Análisis por modelo
for name, df in dfs.items():
    section(f"MODELO: {name}  ({len(df)} filas)")
    s = roi_summary(df)
    print(f"\n  GLOBAL:{fmt(s)}")
    breakdown(df, "categoria", "POR CATEGORIA (mercado)", min_n=20)
    breakdown(df, ["categoria", "alcance"], "POR CATEGORIA + ALCANCE (sub-mercado)", min_n=15)
    breakdown(df, "competicion", "POR COMPETICION", min_n=20, top=15)

# Comparativa cruzada
section("COMPARATIVA: GLOBAL POR MODELO", ch="#")
print(f"\n  {'modelo':<12} {'N':>5} {'W':>4} {'hit':>6} {'avgO':>5} {'ROI':>8} {'profit':>9} {'CLV':>10}")
print(f"  {'-'*12} {'-'*5} {'-'*4} {'-'*6} {'-'*5} {'-'*8} {'-'*9} {'-'*10}")
for name, df in dfs.items():
    s = roi_summary(df)
    if s["n"] == 0:
        continue
    clv_str = f"{s['clv']:+.2f}%" if not np.isnan(s["clv"]) else "n/a"
    print(f"  {name:<12} {s['n']:>5} {s['wins']:>4} {s['hit']:>5.1f}% {s['avg_odds']:>5.2f} {s['roi']:>+7.1f}% {s['profit']:>+8.2f}u {clv_str:>10}")

# Top sub-mercados rentables (raw, modelo de referencia con más bets)
section("TOP SUB-MERCADOS (cat+alcance) en raw / cal / v36", ch="#")
for name in ("raw", "cal", "v36"):
    if name not in dfs:
        continue
    print(f"\n--- {name} ---")
    breakdown(dfs[name], ["categoria", "alcance"], f"sub-mercados {name}", min_n=15, top=20)

# Análisis cruzado: categoria x competicion (en raw que tiene más datos)
if "raw" in dfs:
    section("RAW: TOP COMBOS (categoria + competicion)", ch="#")
    breakdown(dfs["raw"], ["categoria", "competicion"], "cat x comp", min_n=20, top=25)
