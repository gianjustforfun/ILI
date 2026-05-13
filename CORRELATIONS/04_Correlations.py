"""
=============================================================
SCRIPT 4 — PICCHI E CROSS-CORRELAZIONE AMBIENTE vs ILI ATS
=============================================================

COSA FA QUESTO SCRIPT:
    Legge i file CSV delle settimane di interesse (già filtrate
    dalla pipeline precedente) per ciascuna variabile ambientale
    ARPA e i CSV ILI di ATS Bergamo e ATS Montagna (prodotti dallo Script 1)
    che vengono sommati per ottenere il totale ILI del territorio Bergamo + Montagna.

    L'obiettivo è condurre DUE analisi complementari:

    ── ANALISI 1: PICCHI ──────────────────────────────────────
        Identifica i picchi più rilevanti in ogni stagione
        per ciascuna variabile ambientale e per l'ILI,
        quindi li abbina temporalmente per rango per stimare
        un lag e una correlazione tra valori dei picchi.

        In particolare analizza:
            - TEMPERATURE  -> picchi MINIMI  (freddo intenso)
            - HUMIDITY     -> picchi MASSIMI (umidità elevata)
            - PM10         -> picchi MASSIMI (inquinamento elevato)
            - PM25         -> picchi MASSIMI (inquinamento elevato)
            - NO2          -> picchi MASSIMI (inquinamento elevato)
            - ILI ATS      -> picchi MASSIMI (accessi PS per ILI)

        Limitazione nota: l'abbinamento per rango temporale
        ha un bias intrinseco quando i picchi ILI si concentrano
        all'inizio della stagione (dicembre) mentre i picchi
        ambientali si distribuiscono lungo tutta la finestra.
        In quel caso il metodo produce lag negativi per costruzione,
        non per biologia. Usare i risultati con cautela e in
        abbinamento all'analisi 2.

    ── ANALISI 2: CROSS-CORRELAZIONE ─────────────────────────
        Calcola la correlazione tra la serie ambientale e la
        serie ILI sfalsate di un lag variabile da 0 a
        MAX_LAG_SETTIMANE settimane.

        Per ogni (variabile, stagione, lag) calcola DUE misure
        di correlazione in parallelo:

            ▸ r di Pearson
            ▸ ρ di Spearman

        COME INTERPRETARE I DUE COEFFICIENTI:
            r ≈ ρ   → relazione lineare e monotona, risultati robusti
            ρ > r   → relazione monotona ma non lineare; preferire ρ
            r > ρ   → Pearson spinto da outlier; segnale di allarme
            entrambi bassi → nessuna relazione stabile a quel lag

        NOTA per TEMPERATURA:
            La serie viene invertita (−T) così che un coefficiente
            positivo significhi "più freddo → più ILI".

STRUTTURA FILE DI INPUT:
    ARPA/SETTIMANE_DI_INTERESSE/
        HUMIDITY/TEMPERATURE/PM10/PM25/NO2/

    SORVEGLIANZA ACCESSI PS/
        ATS_BERGAMO/ili_ats_bergamo_stagionale.csv   ← Script 1
        ATS_MONTAGNA/ili_ats_montagna_stagionale.csv ← Script 1

    Struttura CSV ILI (long format):
        STAGIONE | WEEK | ORDINE | ACCESSI_ILI_ATS_<area>
        22-23    |   48 |      1 |                    352

    CONVENZIONE STAGIONI:
        Script 1 "21-22"  ↔  ARPA "*_2021_2022.csv"
        Script 1 "22-23"  ↔  ARPA "*_2022_2023.csv"
        Script 1 "23-24"  ↔  ARPA "*_2023_2024.csv"
        Script 1 "24-25"  ↔  ARPA "*_2024_2025.csv"
        Script 1 "25-26"  ↔  ARPA "*_2025_2026.csv"

OUTPUT:
    CORRELATIONS/output/
        picchi_ambientali_settimane.csv
        picchi_ili_ats_bergamo_montagna.csv
        abbinamenti_picchi_vs_ili.csv
        riepilogo_correlazioni_stagionale.csv
        cross_correlazione_per_lag.csv
        cross_correlazione_lag_ottimale.csv
    CORRELATIONS/output/grafici/
        <variabile>_vs_ili_<stagione>.png
        xcorr_<variabile>.png

REQUISITI:
    pip install pandas numpy matplotlib scipy
=============================================================
"""

from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.signal import find_peaks
from scipy.stats import pearsonr, spearmanr

warnings.filterwarnings("ignore")
plt.style.use("seaborn-v0_8-whitegrid")

# =============================================================================
# CONFIGURAZIONE PERCORSI
# =============================================================================

BASE_DIR     = Path(__file__).resolve().parent   # .../CORRELATIONS/
PROJECT_ROOT = BASE_DIR.parent                   # .../ILI/

SETTIMANE_DIR = PROJECT_ROOT / "ARPA" / "SETTIMANE_DI_INTERESSE"
ILI_DIR       = PROJECT_ROOT / "SORVEGLIANZA ACCESSI PS"/"output"
OUTPUT_DIR    = BASE_DIR / "output"
PLOTS_DIR     = OUTPUT_DIR / "grafici"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# CONFIGURAZIONE VARIABILI E STAGIONI
# =============================================================================

VARIABILI: dict[str, tuple[str, str]] = {
    "TEMPERATURE": ("TEMPERATURE", "min"),
    "HUMIDITY":    ("HUMIDITY",    "max"),
    "PM10":        ("PM10",        "max"),
    "PM25":        ("PM25",        "max"),
    "NO2":         ("NO2",         "max"),
}

TIPOLOGIE_CON_SOTTOSTAT: set[str] = {"TEMPERATURE", "HUMIDITY"}

# File ILI — long format prodotti dallo Script 1
ILI_FILE_BERGAMO  = ILI_DIR / "ATS_BERGAMO"  / "ili_ats_bergamo_stagionale.csv"
ILI_FILE_MONTAGNA = ILI_DIR / "ATS_MONTAGNA" / "ili_ats_montagna_stagionale.csv"

# Mapping stagione ILI (convenzione "YY-YY" di Script 1) → stagione ARPA ("YYYY_YYYY")
STAGIONE_ILI_TO_ARPA: dict[str, str] = {
    "21-22": "2021_2022",
    "22-23": "2022_2023",
    "23-24": "2023_2024",
    "24-25": "2024_2025",
    "25-26": "2025_2026",
}

# ── Parametri analisi 1 ───────────────────────────────────────────────────────
TOP_K_PEAKS         = 5
MIN_DISTANCE_WEEKS  = 2
MIN_PROMINENCE_FRAC = 0.15

# ── Parametri analisi 2 ───────────────────────────────────────────────────────
MAX_LAG_SETTIMANE      = 8
SOGLIA_R_SIGNIFICATIVO = 0.44

# =============================================================================
# FUNZIONI DI SUPPORTO — ORDINAMENTO STAGIONALE
# =============================================================================

def ordine_settimana(s: int) -> int:
    """Sett. 48→1, 52→5, 1→6, 15→20 (ordine cronologico stagionale)."""
    return s - 47 if s >= 48 else s + 5


def settimane_stagione() -> list[int]:
    """[48, 49, 50, 51, 52, 1, 2, ..., 15]"""
    return [48, 49, 50, 51, 52] + list(range(1, 16))

# =============================================================================
# CARICAMENTO ILI ATS BERGAMO E MONTAGNA
# =============================================================================

def _carica_ili_long(path: Path, col_valore: str, label: str) -> pd.DataFrame:
    """
    Legge un CSV long prodotto dallo Script 1 e lo normalizza nel formato:
        WEEK | Stagione_ILI | Stagione_ARPA | Valore | Ordine

    Struttura attesa input:
        STAGIONE | WEEK | ORDINE | <col_valore>
        22-23    |   48 |      1 |         352
    """
    if not path.exists():
        print(f"   ❌ File ILI {label} non trovato: {path}")
        return pd.DataFrame()

    df = pd.read_csv(path)
    df.columns = [str(c).strip() for c in df.columns]

    required = {"STAGIONE", "WEEK", col_valore}
    missing  = required - set(df.columns)
    if missing:
        print(f"   ❌ Colonne mancanti in {path.name}: {missing}")
        return pd.DataFrame()

    df["WEEK"]     = pd.to_numeric(df["WEEK"],     errors="coerce")
    df[col_valore] = pd.to_numeric(df[col_valore], errors="coerce")
    df = df.dropna(subset=["WEEK", col_valore])
    df["WEEK"] = df["WEEK"].astype(int)

    df = df[df["WEEK"].isin(set(settimane_stagione()))].copy()

    df = df.rename(columns={"STAGIONE": "Stagione_ILI", col_valore: "Valore"})
    df["Ordine"]       = df["WEEK"].apply(ordine_settimana)
    df["Stagione_ARPA"] = df["Stagione_ILI"].map(STAGIONE_ILI_TO_ARPA)

    sconosciute = df[df["Stagione_ARPA"].isna()]["Stagione_ILI"].unique()
    if len(sconosciute) > 0:
        print(f"   ⚠️  Stagioni {label} senza mapping ARPA: {sconosciute}")
        print(f"       → Aggiorna STAGIONE_ILI_TO_ARPA in cima allo script.")
        df = df.dropna(subset=["Stagione_ARPA"])

    df = df[["WEEK", "Stagione_ILI", "Stagione_ARPA", "Valore", "Ordine"]]
    df = df.sort_values(["Stagione_ILI", "Ordine"]).reset_index(drop=True)

    print(f"   ✅ ILI {label} — stagioni: {sorted(df['Stagione_ILI'].unique())}")
    return df


def carica_ili_ats_bergamo_montagna() -> pd.DataFrame:
    """
    Carica i CSV ILI di Bergamo e Montagna (output Script 1) e
    li SOMMA per settimana/stagione → totale ILI Bergamo + Montagna.
    """
    df_bg = _carica_ili_long(ILI_FILE_BERGAMO,  "ACCESSI_ILI_ATS_BERGAMO",  "Bergamo")
    df_mt = _carica_ili_long(ILI_FILE_MONTAGNA, "ACCESSI_ILI_ATS_MONTAGNA", "Montagna")

    dfs = [d for d in [df_bg, df_mt] if not d.empty]
    if not dfs:
        print("   ❌ Nessun dato ILI disponibile.")
        return pd.DataFrame()

    df_all = pd.concat(dfs, ignore_index=True)
    df_agg = (
        df_all
        .groupby(["WEEK", "Stagione_ILI", "Stagione_ARPA", "Ordine"], as_index=False)["Valore"]
        .sum()
    )
    df_agg = df_agg.sort_values(["Stagione_ILI", "Ordine"]).reset_index(drop=True)

    n_bg = len(df_bg) if not df_bg.empty else 0
    n_mt = len(df_mt) if not df_mt.empty else 0
    print(f"   ✅ ILI Bergamo + Montagna: {n_bg} + {n_mt} → {len(df_agg)} righe aggregate")
    return df_agg

# =============================================================================
# CARICAMENTO VARIABILI AMBIENTALI ARPA
# =============================================================================

def carica_variabile(nome_variabile: str, sottocartella: str) -> pd.DataFrame:
    cartella = SETTIMANE_DIR / sottocartella
    if not cartella.exists():
        print(f"   ⚠️  Cartella non trovata: {cartella}")
        return pd.DataFrame()

    files = sorted(cartella.glob("*.csv"))
    if not files:
        print(f"   ⚠️  Nessun CSV trovato in: {cartella}")
        return pd.DataFrame()

    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            if df.empty or "Settimana" not in df.columns:
                print(f"   ⚠️  File ignorato (vuoto o senza 'Settimana'): {f.name}")
                continue
            parts = f.stem.split("_")
            stagione_arpa = "_".join(parts[-2:]) if len(parts) >= 3 else f.stem
            df["Stagione_ARPA"] = stagione_arpa
            dfs.append(df)
        except Exception as e:
            print(f"   ⚠️  Errore leggendo {f.name}: {e}")

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def prepara_serie(df: pd.DataFrame, nome_variabile: str) -> pd.DataFrame:
    """
    Calcola la serie rappresentativa settimanale.
    TEMPERATURE/HUMIDITY → media delle colonne '_media'
    PM10/PM25/NO2        → media di tutte le colonne numeriche (una per stazione)
    """
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["Settimana"] = pd.to_numeric(df["Settimana"], errors="coerce")
    df = df.dropna(subset=["Settimana"])
    df["Settimana"] = df["Settimana"].astype(int)

    if nome_variabile.upper() in TIPOLOGIE_CON_SOTTOSTAT:
        cols = [c for c in df.columns
                if c not in ("Settimana", "Stagione_ARPA") and c.endswith("_media")]
        if not cols:
            print(f"   ⚠️  Nessuna colonna '_media' per {nome_variabile}. "
                  f"Colonne: {list(df.columns)}")
            return pd.DataFrame()
    else:
        cols = [c for c in df.columns if c not in ("Settimana", "Stagione_ARPA")]

    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["Valore"] = df[cols].mean(axis=1, skipna=True)

    serie = (
        df.groupby(["Stagione_ARPA", "Settimana"], as_index=False)["Valore"].mean()
    )
    serie["Ordine"] = serie["Settimana"].apply(ordine_settimana)
    return serie.sort_values(["Stagione_ARPA", "Ordine"]).reset_index(drop=True)

# =============================================================================
# ANALISI 1 — RILEVAMENTO PICCHI
# =============================================================================

def trova_picchi_candidati(valori: np.ndarray, mode: str) -> tuple[np.ndarray, np.ndarray]:
    x = valori if mode == "max" else -valori
    prominenza_min = max(np.nanstd(x) * MIN_PROMINENCE_FRAC, 1e-6)
    idx, props = find_peaks(x, prominence=prominenza_min, distance=max(1, MIN_DISTANCE_WEEKS))
    return idx, props.get("prominences", np.ones(len(idx)))


def seleziona_top_k_picchi(df_stagione: pd.DataFrame, mode: str,
                           top_k: int = TOP_K_PEAKS) -> pd.DataFrame:
    """
    Seleziona i TOP_K picchi più rilevanti in una stagione.
    Il DataFrame in input deve avere le colonne: Settimana, Valore, Ordine.
    """
    df = df_stagione.dropna(subset=["Valore"]).sort_values("Ordine").reset_index(drop=True)
    if df.empty:
        return pd.DataFrame()

    x = df["Valore"].values
    idx_peaks, prominenze = trova_picchi_candidati(x, mode)

    if len(idx_peaks) >= top_k:
        candidati = pd.DataFrame({
            "Settimana":  df["Settimana"].iloc[idx_peaks].values,
            "Valore":     df["Valore"].iloc[idx_peaks].values,
            "Ordine":     df["Ordine"].iloc[idx_peaks].values,
            "Prominenza": prominenze,
        })
        asc = (mode == "min")
        top = candidati.sort_values(["Valore", "Prominenza"], ascending=[asc, False]).head(top_k)
    else:
        top_idx = (
            df["Valore"].nsmallest(top_k).index if mode == "min"
            else df["Valore"].nlargest(top_k).index
        )
        top = df.loc[top_idx, ["Settimana", "Valore", "Ordine"]].copy()
        top["Prominenza"] = np.nan

    top = top.sort_values("Ordine").reset_index(drop=True)
    top["Rango"] = np.arange(1, len(top) + 1)
    return top[["Settimana", "Valore", "Ordine", "Rango"]]


def correla_picchi(env_peaks: pd.DataFrame, ili_peaks: pd.DataFrame,
                   variabile: str, stagione_ili: str) -> tuple[dict | None, pd.DataFrame]:
    """
    Abbina picchi ambientali e ILI per rango temporale.
    Lag = Ordine(ILI) − Ordine(ambiente):
        > 0 → ILI arriva DOPO il picco ambientale (biologicamente atteso)
        < 0 → possibile artefatto da bias di ranking
    """
    n = min(len(env_peaks), len(ili_peaks), TOP_K_PEAKS)
    if n == 0:
        return None, pd.DataFrame()

    e = env_peaks.sort_values("Ordine").head(n).reset_index(drop=True)
    i = ili_peaks.sort_values("Ordine").head(n).reset_index(drop=True)

    coppie = pd.DataFrame({
        "Stagione_ILI":  stagione_ili,
        "Variabile":     variabile,
        "Rango":         np.arange(1, n + 1),
        "Env_Settimana": e["Settimana"].values,
        "Env_Valore":    e["Valore"].values,
        "ILI_Settimana": i["Settimana"].values,
        "ILI_Valore":    i["Valore"].values,
        "Lag_Settimane": i["Ordine"].values - e["Ordine"].values,
    })

    corr = coppie["Env_Valore"].corr(coppie["ILI_Valore"]) if n >= 2 else np.nan

    summary = {
        "Stagione_ILI":           stagione_ili,
        "Variabile":              variabile,
        "N_picchi_usati":         n,
        "Lag_medio_settimane":    float(coppie["Lag_Settimane"].mean()),
        "Lag_mediano_settimane":  float(coppie["Lag_Settimane"].median()),
        "Lag_min_settimane":      int(coppie["Lag_Settimane"].min()),
        "Lag_max_settimane":      int(coppie["Lag_Settimane"].max()),
        "Correlazione_Pearson":   float(corr) if pd.notna(corr) else np.nan,
        "Env_valore_medio_picco": float(coppie["Env_Valore"].mean()),
        "ILI_valore_medio_picco": float(coppie["ILI_Valore"].mean()),
    }
    return summary, coppie


def crea_grafico_picchi(env_serie: pd.DataFrame, env_picchi: pd.DataFrame,
                        ili_serie: pd.DataFrame, ili_picchi: pd.DataFrame,
                        variabile: str, stagione_ili: str) -> None:
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    sett_lista = settimane_stagione()
    tick_pos   = [ordine_settimana(s) for s in sett_lista]
    tick_label = [str(s) for s in sett_lista]
    tipo_picco = "minimi (freddo)" if "TEMP" in variabile.upper() else "massimi"

    ax1.plot(env_serie["Ordine"], env_serie["Valore"], color="#1a5276", lw=2, label=variabile)
    if env_picchi is not None and not env_picchi.empty:
        ax1.scatter(env_picchi["Ordine"], env_picchi["Valore"],
                    color="#c0392b", s=65, zorder=4, label=f"Top {TOP_K_PEAKS} picchi {tipo_picco}")
        for _, r in env_picchi.iterrows():
            ax1.axvline(r["Ordine"], color="#c0392b", alpha=0.2, lw=1.2, ls="--")

    ax1.set_ylabel(variabile, fontsize=11)
    ax1.set_title(f"{variabile} vs ILI ATS Bergamo e Montagna  —  Stagione {stagione_ili}",
                  fontsize=13, fontweight="bold")
    ax1.legend(loc="upper right", fontsize=9)
    ax1.grid(True, alpha=0.3)

    ax2.plot(ili_serie["Ordine"], ili_serie["Valore"],
             color="#1e8449", lw=2, label="ILI ATS Bergamo e Montagna")
    if ili_picchi is not None and not ili_picchi.empty:
        ax2.scatter(ili_picchi["Ordine"], ili_picchi["Valore"],
                    color="#c0392b", s=65, zorder=4, label=f"Top {TOP_K_PEAKS} picchi ILI")
        for _, r in ili_picchi.iterrows():
            ax2.axvline(r["Ordine"], color="#c0392b", alpha=0.2, lw=1.2, ls="--")

    ax2.set_ylabel("Accessi PS ILI (n)", fontsize=11)
    ax2.set_xlabel("Settimana (numero ISO)", fontsize=11)
    ax2.legend(loc="upper right", fontsize=9)
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(tick_pos)
    ax2.set_xticklabels(tick_label, fontsize=8)

    plt.tight_layout()
    out_path = PLOTS_DIR / f"{variabile.lower()}_vs_ili_{stagione_ili}.png"
    fig.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    print(f"   📈 {out_path.name}")

# =============================================================================
# ANALISI 2 — CROSS-CORRELAZIONE (Pearson + Spearman)
# =============================================================================

def calcola_cross_correlazione_stagione(
    env_serie: pd.DataFrame, ili_serie: pd.DataFrame,
    variabile: str, stagione_ili: str,
    max_lag: int = MAX_LAG_SETTIMANE,
) -> pd.DataFrame:
    """
    Calcola r di Pearson e ρ di Spearman tra serie ambientale e ILI
    per ogni lag da 0 a max_lag settimane.
    Per TEMPERATURE la serie viene invertita (−T) prima del calcolo.
    """
    MIN_PUNTI_XCORR = 5

    env_ord = env_serie.set_index("Ordine")["Valore"].sort_index()
    ili_ord = ili_serie.set_index("Ordine")["Valore"].sort_index()

    if variabile.upper() == "TEMPERATURE":
        env_ord = -env_ord

    ordini_comuni = sorted(set(env_ord.index) & set(ili_ord.index))
    righe = []

    for lag in range(0, max_lag + 1):
        t_vals     = [o for o in ordini_comuni if (o + lag) in ordini_comuni]
        t_lag_vals = [o + lag for o in t_vals]
        n_punti    = len(t_vals)

        base_row = {"Variabile": variabile, "Stagione_ILI": stagione_ili, "Lag": lag}

        if n_punti < MIN_PUNTI_XCORR:
            righe.append({**base_row, "r_Pearson": np.nan, "p_Pearson": np.nan,
                           "rho_Spearman": np.nan, "p_Spearman": np.nan, "N_punti": n_punti})
            continue

        x = env_ord.loc[t_vals].values
        y = ili_ord.loc[t_lag_vals].values
        mask = ~(np.isnan(x) | np.isnan(y))
        x, y = x[mask], y[mask]

        if len(x) < MIN_PUNTI_XCORR:
            righe.append({**base_row, "r_Pearson": np.nan, "p_Pearson": np.nan,
                           "rho_Spearman": np.nan, "p_Spearman": np.nan, "N_punti": len(x)})
            continue

        r_p, p_p = pearsonr(x, y)
        r_s, p_s = spearmanr(x, y)
        righe.append({**base_row, "r_Pearson": float(r_p), "p_Pearson": float(p_p),
                       "rho_Spearman": float(r_s), "p_Spearman": float(p_s), "N_punti": len(x)})

    return pd.DataFrame(righe)


def crea_grafico_xcorr(df_xcorr: pd.DataFrame, variabile: str) -> None:
    df_v = df_xcorr[df_xcorr["Variabile"] == variabile].copy()
    if df_v.empty:
        return

    stagioni = sorted(df_v["Stagione_ILI"].unique())
    lags     = sorted(df_v["Lag"].unique())
    fig, ax  = plt.subplots(figsize=(11, 5))
    colori   = plt.cm.tab10(np.linspace(0, 0.8, len(stagioni)))

    for stagione, colore in zip(stagioni, colori):
        df_s = df_v[df_v["Stagione_ILI"] == stagione].sort_values("Lag")
        ax.plot(df_s["Lag"], df_s["r_Pearson"],
                marker="o", lw=1.5, markersize=5, color=colore, alpha=0.7,
                label=f"Stagione {stagione} — Pearson")
        ax.plot(df_s["Lag"], df_s["rho_Spearman"],
                marker="^", lw=1.0, markersize=4, color=colore, alpha=0.45, ls="--")

    media_p = df_v.groupby("Lag")["r_Pearson"].mean()
    ax.plot(media_p.index, media_p.values, color="black", lw=2.5, marker="D", markersize=6,
            label="Media Pearson (r)", zorder=5)

    media_s = df_v.groupby("Lag")["rho_Spearman"].mean()
    ax.plot(media_s.index, media_s.values, color="#8e44ad", lw=2.5, marker="s", markersize=6,
            ls="--", label="Media Spearman (ρ)", zorder=5)

    ax.axhspan(-SOGLIA_R_SIGNIFICATIVO, SOGLIA_R_SIGNIFICATIVO, color="gray", alpha=0.12,
               label=f"|coeff.| < {SOGLIA_R_SIGNIFICATIVO} (p > 0.05 indicativo)")
    ax.axhline(0, color="black", lw=0.8, ls="--", alpha=0.5)

    lag_ott_p = None
    if not media_p.dropna().empty:
        lag_ott_p = int(media_p.idxmax())
        ax.axvline(lag_ott_p, color="#27ae60", lw=2, ls=":",
                   label=f"Lag ottimale Pearson: {lag_ott_p} sett.")

    if not media_s.dropna().empty:
        lag_ott_s = int(media_s.idxmax())
        if lag_ott_p is not None and lag_ott_s != lag_ott_p:
            ax.axvline(lag_ott_s, color="#8e44ad", lw=2, ls=":",
                       label=f"Lag ottimale Spearman: {lag_ott_s} sett.")

    etichetta_var = variabile
    if variabile == "TEMPERATURE":
        etichetta_var = "TEMPERATURE (invertita: +coeff = più freddo → più ILI)"

    ax.set_title(
        f"Cross-correlazione {etichetta_var} vs ILI ATS Bergamo e Montagna\n"
        f"linee continue = Pearson r   |   linee tratteggiate = Spearman ρ",
        fontsize=11, fontweight="bold"
    )
    ax.set_xlabel("Lag (settimane): ambiente a t, ILI a t + lag", fontsize=10)
    ax.set_ylabel("Coefficiente di correlazione", fontsize=10)
    ax.set_xticks(lags)
    ax.set_ylim(-1.05, 1.05)
    ax.legend(loc="lower right", fontsize=8, ncol=2)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    out_path = PLOTS_DIR / f"xcorr_{variabile.lower()}.png"
    fig.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    print(f"   📊 {out_path.name}")


def analisi_cross_correlazione(
    df_env_all: dict[str, pd.DataFrame],
    ili_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    print("\n" + "─" * 55)
    print("📐 ANALISI 2 — CROSS-CORRELAZIONE (Pearson + Spearman)")
    print("─" * 55)

    tutti_xcorr = []

    for variabile, (_, mode) in VARIABILI.items():
        if variabile not in df_env_all:
            continue
        df_env = df_env_all[variabile]
        print(f"\n   🔗 Cross-correlazione {variabile}...")

        for stagione_ili, stagione_arpa in STAGIONE_ILI_TO_ARPA.items():
            env_stagione = df_env[df_env["Stagione_ARPA"] == stagione_arpa].copy()
            ili_stagione = ili_df[ili_df["Stagione_ILI"] == stagione_ili].copy()
            if env_stagione.empty or ili_stagione.empty:
                continue

            # Rinomina WEEK → Settimana per compatibilità con set_index("Ordine")
            ili_stagione = ili_stagione.rename(columns={"WEEK": "Settimana"})

            df_lag = calcola_cross_correlazione_stagione(
                env_serie=env_stagione, ili_serie=ili_stagione,
                variabile=variabile, stagione_ili=stagione_ili,
            )
            tutti_xcorr.append(df_lag)

        if tutti_xcorr:
            crea_grafico_xcorr(pd.concat(tutti_xcorr, ignore_index=True), variabile)

    if not tutti_xcorr:
        return pd.DataFrame(), pd.DataFrame()

    df_xcorr_tutti = pd.concat(tutti_xcorr, ignore_index=True)

    media_per_lag = (
        df_xcorr_tutti
        .groupby(["Variabile", "Lag"])[["r_Pearson", "rho_Spearman"]]
        .mean().reset_index()
    )

    lag_ottimale_rows = []
    for var in media_per_lag["Variabile"].unique():
        sub   = media_per_lag[media_per_lag["Variabile"] == var]
        sub_p = sub.dropna(subset=["r_Pearson"])
        sub_s = sub.dropna(subset=["rho_Spearman"])
        row   = {"Variabile": var}
        if not sub_p.empty:
            idx_p = sub_p["r_Pearson"].idxmax()
            row["Lag_ottimale_Pearson"]      = int(sub_p.loc[idx_p, "Lag"])
            row["r_Pearson_medio_ottimale"]   = float(sub_p.loc[idx_p, "r_Pearson"])
        if not sub_s.empty:
            idx_s = sub_s["rho_Spearman"].idxmax()
            row["Lag_ottimale_Spearman"]       = int(sub_s.loc[idx_s, "Lag"])
            row["rho_Spearman_medio_ottimale"] = float(sub_s.loc[idx_s, "rho_Spearman"])
        if "Lag_ottimale_Pearson" in row and "Lag_ottimale_Spearman" in row:
            row["Lag_concordano"] = row["Lag_ottimale_Pearson"] == row["Lag_ottimale_Spearman"]
        lag_ottimale_rows.append(row)

    return df_xcorr_tutti, pd.DataFrame(lag_ottimale_rows)

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 60)
    print("SCRIPT 4 — PICCHI E CROSS-CORRELAZIONE AMBIENTE vs ILI")
    print("           ATS Bergamo + ATS Montagna")
    print("=" * 60)

    # ── 1. Carica ILI ATS Bergamo e Montagna (CSV da Script 1) ──────────────
    print("\n📂 Caricamento ILI ATS Bergamo e Montagna (Script 1 CSV)...")
    ili_df = carica_ili_ats_bergamo_montagna()
    if ili_df.empty:
        print("❌ Impossibile procedere senza dati ILI.")
        return

    # ── 2. Carica variabili ambientali ARPA ──────────────────────────────────
    print("\n📂 Caricamento variabili ambientali ARPA...")
    df_env_all: dict[str, pd.DataFrame] = {}
    for variabile, (sottocartella, _) in VARIABILI.items():
        print(f"\n   📁 {variabile}...")
        df_raw = carica_variabile(variabile, sottocartella)
        df_serie = prepara_serie(df_raw, variabile) if not df_raw.empty else pd.DataFrame()
        if not df_serie.empty:
            df_env_all[variabile] = df_serie
            print(f"   ✅ {variabile}: {sorted(df_serie['Stagione_ARPA'].unique())}")
        else:
            print(f"   ⚠️  {variabile}: nessun dato, skip.")

    if not df_env_all:
        print("❌ Nessuna variabile ambientale disponibile.")
        return

    # ── 3. ANALISI 1: Picchi ─────────────────────────────────────────────────
    print("\n" + "─" * 55)
    print("🔍 ANALISI 1 — PICCHI")
    print("─" * 55)

    tutti_env_picchi: list[pd.DataFrame] = []
    tutti_ili_picchi: list[pd.DataFrame] = []
    tutte_coppie:     list[pd.DataFrame] = []
    tutti_summary:    list[dict]         = []

    for variabile, (_, mode) in VARIABILI.items():
        if variabile not in df_env_all:
            continue
        df_serie = df_env_all[variabile]
        print(f"\n   {'─'*45}")
        print(f"   🔍 {variabile}  →  picchi {'MINIMI (freddo)' if mode=='min' else 'MASSIMI'}")
        stagioni_arpa_disponibili = set(df_serie["Stagione_ARPA"].unique())

        for stagione_ili, stagione_arpa in STAGIONE_ILI_TO_ARPA.items():
            if stagione_arpa not in stagioni_arpa_disponibili:
                continue

            ili_stagione = ili_df[ili_df["Stagione_ILI"] == stagione_ili].copy()
            if ili_stagione.empty:
                continue
            env_stagione = df_serie[df_serie["Stagione_ARPA"] == stagione_arpa].copy()

            env_p = seleziona_top_k_picchi(env_stagione, mode=mode)
            # ↓ FIX: rinomina WEEK→Settimana prima di passare a seleziona_top_k_picchi
            ili_p = seleziona_top_k_picchi(
                ili_stagione.rename(columns={"WEEK": "Settimana"}),
                mode="max"
            )

            if env_p.empty or ili_p.empty:
                print(f"   ⚠️  Picchi non trovati: {variabile} / ILI {stagione_ili}.")
                continue

            env_out = env_p.copy()
            env_out.insert(0, "Stagione_ILI",  stagione_ili)
            env_out.insert(1, "Stagione_ARPA", stagione_arpa)
            env_out.insert(2, "Variabile",     variabile)
            tutti_env_picchi.append(env_out)

            ili_out = ili_p.copy()
            ili_out.insert(0, "Stagione_ILI", stagione_ili)
            ili_out.insert(1, "Variabile",    variabile)
            tutti_ili_picchi.append(ili_out)

            summary, coppie = correla_picchi(env_p, ili_p, variabile, stagione_ili)
            if summary:
                tutti_summary.append(summary)
            if not coppie.empty:
                tutte_coppie.append(coppie)

            # Grafico: usa versione rinominata anche per il plot
            ili_per_plot = ili_stagione.rename(columns={"WEEK": "Settimana"})
            crea_grafico_picchi(
                env_serie=env_stagione, env_picchi=env_p,
                ili_serie=ili_per_plot,  ili_picchi=ili_p,
                variabile=variabile, stagione_ili=stagione_ili,
            )

            lag  = summary["Lag_medio_settimane"]  if summary else float("nan")
            corr = summary["Correlazione_Pearson"] if summary else float("nan")
            print(f"   ✅ ILI {stagione_ili} ↔ ARPA {stagione_arpa}: "
                  f"lag medio = {lag:+.1f} sett. | corr = {corr:.3f}")

    # ── 4. ANALISI 2: Cross-correlazione ─────────────────────────────────────
    df_xcorr, df_lag_ottimale = analisi_cross_correlazione(df_env_all, ili_df)

    # ── 5. Salva CSV ─────────────────────────────────────────────────────────
    print(f"\n{'=' * 55}")
    print("💾 Salvataggio output CSV...")

    def salva_csv(lista_o_df, nome: str) -> None:
        df = pd.concat(lista_o_df, ignore_index=True) if isinstance(lista_o_df, list) else lista_o_df
        if df is None or df.empty:
            print(f"   ⚠️  Nessun dato per {nome}")
            return
        path = OUTPUT_DIR / nome
        df.to_csv(path, index=False)
        print(f"   ✅ {nome}  ({len(df)} righe)")

    salva_csv(tutti_env_picchi, "picchi_ambientali_settimane.csv")
    salva_csv(tutti_ili_picchi, "picchi_ili_ats_bergamo_montagna.csv")
    salva_csv(tutte_coppie,     "abbinamenti_picchi_vs_ili.csv")

    if tutti_summary:
        summary_df = pd.DataFrame(tutti_summary)
        salva_csv(summary_df, "riepilogo_correlazioni_stagionale.csv")
        print(f"\n{'─' * 55}")
        print("RIEPILOGO ANALISI 1 — PICCHI")
        print("─" * 55)
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", 140)
        print(summary_df.to_string(index=False))

    salva_csv(df_xcorr,        "cross_correlazione_per_lag.csv")
    salva_csv(df_lag_ottimale, "cross_correlazione_lag_ottimale.csv")

    if not df_lag_ottimale.empty:
        print(f"\n{'─' * 55}")
        print("RIEPILOGO ANALISI 2 — LAG OTTIMALE (Pearson r + Spearman ρ)")
        print("─" * 55)
        print(df_lag_ottimale.to_string(index=False))
        print()
        print("  NB: 'Lag_concordano = True' → i due metodi concordano sul lag → risultato robusto.")
        print("  NB: se False → preferire Spearman per PM/NO2, Pearson per T/Umidità.")

    print(f"\n✅ ELABORAZIONE COMPLETATA")
    print(f"📂 Output in:  {OUTPUT_DIR}")
    print(f"📂 Grafici in: {PLOTS_DIR}")


if __name__ == "__main__":
    main()