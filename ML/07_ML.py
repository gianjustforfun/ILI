"""
=============================================================
SCRIPT 7 — MODELLO MULTIVARIATO: FATTORI AMBIENTALI vs ILI
        (ATS Milano, ATS Bergamo, ATS Montagna)
=============================================================

RESEARCH QUESTION:
    Quali fattori ambientali (temperatura, umidità, PM2.5, PM10, NO2)
    hanno un'associazione indipendente con gli accessi al Pronto Soccorso
    per ILI nelle tre ATS analizzate (Milano, Bergamo, Montagna)?
    Il peso relativo dei fattori varia tra ATS con caratteristiche
    geografiche diverse (pianura padana vs area alpina)?

FRAMEWORK IPCC:
    Componente HAZARD: stima l'associazione tra stressor ambientali
    e outcome sanitario (accessi PS per ILI normalizzati).
    La variabilità tra ATS permette un confronto del ruolo del contesto
    geografico (componente EXPOSURE).

LOGICA DELLO SCRIPT:
    Lo script è parametrico: la stessa pipeline viene eseguita
    in sequenza per ciascuna ATS definita in ATS_CONFIG.
    Aggiungere una nuova ATS richiede solo di estendere ATS_CONFIG
    con i path corretti — nessuna modifica al codice.

GERARCHIA METODOLOGICA (Capitolo 7 del corso):
    Il corso chiarisce che ML va scelto solo quando appropriato.
    Con ~80 osservazioni per ATS, i modelli classici sono preferibili.
        Livello 1 (sempre):      Correlazione predittori + Odds Ratio
        Livello 2 (principale):  OLS multiplo + Ridge con LOGO-CV
        Livello 3 (esplorativo): Random Forest + SHAP (con disclaimer)

NOTE SUI DATI AMBIENTALI:
    I dati ARPA (Script 3) sono gli stessi per tutte e tre le ATS:
    si usa la media delle stazioni disponibili in Lombardia.
    Idealmente si userebbero solo le stazioni dentro il territorio
    di ciascuna ATS, ma con i dati disponibili questa è la scelta
    più praticabile. Va dichiarato come limitazione nel report.

    I lag ottimali (da Script 4) sono stimati su ATS Milano e applicati
    anche alle altre ATS: semplificazione giustificata dal fatto che
    con ~20 punti per stagione non è possibile stimare lag separati
    per ciascuna ATS in modo affidabile.

BIAS E LIMITAZIONI (da dichiarare nel report):
    1. Bias ecologico: dati aggregati a livello ATS, non individuale
    2. n piccolo (~80 obs, 4 stagioni): risultati esplorativi
    3. Autocorrelazione temporale: mitigata con termine ILI_{t-1}
    4. Dati ambientali non stratificati per ATS (stesse stazioni ARPA)
    5. ATS Montagna: n ridotto per possibili dati ILI mancanti
    6. Lag uniformi tra ATS (stimati su ATS Milano)
    7. Confounding non misurato: vaccinazioni, varianti virali

OUTPUT PRODOTTI:
    output/script7/ATS_MILANO/     → risultati ATS Milano
    output/script7/ATS_BERGAMO/    → risultati ATS Bergamo
    output/script7/ATS_MONTAGNA/   → risultati ATS Montagna
    output/script7/               → confronto comparativo tra ATS

REQUISITI:
    pip install pandas numpy matplotlib seaborn scikit-learn statsmodels shap scipy
=============================================================
"""

from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from scipy.stats import pearsonr
import statsmodels.api as sm
from sklearn.linear_model import RidgeCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import LeaveOneGroupOut
from sklearn.metrics import r2_score

try:
    import shap
    SHAP_DISPONIBILE = True
except ImportError:
    SHAP_DISPONIBILE = False
    print("  ⚠ shap non installato — grafico SHAP saltato.")
    print("    Installa con: pip install shap")

warnings.filterwarnings("ignore")
plt.style.use("seaborn-v0_8-whitegrid")


# =============================================================================
# SEZIONE 1 — CONFIGURAZIONE PERCORSI E ATS
# =============================================================================

BASE_DIR     = Path(__file__).resolve().parent   # .../CORRELATIONS/
PROJECT_ROOT = BASE_DIR.parent                   # .../ILI/
ARPA_DIR     = PROJECT_ROOT / "ARPA" / "SETTIMANE_DI_INTERESSE"
LAG_FILE     = BASE_DIR / "output" / "cross_correlazione_lag_ottimale.csv"
OUT_BASE     = BASE_DIR / "output" / "script7"
OUT_BASE.mkdir(parents=True, exist_ok=True)

ILI_BASE = PROJECT_ROOT / "SORVEGLIANZA ACCESSI PS" / "output"

# Configurazione delle tre ATS.
# Per aggiungere una nuova ATS basta aggiungere una voce qui.
ATS_CONFIG = {
    "ATS_MILANO": {
        "ili_file": ILI_BASE / "ATS_MILANO"   / "ili_ats_milano_stagionale.csv",
        "tot_file": ILI_BASE / "ATS_MILANO"   / "access_tot_milano_stagionale.csv",
        "col_ili":  "ACCESSI_ILI_ATS_MILANO",
        "col_tot":  "ACCESSI_TOTALI_ER_MILANO",
        "label":    "ATS Milano (pianura padana)",
    },
    "ATS_BERGAMO": {
        "ili_file": ILI_BASE / "ATS_BERGAMO"  / "ili_ats_bergamo_stagionale.csv",
        "tot_file": ILI_BASE / "ATS_BERGAMO"  / "access_tot_bergamo_stagionale.csv",
        "col_ili":  "ACCESSI_ILI_ATS_BERGAMO",
        "col_tot":  "ACCESSI_TOTALI_ER_BERGAMO",
        "label":    "ATS Bergamo (pianura/colline)",
    },
    "ATS_MONTAGNA": {
        "ili_file": ILI_BASE / "ATS_MONTAGNA" / "ili_ats_montagna_stagionale.csv",
        "tot_file": ILI_BASE / "ATS_MONTAGNA" / "access_tot_montagna_stagionale.csv",
        "col_ili":  "ACCESSI_ILI_ATS_MONTAGNA",
        "col_tot":  "ACCESSI_TOTALI_ER_MONTAGNA",
        "label":    "ATS Montagna (area alpina)",
    },
}

# Variabili ambientali: chiave = nome colonna nel dataset, valore = sottocartella ARPA
VARIABILI_ENV = {
    "TEMP":     "TEMPERATURE",
    "HUMIDITY": "HUMIDITY",
    "PM25":     "PM25",
    "PM10":     "PM10",
    "NO2":      "NO2",
}

# Per TEMPERATURE e HUMIDITY si usano solo le colonne "_media" (non _min/_max)
# per evitare di mischiare grandezze diverse nello stesso calcolo
TIPOLOGIE_CON_SOTTOSTAT = {"TEMPERATURE", "HUMIDITY"}

# Mapping stagione ARPA (anno_inizio_anno_fine) → stagione ILI (formato breve)
STAGIONI_MAP = {
    "2022_2023": "22-23",
    "2023_2024": "23-24",
    "2024_2025": "24-25",
    "2025_2026": "25-26",
}

# Lag di default in settimane (sovrascritti da Script 4 se disponibile)
# Basati sulla letteratura: Shaman et al. 2010, Feng et al. 2016
LAG_DEFAULT = {
    "TEMP":     2,
    "HUMIDITY": 1,
    "PM25":     1,
    "PM10":     1,
    "NO2":      1,
}

# Parametri modelli
ALPHAS_RIDGE          = [0.01, 0.1, 1.0, 10.0, 100.0]
RF_N_TREES            = 200
RF_MAX_DEPTH          = 3   # basso deliberatamente: limita overfitting con n piccolo
SOGLIA_PERCENTILE_ILI = 75  # soglia per binarizzare l'outcome nell'analisi OR
SOGLIA_PERCENTILE_ENV = 75  # soglia per binarizzare l'esposizione nell'analisi OR


# =============================================================================
# SEZIONE 2 — FUNZIONI CONDIVISE (usate per tutte e tre le ATS)
# =============================================================================

def ordine_settimana(s: int) -> int:
    """
    Mappa settimana ISO → posizione progressiva nella stagione influenzale.
    Sett. 48 → 1, sett. 52 → 5, sett. 1 → 6, sett. 15 → 20.
    Necessario per ordinare correttamente le serie temporali stagionali
    senza che matplotlib metta le settimane 1-15 prima delle 48-52.
    """
    return s - 47 if s >= 48 else s + 5


def carica_lag_ottimali() -> dict[str, int]:
    """
    Legge i lag ottimali stimati da Script 4 (cross-correlazione).

    I lag vengono applicati uniformemente a tutte le ATS.
    Questo è una semplificazione: idealmente si stimerebbero lag
    separati per ogni ATS, ma con ~20 punti per stagione la stima
    sarebbe troppo instabile. Dichiarare questa scelta nel report.

    Se il file non esiste, usa i valori di default da LAG_DEFAULT.
    """
    nome_map = {
        "TEMPERATURE": "TEMP",
        "HUMIDITY":    "HUMIDITY",
        "PM25":        "PM25",
        "PM10":        "PM10",
        "NO2":         "NO2",
    }

    if not LAG_FILE.exists():
        print(f"  ⚠ File lag non trovato: {LAG_FILE}")
        print(f"     Uso lag di default: {LAG_DEFAULT}")
        return LAG_DEFAULT.copy()

    df      = pd.read_csv(LAG_FILE)
    lag_map = {}

    for _, row in df.iterrows():
        var_arpa   = str(row.get("Variabile", "")).upper()
        var_script = nome_map.get(var_arpa, var_arpa)
        lag_val    = int(row.get("Lag_ottimale_sett", LAG_DEFAULT.get(var_script, 1)))
        lag_map[var_script] = lag_val

    # Completa con default per variabili mancanti nel file
    for var, default in LAG_DEFAULT.items():
        if var not in lag_map:
            lag_map[var] = default
            print(f"  ℹ Lag per {var} non in Script 4 → default {default}")

    print(f"  ✅ Lag ottimali caricati: {lag_map}")
    return lag_map


def carica_serie_ambientale(nome_var: str, sottocartella: str) -> pd.DataFrame:
    """
    Legge tutti i CSV stagionali di una variabile ambientale (output Script 3)
    e restituisce un DataFrame long:
        Stagione_ARPA | Settimana | <nome_var>

    Per TEMPERATURE e HUMIDITY usa solo le colonne '_media' per ogni sensore,
    evitando di mediare insieme grandezze diverse (massimo, media, minimo).
    Per PM25, PM10, NO2 usa tutte le colonne (struttura già piatta).
    """
    cartella = ARPA_DIR / sottocartella
    if not cartella.exists():
        print(f"  ⚠ Cartella ARPA non trovata: {cartella}")
        return pd.DataFrame()

    dfs = []
    for f in sorted(cartella.glob("*.csv")):
        try:
            df = pd.read_csv(f)
            if "Settimana" not in df.columns:
                continue
            parts   = f.stem.split("_")
            stagione = "_".join(parts[-2:]) if len(parts) >= 3 else f.stem
            df["Stagione_ARPA"] = stagione
            dfs.append(df)
        except Exception as e:
            print(f"  ⚠ Errore leggendo {f.name}: {e}")

    if not dfs:
        return pd.DataFrame()

    df_tot = pd.concat(dfs, ignore_index=True)
    df_tot["Settimana"] = pd.to_numeric(df_tot["Settimana"], errors="coerce")
    df_tot = df_tot.dropna(subset=["Settimana"])
    df_tot["Settimana"] = df_tot["Settimana"].astype(int)

    # Selezione colonne in base alla tipologia
    if sottocartella.upper() in TIPOLOGIE_CON_SOTTOSTAT:
        cols_dati = [c for c in df_tot.columns
                     if c not in ("Settimana", "Stagione_ARPA")
                     and c.endswith("_media")]
    else:
        cols_dati = [c for c in df_tot.columns
                     if c not in ("Settimana", "Stagione_ARPA")]

    if not cols_dati:
        print(f"  ⚠ Nessuna colonna dati trovata per {nome_var}")
        return pd.DataFrame()

    # Converti a numerico e sostituisci -999 (codice ARPA per dato mancante)
    for c in cols_dati:
        df_tot[c] = pd.to_numeric(df_tot[c], errors="coerce")
        df_tot.loc[df_tot[c] == -999, c] = np.nan

    # Media tra stazioni/sensori → un valore rappresentativo per settimana
    df_tot["Valore"] = df_tot[cols_dati].mean(axis=1, skipna=True)

    serie = (
        df_tot
        .groupby(["Stagione_ARPA", "Settimana"], as_index=False)["Valore"]
        .mean()
        .rename(columns={"Valore": nome_var})
    )
    return serie


def applica_lag(df: pd.DataFrame, col: str, lag: int) -> pd.DataFrame:
    """
    Applica un lag temporale a una colonna ambientale, separatamente
    per ogni stagione (evita di connettere stagioni diverse).

    Risultato: la riga t contiene il valore ambientale di t-lag,
    cioè "l'ambiente lag settimane fa precede l'ILI di oggi".

    Le prime `lag` righe di ogni stagione diventano NaN:
    verranno rimosse nel dropna() finale.
    """
    if lag == 0:
        return df

    col_lagged = f"{col}_lag{lag}"
    df         = df.copy()
    df[col_lagged] = np.nan

    for stagione in df["STAGIONE"].unique():
        mask = df["STAGIONE"] == stagione
        df.loc[mask, col_lagged] = df.loc[mask, col].shift(lag).values

    df = df.drop(columns=[col])
    return df


# =============================================================================
# SEZIONE 3 — COSTRUZIONE DATASET PER UNA SINGOLA ATS
# =============================================================================

def costruisci_dataset_ats(ats_key: str, cfg: dict,
                            lag_map: dict[str, int]) -> tuple[pd.DataFrame, list[str]]:
    """
    Costruisce il dataset unificato per una singola ATS.

    Passi:
        1. Carica ILI e accessi totali (da Script 1)
        2. Calcola %ILI = accessi ILI / accessi totali * 100
           (normalizzazione per confronto tra ATS con volumi diversi)
        3. Carica ogni variabile ambientale (da Script 3)
        4. Merge su STAGIONE + SETTIMANA
        5. Applica lag ottimali (da Script 4)
        6. Aggiunge ILI_{t-1} come predittore autoregressivo
           (mitiga la violazione dell'indipendenza dei residui in OLS)
        7. Rimuove righe con NaN residui

    Perché %ILI e non conteggi assoluti:
        ATS Bergamo e ATS Montagna hanno popolazioni diverse da ATS Milano.
        Usare i conteggi assoluti renderebbe i coefficienti non confrontabili
        tra ATS. Il %ILI normalizza per il volume di attività del PS.

    Returns:
        df_finale   : DataFrame una riga per settimana per stagione
        cols_pred   : lista nomi colonne predittore (con lag applicato)
    """
    print(f"\n  {'─'*50}")
    print(f"  📂 {ats_key} — costruzione dataset")
    print(f"  {'─'*50}")

    # ── 1. Carica ILI ─────────────────────────────────────────────────────
    if not cfg["ili_file"].exists():
        print(f"  ❌ File ILI non trovato: {cfg['ili_file']}")
        print(f"     Esegui prima Script 1.")
        return pd.DataFrame(), []

    df_ili = pd.read_csv(cfg["ili_file"])
    print(f"  ILI caricato: {len(df_ili)} righe, "
          f"stagioni: {sorted(df_ili['STAGIONE'].unique())}")

    # ── 2. Calcola %ILI ───────────────────────────────────────────────────
    if cfg["tot_file"].exists():
        df_tot = pd.read_csv(cfg["tot_file"])
        df_base = pd.merge(
            df_ili[["STAGIONE", "WEEK", "ORDINE", cfg["col_ili"]]],
            df_tot[["STAGIONE", "WEEK", cfg["col_tot"]]],
            on=["STAGIONE", "WEEK"],
            how="left"
        )
        df_base["ILI_NORM"] = (
            df_base[cfg["col_ili"]].astype(float) /
            df_base[cfg["col_tot"]].astype(float) * 100
        ).where(df_base[cfg["col_tot"]] > 0)
        print(f"  %ILI calcolato (ILI/totale*100)")
    else:
        print(f"  ⚠ File totali non trovato → uso conteggi assoluti")
        df_base            = df_ili.copy()
        df_base["ILI_NORM"] = df_base[cfg["col_ili"]].astype(float)

    df_base = df_base[["STAGIONE", "WEEK", "ORDINE", "ILI_NORM"]].copy()
    df_base = df_base.rename(columns={"WEEK": "SETTIMANA"})
    df_base["ORDINE"] = df_base["SETTIMANA"].apply(ordine_settimana)

    # ── 3. Carica e merge variabili ambientali ────────────────────────────
    for nome_var, sottocartella in VARIABILI_ENV.items():
        serie = carica_serie_ambientale(nome_var, sottocartella)
        if serie.empty:
            print(f"  ⚠ Nessun dato per {nome_var} — esclusa")
            continue

        serie["STAGIONE"] = serie["Stagione_ARPA"].map(STAGIONI_MAP)
        serie = serie.dropna(subset=["STAGIONE"])
        serie = serie.rename(columns={"Settimana": "SETTIMANA"})
        serie = serie[["STAGIONE", "SETTIMANA", nome_var]]

        n_pre  = len(df_base)
        df_base = pd.merge(df_base, serie, on=["STAGIONE", "SETTIMANA"], how="left")
        n_match = df_base[nome_var].notna().sum()
        print(f"  {nome_var}: {n_match}/{n_pre} settimane con dato")

    # ── 4. Applica lag ────────────────────────────────────────────────────
    df_base = df_base.sort_values(["STAGIONE", "ORDINE"]).reset_index(drop=True)
    cols_pred_lagged = []

    for nome_var in VARIABILI_ENV:
        if nome_var not in df_base.columns:
            continue
        lag = lag_map.get(nome_var, 0)
        df_base = applica_lag(df_base, nome_var, lag)
        col_lagged = f"{nome_var}_lag{lag}" if lag > 0 else nome_var
        cols_pred_lagged.append(col_lagged)

    # ── 5. Termine autoregressivo ILI_{t-1} ──────────────────────────────
    # Mitiga la violazione dell'indipendenza dei residui in OLS:
    # la settimana precedente di ILI è il predittore più forte
    # dell'ILI corrente (l'epidemia non si azzera da una settimana all'altra).
    df_base["ILI_NORM_lag1"] = np.nan
    for stagione in df_base["STAGIONE"].unique():
        mask = df_base["STAGIONE"] == stagione
        df_base.loc[mask, "ILI_NORM_lag1"] = \
            df_base.loc[mask, "ILI_NORM"].shift(1).values
    cols_pred_lagged.append("ILI_NORM_lag1")

    # ── 6. Rimuovi NaN ────────────────────────────────────────────────────
    n_pre = len(df_base)
    cols_valide = [c for c in cols_pred_lagged if c in df_base.columns]
    df_finale = df_base.dropna(subset=["ILI_NORM"] + cols_valide).copy()
    print(f"  Righe finali: {len(df_finale)}/{n_pre} "
          f"({n_pre - len(df_finale)} rimosse per NaN/lag)")
    print(f"  Predittori: {cols_valide}")

    return df_finale, cols_valide


# =============================================================================
# SEZIONE 4 — LIVELLO 1: CORRELAZIONE E ODDS RATIO
# =============================================================================

def analisi_correlazione(df: pd.DataFrame, cols_pred: list[str],
                          out_dir: Path, label: str) -> None:
    """
    Matrice di correlazione di Pearson tra predittori.

    Scopo: documentare la multicollinearità prima del modello.
    Correlazioni elevate (|r| > 0.7) tra predittori giustificano
    la scelta di Ridge rispetto a OLS puro.
    """
    cols_disp = [c for c in cols_pred if c in df.columns]
    corr = df[cols_disp].corr(method="pearson")

    # Calcola p-value per aggiungere asterischi
    annot = corr.round(2).astype(str)
    for c1 in cols_disp:
        for c2 in cols_disp:
            if c1 != c2:
                vals = df[[c1, c2]].dropna()
                if len(vals) >= 5:
                    _, p = pearsonr(vals[c1], vals[c2])
                    if p < 0.05:
                        annot.loc[c1, c2] = annot.loc[c1, c2] + "*"

    fig, ax = plt.subplots(figsize=(9, 7))
    sns.heatmap(corr, annot=annot, fmt="", cmap="coolwarm",
                vmin=-1, vmax=1, center=0, square=True, ax=ax,
                linewidths=0.5, cbar_kws={"shrink": 0.8})
    ax.set_title(
        f"Correlazione predittori — {label}\n"
        f"* = p<0.05 | |r|>0.7 segnala multicollinearità",
        fontsize=11, fontweight="bold"
    )
    plt.tight_layout()
    fig.savefig(out_dir / "01_correlazione_predittori.png", dpi=150)
    plt.close(fig)

    # Stampa coppie problematiche
    for i, c1 in enumerate(cols_disp):
        for c2 in cols_disp[i+1:]:
            r = corr.loc[c1, c2]
            if abs(r) > 0.7:
                print(f"  ⚠ Alta correlazione: {c1} vs {c2}: r={r:.3f}")


def calcola_odds_ratio(df: pd.DataFrame, cols_pred: list[str],
                        out_dir: Path, label: str) -> pd.DataFrame:
    """
    Calcola OR per ogni variabile ambientale rispetto all'outcome
    binario "ILI elevato" (> 75° percentile).

    Dal §7.4.1 del corso: OR è uno strumento di feature selection
    e può essere usato come output standalone in studi epidemiologici.

    LIMITAZIONE: OR è formalmente corretto per studi caso-controllo.
    Qui è usato come approssimazione per la feature selection su
    serie temporali. Dichiararlo nel report.
    """
    soglia_ili = np.percentile(df["ILI_NORM"].dropna(), SOGLIA_PERCENTILE_ILI)
    df = df.copy()
    df["ILI_alto"] = (df["ILI_NORM"] > soglia_ili).astype(int)

    risultati = []
    cols_env  = [c for c in cols_pred if c != "ILI_NORM_lag1" and c in df.columns]

    for col in cols_env:
        soglia_env = np.percentile(df[col].dropna(), SOGLIA_PERCENTILE_ENV)
        df["esposto"] = (df[col] > soglia_env).astype(int)

        A = len(df[(df["esposto"]==1) & (df["ILI_alto"]==1)])
        B = len(df[(df["esposto"]==1) & (df["ILI_alto"]==0)])
        C = len(df[(df["esposto"]==0) & (df["ILI_alto"]==1)])
        D = len(df[(df["esposto"]==0) & (df["ILI_alto"]==0)])

        # Correzione di continuità (evita divisione per zero)
        A_, B_, C_, D_ = A+0.5, B+0.5, C+0.5, D+0.5
        or_val  = (A_ * D_) / (B_ * C_)
        se_log  = np.sqrt(1/A_ + 1/B_ + 1/C_ + 1/D_)
        ci_low  = np.exp(np.log(or_val) - 1.96 * se_log)
        ci_high = np.exp(np.log(or_val) + 1.96 * se_log)

        risultati.append({
            "Variabile":     col,
            "Soglia_env":    round(soglia_env, 2),
            "OR":            round(or_val, 3),
            "IC95_low":      round(ci_low, 3),
            "IC95_high":     round(ci_high, 3),
            "Significativo": "Sì" if (ci_low > 1 or ci_high < 1) else "No",
        })

    df_or = pd.DataFrame(risultati)
    df_or.to_csv(out_dir / "or_feature_selection.csv", index=False)

    # Forest plot OR
    fig, ax = plt.subplots(figsize=(8, max(4, len(df_or) * 0.7)))
    for i, row in df_or.iterrows():
        colore = "#c0392b" if row["Significativo"] == "Sì" else "#7f8c8d"
        ax.errorbar(
            x=row["OR"], y=i,
            xerr=[[row["OR"]-row["IC95_low"]], [row["IC95_high"]-row["OR"]]],
            fmt="o", color=colore, capsize=4, markersize=7, linewidth=1.5
        )
    ax.axvline(1.0, color="black", linestyle="--", linewidth=1, alpha=0.7)
    ax.set_yticks(range(len(df_or)))
    ax.set_yticklabels(df_or["Variabile"].tolist())
    ax.set_xlabel("Odds Ratio (IC 95%)", fontsize=10)
    ax.set_title(
        f"Odds Ratio — {label}\n"
        f"Rosso = IC 95% non include 1 | Grigio = non significativo",
        fontsize=10, fontweight="bold"
    )
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(out_dir / "06_or_feature_selection.png", dpi=150)
    plt.close(fig)

    return df_or


# =============================================================================
# SEZIONE 5 — LIVELLO 2A: OLS MULTIPLO
# =============================================================================

def modello_ols(df: pd.DataFrame, cols_pred: list[str],
                out_dir: Path, label: str) -> sm.regression.linear_model.RegressionResultsWrapper:
    """
    Regressione lineare multipla con statsmodels.

    Perché statsmodels invece di sklearn:
        Restituisce p-value, IC 95%, R², F-statistic — essenziali per
        una tabella scientifica in un report epidemiologico.

    I predittori sono standardizzati (media=0, SD=1) prima della stima
    per rendere i coefficienti direttamente confrontabili tra variabili
    su scale diverse (°C vs µg/m³ vs %).

    Limitazione: OLS assume residui indipendenti. Con serie temporali
    questa assunzione è violata (autocorrelazione). Il termine ILI_lag1
    mitiga parzialmente il problema ma non lo elimina.
    """
    cols_disp = [c for c in cols_pred if c in df.columns]
    df_clean  = df[["ILI_NORM"] + cols_disp].dropna().copy()
    print(f"\n  OLS: {len(df_clean)} osservazioni, {len(cols_disp)} predittori")

    scaler   = StandardScaler()
    X_scaled = pd.DataFrame(
        scaler.fit_transform(df_clean[cols_disp]),
        columns=cols_disp, index=df_clean.index
    )
    X_sm  = sm.add_constant(X_scaled)
    model = sm.OLS(df_clean["ILI_NORM"], X_sm).fit()

    print(model.summary().as_text())

    # Salva tabella coefficienti
    df_coef = pd.DataFrame({
        "Variabile":    model.params.index,
        "Coefficiente": model.params.values.round(4),
        "Std_err":      model.bse.values.round(4),
        "t_stat":       model.tvalues.values.round(4),
        "p_value":      model.pvalues.values.round(4),
        "IC95_low":     model.conf_int()[0].values.round(4),
        "IC95_high":    model.conf_int()[1].values.round(4),
    })
    df_coef = df_coef[df_coef["Variabile"] != "const"]
    df_coef.to_csv(out_dir / "ols_coefficienti.csv", index=False)

    # Forest plot coefficienti OLS
    fig, ax = plt.subplots(figsize=(8, max(4, len(df_coef) * 0.7)))
    for i, (_, row) in enumerate(df_coef.iterrows()):
        colore = "#2980b9" if row["p_value"] < 0.05 else "#95a5a6"
        ax.errorbar(
            x=row["Coefficiente"], y=i,
            xerr=[[row["Coefficiente"]-row["IC95_low"]],
                  [row["IC95_high"]-row["Coefficiente"]]],
            fmt="o", color=colore, capsize=4, markersize=7, linewidth=1.5
        )
    ax.axvline(0, color="black", linestyle="--", linewidth=1, alpha=0.7)
    ax.set_yticks(range(len(df_coef)))
    ax.set_yticklabels(df_coef["Variabile"].tolist())
    ax.set_xlabel("Coefficiente OLS standardizzato (IC 95%)", fontsize=10)
    ax.set_title(
        f"OLS multiplo — {label}\n"
        f"R²={model.rsquared:.3f} | Blu=p<0.05 | Grigio=p≥0.05 | n={len(df_clean)}",
        fontsize=10, fontweight="bold"
    )
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(out_dir / "02_ols_coefficienti.png", dpi=150)
    plt.close(fig)

    return model


# =============================================================================
# SEZIONE 6 — LIVELLO 2B: RIDGE + LEAVE-ONE-SEASON-OUT CV
# =============================================================================

def modello_ridge_cv(df: pd.DataFrame, cols_pred: list[str],
                      out_dir: Path, label: str) -> dict:
    """
    Ridge Regression con LeaveOneGroupOut cross-validation.

    Perché Ridge:
        Gestisce la multicollinearità tra predittori ambientali
        aggiungendo una penalità L2 che riduce i coefficienti verso zero
        senza escludere variabili. Dal corso §7.3.1: "regularised variants
        should be preferred" con feature set correlato.

    Perché LeaveOneGroupOut (non k-fold random):
        Dal corso §7.5.2: "Shuffling must not be applied to time series."
        LOGO-CV usa ogni stagione come fold di test: addestra su 3 stagioni,
        testa sulla 4ª. Simula la previsione reale su stagioni non viste.
        Con 4 stagioni → 4 fold → alta varianza della stima (dichiararlo).

    Standardizzazione fit-sul-train-only:
        Lo StandardScaler viene fittato SOLO sul training set di ogni fold
        e poi applicato al test. Questo evita il data leakage: se si
        standardizzasse sull'intero dataset, il test "vedrebbe" informazioni
        future attraverso la media e la SD globali.

    Returns: dizionario con R² per fold e coefficienti medi.
    """
    cols_disp = [c for c in cols_pred if c in df.columns]
    df_clean  = df[["STAGIONE", "ILI_NORM"] + cols_disp].dropna().copy()

    X       = df_clean[cols_disp].values
    y       = df_clean["ILI_NORM"].values
    gruppi  = df_clean["STAGIONE"].values
    stagioni_uniche = df_clean["STAGIONE"].unique()

    print(f"\n  Ridge LOGO-CV: {len(df_clean)} obs, "
          f"stagioni: {list(stagioni_uniche)}")

    logo            = LeaveOneGroupOut()
    r2_per_fold     = []
    coef_per_fold   = []
    y_pred_tutti    = np.full(len(y), np.nan)

    for train_idx, test_idx in logo.split(X, y, gruppi):
        stagione_test = gruppi[test_idx][0]

        X_train, X_test = X[train_idx], X[test_idx]
        y_train, y_test = y[train_idx], y[test_idx]

        # Standardizza solo sul training (evita data leakage)
        scaler_fold  = StandardScaler()
        X_train_s    = scaler_fold.fit_transform(X_train)
        X_test_s     = scaler_fold.transform(X_test)

        ridge = RidgeCV(alphas=ALPHAS_RIDGE)
        ridge.fit(X_train_s, y_train)
        y_pred = ridge.predict(X_test_s)
        y_pred_tutti[test_idx] = y_pred

        r2 = r2_score(y_test, y_pred)
        r2_per_fold.append({
            "ATS":           label,
            "Stagione_test": stagione_test,
            "R2":            round(r2, 4),
            "Alpha_scelto":  ridge.alpha_,
        })
        coef_per_fold.append(ridge.coef_)
        print(f"  Fold test={stagione_test}: R²={r2:.4f}, alpha={ridge.alpha_}")

    coef_medio = np.mean(coef_per_fold, axis=0)
    coef_std   = np.std(coef_per_fold, axis=0)

    df_ridge = pd.DataFrame({
        "Variabile":  cols_disp,
        "Coef_medio": coef_medio.round(4),
        "Coef_std":   coef_std.round(4),
        "Importanza": np.abs(coef_medio).round(4),
    }).sort_values("Importanza", ascending=False)

    df_cv = pd.DataFrame(r2_per_fold)
    df_ridge.to_csv(out_dir / "ridge_coefficienti.csv", index=False)
    df_cv.to_csv(out_dir / "performance_cv.csv", index=False)

    r2_medio = df_cv["R2"].mean()
    r2_std   = df_cv["R2"].std()
    print(f"  R² medio: {r2_medio:.4f} ± {r2_std:.4f}")
    print(f"  ⚠ 4 fold → stima instabile. Risultati esplorativi.")

    # Grafico importanza feature Ridge
    fig, ax = plt.subplots(figsize=(8, max(4, len(df_ridge) * 0.7)))
    colori = ["#c0392b" if c > 0 else "#2980b9" for c in df_ridge["Coef_medio"]]
    ax.barh(df_ridge["Variabile"], df_ridge["Coef_medio"],
            xerr=df_ridge["Coef_std"], color=colori, alpha=0.8,
            capsize=4, edgecolor="white")
    ax.axvline(0, color="black", linewidth=0.8, linestyle="--")
    ax.set_xlabel("Coefficiente Ridge std. medio (±SD tra fold)", fontsize=10)
    ax.set_title(
        f"Feature importance Ridge — {label}\n"
        f"R²={r2_medio:.3f}±{r2_std:.3f} | Rosso=+ILI | Blu=-ILI",
        fontsize=10, fontweight="bold"
    )
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(out_dir / "03_ridge_coefficienti.png", dpi=150)
    plt.close(fig)

    # Grafico predicted vs actual
    mask = ~np.isnan(y_pred_tutti)
    fig, ax = plt.subplots(figsize=(7, 6))
    scatter = ax.scatter(
        y[mask], y_pred_tutti[mask],
        c=pd.factorize(gruppi[mask])[0],
        cmap="tab10", alpha=0.7, s=50,
        edgecolors="white", linewidth=0.5
    )
    lim_min = min(y[mask].min(), y_pred_tutti[mask].min()) * 0.9
    lim_max = max(y[mask].max(), y_pred_tutti[mask].max()) * 1.1
    ax.plot([lim_min, lim_max], [lim_min, lim_max],
            "k--", lw=1.5, label="Perfetto (pred=actual)")
    r2_glob = r2_score(y[mask], y_pred_tutti[mask])

    # Legenda colori stagioni
    stagioni_list = sorted(df_clean["STAGIONE"].unique())
    patches = [mpatches.Patch(color=plt.cm.tab10(i/9), label=s)
               for i, s in enumerate(stagioni_list)]
    ax.legend(handles=patches, title="Stagione", fontsize=8, loc="upper left")

    ax.set_xlabel("%ILI reale", fontsize=11)
    ax.set_ylabel("%ILI predetto", fontsize=11)
    ax.set_title(
        f"Predicted vs Actual — {label}\n"
        f"R² globale (LOGO-CV) = {r2_glob:.3f}",
        fontsize=10, fontweight="bold"
    )
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(out_dir / "05_predicted_vs_actual.png", dpi=150)
    plt.close(fig)

    return {
        "label":      label,
        "r2_medio":   r2_medio,
        "r2_std":     r2_std,
        "df_ridge":   df_ridge,
        "df_cv":      df_cv,
    }


# =============================================================================
# SEZIONE 7 — LIVELLO 3: RANDOM FOREST + SHAP (ESPLORATIVO)
# =============================================================================

def modello_rf_shap(df: pd.DataFrame, cols_pred: list[str],
                     out_dir: Path, label: str) -> None:
    """
    Random Forest + SHAP values per esplorare pattern non-lineari.

    Contesto (§7.7 del corso):
        SHAP assegna a ogni feature il contributo marginale medio
        alla previsione. Il summary plot mostra:
        - Asse Y: feature per importanza (SHAP medio assoluto)
        - Asse X: valore SHAP per ogni osservazione
        - Colore: valore feature (rosso=alto, blu=basso)

    ⚠ DISCLAIMER (obbligatorio nel report):
        Con n~80, Random Forest è al limite dell'applicabilità.
        max_depth=3 limita la complessità ma non elimina l'overfitting.
        Dal corso §7.1: "classical statistical approaches often outperform
        ML when data are limited."
        I risultati SHAP qui sono esplorativi — il modello principale è Ridge.
    """
    if not SHAP_DISPONIBILE:
        print(f"  ⚠ SHAP non disponibile — sezione saltata per {label}.")
        return

    cols_disp = [c for c in cols_pred if c in df.columns]
    df_clean  = df[["ILI_NORM"] + cols_disp].dropna().copy()

    scaler   = StandardScaler()
    X_scaled = pd.DataFrame(
        scaler.fit_transform(df_clean[cols_disp]),
        columns=cols_disp, index=df_clean.index
    )
    y = df_clean["ILI_NORM"]

    rf = RandomForestRegressor(
        n_estimators=RF_N_TREES,
        max_depth=RF_MAX_DEPTH,
        random_state=42,
        n_jobs=-1
    )
    rf.fit(X_scaled, y)
    r2_train = r2_score(y, rf.predict(X_scaled))
    print(f"  RF R² training (ottimistico per n piccolo): {r2_train:.4f}")

    explainer   = shap.TreeExplainer(rf)
    shap_values = explainer.shap_values(X_scaled)

    # Nota: shap.summary_plot() gestisce internamente la figura.
    # Non passiamo `ax` perché versioni vecchie di shap non lo accettano.
    # Usiamo plot_size per controllare le dimensioni, poi salviamo con
    # plt.savefig() prima che show=False chiuda la figura.
    shap.summary_plot(
        shap_values, X_scaled,
        feature_names=cols_disp,
        show=False,
        plot_size=(9, max(5, len(cols_disp) * 0.6 + 2))
    )
    # Aggiunge il titolo alla figura corrente creata da shap
    plt.title(
        f"SHAP Summary — {label} (ESPLORATIVO)\n"
        f"⚠ n~80: instabile. Modello principale = Ridge.",
        fontsize=10, fontweight="bold"
    )
    plt.tight_layout()
    plt.savefig(out_dir / "04_shap_summary.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  ✅ SHAP salvato: 04_shap_summary.png")


# =============================================================================
# SEZIONE 8 — CONFRONTO COMPARATIVO TRA LE TRE ATS
# =============================================================================

def confronto_ats(risultati: list[dict]) -> None:
    """
    Produce due grafici comparativi tra le tre ATS:

    Grafico 1 — R² LOGO-CV per ATS:
        Confronta la performance predittiva del modello Ridge nelle
        tre ATS. Un R² più alto in una ATS suggerisce che i fattori
        ambientali spiegano meglio l'ILI in quel contesto geografico.
        ATTENZIONE: con 4 fold la varianza è alta — non sovra-interpretare
        le differenze di R² tra ATS.

    Grafico 2 — Feature importance Ridge per ATS (heatmap):
        Mappa il coefficiente Ridge medio di ogni variabile per ogni ATS.
        Permette di vedere se il peso relativo dei fattori ambientali
        varia tra contesti geografici diversi (es. PM2.5 più importante
        in pianura padana che in montagna — coerente con la letteratura).
        Questo è il grafico più interessante per la discussion del report.
    """
    if not risultati:
        return

    print("\n" + "─" * 55)
    print("📊 CONFRONTO COMPARATIVO TRA LE TRE ATS")
    print("─" * 55)

    # ── Grafico 1: R² per ATS ─────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(8, 5))
    labels   = [r["label"] for r in risultati]
    r2_medi  = [r["r2_medio"] for r in risultati]
    r2_stds  = [r["r2_std"]   for r in risultati]
    colori   = ["#2980b9", "#27ae60", "#c0392b"]

    bars = ax.bar(labels, r2_medi, yerr=r2_stds, color=colori,
                  alpha=0.8, capsize=6, edgecolor="white", linewidth=1.2)
    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
    ax.set_ylabel("R² medio LOGO-CV (±SD tra fold)", fontsize=11)
    ax.set_title(
        "Performance predittiva Ridge — confronto tra ATS\n"
        "⚠ 4 fold per ATS: alta varianza, interpretare con cautela",
        fontsize=11, fontweight="bold"
    )
    for bar, r2, std in zip(bars, r2_medi, r2_stds):
        ax.text(bar.get_x() + bar.get_width()/2,
                bar.get_height() + std + 0.01,
                f"{r2:.3f}", ha="center", va="bottom", fontsize=10)
    ax.set_ylim(bottom=min(0, min(r2_medi) - 0.1))
    ax.grid(True, alpha=0.3, axis="y")
    plt.tight_layout()
    fig.savefig(OUT_BASE / "confronto_r2_cv.png", dpi=150)
    plt.close(fig)
    print(f"  ✅ Salvato: confronto_r2_cv.png")

    # ── Grafico 2: heatmap feature importance ─────────────────────────────
    # Costruisce una matrice: righe = variabili, colonne = ATS
    variabili_all = set()
    for r in risultati:
        variabili_all.update(r["df_ridge"]["Variabile"].tolist())
    variabili_all = sorted(variabili_all)

    mat = pd.DataFrame(index=variabili_all, columns=labels, dtype=float)
    for r in risultati:
        for _, row in r["df_ridge"].iterrows():
            mat.loc[row["Variabile"], r["label"]] = row["Coef_medio"]

    fig, ax = plt.subplots(figsize=(max(8, len(labels)*2.5),
                                    max(5, len(variabili_all)*0.6)))
    sns.heatmap(mat.astype(float), annot=True, fmt=".3f",
                cmap="coolwarm", center=0, ax=ax,
                linewidths=0.5, cbar_kws={"label": "Coeff. Ridge std."})
    ax.set_title(
        "Feature importance Ridge per ATS\n"
        "Rosso = associazione positiva con %ILI | Blu = negativa",
        fontsize=11, fontweight="bold"
    )
    plt.tight_layout()
    fig.savefig(OUT_BASE / "confronto_ridge_importanza.png", dpi=150)
    plt.close(fig)
    print(f"  ✅ Salvato: confronto_ridge_importanza.png")

    # ── CSV riepilogativo ─────────────────────────────────────────────────
    rows = []
    for r in risultati:
        rows.append({
            "ATS":       r["label"],
            "R2_medio":  round(r["r2_medio"], 4),
            "R2_std":    round(r["r2_std"], 4),
            "N_stagioni": len(r["df_cv"]),
        })
    pd.DataFrame(rows).to_csv(OUT_BASE / "riepilogo_comparativo_ats.csv", index=False)
    print(f"  ✅ Salvato: riepilogo_comparativo_ats.csv")


# =============================================================================
# SEZIONE 9 — MAIN
# =============================================================================

def main() -> None:
    print("\n" + "=" * 65)
    print("SCRIPT 7 — MODELLO MULTIVARIATO AMBIENTE vs ILI")
    print("ATS Milano | ATS Bergamo | ATS Montagna")
    print("=" * 65)

    # Carica i lag ottimali una volta sola (condivisi tra ATS)
    print("\n📂 Caricamento lag ottimali (Script 4)...")
    lag_map = carica_lag_ottimali()

    risultati_ridge = []  # raccoglie i risultati per il confronto finale

    # ── Pipeline per ogni ATS ────────────────────────────────────────────────
    for ats_key, cfg in ATS_CONFIG.items():

        print(f"\n\n{'='*65}")
        print(f"  ELABORAZIONE: {ats_key}  ({cfg['label']})")
        print(f"{'='*65}")

        # Crea cartella output per questa ATS
        out_dir = OUT_BASE / ats_key
        out_dir.mkdir(parents=True, exist_ok=True)
        plot_dir = out_dir / "grafici"
        plot_dir.mkdir(exist_ok=True)

        # ── Costruisci dataset ──────────────────────────────────────────
        df, cols_pred = costruisci_dataset_ats(ats_key, cfg, lag_map)

        if df.empty or len(df) < 15:
            print(f"  ❌ Dataset troppo piccolo ({len(df)} righe) — ATS saltata.")
            continue

        # Salva dataset per riproducibilità
        df.to_csv(out_dir / "dataset_unificato.csv", index=False)

        # ── Livello 1: analisi esplorativa ─────────────────────────────
        print(f"\n  --- LIVELLO 1: Correlazione + OR ---")
        analisi_correlazione(df, cols_pred, plot_dir, cfg["label"])
        df_or = calcola_odds_ratio(df, cols_pred, plot_dir, cfg["label"])
        print(df_or[["Variabile","OR","IC95_low","IC95_high","Significativo"]]
              .to_string(index=False))

        # ── Livello 2A: OLS ────────────────────────────────────────────
        print(f"\n  --- LIVELLO 2A: OLS multiplo ---")
        modello_ols(df, cols_pred, plot_dir, cfg["label"])

        # ── Livello 2B: Ridge + LOGO-CV ────────────────────────────────
        print(f"\n  --- LIVELLO 2B: Ridge + LOGO-CV ---")
        res = modello_ridge_cv(df, cols_pred, plot_dir, cfg["label"])
        risultati_ridge.append(res)

        # Copia i CSV nella cartella ATS (non solo in grafici/)
        for csv_file in plot_dir.parent.glob("*.csv"):
            pass  # già salvati in out_dir direttamente

        # ── Livello 3: RF + SHAP ───────────────────────────────────────
        print(f"\n  --- LIVELLO 3: Random Forest + SHAP (esplorativo) ---")
        modello_rf_shap(df, cols_pred, plot_dir, cfg["label"])

        print(f"\n  ✅ {ats_key} completata → {out_dir}")

    # ── Confronto finale tra ATS ─────────────────────────────────────────────
    if len(risultati_ridge) > 1:
        confronto_ats(risultati_ridge)

    # ── Riepilogo output ─────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("✅ SCRIPT 7 COMPLETATO")
    print(f"\n📂 Output in: {OUT_BASE}")
    print("\nStruttura file prodotti:")
    for f in sorted(OUT_BASE.rglob("*")):
        if f.is_file():
            print(f"  └─ {f.relative_to(OUT_BASE)}")

    print("\n" + "─" * 65)
    print("⚠ BIAS DA DICHIARARE NEL REPORT:")
    print("  1. Bias ecologico: dati aggregati ATS, non individuali")
    print("  2. n~80 per ATS: risultati esplorativi, non conclusivi")
    print("  3. Autocorrelazione residui OLS → mitigata con ILI_lag1")
    print("  4. Dati ARPA condivisi tra ATS (non stratificati per territorio)")
    print("  5. Lag uniformi tra ATS (stimati su ATS Milano da Script 4)")
    print("  6. 4 fold LOGO-CV: varianza della stima R² elevata")
    print("  7. RF + SHAP solo esplorativo (n piccolo → overfitting)")
    print("─" * 65)


if __name__ == "__main__":
    main()