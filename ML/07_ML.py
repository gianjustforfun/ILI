"""
=============================================================
SCRIPT 7 — MODELLO MULTIVARIATO: FATTORI AMBIENTALI vs ILI
=============================================================

RESEARCH QUESTION:
    Quali fattori ambientali (temperatura, umidità, PM2.5, PM10, NO2)
    hanno un'associazione indipendente con gli accessi al Pronto Soccorso
    per ILI ad ATS Milano, e qual è il loro peso relativo?

FRAMEWORK IPCC (riferimento al progetto):
    Questo script si colloca nel componente HAZARD del framework IPCC:
    stima l'associazione tra stressor ambientali (hazard) e outcome
    sanitario (rischio osservato = accessi PS per ILI normalizzati).

CONTESTO METODOLOGICO:
    Con ~80 osservazioni, i modelli statistici classici (OLS, Ridge)
    sono preferibili ai modelli ML complessi, che soffrirebbero di
    overfitting quasi garantito.

    Per questo motivo lo script segue una gerarchia metodologica esplicita:

    LIVELLO 1 — Analisi esplorativa (sempre appropriata):
        - Matrice di correlazione dei predittori (verifica multicollinearità)
        - Odds Ratio (OR) con soglie di esposizione (feature selection)

    LIVELLO 2 — Modello principale (appropriato per n~80):
        - OLS multiplo (statsmodels): coefficienti, p-value, IC 95%, R²
        - Ridge Regression (sklearn): gestisce la multicollinearità

    LIVELLO 3 — Modello esplorativo (usare con cautela per n~80):
        - Random Forest + SHAP values: cattura non-linearità, ma
          con n piccolo i risultati sono instabili → solo esplorativo

    CROSS-VALIDATION TEMPORALE (obbligatoria per serie temporali):
        Il shuffling è vietato per serie temporali. Si usa LeaveOneGroupOut
        con le stagioni come gruppi: addestra su 3 stagioni, testa sulla 4ª,
        ripete 4 volte. Simula la previsione reale su stagioni mai viste.

DATASET COSTRUITO DA QUESTO SCRIPT:
    Il merge dei CSV prodotti dagli script precedenti crea un dataset
    con una riga per settimana per stagione:

        STAGIONE | WEEK | ILI_norm | TEMP_lag | HUM_lag | PM25_lag | ...

    Il "lag" è quello ottimale stimato dallo Script 4: le variabili
    ambientali della settimana t vengono usate per predire l'ILI
    della settimana t + lag_ottimale. Questo rispetta la temporalità
    causa-effetto e usa i risultati di Script 4 come input.

BIAS E LIMITAZIONI (obbligatori da dichiarare nel report):
    1. Bias ecologico: dati aggregati a livello ATS, non individuale.
       Le associazioni stimate non implicano causalità individuale.
    2. n piccolo (~80 osservazioni, 4 stagioni): le stime sono instabili.
       Con LeaveOneGroupOut si hanno solo 4 fold → alta varianza della
       stima di performance. I risultati sono ESPLORATIVI, non conclusivi.
    3. Autocorrelazione temporale: settimane consecutive sono correlate.
       OLS assume residui indipendenti — questa assunzione è violata.
       Mitigazione parziale: aggiunta di ILI_{t-1} come predittore
       (modello autoregressivo). Dichiarare il limite nel report.
    4. Multicollinearità: temperatura e PM2.5 sono correlate in inverno.
       Ridge corregge parzialmente; la matrice di correlazione lo documenta.
    5. Confounding non misurato: vaccinazioni, varianti virali, festività.

STRUTTURA FILE DI INPUT ATTESI:
    Prodotti dagli script precedenti (paths configurabili in SEZIONE 1):

    Script 1 output:
        SORVEGLIANZA ACCESSI PS/output/ATS_MILANO/ili_ats_milano_stagionale.csv
        SORVEGLIANZA ACCESSI PS/output/ATS_MILANO/access_tot_milano_stagionale.csv

    Script 3 output (medie settimanali ambientali):
        ARPA/SETTIMANE_DI_INTERESSE/TEMPERATURE/TEMPERATURE_<stagione>.csv
        ARPA/SETTIMANE_DI_INTERESSE/HUMIDITY/HUMIDITY_<stagione>.csv
        ARPA/SETTIMANE_DI_INTERESSE/PM25/PM25_<stagione>.csv
        ARPA/SETTIMANE_DI_INTERESSE/PM10/PM10_<stagione>.csv
        ARPA/SETTIMANE_DI_INTERESSE/NO2/NO2_<stagione>.csv

    Script 4 output (lag ottimali):
        CORRELATIONS/output/cross_correlazione_lag_ottimale.csv

OUTPUT PRODOTTI (in output/script7/):
    CSV:
        dataset_unificato.csv           ← dataset finale usato per i modelli
        ols_coefficienti.csv            ← coefficienti OLS con p-value e IC
        ridge_coefficienti.csv          ← coefficienti Ridge standardizzati
        or_feature_selection.csv        ← Odds Ratio per feature selection
        performance_cv.csv              ← R² per stagione (cross-validation)

    GRAFICI (output/script7/grafici/):
        01_correlazione_predittori.png  ← matrice correlazione (multicollinearità)
        02_ols_coefficienti.png         ← forest plot coefficienti OLS
        03_ridge_coefficienti.png       ← importanza feature Ridge
        04_shap_summary.png             ← SHAP summary plot (esplorativo)
        05_predicted_vs_actual.png      ← fit del modello Ridge (CV)
        06_or_feature_selection.png     ← Odds Ratio per soglia ILI elevato

REQUISITI:
    pip install pandas numpy matplotlib seaborn scikit-learn statsmodels shap scipy

COME ESEGUIRE:
    Posizionarsi nella cartella del progetto (es. ILI/) e lanciare:
        python CORRELATIONS/script7_modello_multivariato.py
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
from sklearn.model_selection import LeaveOneGroupOut, cross_val_score
from sklearn.metrics import r2_score

try:
    import shap
    SHAP_DISPONIBILE = True
except ImportError:
    SHAP_DISPONIBILE = False
    print("  ⚠ shap non installato — grafico SHAP saltato. "
          "  Installa con: pip install shap")

warnings.filterwarnings("ignore")
plt.style.use("seaborn-v0_8-whitegrid")

# =============================================================================
# SEZIONE 1 — CONFIGURAZIONE PERCORSI
# =============================================================================
# Modifica questi percorsi se la struttura delle cartelle è diversa.
# Tutti i percorsi sono relativi alla root del progetto (es. ILI/).

BASE_DIR     = Path(__file__).resolve().parent        # .../CORRELATIONS/
PROJECT_ROOT = BASE_DIR.parent                        # .../ILI/

# --- Input: dati ILI (Script 1) ---
ILI_FILE  = PROJECT_ROOT / "SORVEGLIANZA ACCESSI PS" / "output" / "ATS_MILANO" / \
            "ili_ats_milano_stagionale.csv"
TOT_FILE  = PROJECT_ROOT / "SORVEGLIANZA ACCESSI PS" / "output" / "ATS_MILANO" / \
            "access_tot_milano_stagionale.csv"

# --- Input: dati ambientali settimanali (Script 3) ---
ARPA_DIR  = PROJECT_ROOT / "ARPA" / "SETTIMANE_DI_INTERESSE"

# --- Input: lag ottimali (Script 4) ---
LAG_FILE  = BASE_DIR / "output" / "cross_correlazione_lag_ottimale.csv"

# --- Output ---
OUT_DIR   = BASE_DIR / "output" / "script7"
PLOT_DIR  = OUT_DIR / "grafici"
OUT_DIR.mkdir(parents=True, exist_ok=True)
PLOT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# SEZIONE 2 — CONFIGURAZIONE MODELLO
# =============================================================================

# Stagioni da includere nell'analisi
# Formato ARPA (anno_inizio_anno_fine) → formato ILI (stagione breve)
STAGIONI_MAP = {
    "2022_2023": "22-23",
    "2023_2024": "23-24",
    "2024_2025": "24-25",
    "2025_2026": "25-26",
}

# Variabili ambientali da includere come predittori
# Chiave = nome visualizzato; Valore = sottocartella ARPA
VARIABILI_ENV = {
    "TEMP":     "TEMPERATURE",
    "HUMIDITY": "HUMIDITY",
    "PM25":     "PM25",
    "PM10":     "PM10",
    "NO2":      "NO2",
}

# Variabili che hanno struttura _media/_min/_max → usare solo _media
TIPOLOGIE_CON_SOTTOSTAT = {"TEMPERATURE", "HUMIDITY"}

# Lag di default per ogni variabile (settimane).
# Viene sovrascritto dai risultati di Script 4 se il file esiste.
# Fonte letteratura: Shaman et al. 2010, Feng et al. 2016 → lag 1-3 sett.
LAG_DEFAULT = {
    "TEMP":     2,
    "HUMIDITY": 1,
    "PM25":     1,
    "PM10":     1,
    "NO2":      1,
}

# Soglia per variabile target binarizzata (OR/RR):
# settimane con ILI_norm > SOGLIA_ILI_ALTO sono "episodi ad alto ILI"
# Usa il 75° percentile calcolato sui dati (definito dinamicamente sotto)
SOGLIA_PERCENTILE_ILI = 75

# Parametri Ridge
ALPHAS_RIDGE = [0.01, 0.1, 1.0, 10.0, 100.0]

# Parametri Random Forest (volutamente conservativi per n piccolo)
RF_N_TREES  = 200
RF_MAX_DEPTH = 3   # profondità bassa: limita overfitting con n~80

# Parametro per la soglia di esposizione nelle variabili ambientali (OR)
SOGLIA_PERCENTILE_ENV = 75  # "esposto" = valore > 75° percentile

# =============================================================================
# SEZIONE 3 — FUNZIONI DI SUPPORTO
# =============================================================================

def ordine_settimana(s: int) -> int:
    """
    Mappa settimana ISO → posizione progressiva nella stagione influenzale.
    Sett. 48 → 1, sett. 52 → 5, sett. 1 → 6, sett. 15 → 20.
    Necessario per ordinare correttamente le serie temporali stagionali.
    """
    return s - 47 if s >= 48 else s + 5


def carica_serie_ambientale(nome_var: str, sottocartella: str) -> pd.DataFrame:
    """
    Legge tutti i CSV stagionali di una variabile ambientale (da Script 3)
    e restituisce una serie long con colonne: Stagione_ARPA | Settimana | Valore.

    Per TEMPERATURE e HUMIDITY usa solo le colonne '_media' per evitare
    di mischiare grandezze diverse (massimo, media, minimo).
    """
    cartella = ARPA_DIR / sottocartella
    if not cartella.exists():
        print(f"  ⚠ Cartella non trovata: {cartella}")
        return pd.DataFrame()

    dfs = []
    for f in sorted(cartella.glob("*.csv")):
        try:
            df = pd.read_csv(f)
            if "Settimana" not in df.columns:
                continue
            # Estrai stagione dal nome file (es. TEMPERATURE_2022_2023 → 2022_2023)
            parts = f.stem.split("_")
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

    # Selezione colonne dati in base alla tipologia
    if sottocartella.upper() in TIPOLOGIE_CON_SOTTOSTAT:
        cols_dati = [c for c in df_tot.columns
                     if c not in ("Settimana", "Stagione_ARPA")
                     and c.endswith("_media")]
    else:
        cols_dati = [c for c in df_tot.columns
                     if c not in ("Settimana", "Stagione_ARPA")]

    if not cols_dati:
        print(f"  ⚠ Nessuna colonna dati per {nome_var}")
        return pd.DataFrame()

    # Converti a numerico e sostituisci -999 (codice ARPA per dato mancante)
    for c in cols_dati:
        df_tot[c] = pd.to_numeric(df_tot[c], errors="coerce")
        df_tot.loc[df_tot[c] == -999, c] = np.nan

    # Media tra stazioni/sensori → un valore per settimana per stagione
    df_tot["Valore"] = df_tot[cols_dati].mean(axis=1, skipna=True)

    serie = (
        df_tot
        .groupby(["Stagione_ARPA", "Settimana"], as_index=False)["Valore"]
        .mean()
        .rename(columns={"Valore": nome_var})
    )

    return serie


def carica_lag_ottimali() -> dict[str, int]:
    """
    Legge i lag ottimali stimati da Script 4.
    Se il file non esiste, usa i valori di default da LAG_DEFAULT.

    I lag ottimali sono il risultato della cross-correlazione: indicano
    quante settimane dopo un picco ambientale si osserva un picco ILI.
    Usarli qui è metodologicamente corretto: sfrutta i risultati di
    Script 4 come input per il modello predittivo.
    """
    if not LAG_FILE.exists():
        print(f"  ⚠ File lag non trovato: {LAG_FILE}")
        print(f"     Uso lag di default: {LAG_DEFAULT}")
        return LAG_DEFAULT.copy()

    df = pd.read_csv(LAG_FILE)
    lag_map = {}

    # Il file di Script 4 usa nomi come "TEMPERATURE", "PM25", ecc.
    # Li mappiamo ai nomi abbreviati usati in questo script
    nome_map = {
        "TEMPERATURE": "TEMP",
        "HUMIDITY":    "HUMIDITY",
        "PM25":        "PM25",
        "PM10":        "PM10",
        "NO2":         "NO2",
    }

    for _, row in df.iterrows():
        var_arpa = str(row.get("Variabile", "")).upper()
        var_script = nome_map.get(var_arpa, var_arpa)
        lag_val = int(row.get("Lag_ottimale_sett", LAG_DEFAULT.get(var_script, 1)))
        lag_map[var_script] = lag_val

    # Completa con i default per variabili mancanti
    for var, default in LAG_DEFAULT.items():
        if var not in lag_map:
            lag_map[var] = default
            print(f"  ℹ Lag per {var} non trovato in Script 4 → uso default {default}")

    print(f"  ✅ Lag ottimali caricati: {lag_map}")
    return lag_map


def applica_lag(df: pd.DataFrame, col: str, lag: int) -> pd.DataFrame:
    """
    Applica un lag a una colonna ambientale all'interno di ogni stagione.

    Per ogni stagione:
        - La colonna ambientale viene shiftata di `lag` posizioni
          verso il basso (cioè il valore della settimana t viene
          associato all'ILI della settimana t + lag).
        - Le prime `lag` righe di ogni stagione diventano NaN
          (non ci sono dati ambientali sufficientemente in anticipo).

    Perché è importante:
        La cross-correlazione di Script 4 ha mostrato che l'effetto
        ambientale sull'ILI non è istantaneo ma ha un ritardo.
        Applicare il lag allinea correttamente causa (ambiente) ed
        effetto (ILI), riducendo il bias di stima dei coefficienti.

    NOTA: lo shift avviene PER STAGIONE, non globalmente. Senza questo
    accorgimento, il lag connetter ebbe le ultime settimane di una
    stagione con le prime della successiva, introducendo un errore
    sistematico.
    """
    if lag == 0:
        return df  # nessun lag: niente da fare

    col_lagged = f"{col}_lag{lag}"
    df = df.copy()
    df[col_lagged] = np.nan

    for stagione in df["STAGIONE"].unique():
        mask = df["STAGIONE"] == stagione
        # shift(lag) sposta la colonna verso il basso di `lag` posizioni
        # → la riga t contiene il valore ambientale di t-lag
        # → equivale a dire: l'ambiente di t-lag precede l'ILI di t
        df.loc[mask, col_lagged] = df.loc[mask, col].shift(lag).values

    # Rimuovi la colonna originale non laggata
    df = df.drop(columns=[col])
    return df


# =============================================================================
# SEZIONE 4 — COSTRUZIONE DEL DATASET UNIFICATO
# =============================================================================

def costruisci_dataset() -> tuple[pd.DataFrame, list[str]]:
    """
    Costruisce il dataset finale unificando dati ILI e ambientali.

    Passi:
        1. Carica ILI ATS Milano (da Script 1)
        2. Calcola ILI normalizzato = accessi ILI / accessi totali * 100
           (evita di usare conteggi assoluti che dipendono dal volume del PS)
        3. Carica ogni variabile ambientale (da Script 3)
        4. Fa il merge per STAGIONE + SETTIMANA
        5. Applica i lag ottimali (da Script 4)
        6. Rimuove le righe con NaN residui

    Returns:
        df_finale   : DataFrame con una riga per settimana per stagione
        cols_pred   : lista dei nomi delle colonne predittore (con lag)
    """
    print("\n" + "─" * 55)
    print("📂 COSTRUZIONE DATASET UNIFICATO")
    print("─" * 55)

    # ── 1. Carica ILI ────────────────────────────────────────────────────────
    if not ILI_FILE.exists():
        raise FileNotFoundError(
            f"File ILI non trovato: {ILI_FILE}\n"
            f"Esegui prima Script 1."
        )

    df_ili = pd.read_csv(ILI_FILE)
    print(f"  ✅ ILI caricato: {len(df_ili)} righe, "
          f"stagioni: {sorted(df_ili['STAGIONE'].unique())}")

    # ── 2. Calcola ILI normalizzato (%ILI = ILI / totale * 100) ──────────────
    # Usare %ILI invece dei conteggi assoluti è più robusto:
    # il volume assoluto degli accessi varia per ragioni non legate
    # all'influenza (apertura/chiusura PS, campagne, ecc.)
    if TOT_FILE.exists():
        df_tot = pd.read_csv(TOT_FILE)
        df_merged_ili = pd.merge(
            df_ili[["STAGIONE", "WEEK", "ORDINE", "ACCESSI_ILI_ATS_MILANO"]],
            df_tot[["STAGIONE", "WEEK", "ACCESSI_TOTALI_ER_MILANO"]],
            on=["STAGIONE", "WEEK"],
            how="left"
        )
        df_merged_ili["ILI_NORM"] = (
            df_merged_ili["ACCESSI_ILI_ATS_MILANO"].astype(float) /
            df_merged_ili["ACCESSI_TOTALI_ER_MILANO"].astype(float) * 100
        ).where(df_merged_ili["ACCESSI_TOTALI_ER_MILANO"] > 0)
        print(f"  ✅ ILI normalizzato calcolato (%ILI = ILI/totale*100)")
    else:
        # Fallback: usa i conteggi assoluti
        print(f"  ⚠ File totali non trovato → uso conteggi ILI assoluti")
        df_merged_ili = df_ili.copy()
        df_merged_ili["ILI_NORM"] = df_merged_ili["ACCESSI_ILI_ATS_MILANO"].astype(float)

    df_base = df_merged_ili[["STAGIONE", "WEEK", "ORDINE", "ILI_NORM"]].copy()
    df_base = df_base.rename(columns={"WEEK": "SETTIMANA"})
    df_base["ORDINE"] = df_base["SETTIMANA"].apply(ordine_settimana)

    # ── 3. Carica variabili ambientali e merge ────────────────────────────────
    for nome_var, sottocartella in VARIABILI_ENV.items():
        print(f"\n  📁 Caricamento {nome_var}...")
        serie = carica_serie_ambientale(nome_var, sottocartella)

        if serie.empty:
            print(f"     ⚠ Nessun dato per {nome_var} — variabile esclusa")
            continue

        # Mappa stagione ARPA → stagione ILI per il merge
        # Es. "2022_2023" → "22-23"
        serie["STAGIONE"] = serie["Stagione_ARPA"].map(STAGIONI_MAP)
        serie = serie.dropna(subset=["STAGIONE"])
        serie = serie.rename(columns={"Settimana": "SETTIMANA"})
        serie = serie[["STAGIONE", "SETTIMANA", nome_var]]

        n_prima = len(df_base)
        df_base = pd.merge(df_base, serie, on=["STAGIONE", "SETTIMANA"], how="left")
        n_match = df_base[nome_var].notna().sum()
        print(f"     ✅ {n_match}/{n_prima} settimane con dati {nome_var}")

    # ── 4. Applica lag ottimali ───────────────────────────────────────────────
    print(f"\n  ⏱ Applicazione lag ottimali...")
    lag_map = carica_lag_ottimali()

    df_base = df_base.sort_values(["STAGIONE", "ORDINE"]).reset_index(drop=True)

    cols_pred_lagged = []
    for nome_var in VARIABILI_ENV:
        if nome_var not in df_base.columns:
            continue
        lag = lag_map.get(nome_var, 0)
        df_base = applica_lag(df_base, nome_var, lag)
        col_lagged = f"{nome_var}_lag{lag}" if lag > 0 else nome_var
        cols_pred_lagged.append(col_lagged)
        print(f"     {nome_var}: lag = {lag} settimane → colonna '{col_lagged}'")

    # ── 5. Aggiungi ILI della settimana precedente (termine autoregressivo) ───
    # Questo termine cattura l'autocorrelazione temporale: una settimana
    # ad alto ILI tende ad essere preceduta da un'altra ad alto ILI.
    # Includerlo come predittore mitiga la violazione dell'assunzione di
    # indipendenza dei residui in OLS (dichiarare comunque il limite).
    df_base = df_base.sort_values(["STAGIONE", "ORDINE"]).reset_index(drop=True)
    df_base["ILI_NORM_lag1"] = np.nan
    for stagione in df_base["STAGIONE"].unique():
        mask = df_base["STAGIONE"] == stagione
        df_base.loc[mask, "ILI_NORM_lag1"] = df_base.loc[mask, "ILI_NORM"].shift(1).values

    cols_pred_lagged.append("ILI_NORM_lag1")

    # ── 6. Rimuovi righe con NaN (dovute al lag e alle prime settimane) ───────
    n_prima_drop = len(df_base)
    df_finale = df_base.dropna(subset=["ILI_NORM"] + cols_pred_lagged).copy()
    n_dopo_drop = len(df_finale)
    print(f"\n  ℹ Righe dopo rimozione NaN: {n_dopo_drop}/{n_prima_drop} "
          f"({n_prima_drop - n_dopo_drop} rimosse per lag/NaN ambientali)")

    # Salva dataset per riproducibilità
    path_ds = OUT_DIR / "dataset_unificato.csv"
    df_finale.to_csv(path_ds, index=False)
    print(f"  💾 Dataset salvato: {path_ds}")
    print(f"  📊 Stagioni presenti: {sorted(df_finale['STAGIONE'].unique())}")
    print(f"  📊 Predittori: {cols_pred_lagged}")
    print(f"  📊 Osservazioni totali: {len(df_finale)}")

    return df_finale, cols_pred_lagged


# =============================================================================
# SEZIONE 5 — ANALISI ESPLORATIVA: CORRELAZIONE E ODDS RATIO
# =============================================================================

def analisi_correlazione(df: pd.DataFrame, cols_pred: list[str]) -> None:
    """
    Produce la matrice di correlazione di Pearson tra tutti i predittori.

    PERCHÉ è importante prima del modello:
        La multicollinearità (predittori molto correlati tra loro) distorce
        i coefficienti OLS rendendoli instabili. Questo grafico la documenta
        e giustifica la scelta di Ridge (che la gestisce) rispetto a OLS puro.

    Il grafico mostra:
        - Correlazione tra predittori ambientali (heatmap colorata)
        - Valori numerici nelle celle per leggibilità
        - Asterischi per correlazioni statisticamente significative (p<0.05)
    """
    print("\n  📊 Matrice di correlazione predittori...")

    # Usa solo le colonne disponibili nel dataset
    cols_disp = [c for c in cols_pred if c in df.columns]
    df_corr = df[cols_disp].copy()

    corr_matrix = df_corr.corr(method="pearson")

    # Calcola p-value per ogni coppia (per aggiungere asterischi)
    n = len(df_corr)
    pval_matrix = pd.DataFrame(np.ones_like(corr_matrix),
                                index=corr_matrix.index,
                                columns=corr_matrix.columns)
    for c1 in cols_disp:
        for c2 in cols_disp:
            if c1 != c2:
                valid = df_corr[[c1, c2]].dropna()
                if len(valid) >= 5:
                    _, p = pearsonr(valid[c1], valid[c2])
                    pval_matrix.loc[c1, c2] = p

    # Annota con asterisco se p < 0.05
    annot = corr_matrix.round(2).astype(str)
    for c1 in cols_disp:
        for c2 in cols_disp:
            if pval_matrix.loc[c1, c2] < 0.05 and c1 != c2:
                annot.loc[c1, c2] = annot.loc[c1, c2] + "*"

    fig, ax = plt.subplots(figsize=(9, 7))
    sns.heatmap(corr_matrix, annot=annot, fmt="", cmap="coolwarm",
                vmin=-1, vmax=1, center=0, square=True, ax=ax,
                linewidths=0.5, cbar_kws={"shrink": 0.8})

    ax.set_title(
        "Correlazione di Pearson tra predittori\n"
        "* = p < 0.05 | Alta correlazione tra variabili → giustifica Ridge",
        fontsize=11, fontweight="bold"
    )
    plt.tight_layout()
    fig.savefig(PLOT_DIR / "01_correlazione_predittori.png", dpi=150)
    plt.close(fig)
    print(f"  ✅ Salvato: 01_correlazione_predittori.png")

    # Segnala coppie ad alta correlazione (|r| > 0.7)
    alta_corr = []
    for i, c1 in enumerate(cols_disp):
        for c2 in cols_disp[i+1:]:
            r = corr_matrix.loc[c1, c2]
            if abs(r) > 0.7:
                alta_corr.append((c1, c2, r))
    if alta_corr:
        print("  ⚠ Coppie ad alta correlazione (|r|>0.7) — multicollinearità rilevante:")
        for c1, c2, r in alta_corr:
            print(f"     {c1} vs {c2}: r = {r:.3f}")
    else:
        print("  ✅ Nessuna coppia con |r| > 0.7")


def calcola_odds_ratio(df: pd.DataFrame, cols_pred: list[str],
                        target_col: str = "ILI_NORM") -> pd.DataFrame:
    """
    Calcola l'Odds Ratio (OR) per ogni variabile ambientale rispetto
    all'outcome binario "ILI elevato" (> 75° percentile).

    Dal §7.4.1 del corso: OR è uno strumento di feature selection e può
    essere usato come output standalone in studi epidemiologici descrittivi.

    Come funziona:
        - Binarizza il target: ILI_alto = 1 se ILI_NORM > soglia, 0 altrimenti
        - Binarizza ogni predittore: esposto = 1 se valore > 75° percentile
        - Costruisce la tabella di contingenza 2×2
        - Calcola OR = (A*D) / (B*C) con IC 95% via log-normale

    Interpretazione:
        OR > 1: il predittore alto è associato a ILI alto
        OR < 1: il predittore alto è associato a ILI basso (protettivo)
        IC 95% include 1: evidenza inconcludente (dal corso §7.4.1)

    LIMITAZIONE: OR è appropriato per studi caso-controllo (retrospettivi).
    Il vostro dataset è una serie temporale, non un caso-controllo.
    Usare l'OR qui è un'approssimazione per la feature selection, non
    un'analisi epidemiologica formale. Dichiararlo nel report.
    """
    print("\n  📊 Calcolo Odds Ratio (feature selection)...")

    soglia_ili = np.percentile(df[target_col].dropna(), SOGLIA_PERCENTILE_ILI)
    df = df.copy()
    df["ILI_alto"] = (df[target_col] > soglia_ili).astype(int)

    risultati = []
    cols_env = [c for c in cols_pred if c != "ILI_NORM_lag1" and c in df.columns]

    for col in cols_env:
        soglia_env = np.percentile(df[col].dropna(), SOGLIA_PERCENTILE_ENV)
        df["esposto"] = (df[col] > soglia_env).astype(int)

        # Tabella di contingenza 2×2
        A = len(df[(df["esposto"] == 1) & (df["ILI_alto"] == 1)])  # esposto + outcome
        B = len(df[(df["esposto"] == 1) & (df["ILI_alto"] == 0)])  # esposto + no outcome
        C = len(df[(df["esposto"] == 0) & (df["ILI_alto"] == 1)])  # non esposto + outcome
        D = len(df[(df["esposto"] == 0) & (df["ILI_alto"] == 0)])  # non esposto + no outcome

        # OR con correzione di continuità (aggiunge 0.5 se una cella è 0)
        A_, B_, C_, D_ = A + 0.5, B + 0.5, C + 0.5, D + 0.5
        or_val = (A_ * D_) / (B_ * C_)

        # IC 95% via approssimazione log-normale
        se_log_or = np.sqrt(1/A_ + 1/B_ + 1/C_ + 1/D_)
        ci_low  = np.exp(np.log(or_val) - 1.96 * se_log_or)
        ci_high = np.exp(np.log(or_val) + 1.96 * se_log_or)

        risultati.append({
            "Variabile":    col,
            "Soglia_env":   round(soglia_env, 2),
            "A_esp_out":    A,
            "B_esp_no":     B,
            "C_no_out":     C,
            "D_no_no":      D,
            "OR":           round(or_val, 3),
            "IC95_low":     round(ci_low, 3),
            "IC95_high":    round(ci_high, 3),
            "Significativo": "Sì" if ci_low > 1 or ci_high < 1 else "No",
        })

    df_or = pd.DataFrame(risultati)
    df_or.to_csv(OUT_DIR / "or_feature_selection.csv", index=False)
    print(f"  ✅ OR salvati: or_feature_selection.csv")

    # Grafico forest plot OR
    fig, ax = plt.subplots(figsize=(8, max(4, len(df_or) * 0.7)))
    y_pos = range(len(df_or))

    for i, row in df_or.iterrows():
        colore = "#c0392b" if row["Significativo"] == "Sì" else "#7f8c8d"
        ax.errorbar(
            x=row["OR"], y=i,
            xerr=[[row["OR"] - row["IC95_low"]],
                  [row["IC95_high"] - row["OR"]]],
            fmt="o", color=colore, capsize=4, markersize=7, linewidth=1.5
        )

    ax.axvline(1.0, color="black", linestyle="--", linewidth=1, alpha=0.7,
               label="OR = 1 (nessuna associazione)")
    ax.set_yticks(list(y_pos))
    ax.set_yticklabels(df_or["Variabile"].tolist())
    ax.set_xlabel("Odds Ratio (IC 95%)", fontsize=10)
    ax.set_title(
        f"Odds Ratio: predittore > p75 → ILI > p{SOGLIA_PERCENTILE_ILI}\n"
        f"Rosso = IC 95% non include 1 (significativo)",
        fontsize=10, fontweight="bold"
    )
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(PLOT_DIR / "06_or_feature_selection.png", dpi=150)
    plt.close(fig)
    print(f"  ✅ Salvato: 06_or_feature_selection.png")

    return df_or


# =============================================================================
# SEZIONE 6 — MODELLO OLS (LIVELLO 2: MODELLO PRINCIPALE)
# =============================================================================

def modello_ols(df: pd.DataFrame, cols_pred: list[str],
                target_col: str = "ILI_NORM") -> None:
    """
    Regressione lineare multipla con statsmodels (OLS).

    PERCHÉ statsmodels invece di sklearn:
        statsmodels restituisce p-value, intervalli di confidenza al 95%,
        R², F-statistic — tutto quello che serve per una tabella scientifica
        in un report epidemiologico. sklearn è ottimizzato per la
        performance predittiva, non per l'inferenza statistica.

    OUTPUT:
        - Summary completo stampato a console
        - Tabella CSV con coefficienti, p-value, IC 95%
        - Forest plot dei coefficienti standardizzati

    INTERPRETAZIONE COEFFICIENTI:
        Un coefficiente β per la variabile X significa:
        "A parità di tutte le altre variabili, un aumento di 1 unità
        in X è associato a una variazione di β nel %ILI."

        Coefficienti standardizzati (dividendo per SD):
        "Un aumento di 1 deviazione standard in X è associato a
        una variazione di β_std deviazioni standard nel %ILI."
        → consentono il confronto diretto tra variabili su scale diverse.

    LIMITAZIONE ESPLICITA:
        Con n~80 e 4 stagioni, la significatività statistica (p-value)
        è instabile. Un p < 0.05 con n piccolo e dati autocorrelati
        NON è prova di associazione robusta. Interpretare con cautela.
    """
    print("\n" + "─" * 55)
    print("📐 MODELLO OLS (statsmodels)")
    print("─" * 55)

    cols_disp = [c for c in cols_pred if c in df.columns]
    df_clean = df[[target_col] + cols_disp].dropna().copy()

    print(f"  Osservazioni usate: {len(df_clean)}")
    print(f"  Predittori: {cols_disp}")

    X = df_clean[cols_disp]
    y = df_clean[target_col]

    # Standardizza i predittori per ottenere coefficienti confrontabili
    scaler = StandardScaler()
    X_scaled = pd.DataFrame(
        scaler.fit_transform(X),
        columns=cols_disp,
        index=X.index
    )

    X_sm = sm.add_constant(X_scaled)
    modello = sm.OLS(y, X_sm).fit()

    print("\n" + modello.summary().as_text())

    # Salva tabella coefficienti
    df_coef = pd.DataFrame({
        "Variabile":    modello.params.index,
        "Coefficiente": modello.params.values,
        "Std_err":      modello.bse.values,
        "t_stat":       modello.tvalues.values,
        "p_value":      modello.pvalues.values,
        "IC95_low":     modello.conf_int()[0].values,
        "IC95_high":    modello.conf_int()[1].values,
    })
    df_coef = df_coef[df_coef["Variabile"] != "const"]
    df_coef.to_csv(OUT_DIR / "ols_coefficienti.csv", index=False)
    print(f"\n  💾 Coefficienti OLS salvati: ols_coefficienti.csv")

    # Forest plot coefficienti OLS
    fig, ax = plt.subplots(figsize=(8, max(4, len(df_coef) * 0.7)))
    y_pos = range(len(df_coef))

    for i, row in df_coef.iterrows():
        sig = row["p_value"] < 0.05
        colore = "#2980b9" if sig else "#95a5a6"
        ax.errorbar(
            x=row["Coefficiente"], y=list(y_pos)[list(df_coef.index).index(i)],
            xerr=[[row["Coefficiente"] - row["IC95_low"]],
                  [row["IC95_high"] - row["Coefficiente"]]],
            fmt="o", color=colore, capsize=4, markersize=7, linewidth=1.5
        )

    ax.axvline(0, color="black", linestyle="--", linewidth=1, alpha=0.7)
    ax.set_yticks(list(y_pos))
    ax.set_yticklabels(df_coef["Variabile"].tolist())
    ax.set_xlabel("Coefficiente OLS standardizzato (IC 95%)", fontsize=10)
    ax.set_title(
        f"OLS multiplo — effetto netto su %ILI (R²={modello.rsquared:.3f})\n"
        f"Blu = p<0.05 | Grigio = p≥0.05 | n={len(df_clean)}",
        fontsize=10, fontweight="bold"
    )
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(PLOT_DIR / "02_ols_coefficienti.png", dpi=150)
    plt.close(fig)
    print(f"  ✅ Salvato: 02_ols_coefficienti.png")

    return modello


# =============================================================================
# SEZIONE 7 — RIDGE REGRESSION CON CROSS-VALIDATION TEMPORALE
# =============================================================================

def modello_ridge_cv(df: pd.DataFrame, cols_pred: list[str],
                     target_col: str = "ILI_NORM") -> None:
    """
    Ridge Regression con LeaveOneGroupOut cross-validation per serie temporali.

    PERCHÉ Ridge invece di OLS puro:
        Ridge aggiunge una penalità L2 ai coefficienti, riducendoli verso zero.
        Questo gestisce la multicollinearità (variabili ambientali correlate)
        senza escludere manualmente variabili. Dal corso §7.3.1:
        "For any regression problem with a high-dimensional feature set,
        regularised variants should be preferred over standard linear regression."

    PERCHÉ LeaveOneGroupOut:
        Dal corso §7.5.2: "Shuffling must not be applied to time series."
        Con LeaveOneGroupOut, ogni fold usa 3 stagioni per addestrare e
        1 stagione per testare. Questo rispetta l'ordine temporale e simula
        la previsione reale: il modello non "vede" la stagione di test.

        Con 4 stagioni → 4 fold. La varianza dell'R² sarà alta (normale
        con pochi fold): dichiararlo nel report.

    OUTPUT:
        - R² per ogni fold (stagione test)
        - Coefficienti medi standardizzati (feature importance)
        - Grafico predicted vs actual
        - Grafico importanza feature Ridge
    """
    print("\n" + "─" * 55)
    print("📐 RIDGE REGRESSION + LeaveOneGroupOut CV")
    print("─" * 55)

    cols_disp = [c for c in cols_pred if c in df.columns]
    df_clean = df[["STAGIONE", target_col] + cols_disp].dropna().copy()

    X = df_clean[cols_disp].values
    y = df_clean[target_col].values
    gruppi = df_clean["STAGIONE"].values  # una stagione = un gruppo

    stagioni_uniche = df_clean["STAGIONE"].unique()
    print(f"  Osservazioni: {len(df_clean)} | Stagioni: {list(stagioni_uniche)}")
    print(f"  Predittori: {cols_disp}")

    # Standardizza i predittori
    scaler = StandardScaler()

    # ── Cross-validation temporale ────────────────────────────────────────────
    logo = LeaveOneGroupOut()
    r2_per_fold = []
    coef_per_fold = []
    y_pred_tutti = np.full(len(y), np.nan)
    y_true_tutti = y.copy()

    for fold_idx, (train_idx, test_idx) in enumerate(logo.split(X, y, gruppi)):
        stagione_test = gruppi[test_idx][0]

        X_train = X[train_idx]
        X_test  = X[test_idx]
        y_train = y[train_idx]
        y_test  = y[test_idx]

        # Standardizza sul train, applica al test (evita data leakage)
        scaler_fold = StandardScaler()
        X_train_s = scaler_fold.fit_transform(X_train)
        X_test_s  = scaler_fold.transform(X_test)

        # RidgeCV sceglie automaticamente il miglior alpha
        ridge = RidgeCV(alphas=ALPHAS_RIDGE)
        ridge.fit(X_train_s, y_train)

        y_pred = ridge.predict(X_test_s)
        y_pred_tutti[test_idx] = y_pred

        r2 = r2_score(y_test, y_pred)
        r2_per_fold.append({"Stagione_test": stagione_test, "R2": round(r2, 4),
                             "Alpha_scelto": ridge.alpha_})
        coef_per_fold.append(ridge.coef_)

        print(f"  Fold {fold_idx+1}: test={stagione_test}, "
              f"R²={r2:.4f}, alpha={ridge.alpha_}")

    # Coefficienti medi tra fold (feature importance media)
    coef_medio = np.mean(coef_per_fold, axis=0)
    coef_std   = np.std(coef_per_fold, axis=0)

    df_ridge_coef = pd.DataFrame({
        "Variabile":    cols_disp,
        "Coef_medio":   coef_medio.round(4),
        "Coef_std":     coef_std.round(4),
        "Importanza":   np.abs(coef_medio).round(4),
    }).sort_values("Importanza", ascending=False)

    df_ridge_coef.to_csv(OUT_DIR / "ridge_coefficienti.csv", index=False)

    df_cv = pd.DataFrame(r2_per_fold)
    df_cv.to_csv(OUT_DIR / "performance_cv.csv", index=False)

    print(f"\n  R² medio: {df_cv['R2'].mean():.4f} ± {df_cv['R2'].std():.4f}")
    print(f"  ⚠ Con 4 fold, la stima è instabile — risultati esplorativi")

    # ── Grafico 1: importanza feature Ridge ───────────────────────────────────
    fig, ax = plt.subplots(figsize=(8, max(4, len(df_ridge_coef) * 0.7)))
    colori = ["#c0392b" if c > 0 else "#2980b9" for c in df_ridge_coef["Coef_medio"]]

    ax.barh(df_ridge_coef["Variabile"], df_ridge_coef["Coef_medio"],
            xerr=df_ridge_coef["Coef_std"], color=colori, alpha=0.8,
            capsize=4, edgecolor="white")
    ax.axvline(0, color="black", linewidth=0.8, linestyle="--")
    ax.set_xlabel("Coefficiente Ridge standardizzato medio (±SD tra fold)", fontsize=10)
    ax.set_title(
        "Feature importance — Ridge Regression\n"
        "Rosso = associazione positiva con ILI | Blu = associazione negativa\n"
        "Barre = variabilità tra stagioni",
        fontsize=10, fontweight="bold"
    )
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(PLOT_DIR / "03_ridge_coefficienti.png", dpi=150)
    plt.close(fig)

    # ── Grafico 2: predicted vs actual ────────────────────────────────────────
    mask_valid = ~np.isnan(y_pred_tutti)

    fig, ax = plt.subplots(figsize=(7, 6))
    scatter = ax.scatter(
        y_true_tutti[mask_valid], y_pred_tutti[mask_valid],
        c=pd.factorize(gruppi[mask_valid])[0],
        cmap="tab10", alpha=0.7, s=50, edgecolors="white", linewidth=0.5
    )

    # Linea di riferimento perfetta (predicted = actual)
    lim_min = min(y_true_tutti[mask_valid].min(), y_pred_tutti[mask_valid].min()) * 0.9
    lim_max = max(y_true_tutti[mask_valid].max(), y_pred_tutti[mask_valid].max()) * 1.1
    ax.plot([lim_min, lim_max], [lim_min, lim_max], "k--", lw=1.5,
            label="Perfetto (pred = actual)")

    r2_globale = r2_score(y_true_tutti[mask_valid], y_pred_tutti[mask_valid])
    ax.set_xlabel("ILI reale (%ILI)", fontsize=11)
    ax.set_ylabel("ILI predetto (%ILI)", fontsize=11)
    ax.set_title(
        f"Predicted vs Actual — Ridge + LOGO-CV\n"
        f"R² globale = {r2_globale:.3f} | Colori = stagioni diverse",
        fontsize=10, fontweight="bold"
    )
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)

    # Legenda stagioni
    stagioni_list = sorted(df_clean["STAGIONE"].unique())
    patches = [mpatches.Patch(color=plt.cm.tab10(i / 9), label=s)
               for i, s in enumerate(stagioni_list)]
    ax.legend(handles=patches, title="Stagione", fontsize=8, loc="upper left")

    plt.tight_layout()
    fig.savefig(PLOT_DIR / "05_predicted_vs_actual.png", dpi=150)
    plt.close(fig)

    print(f"  ✅ Salvati: 03_ridge_coefficienti.png, 05_predicted_vs_actual.png")
    print(f"  💾 Salvati: ridge_coefficienti.csv, performance_cv.csv")


# =============================================================================
# SEZIONE 8 — RANDOM FOREST + SHAP (LIVELLO 3: ESPLORATIVO)
# =============================================================================

def modello_random_forest_shap(df: pd.DataFrame, cols_pred: list[str],
                                target_col: str = "ILI_NORM") -> None:
    """
    Random Forest con SHAP values per l'interpretabilità.

    CONTESTO (Capitolo 7 del corso, §7.7):
        SHAP (SHapley Additive exPlanations) è il metodo XAI più adottato.
        Ogni SHAP value rappresenta il contributo marginale medio di una
        feature alla previsione, rispetto alla baseline (media del dataset).

        Il SHAP summary plot mostra:
        - Asse Y: feature ordinate per importanza (SHAP medio assoluto)
        - Asse X: valore SHAP per ogni osservazione
        - Colore: valore della feature (rosso = alto, blu = basso)

        Un cluster di punti rossi a destra (SHAP > 0) significa:
        "Valori alti di questa feature aumentano la previsione di ILI."

    ⚠ DISCLAIMER IMPORTANTE (da includere nel report):
        Con n~80 osservazioni, Random Forest è al limite dell'applicabilità.
        max_depth=3 limita la complessità ma non elimina il rischio di
        overfitting. I risultati SHAP sono qui usati solo per esplorare
        pattern non-lineari, NON per conclusioni causali.
        Il modello principale resta Ridge (Sezione 7).

        Dal corso §7.1: "classical statistical approaches often outperform ML
        when data are limited."
    """
    if not SHAP_DISPONIBILE:
        print("\n  ⚠ SHAP non disponibile — sezione saltata.")
        return

    print("\n" + "─" * 55)
    print("🌲 RANDOM FOREST + SHAP (ESPLORATIVO — n piccolo)")
    print("─" * 55)
    print("  ⚠ Con ~80 osservazioni, RF è esplorativo. Modello principale = Ridge.")

    cols_disp = [c for c in cols_pred if c in df.columns]
    df_clean = df[[target_col] + cols_disp].dropna().copy()

    X = df_clean[cols_disp]
    y = df_clean[target_col]

    scaler = StandardScaler()
    X_scaled = pd.DataFrame(
        scaler.fit_transform(X), columns=cols_disp, index=X.index
    )

    # Addestra RF su tutto il dataset (per SHAP globale)
    # max_depth=3 è volutamente basso per limitare overfitting con n piccolo
    rf = RandomForestRegressor(
        n_estimators=RF_N_TREES,
        max_depth=RF_MAX_DEPTH,
        random_state=42,
        n_jobs=-1
    )
    rf.fit(X_scaled, y)

    r2_train = r2_score(y, rf.predict(X_scaled))
    print(f"  R² su training (ATTENZIONE: ottimistico per n piccolo): {r2_train:.4f}")

    # SHAP values
    explainer   = shap.TreeExplainer(rf)
    shap_values = explainer.shap_values(X_scaled)

    # SHAP summary plot
    fig, ax = plt.subplots(figsize=(9, max(5, len(cols_disp) * 0.6 + 2)))
    shap.summary_plot(
        shap_values, X_scaled,
        feature_names=cols_disp,
        show=False,
        plot_size=None,
        ax=ax
    )
    ax.set_title(
        "SHAP Summary Plot — Random Forest (ESPLORATIVO)\n"
        "⚠ n~80: risultati instabili, solo esplorativo. Modello principale = Ridge.",
        fontsize=10, fontweight="bold"
    )
    plt.tight_layout()
    fig.savefig(PLOT_DIR / "04_shap_summary.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    print(f"  ✅ Salvato: 04_shap_summary.png")


# =============================================================================
# SEZIONE 9 — MAIN
# =============================================================================

def main() -> None:
    print("\n" + "=" * 65)
    print("SCRIPT 7 — MODELLO MULTIVARIATO FATTORI AMBIENTALI vs ILI")
    print("=" * 65)
    print(
        "\n  GERARCHIA METODOLOGICA:\n"
        "  Livello 1 (sempre): Correlazione predittori + Odds Ratio\n"
        "  Livello 2 (principale): OLS multiplo + Ridge con LOGO-CV\n"
        "  Livello 3 (esplorativo): Random Forest + SHAP\n"
    )

    # ── 1. Costruisci dataset ─────────────────────────────────────────────────
    try:
        df, cols_pred = costruisci_dataset()
    except FileNotFoundError as e:
        print(f"\n❌ {e}")
        return

    if df.empty or len(df) < 20:
        print(f"\n❌ Dataset troppo piccolo ({len(df)} righe) — impossibile procedere.")
        return

    # ── 2. Livello 1: analisi esplorativa ─────────────────────────────────────
    print("\n" + "=" * 65)
    print("LIVELLO 1 — ANALISI ESPLORATIVA")
    print("=" * 65)
    analisi_correlazione(df, cols_pred)
    df_or = calcola_odds_ratio(df, cols_pred)
    print("\n  Odds Ratio calcolati:")
    print(df_or[["Variabile", "OR", "IC95_low", "IC95_high", "Significativo"]].to_string(index=False))

    # ── 3. Livello 2: OLS ─────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("LIVELLO 2A — OLS MULTIPLO (statsmodels)")
    print("=" * 65)
    modello_ols(df, cols_pred)

    # ── 4. Livello 2: Ridge + CV temporale ───────────────────────────────────
    print("\n" + "=" * 65)
    print("LIVELLO 2B — RIDGE + LeaveOneGroupOut CV")
    print("=" * 65)
    modello_ridge_cv(df, cols_pred)

    # ── 5. Livello 3: RF + SHAP ───────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("LIVELLO 3 — RANDOM FOREST + SHAP (ESPLORATIVO)")
    print("=" * 65)
    modello_random_forest_shap(df, cols_pred)

    # ── 6. Riepilogo finale ───────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("✅ SCRIPT 7 COMPLETATO")
    print(f"\n📂 Output salvati in: {OUT_DIR}")
    print(f"📂 Grafici salvati in: {PLOT_DIR}")
    print("\nFile prodotti:")
    for f in sorted(OUT_DIR.rglob("*")):
        if f.is_file():
            print(f"  └─ {f.relative_to(OUT_DIR)}")

    print("\n" + "─" * 65)
    print("⚠ NOTA PER IL REPORT — BIAS DA DICHIARARE:")
    print("  1. Bias ecologico: dati aggregati ATS, non individuali")
    print("  2. n~80 osservazioni: risultati esplorativi, non conclusivi")
    print("  3. Autocorrelazione: OLS assume residui indipendenti (violato)")
    print("     → mitigato con termine autoregressivo ILI_lag1")
    print("  4. 4 fold LOGO-CV: stima performance intrinsecamente instabile")
    print("  5. Random Forest: solo esplorativo (n piccolo → overfitting)")
    print("  6. Confounding non misurato: vaccinazioni, varianti virali")
    print("─" * 65)


if __name__ == "__main__":
    main()