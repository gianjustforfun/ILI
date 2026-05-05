"""
=============================================================
SCRIPT 4 — PICCHI E CROSS-CORRELAZIONE AMBIENTE vs ILI ATS
=============================================================

COSA FA QUESTO SCRIPT:
    Legge i file CSV delle settimane di interesse (già filtrate
    dalla pipeline precedente) per ciascuna variabile ambientale
    ARPA e il file ili_ats_milano.csv con i dati ILI settimanali
    di ATS Milano.

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
        Calcola la correlazione di Pearson tra la serie
        ambientale e la serie ILI sfalsate di un lag
        variabile da 0 a MAX_LAG_SETTIMANE settimane.

        Per ogni (variabile, stagione, lag) calcola r di Pearson
        tra la serie ambientale alla settimana t e la serie ILI
        alla settimana t + lag.

        NOTA IMPORTANTE per la TEMPERATURA:
            Per la temperatura il segnale biologico atteso è che
            temperature PIÙ BASSE precedano picchi ILI. Poiché
            la correlazione di Pearson è simmetrica rispetto al
            segno, per la temperatura si usa la serie invertita
            (-temperatura) in modo che un aumento del segnale
            corrisponda a "più freddo", rendendo l'interpretazione
            coerente con le altre variabili (coefficiente positivo
            = variabile alta precede ILI alto).

        Per ogni variabile vengono prodotti:
            - tabella lag vs correlazione media tra stagioni
            - lag ottimale: quello con la correlazione media più alta
            - grafico a linee (lag in asse X, r in asse Y) con una
              linea per stagione e la media evidenziata

        Questo metodo è più robusto dell'analisi 1 perché:
            a) usa TUTTA la serie stagionale (20 punti), non solo 5 picchi
            b) non richiede la selezione soggettiva dei picchi
            c) produce un risultato continuo (r per ogni lag)
               che permette di identificare il lag con il segnale
               più forte in modo oggettivo
            d) non soffre del bias di abbinamento per rango

        Limitazioni:
            - con 20 punti per stagione la correlazione è instabile:
              servono |r| > 0.44 per p < 0.05 (test a due code)
            - il lag ottimale potrebbe variare tra stagioni,
              riflettendo eterogeneità reale o rumore
            - NON implica causalità: due serie correlate con lag
              potrebbero essere entrambe causate da un terzo fattore

STRUTTURA FILE DI INPUT:
    ARPA/SETTIMANE_DI_INTERESSE/
        HUMIDITY/     HUMIDITY_2022_2023.csv, HUMIDITY_2023_2024.csv, ...
        NO2/          NO2_2022_2023.csv, ...
        PM10/         PM10_2022_2023.csv, ...
        PM25/         PM25_2022_2023.csv, ...
        TEMPERATURE/  TEMPERATURE_2022_2023.csv, ...

    Tutti i CSV ARPA hanno colonna "Settimana" (numero ISO 48-52, 1-15)
    più colonne per sensore/ATS.
    - TEMPERATURE e HUMIDITY: colonne tipo <ATS>_max, <ATS>_media, <ATS>_min
      → si usa SOLO la colonna "_media" per evitare di mescolare grandezze
    - PM10, PM25, NO2: una colonna per stazione (struttura piatta)
      → si usa la media di tutte le stazioni

    SORVEGLIANZA ACCESSI PS/
        ili_ats_milano.csv

    Struttura ili_ats_milano.csv (wide format):
        WEEK | 2022 | 2023 | 2024 | 2025 | 2026
          48 | 1692 | 1252 |  988 | 1353 |  NaN
         ...
          15 |  867 |  891 | 1007 |  883 |  866

    CONVENZIONE STAGIONI — punto critico del mapping:
        La colonna ILI "2022" contiene:
            - sett. 48-52 di fine 2021  (fisicamente a cavallo tra anni)
            - sett. 1-15  di inizio 2022
        Per convenzione la stagione viene chiamata con l'anno in cui TERMINA.

        I file ARPA usano invece il formato "anno_inizio_anno_fine":
            ARPA "*_2021_2022.csv"  ↔  colonna ILI "2022"
            ARPA "*_2022_2023.csv"  ↔  colonna ILI "2023"
            ARPA "*_2023_2024.csv"  ↔  colonna ILI "2024"
            ARPA "*_2024_2025.csv"  ↔  colonna ILI "2025"
            ARPA "*_2025_2026.csv"  ↔  colonna ILI "2026"

        Il mapping è gestito dal dizionario STAGIONE_ILI_TO_ARPA.

NOTA METODOLOGICA (bias da dichiarare nel report):
    - Dati a livello ATS (aggregazione ecologica): non è possibile
      inferire relazioni causali a livello individuale.
    - Con ~20 settimane per stagione e 4-5 stagioni disponibili
      il rischio di spurious correlation è elevato.
      Tutti i risultati sono descrittivi, non causali.
    - Soglia indicativa di significatività per r con n=20:
      |r| > 0.44 per p < 0.05 (test t a due code).
      Le correlazioni sotto questa soglia non sono interpretabili.

OUTPUT:
    CORRELATIONS/output/
        picchi_ambientali_settimane.csv
        picchi_ili_ats_milano.csv
        abbinamenti_picchi_vs_ili.csv
        riepilogo_correlazioni_stagionale.csv      ← analisi 1
        cross_correlazione_per_lag.csv             ← analisi 2
        cross_correlazione_lag_ottimale.csv        ← analisi 2 (sintesi)
    CORRELATIONS/output/grafici/
        <variabile>_vs_ili_<stagione>.png          ← analisi 1
        xcorr_<variabile>.png                      ← analisi 2

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
from scipy.stats import pearsonr

warnings.filterwarnings("ignore")
plt.style.use("seaborn-v0_8-whitegrid")

# =============================================================================
# CONFIGURAZIONE PERCORSI
# =============================================================================

BASE_DIR     = Path(__file__).resolve().parent   # .../CORRELATIONS/
PROJECT_ROOT = BASE_DIR.parent                   # .../ILI/

SETTIMANE_DIR = PROJECT_ROOT / "ARPA" / "SETTIMANE_DI_INTERESSE"
ILI_DIR       = PROJECT_ROOT / "SORVEGLIANZA ACCESSI PS"
OUTPUT_DIR    = BASE_DIR / "output"
PLOTS_DIR     = OUTPUT_DIR / "grafici"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# CONFIGURAZIONE VARIABILI E STAGIONI
# =============================================================================

# Mappa: nome variabile -> (sottocartella ARPA, tipo di picco da cercare)
#   "min" = valori PIÙ BASSI  → TEMPERATURE (freddo intenso)
#   "max" = valori PIÙ ALTI   → tutto il resto
VARIABILI: dict[str, tuple[str, str]] = {
    "TEMPERATURE": ("TEMPERATURE", "min"),
    "HUMIDITY":    ("HUMIDITY",    "max"),
    "PM10":        ("PM10",        "max"),
    "PM25":        ("PM25",        "max"),
    "NO2":         ("NO2",         "max"),
}

# Variabili con struttura multi-misura per sensore (colonne _max/_media/_min):
# per queste si usa SOLO la colonna "_media" per la serie rappresentativa.
TIPOLOGIE_CON_SOTTOSTAT: set[str] = {"TEMPERATURE", "HUMIDITY"}

# File ILI ATS Milano (wide format, una colonna per stagione)
ILI_FILE = ILI_DIR / "ili_ats_milano.csv"

# Mapping stagione ILI → etichetta stagione ARPA
# Aggiornare se vengono aggiunte nuove stagioni.
STAGIONE_ILI_TO_ARPA: dict[str, str] = {
    "2022": "2021_2022",
    "2023": "2022_2023",
    "2024": "2023_2024",
    "2025": "2024_2025",
    "2026": "2025_2026",
}

# ── Parametri analisi 1: picchi ───────────────────────────────────────────────
TOP_K_PEAKS         = 5     # numero di picchi da selezionare per stagione
MIN_DISTANCE_WEEKS  = 2     # distanza minima (settimane) tra due picchi consecutivi
MIN_PROMINENCE_FRAC = 0.15  # fraction * std della serie = prominenza minima adattiva

# ── Parametri analisi 2: cross-correlazione ───────────────────────────────────
MAX_LAG_SETTIMANE = 8
# Lag massimo da testare in settimane.
# Motivazione: 8 settimane (~2 mesi) è un limite superiore ragionevole
# per un effetto ambientale sull'ILI. La letteratura (Shaman et al.,
# Feng et al.) riporta lag di 1-4 settimane. Con una finestra stagionale
# di 20 settimane, un lag di 8 lascia almeno 12 coppie per il calcolo di r.

SOGLIA_R_SIGNIFICATIVO = 0.44
# Soglia indicativa di significatività statistica per r di Pearson con n=20.
# Deriva dall'inverso della distribuzione t: t = r*sqrt(n-2)/sqrt(1-r^2),
# con t critico a 2 code per p<0.05 e df=18 ≈ 2.101.
# Usata solo per evidenziare visivamente i risultati nel grafico,
# non come soglia di decisione formale.

# =============================================================================
# FUNZIONI DI SUPPORTO — ORDINAMENTO STAGIONALE
# =============================================================================

def ordine_settimana(s: int) -> int:
    """
    Mappa il numero di settimana ISO a una posizione progressiva
    che rispetta l'ordine cronologico della stagione influenzale:

        Settimana 48 → posizione  1   (inizio stagione, dicembre)
        Settimana 52 → posizione  5
        Settimana  1 → posizione  6   (inizio anno solare nuovo)
        Settimana 15 → posizione 20   (fine stagione, aprile)

    Senza questa trasformazione matplotlib ordinerebbe le settimane
    1-15 prima delle 48-52, spezzando visivamente la curva stagionale.
    """
    return s - 47 if s >= 48 else s + 5


def settimane_stagione() -> list[int]:
    """
    Lista delle settimane della stagione influenzale in ordine cronologico:
        [48, 49, 50, 51, 52, 1, 2, ..., 15]
    """
    return [48, 49, 50, 51, 52] + list(range(1, 16))

# =============================================================================
# CARICAMENTO ILI ATS MILANO
# =============================================================================

def carica_ili_ats_milano() -> pd.DataFrame:
    """
    Legge ili_ats_milano.csv (wide format) e lo trasforma in formato long.

    Struttura attesa del file di input:
        WEEK | 2022 | 2023 | 2024 | 2025 | 2026
          48 | 1692 | 1252 |  988 | 1353 |  NaN
          49 | 1783 | 1872 | 1052 | 1916 |  NaN
         ...
          15 |  867 |  891 | 1007 |  883 |  866

    La prima colonna (WEEK) contiene i numeri di settimana ISO.
    Le colonne successive sono le stagioni (anno di fine stagione).
    I NaN corrispondono a settimane non ancora avvenute.

    Dopo la trasformazione il DataFrame ha una riga per settimana per stagione:
        WEEK | Stagione_ILI | Stagione_ARPA | Valore | Ordine

    Returns
    -------
    pd.DataFrame  Long format, DataFrame vuoto se file non trovato.
    """
    if not ILI_FILE.exists():
        print(f"   ❌ File ILI non trovato: {ILI_FILE}")
        return pd.DataFrame()

    df = pd.read_csv(ILI_FILE)
    df.columns = [str(c).strip() for c in df.columns]

    col_week = df.columns[0]
    col_anni = [c for c in df.columns if c != col_week]

    df[col_week] = pd.to_numeric(df[col_week], errors="coerce")
    df = df.dropna(subset=[col_week])
    df[col_week] = df[col_week].astype(int)

    # Trasformazione wide → long
    df_long = df.melt(
        id_vars=col_week,
        value_vars=col_anni,
        var_name="Stagione_ILI",
        value_name="Valore"
    ).rename(columns={col_week: "WEEK"})

    df_long = df_long.dropna(subset=["Valore"]).copy()
    df_long["WEEK"]   = df_long["WEEK"].astype(int)
    df_long["Valore"] = pd.to_numeric(df_long["Valore"], errors="coerce")

    # Filtra solo le settimane della finestra influenzale
    df_long = df_long[df_long["WEEK"].isin(set(settimane_stagione()))].copy()

    df_long["Ordine"]       = df_long["WEEK"].apply(ordine_settimana)
    df_long["Stagione_ARPA"] = df_long["Stagione_ILI"].map(STAGIONE_ILI_TO_ARPA)

    sconosciute = df_long[df_long["Stagione_ARPA"].isna()]["Stagione_ILI"].unique()
    if len(sconosciute) > 0:
        print(f"   ⚠️  Stagioni ILI senza mapping ARPA: {sconosciute}")
        print(f"       → Aggiorna STAGIONE_ILI_TO_ARPA in cima allo script.")
        df_long = df_long.dropna(subset=["Stagione_ARPA"])

    df_long = df_long.sort_values(["Stagione_ILI", "Ordine"]).reset_index(drop=True)

    print(f"   ✅ Stagioni ILI caricate: {sorted(df_long['Stagione_ILI'].unique())}")
    return df_long

# =============================================================================
# CARICAMENTO VARIABILI AMBIENTALI ARPA
# =============================================================================

def carica_variabile(nome_variabile: str, sottocartella: str) -> pd.DataFrame:
    """
    Legge tutti i CSV stagionali di una variabile dalla sottocartella
    SETTIMANE_DI_INTERESSE e li unifica in un unico DataFrame.

    Il nome della stagione ARPA viene estratto dal nome del file:
        pattern: <VARIABILE>_<anno>_<anno>.csv → stagione = "<anno>_<anno>"

    Returns
    -------
    pd.DataFrame  Colonne: Settimana | Stagione_ARPA | <colonne sensori>
                  DataFrame vuoto se nessun file valido trovato.
    """
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

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def prepara_serie(df: pd.DataFrame, nome_variabile: str) -> pd.DataFrame:
    """
    Calcola la serie rappresentativa settimanale mediando le colonne appropriate.

    Logica di selezione colonne:
        TEMPERATURE / HUMIDITY → solo colonne "_media"
            Motivazione: mediare _max, _media e _min insieme produce un
            numero senza interpretazione statistica chiara.
        PM10 / PM25 / NO2 → tutte le colonne numeriche
            Struttura piatta: un valore per stazione.

    Returns
    -------
    pd.DataFrame  Colonne: Settimana | Stagione_ARPA | Valore | Ordine
                  Ordinato per (Stagione_ARPA, Ordine).
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
        df.groupby(["Stagione_ARPA", "Settimana"], as_index=False)["Valore"]
        .mean()
    )

    serie["Ordine"] = serie["Settimana"].apply(ordine_settimana)
    serie = serie.sort_values(["Stagione_ARPA", "Ordine"]).reset_index(drop=True)

    return serie

# =============================================================================
# ANALISI 1 — RILEVAMENTO E ABBINAMENTO PICCHI
# =============================================================================

def trova_picchi_candidati(
    valori: np.ndarray, mode: str
) -> tuple[np.ndarray, np.ndarray]:
    """
    Individua i candidati picchi con scipy.signal.find_peaks.

    Per i minimi (TEMPERATURE) inverte il segnale prima di passarlo
    a find_peaks (che lavora solo su massimi locali), identificando
    i periodi di FREDDO INTENSO anziché i periodi caldi.

    Prominenza minima adattiva = MAX(std * MIN_PROMINENCE_FRAC, ε):
        evita falsi positivi su serie piatte, evita falsi negativi
        su serie ad alta varianza.

    Returns: indici dei picchi, prominenze corrispondenti.
    """
    x = valori if mode == "max" else -valori
    prominenza_min = max(np.nanstd(x) * MIN_PROMINENCE_FRAC, 1e-6)
    idx, props = find_peaks(
        x,
        prominence=prominenza_min,
        distance=max(1, MIN_DISTANCE_WEEKS)
    )
    prominenze = props.get("prominences", np.ones(len(idx)))
    return idx, prominenze


def seleziona_top_k_picchi(
    df_stagione: pd.DataFrame,
    mode: str,
    top_k: int = TOP_K_PEAKS
) -> pd.DataFrame:
    """
    Seleziona i TOP_K picchi più rilevanti in una stagione.

    Strategia a due fasi:
        Fase 1 — find_peaks + prominenza adattiva:
            Se trova almeno top_k candidati, seleziona i migliori per valore.
        Fase 2 — Fallback su nlargest / nsmallest:
            Se i candidati sono meno di top_k (serie piatta, dati scarsi),
            seleziona direttamente i top_k valori estremi.

    NOTA per TEMPERATURE (mode="min"):
        Seleziona i valori PIÙ BASSI (freddo intenso), coerente con la
        letteratura che associa il freddo all'aumento dell'ILI invernale.

    Il risultato è ordinato per Ordine stagionale (sequenza temporale).

    Returns
    -------
    pd.DataFrame  Colonne: Settimana | Valore | Ordine | Rango
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
        top = (
            candidati
            .sort_values(["Valore", "Prominenza"], ascending=[asc, False])
            .head(top_k)
        )
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


def correla_picchi(
    env_peaks:    pd.DataFrame,
    ili_peaks:    pd.DataFrame,
    variabile:    str,
    stagione_ili: str,
) -> tuple[dict | None, pd.DataFrame]:
    """
    Abbina picchi ambientali e ILI per rango temporale e calcola
    lag (settimane) e correlazione di Pearson sui valori abbinati.

    Abbinamento: picco ambientale rango k ↔ picco ILI rango k
    Lag = Ordine(ILI) - Ordine(ambiente):
        > 0 → ILI arriva DOPO il picco ambientale (atteso)
        < 0 → ILI precede il picco (possibile artefatto, vedi doc script)

    Returns: dizionario riassuntivo + DataFrame coppie.
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


def crea_grafico_picchi(
    env_serie:    pd.DataFrame,
    env_picchi:   pd.DataFrame,
    ili_serie:    pd.DataFrame,
    ili_picchi:   pd.DataFrame,
    variabile:    str,
    stagione_ili: str,
) -> None:
    """
    Grafico a due pannelli sovrapposti (asse X condiviso):
        Superiore: serie ambientale con picchi (punti rossi)
        Inferiore: serie ILI con picchi (punti rossi)

    Le linee verticali tratteggiate aiutano a leggere il lag tra pannelli.
    Salvato in: PLOTS_DIR/<variabile>_vs_ili_<stagione>.png
    """
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    sett_lista = settimane_stagione()
    tick_pos   = [ordine_settimana(s) for s in sett_lista]
    tick_label = [str(s) for s in sett_lista]
    tipo_picco = "minimi (freddo)" if "TEMP" in variabile.upper() else "massimi"

    ax1.plot(env_serie["Ordine"], env_serie["Valore"],
             color="#1a5276", lw=2, label=variabile)
    if env_picchi is not None and not env_picchi.empty:
        ax1.scatter(env_picchi["Ordine"], env_picchi["Valore"],
                    color="#c0392b", s=65, zorder=4,
                    label=f"Top {TOP_K_PEAKS} picchi {tipo_picco}")
        for _, r in env_picchi.iterrows():
            ax1.axvline(r["Ordine"], color="#c0392b", alpha=0.2, lw=1.2, ls="--")

    ax1.set_ylabel(variabile, fontsize=11)
    ax1.set_title(
        f"{variabile} vs ILI ATS Milano  —  Stagione {stagione_ili}",
        fontsize=13, fontweight="bold"
    )
    ax1.legend(loc="upper right", fontsize=9)
    ax1.grid(True, alpha=0.3)

    ax2.plot(ili_serie["Ordine"], ili_serie["Valore"],
             color="#1e8449", lw=2, label="ILI ATS Milano")
    if ili_picchi is not None and not ili_picchi.empty:
        ax2.scatter(ili_picchi["Ordine"], ili_picchi["Valore"],
                    color="#c0392b", s=65, zorder=4,
                    label=f"Top {TOP_K_PEAKS} picchi ILI")
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
# ANALISI 2 — CROSS-CORRELAZIONE
# =============================================================================

def calcola_cross_correlazione_stagione(
    env_serie:   pd.DataFrame,
    ili_serie:   pd.DataFrame,
    variabile:   str,
    stagione_ili: str,
    max_lag:     int = MAX_LAG_SETTIMANE,
) -> pd.DataFrame:
    """
    Calcola la correlazione di Pearson tra la serie ambientale e la serie
    ILI per ogni lag da 0 a max_lag settimane, su una singola stagione.

    Meccanismo:
        Per ogni lag L (in settimane):
            - Prende la serie ambientale alle posizioni t = 1, ..., N-L
            - Prende la serie ILI alle posizioni t+L = L+1, ..., N
            - Calcola r di Pearson tra le due sottosequenze allineate

        Questo risponde alla domanda:
            "La variabile ambientale alla settimana t è correlata con
             l'ILI L settimane dopo?"

    NOTA per TEMPERATURE:
        La serie di temperatura viene invertita (-T) prima del calcolo.
        Motivazione: temperature basse → ILI alto è la relazione attesa.
        Con l'inversione, un r positivo a lag L significa "quando fa
        più freddo, L settimane dopo gli accessi ILI aumentano",
        coerente con l'interpretazione delle altre variabili.

    Richiede almeno MIN_PUNTI_XCORR punti sovrapposti per calcolare r.
    Con max_lag=8 e N=20, il minimo è 12 punti (sufficiente).

    Parameters
    ----------
    env_serie    : pd.DataFrame  Serie ambientale (colonne: Ordine, Valore)
    ili_serie    : pd.DataFrame  Serie ILI (colonne: Ordine, Valore)
    variabile    : str           Nome variabile (per output)
    stagione_ili : str           Es. "2023"
    max_lag      : int           Lag massimo da testare

    Returns
    -------
    pd.DataFrame  Colonne: Variabile | Stagione_ILI | Lag | r_Pearson | p_value | N_punti
                  Una riga per ogni lag testato (0, 1, ..., max_lag).
    """
    MIN_PUNTI_XCORR = 5  # minimo di punti sovrapposti per calcolare r

    # Allinea le due serie sull'asse Ordine (posizione stagionale)
    env_ord = env_serie.set_index("Ordine")["Valore"].sort_index()
    ili_ord = ili_serie.set_index("Ordine")["Valore"].sort_index()

    # Per temperatura inverte il segnale: più negativo = più freddo
    # → con l'inversione un valore alto = freddo intenso, coerente con
    #   l'interpretazione "variabile alta precede ILI alto"
    if variabile.upper() == "TEMPERATURE":
        env_ord = -env_ord

    # Indice comune delle posizioni disponibili in entrambe le serie
    ordini_comuni = sorted(set(env_ord.index) & set(ili_ord.index))

    righe = []
    for lag in range(0, max_lag + 1):
        # Posizioni usabili: ambiente a t, ILI a t+lag
        # t deve essere in ordini_comuni, t+lag pure
        t_vals    = [o for o in ordini_comuni if (o + lag) in ordini_comuni]
        t_lag_vals = [o + lag for o in t_vals]

        n_punti = len(t_vals)
        if n_punti < MIN_PUNTI_XCORR:
            # Troppo pochi punti per essere affidabile, inserisce NaN
            righe.append({
                "Variabile":    variabile,
                "Stagione_ILI": stagione_ili,
                "Lag":          lag,
                "r_Pearson":    np.nan,
                "p_value":      np.nan,
                "N_punti":      n_punti,
            })
            continue

        x = env_ord.loc[t_vals].values      # serie ambientale a t
        y = ili_ord.loc[t_lag_vals].values  # serie ILI a t + lag

        # Rimuove coppie con NaN (dati mancanti su qualche settimana)
        mask = ~(np.isnan(x) | np.isnan(y))
        x, y = x[mask], y[mask]

        if len(x) < MIN_PUNTI_XCORR:
            righe.append({
                "Variabile":    variabile,
                "Stagione_ILI": stagione_ili,
                "Lag":          lag,
                "r_Pearson":    np.nan,
                "p_value":      np.nan,
                "N_punti":      len(x),
            })
            continue

        r, p = pearsonr(x, y)
        righe.append({
            "Variabile":    variabile,
            "Stagione_ILI": stagione_ili,
            "Lag":          lag,
            "r_Pearson":    float(r),
            "p_value":      float(p),
            "N_punti":      len(x),
        })

    return pd.DataFrame(righe)


def crea_grafico_xcorr(
    df_xcorr: pd.DataFrame,
    variabile: str,
) -> None:
    """
    Crea il grafico della cross-correlazione per una variabile:
        - Asse X: lag (settimane)
        - Asse Y: r di Pearson
        - Una linea per stagione (colori diversi)
        - Linea nera spessa = media tra stagioni
        - Banda grigia orizzontale = zona ±SOGLIA_R_SIGNIFICATIVO
          (indica approssimativamente p < 0.05 con n=20)
        - Rettangolo verde trasparente sul lag ottimale (r media massima)

    Il grafico aiuta a rispondere:
        "A quale lag la variabile ambientale ha la correlazione
         più stabile e più alta con l'ILI, tra le stagioni?"

    Salvato in: PLOTS_DIR/xcorr_<variabile>.png
    """
    df_v = df_xcorr[df_xcorr["Variabile"] == variabile].copy()
    if df_v.empty:
        return

    stagioni = sorted(df_v["Stagione_ILI"].unique())
    lags     = sorted(df_v["Lag"].unique())

    fig, ax = plt.subplots(figsize=(10, 5))

    colori = plt.cm.tab10(np.linspace(0, 0.8, len(stagioni)))

    for stagione, colore in zip(stagioni, colori):
        df_s = df_v[df_v["Stagione_ILI"] == stagione].sort_values("Lag")
        ax.plot(df_s["Lag"], df_s["r_Pearson"],
                marker="o", lw=1.5, markersize=5,
                color=colore, alpha=0.7, label=f"Stagione {stagione}")

    # Media tra stagioni per ogni lag
    media = df_v.groupby("Lag")["r_Pearson"].mean()
    ax.plot(media.index, media.values,
            color="black", lw=2.5, marker="D", markersize=6,
            label="Media stagioni", zorder=5)

    # Banda di non significatività (|r| < soglia)
    ax.axhspan(-SOGLIA_R_SIGNIFICATIVO, SOGLIA_R_SIGNIFICATIVO,
               color="gray", alpha=0.12,
               label=f"|r| < {SOGLIA_R_SIGNIFICATIVO} (p > 0.05 indicativo)")
    ax.axhline(0, color="black", lw=0.8, ls="--", alpha=0.5)

    # Evidenzia il lag ottimale (media r massima)
    if not media.dropna().empty:
        lag_ottimale = int(media.idxmax())
        ax.axvline(lag_ottimale, color="#27ae60", lw=2, ls=":",
                   label=f"Lag ottimale: {lag_ottimale} sett.")

    etichetta_var = variabile
    if variabile == "TEMPERATURE":
        etichetta_var = "TEMPERATURE (invertita: +r = più freddo → più ILI)"

    ax.set_title(
        f"Cross-correlazione {etichetta_var} vs ILI ATS Milano",
        fontsize=12, fontweight="bold"
    )
    ax.set_xlabel("Lag (settimane): ambiente a t, ILI a t + lag", fontsize=10)
    ax.set_ylabel("r di Pearson", fontsize=10)
    ax.set_xticks(lags)
    ax.set_ylim(-1.05, 1.05)
    ax.legend(loc="lower right", fontsize=8)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    out_path = PLOTS_DIR / f"xcorr_{variabile.lower()}.png"
    fig.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    print(f"   📊 {out_path.name}")


def analisi_cross_correlazione(
    df_env_all: dict[str, pd.DataFrame],
    ili_df:     pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Esegue la cross-correlazione per tutte le variabili e stagioni.

    Coordina:
        1. calcola_cross_correlazione_stagione() per ogni (var, stagione)
        2. Raccoglie tutti i risultati in un unico DataFrame
        3. Calcola il lag ottimale per variabile (media r tra stagioni)
        4. Chiama crea_grafico_xcorr() per ogni variabile

    Parameters
    ----------
    df_env_all : dict  variabile → DataFrame serie ambientale (da prepara_serie)
    ili_df     : pd.DataFrame  Output di carica_ili_ats_milano()

    Returns
    -------
    df_xcorr_tutti   : pd.DataFrame  Tutti i risultati per lag/stagione/variabile
    df_lag_ottimale  : pd.DataFrame  Lag ottimale e r media per variabile
    """
    print("\n" + "─" * 55)
    print("📐 ANALISI 2 — CROSS-CORRELAZIONE")
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

            # Normalizza il nome colonna per ili_stagione
            ili_stagione = ili_stagione.rename(columns={"WEEK": "Settimana"})

            df_lag = calcola_cross_correlazione_stagione(
                env_serie=env_stagione,
                ili_serie=ili_stagione,
                variabile=variabile,
                stagione_ili=stagione_ili,
            )
            tutti_xcorr.append(df_lag)

        # Grafico per questa variabile (tutte le stagioni sovrapposte)
        if tutti_xcorr:
            df_xcorr_parziale = pd.concat(tutti_xcorr, ignore_index=True)
            crea_grafico_xcorr(df_xcorr_parziale, variabile)

    if not tutti_xcorr:
        return pd.DataFrame(), pd.DataFrame()

    df_xcorr_tutti = pd.concat(tutti_xcorr, ignore_index=True)

    # ── Tabella lag ottimale per variabile ────────────────────────────────────
    # Calcola la media di r tra stagioni per ogni (variabile, lag)
    # e seleziona il lag con r media massima
    media_per_lag = (
        df_xcorr_tutti
        .groupby(["Variabile", "Lag"])["r_Pearson"]
        .mean()
        .reset_index()
        .rename(columns={"r_Pearson": "r_media"})
    )

    lag_ottimale_rows = []
    for var in media_per_lag["Variabile"].unique():
        sub = media_per_lag[media_per_lag["Variabile"] == var].dropna(subset=["r_media"])
        if sub.empty:
            continue
        best = sub.loc[sub["r_media"].idxmax()]
        lag_ottimale_rows.append({
            "Variabile":          var,
            "Lag_ottimale_sett":  int(best["Lag"]),
            "r_media_max":        float(best["r_media"]),
            "Interpretazione":    (
                "TEMPERATURE (segnale invertito: +r = freddo precede ILI)"
                if var == "TEMPERATURE"
                else f"{var} alta precede ILI alto"
            ),
        })

    df_lag_ottimale = pd.DataFrame(lag_ottimale_rows)

    return df_xcorr_tutti, df_lag_ottimale

# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    print("\n" + "=" * 60)
    print("SCRIPT 4 — PICCHI E CROSS-CORRELAZIONE AMBIENTE vs ILI")
    print("=" * 60)

    # ── 1. Carica ILI ATS Milano ──────────────────────────────────────────────
    print("\n📂 Caricamento ILI ATS Milano...")
    ili_df = carica_ili_ats_milano()

    if ili_df.empty:
        print("❌ Impossibile procedere senza dati ILI.")
        return

    # ── 2. Carica e prepara tutte le serie ambientali ─────────────────────────
    # Le serie vengono tenute in un dizionario per riutilizzarle
    # sia nell'analisi 1 (picchi) che nell'analisi 2 (cross-correlazione)
    print("\n📂 Caricamento serie ambientali ARPA...")
    df_env_all: dict[str, pd.DataFrame] = {}

    for variabile, (sottocartella, _) in VARIABILI.items():
        df_raw = carica_variabile(variabile, sottocartella)
        df_serie = prepara_serie(df_raw, variabile)
        if not df_serie.empty:
            df_env_all[variabile] = df_serie
            print(f"   ✅ {variabile}: {sorted(df_serie['Stagione_ARPA'].unique())}")
        else:
            print(f"   ⚠️  {variabile}: nessun dato, skip.")

    # ── 3. ANALISI 1: picchi ──────────────────────────────────────────────────
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

    # ── 4. ANALISI 2: cross-correlazione ─────────────────────────────────────
    df_xcorr, df_lag_ottimale = analisi_cross_correlazione(df_env_all, ili_df)

    # ── 5. Salva output CSV ───────────────────────────────────────────────────
    print(f"\n{'=' * 55}")
    print("💾 Salvataggio output CSV...")

    def salva_csv(lista_o_df, nome: str) -> None:
        if isinstance(lista_o_df, list):
            if not lista_o_df:
                print(f"   ⚠️  Nessun dato per {nome}")
                return
            df = pd.concat(lista_o_df, ignore_index=True)
        else:
            df = lista_o_df
        if df is None or df.empty:
            print(f"   ⚠️  Nessun dato per {nome}")
            return
        path = OUTPUT_DIR / nome
        df.to_csv(path, index=False)
        print(f"   ✅ {nome}  ({len(df)} righe)")

    # Analisi 1
    salva_csv(tutti_env_picchi, "picchi_ambientali_settimane.csv")
    salva_csv(tutti_ili_picchi, "picchi_ili_ats_milano.csv")
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

    # Analisi 2
    salva_csv(df_xcorr,         "cross_correlazione_per_lag.csv")
    salva_csv(df_lag_ottimale,  "cross_correlazione_lag_ottimale.csv")

    if not df_lag_ottimale.empty:
        print(f"\n{'─' * 55}")
        print("RIEPILOGO ANALISI 2 — LAG OTTIMALE (cross-correlazione)")
        print("─" * 55)
        print(df_lag_ottimale.to_string(index=False))

    print(f"\n✅ ELABORAZIONE COMPLETATA")
    print(f"📂 Output in:  {OUTPUT_DIR}")
    print(f"📂 Grafici in: {PLOTS_DIR}")


if __name__ == "__main__":
    main()