"""
=============================================================
02_Seasons.py  —  Aggregazione Stagionale dei Dati ARPA per ATS
=============================================================

COLLOCAZIONE: ILI/ARPA_COMUNI/02_Seasons.py
OUTPUT:      ILI/ARPA_COMUNI/seasons_output/

=============================================================
DESCRIZIONE GENERALE
=============================================================

Questo script prende in input i file CSV delle stime comunali
giornaliere scaricati da ARPA Lombardia (prodotti da
01_download_arpa.py), e produce per ogni stagione influenzale
e per ogni parametro (PM10, PM2.5, NO2) un CSV con la
concentrazione MEDIA SETTIMANALE aggregata a livello di ATS:
    - ATS Bergamo  (provincia di Bergamo)
    - ATS Montagna (province SO + subset BS + subset CO)

=============================================================
PERCHÉ QUESTA AGGREGAZIONE È NECESSARIA
=============================================================

I dati sanitari di sorveglianza ILI (accessi al Pronto Soccorso)
sono disponibili a granularità SETTIMANALE e a scala di ATS,
non a livello comunale o giornaliero. Per confrontare i dati
ambientali ARPA con i dati sanitari occorre quindi:

    1. AGGREGAZIONE TEMPORALE:  da giornaliero → settimanale
    2. AGGREGAZIONE SPAZIALE:   da comune      → ATS

Per la PARTE TEMPORALE usiamo la settimana epidemiologica ISO
(ISO 8601, lunedì-domenica). Questa convenzione è quella usata
standard nei sistemi di sorveglianza influenzale (InfluNet,
ECDC), quindi garantisce coerenza con i dati sanitari.

Per la PARTE SPAZIALE seguiamo il principio della
MEDIA ARITMETICA SEMPLICE TRA COMUNI (spatial averaging
senza pesi demografici), che è l'approccio più prudente quando:
    a) non si hanno dati di popolazione per ogni comune
       ad ogni data (che fluttua nel tempo);
    b) si vuole evitare di introdurre bias da pesi arbitrari;
    c) i sensori ARPA non sono distribuiti uniformemente
       per densità di popolazione.

NOTA METODOLOGICA — MEDIA PONDERATA PER POPOLAZIONE:
Un'alternativa sarebbe ponderare ogni comune per la sua
popolazione residente (population-weighted spatial average),
come discusso in Gianquintieri (Human Health Course, 25/26)
in riferimento all'aggregazione area-ponderata usata in
APHREH-ADSMap (eq. 8.6 degli appunti):

        EX_ATS = Σ_c (EX_c · Pop_c) / Σ_c Pop_c

Questo approccio è più rigoroso ma richiede di incrociare i
dati ARPA con le popolazioni ISTAT per comune, che sono
disponibili nello script Script 6 (ISTAT/).
Se il gruppo dispone dei CSV ISTAT per comune (prodotti da
Script 6), è possibile attivare la media ponderata per
popolazione impostando:

        USE_POPULATION_WEIGHTS = True

e fornendo il percorso della cartella ISTAT (vedi sezione
CONFIGURAZIONE più in basso). Lo script carica automaticamente
i dati ISTAT e calcola i pesi. Se i dati ISTAT non sono
disponibili, lo script cade in automatico sulla media semplice
e stampa un avviso.

=============================================================
BIAS E LIMITAZIONI — LEGGERE PRIMA DI USARE I RISULTATI
=============================================================

1. BIAS ECOLOGICO (aggregation bias):
   I dati sono aggregati a livello di ATS, NON individuale.
   Non è possibile trarre conclusioni sulle associazioni a
   livello individuale (ecological fallacy di Robinson, 1950).
   Tutte le analisi downstream devono essere interpretate come
   associazioni ecologiche tra trend temporali.

2. COPERTURA SPAZIALE ETEROGENEA DEI SENSORI ARPA:
   Non tutti i comuni hanno un sensore attivo per ogni
   parametro in ogni anno. La media viene calcolata solo sui
   comuni con almeno una misura valida nella settimana. Comuni
   senza sensori (o con sensore dismesso) non entrano nella
   media, il che può introdurre un bias geografico se la
   distribuzione dei sensori non è casuale rispetto ai livelli
   di inquinamento (tipicamente: i sensori sono concentrati
   nei capoluoghi e nelle aree urbane con maggiore inquinamento).

3. FINESTRA TEMPORALE BREVE:
   Le stagioni disponibili sono al massimo 5 (2021-2026).
   Con così poche osservazioni stagionali, modelli di
   regressione o correlazione rischiano fortemente l'overfitting.
   Le correlazioni con p<0.05 devono essere interpretate con
   estrema cautela (rischio elevato di correlazioni spurie).

4. STAGIONALITÀ CONFONDENTE:
   Sia i livelli di PM10/PM2.5 sia l'ILI hanno un forte
   pattern stagionale invernale. Qualsiasi correlazione
   semplice tra inquinamento e ILI nella stessa finestra
   temporale può riflettere stagionalità comune piuttosto
   che un rapporto causale. È necessario de-stagionalizzare
   o usare modelli che controllino per questo effetto.

5. MISSING DATA:
   Se in una settimana un comune non ha dati validi per un
   parametro, viene escluso dalla media di quella settimana.
   Settimane con meno di MIN_COMUNI_PER_ATS comuni validi
   vengono marcate come NaN nel CSV di output.

6. DEFINIZIONE DI "SETTIMANA INFLUENZALE":
   La stagione influenzale va dalla settimana 46 alla settimana
   15 dell'anno successivo. La prima stagione disponibile
   (21-22) parte dalla settimana 46 del 2021 solo se i dati
   ARPA del 2021 sono presenti nel dataset. Verificare la
   disponibilità effettiva dei dati prima dell'analisi.

=============================================================
STRUTTURA DELL'INPUT
=============================================================

Lo script si aspetta i file prodotti da 01_download_arpa.py
nella cartella dati_arpa_output/:

    ILI/ARPA_COMUNI/dati_arpa_output/
    ├── stime_comunali_2021.csv       (se presente)
    ├── stime_comunali_2022.csv
    ├── stime_comunali_2023.csv
    ├── stime_comunali_2024.csv
    ├── stime_comunali_2025.csv
    └── stime_comunali_2025-2026.csv  (o nome alternativo)

Colonne attese in ogni file:
    - data        : data in formato ISO (YYYY-MM-DD)
    - valore      : concentrazione media giornaliera (µg/m³)
    - parametro   : nome del parametro ('PM10', 'Particelle
                    sospese PM2.5', 'Biossido di Azoto')
    - provincia   : codice provincia ('BG', 'SO', 'BS', 'CO')
    - comune      : nome del comune
    - ats         : etichetta ATS ('ATS_Bergamo', 'ATS_Montagna')

=============================================================
STRUTTURA DELL'OUTPUT
=============================================================

    ILI/ARPA_COMUNI/seasons_output/
    ├── ATS_BERGAMO/
    │   ├── PM10_ATS_Bergamo_stagione_21-22.csv
    │   ├── PM10_ATS_Bergamo_stagione_22-23.csv
    │   ├── ...
    │   ├── PM25_ATS_Bergamo_stagione_21-22.csv
    │   └── NO2_ATS_Bergamo_stagione_21-22.csv
    ├── ATS_MONTAGNA/
    │   ├── PM10_ATS_Montagna_stagione_21-22.csv
    │   └── ...
    └── COMBINED/
        ├── tutti_parametri_ATS_Bergamo.csv   (tutte le stagioni, tutti i param)
        └── tutti_parametri_ATS_Montagna.csv

Ogni file per stagione contiene:
    - STAGIONE          : etichetta stagione (es. '21-22')
    - ANNO_ISO          : anno della settimana ISO
    - SETTIMANA_ISO     : numero settimana ISO (1-52/53)
    - WEEK_LABEL        : etichetta leggibile (es. 'W46/2021')
    - ORDINE            : ordine progressivo settimane nella stagione (1, 2, ...)
    - N_COMUNI          : numero comuni con dati validi nella settimana
    - MEDIA_COMUNALE    : media aritmetica delle medie comunali (µg/m³)
    - STD_COMUNALE      : deviazione standard tra comuni (variabilità spaziale)
    - MIN_COMUNALE      : minimo tra i comuni
    - MAX_COMUNALE      : massimo tra i comuni
    - [POP_WEIGHT_MEDIA]: presente solo se USE_POPULATION_WEIGHTS=True

=============================================================
REQUISITI
=============================================================

    pip install pandas numpy

Opzionale (per media ponderata):
    pip install pandas numpy  (già incluso)

=============================================================
"""

import os
import sys
import warnings
import pandas as pd
import numpy as np
from pathlib import Path

warnings.filterwarnings("ignore")


# ═══════════════════════════════════════════════════════════
# SEZIONE 1 — CONFIGURAZIONE
# ═══════════════════════════════════════════════════════════

# ── Percorsi ────────────────────────────────────────────────

# Cartella in cui si trovano i CSV delle stime comunali ARPA.
# Percorso relativo rispetto a dove viene lanciato lo script.
# Struttura attesa: ILI/ARPA_COMUNI/ (cartella corrente dello script)
INPUT_DIR = "dati_arpa_output"

# Cartella di output per i CSV stagionali aggregati.
OUTPUT_DIR = "seasons_output"

# Cartella contenente i CSV ISTAT per-comune (prodotti da Script 6).
# Usata SOLO se USE_POPULATION_WEIGHTS = True.
# Percorso relativo alla cartella dello script (ARPA_COMUNI/).
# I file attesi hanno nome tipo: ats_montagna_comuni_2023.csv
# con colonne 'Comune', 'Totale', 'PROVINCIA'.
# Per ATS Bergamo: Popolazione residente_ATS_Bergamo_2023.csv
# con colonna 'Comune', 'Totale'.
ISTAT_DIR_MONTAGNA = "../ISTAT/ATS MONTAGNA"   # per i comuni di ATS Montagna
ISTAT_DIR_BERGAMO  = "../ISTAT/ATS BERGAMO"    # per i comuni di ATS Bergamo

# ── Parametri di aggregazione ────────────────────────────────

# True  → media ponderata per popolazione (richiede dati ISTAT)
# False → media aritmetica semplice tra comuni (default robusto)
USE_POPULATION_WEIGHTS = False

# Numero minimo di comuni con dati validi per considerare
# affidabile la media settimanale di una ATS.
# Settimane con meno comuni vengono marcate con NaN.
# Valori consigliati: 3-5 per ATS piccole, 10+ per ATS grandi.
MIN_COMUNI_PER_ATS = {
    "ATS_Bergamo":  5,   # ATS Bergamo ha fino a 243 comuni
    "ATS_Montagna": 3,   # ATS Montagna ha comuni sparsi in più province
}

# ── Definizione delle stagioni influenzali ────────────────────

# Ogni stagione è identificata da un'etichetta e da un intervallo
# di settimane ISO: dalla settimana 46 dell'anno A alla settimana
# 15 dell'anno A+1 (inclusi).
#
# Convenzione settimane ISO (ISO 8601):
#   La settimana 1 è quella che contiene il primo giovedì dell'anno.
#   Le settimane vanno da lunedì a domenica.
#   Alcune settimane di gennaio appartengono all'anno ISO precedente.
#
# NOTA: La stagione "21-22" parte dalla settimana 46 del 2021,
# ma i dati ARPA disponibili potrebbero partire solo dal 2022
# (dataset '2022'). In quel caso verranno usate solo le settimane
# disponibili (1-15 del 2022), e lo script emette un avviso.
#
# Struttura: {etichetta: (anno_inizio_w46, anno_fine_w15)}
STAGIONI = {
    "21-22": (2021, 2022),
    "22-23": (2022, 2023),
    "23-24": (2023, 2024),
    "24-25": (2024, 2025),
    "25-26": (2025, 2026),
}

# ── Mapping nomi parametri → etichette file ───────────────────

# Il dataset ARPA usa nomi verbosi; qui li mappiamo a etichette
# brevi per i nomi dei file di output.
PARAMETRI_MAP = {
    "PM10":                       "PM10",
    "Particelle sospese PM2.5":   "PM25",
    "Biossido di Azoto":          "NO2",
}

# ── Etichette ATS presenti nel dataset ───────────────────────

# I valori della colonna 'ats' nel CSV input (da 01_download_arpa.py)
ATS_LABELS = {
    "ATS_Bergamo":  "ATS_BERGAMO",
    "ATS_Montagna": "ATS_MONTAGNA",
}


# ═══════════════════════════════════════════════════════════
# SEZIONE 2 — FUNZIONI DI SUPPORTO
# ═══════════════════════════════════════════════════════════

def carica_tutti_i_dati(input_dir: str) -> pd.DataFrame:
    """
    Carica e unisce tutti i file CSV delle stime comunali ARPA.

    Strategia:
        Cerca tutti i file .csv nella cartella input_dir il cui
        nome inizia con 'stime_comunali_'. Li carica uno per uno
        e li concatena in un unico DataFrame.

    Perché un unico DataFrame?
        Semplifica il filtraggio per stagione: le stagioni
        attraversano confini di anno solare (es. 22-23 comprende
        weeks 46-52 del 2022 e weeks 1-15 del 2023), quindi è
        più semplice filtrare per data su un dataset unificato.

    Normalizzazione:
        - La colonna 'data' viene convertita in datetime.
        - La colonna 'valore' viene convertita in float.
        - Righe con data o valore nullo vengono scartate.
        - La colonna 'parametro' viene normalizzata (strip).

    Args:
        input_dir: percorso della cartella con i CSV ARPA

    Returns:
        pd.DataFrame con tutti i dati unificati, oppure
        DataFrame vuoto se nessun file è trovato.
    """
    pattern = "stime_comunali_"
    files = sorted([
        f for f in os.listdir(input_dir)
        if f.endswith(".csv") and f.startswith(pattern)
        and "FINALE" not in f  # salta il file aggregato finale se presente
    ])

    if not files:
        print(f"  ⚠ Nessun file 'stime_comunali_*.csv' trovato in '{input_dir}'.")
        return pd.DataFrame()

    print(f"  File trovati: {len(files)}")
    dfs = []
    for fname in files:
        fpath = os.path.join(input_dir, fname)
        try:
            df = pd.read_csv(fpath)
            print(f"    ✓ {fname}: {len(df):,} righe")
            dfs.append(df)
        except Exception as e:
            print(f"    ✗ Errore nel caricare {fname}: {e}")

    if not dfs:
        return pd.DataFrame()

    # Concatena tutti i dataset
    df_all = pd.concat(dfs, ignore_index=True)

    # ── Normalizzazione ──────────────────────────────────────
    # Parsing della colonna data
    df_all["data"] = pd.to_datetime(df_all["data"], errors="coerce")

    # Parsing del valore numerico (concentrazione in µg/m³)
    df_all["valore"] = pd.to_numeric(df_all["valore"], errors="coerce")

    # Rimozione valori negativi (fisicamente impossibili)
    df_all.loc[df_all["valore"] < 0, "valore"] = np.nan

    # Strip degli spazi nei nomi dei parametri e delle ATS
    df_all["parametro"] = df_all["parametro"].astype(str).str.strip()
    if "ats" in df_all.columns:
        df_all["ats"] = df_all["ats"].astype(str).str.strip()
    if "comune" in df_all.columns:
        df_all["comune"] = df_all["comune"].astype(str).str.strip().str.title()
    if "provincia" in df_all.columns:
        df_all["provincia"] = df_all["provincia"].astype(str).str.strip().str.upper()

    # Rimozione righe con data o valore nullo
    n_prima = len(df_all)
    df_all = df_all.dropna(subset=["data", "valore"])
    n_dopo = len(df_all)
    if n_prima > n_dopo:
        print(f"  Rimosse {n_prima - n_dopo:,} righe con data/valore nullo.")

    # Rimozione duplicati: stessa data, stesso comune, stesso parametro
    n_prima = len(df_all)
    df_all = df_all.drop_duplicates(subset=["data", "comune", "parametro"], keep="first")
    n_dopo = len(df_all)
    if n_prima > n_dopo:
        print(f"  Rimossi {n_prima - n_dopo:,} duplicati (stessa data+comune+parametro).")

    print(f"\n  Dataset unificato: {len(df_all):,} righe")
    print(f"  Periodo: {df_all['data'].min().date()} → {df_all['data'].max().date()}")
    print(f"  Parametri: {sorted(df_all['parametro'].unique())}")
    if "ats" in df_all.columns:
        print(f"  ATS: {sorted(df_all['ats'].unique())}")

    return df_all


def assegna_settimana_iso(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggiunge al DataFrame le colonne della settimana ISO.

    Le settimane ISO (ISO 8601) sono il riferimento standard
    nei sistemi di sorveglianza epidemiologica (InfluNet, ECDC).
    La settimana 1 è quella che contiene il primo giovedì
    dell'anno. Le settimane vanno da lunedì a domenica.

    ATTENZIONE: l'anno ISO può differire dall'anno solare!
    Es: 2022-01-03 (lunedì) → anno ISO=2022, settimana ISO=1
        2021-12-31 (venerdì) → anno ISO=2021, settimana ISO=52
        2022-01-01 (sabato) → anno ISO=2021, settimana ISO=52
    Quindi usiamo sempre df["anno_iso"] e df["settimana_iso"]
    per identificare univocamente una settimana.

    Args:
        df: DataFrame con colonna 'data' di tipo datetime

    Returns:
        df con colonne aggiunte:
            - anno_iso:      anno ISO della settimana
            - settimana_iso: numero settimana ISO (1-53)
            - week_label:    etichetta 'WXX/YYYY' (es. 'W46/2021')
    """
    df = df.copy()
    df["anno_iso"]      = df["data"].dt.isocalendar().year.astype(int)
    df["settimana_iso"] = df["data"].dt.isocalendar().week.astype(int)

    # Etichetta leggibile per i grafici e i CSV di output
    df["week_label"] = (
        "W" + df["settimana_iso"].astype(str).str.zfill(2)
        + "/" + df["anno_iso"].astype(str)
    )
    return df


def calcola_settimane_stagione(anno_inizio: int, anno_fine: int) -> pd.DataFrame:
    """
    Costruisce la sequenza ordinata di settimane ISO che compongono
    una stagione influenzale.

    Definizione della stagione:
        Settimane dalla W46 dell'anno_inizio alla W15 dell'anno_fine.
        Questa finestra copre il periodo tipico dell'epidemia
        influenzale nelle regioni temperate dell'Europa (OMS/ECDC).

    Come funziona:
        Genera tutte le date giornaliere nell'intervallo, assegna
        a ciascuna la settimana ISO, e restituisce le settimane
        uniche nell'ordine corretto.

    Args:
        anno_inizio: anno della settimana 46 di inizio stagione
        anno_fine:   anno della settimana 15 di fine stagione

    Returns:
        pd.DataFrame con colonne:
            - anno_iso:      anno ISO
            - settimana_iso: numero settimana ISO
            - week_label:    etichetta 'WXX/YYYY'
            - ordine:        posizione progressiva nella stagione (da 1)
    """
    # Generiamo tutte le date nell'intervallo stagionale.
    # Partiamo dal lunedì della W46 dell'anno_inizio e finiamo
    # alla domenica della W15 dell'anno_fine.
    # Per semplicità, generiamo ogni giorno e poi raggruppiamo.
    date_inizio = pd.Timestamp(f"{anno_inizio}-11-01")  # sicuramente prima di W46
    date_fine   = pd.Timestamp(f"{anno_fine}-04-30")    # sicuramente dopo W15

    all_days = pd.date_range(date_inizio, date_fine, freq="D")
    df_days  = pd.DataFrame({"data": all_days})
    df_days["anno_iso"]      = df_days["data"].dt.isocalendar().year.astype(int)
    df_days["settimana_iso"] = df_days["data"].dt.isocalendar().week.astype(int)
    df_days["week_label"]    = (
        "W" + df_days["settimana_iso"].astype(str).str.zfill(2)
        + "/" + df_days["anno_iso"].astype(str)
    )

    # Seleziona solo le settimane che appartengono alla stagione:
    # - W46-W52/W53 dell'anno_inizio  (parte invernale anno A)
    # - W01-W15 dell'anno_fine        (parte invernale anno A+1)
    mask_inizio = (df_days["anno_iso"] == anno_inizio) & (df_days["settimana_iso"] >= 46)
    mask_fine   = (df_days["anno_iso"] == anno_fine)   & (df_days["settimana_iso"] <= 15)
    df_stagione = df_days[mask_inizio | mask_fine].copy()

    # Settimane uniche in ordine cronologico
    df_settimane = (
        df_stagione
        .drop_duplicates(subset=["anno_iso", "settimana_iso"])
        .sort_values(["anno_iso", "settimana_iso"])
        .reset_index(drop=True)
    )
    df_settimane["ordine"] = range(1, len(df_settimane) + 1)

    return df_settimane[["anno_iso", "settimana_iso", "week_label", "ordine"]]


def carica_pesi_popolazione(ats_label: str, anno: int) -> dict:
    """
    Carica i pesi demografici comunali dall'output di Script 6 (ISTAT).

    Usata solo se USE_POPULATION_WEIGHTS = True.

    Per ATS Bergamo:
        Cerca il file 'Popolazione residente_ATS_Bergamo_{anno}.csv'
        nella cartella ISTAT_DIR_BERGAMO.

    Per ATS Montagna:
        Cerca il file 'ats_montagna_comuni_{anno}.csv'
        (prodotto da salva_csv_comuni_montagna() in Script 6)
        nella cartella '../ISTAT/ATS MONTAGNA/whole/'.

    In entrambi i casi il file deve avere colonne 'Comune' e 'Totale'.

    Args:
        ats_label: 'ATS_Bergamo' o 'ATS_Montagna'
        anno:      anno ISTAT di riferimento (es. 2023)

    Returns:
        dict {nome_comune_normalizzato: popolazione_totale}
        oppure {} se il file non è trovato.
    """
    pesi = {}

    if ats_label == "ATS_Bergamo":
        # Cerca il file per anno nella cartella ATS BERGAMO
        folder = ISTAT_DIR_BERGAMO
        if not os.path.isdir(folder):
            print(f"  ⚠ Cartella ISTAT Bergamo non trovata: {folder}")
            return pesi
        for fname in os.listdir(folder):
            if fname.endswith(".csv") and str(anno) in fname:
                fpath = os.path.join(folder, fname)
                break
        else:
            print(f"  ⚠ Nessun file ISTAT Bergamo per anno {anno}")
            return pesi

    elif ats_label == "ATS_Montagna":
        # Cerca il CSV per-comune prodotto da salva_csv_comuni_montagna()
        folder = os.path.join(ISTAT_DIR_MONTAGNA, "whole")
        fpath_candidate = os.path.join(folder, f"ats_montagna_comuni_{anno}.csv")
        if not os.path.exists(fpath_candidate):
            # Fallback: cerca qualsiasi file con l'anno
            if not os.path.isdir(folder):
                print(f"  ⚠ Cartella ISTAT Montagna 'whole' non trovata: {folder}")
                return pesi
            for fname in os.listdir(folder):
                if fname.endswith(".csv") and str(anno) in fname:
                    fpath_candidate = os.path.join(folder, fname)
                    break
            else:
                print(f"  ⚠ Nessun file ISTAT Montagna per anno {anno}")
                return pesi
        fpath = fpath_candidate
    else:
        return pesi

    try:
        df_pop = pd.read_csv(fpath)
        if "Comune" not in df_pop.columns or "Totale" not in df_pop.columns:
            print(f"  ⚠ Colonne 'Comune'/'Totale' mancanti in {fpath}")
            return pesi
        for _, row in df_pop.iterrows():
            nome = str(row["Comune"]).strip().title()
            pop  = row["Totale"]
            if pd.notna(pop) and pop > 0:
                pesi[nome] = int(pop)
        print(f"    Pesi demografici caricati: {len(pesi)} comuni da {os.path.basename(fpath)}")
    except Exception as e:
        print(f"  ⚠ Errore nel caricare pesi ISTAT: {e}")

    return pesi


def aggrega_settimana_ats(
    df_settimana: pd.DataFrame,
    ats_label:    str,
    pesi:         dict,
    min_comuni:   int
) -> dict:
    """
    Calcola la concentrazione media a livello ATS per una
    singola settimana e un singolo parametro.

    APPROCCIO METODOLOGICO — Media Spaziale:

    L'aggregazione spaziale da comune a ATS segue il principio
    della media delle medie comunali (two-step averaging):

        STEP 1 — Media giornaliera per comune:
            Già presente nei dati ARPA (idoperatore=1 selezionato
            nello script 01). Ogni riga del dataset è già la
            media giornaliera di tutti i sensori del comune.

        STEP 2 — Media settimanale per comune:
            Per ogni comune, calcola la media dei valori
            giornalieri nella settimana ISO.
            N.B.: usiamo la media e non la somma perché vogliamo
            una concentrazione (intensiva), non un totale.

        STEP 3 — Media tra comuni per ATS:
            Se USE_POPULATION_WEIGHTS=False:
                MEDIA_ATS = mean(media_settimanale_per_comune)
            Se USE_POPULATION_WEIGHTS=True:
                MEDIA_ATS = Σ(w_c * media_c) / Σ(w_c)
                dove w_c è la popolazione residente del comune c.

    Questo approccio è analogo all'aggregazione area-ponderata
    descritta in Gianquintieri (eq. 8.6):
        EX_BSA = Σ_c(EX_c · Area_c∩BSA) / Area_BSA
    dove qui sostituiamo l'area con la popolazione (se disponibile)
    o usiamo pesi uniformi (area uguale per tutti i comuni).

    NOTA: usare la popolazione come peso è preferibile all'area
    perché la concentrazione di inquinante che effettivamente
    incide sulla salute è proporzionale al numero di persone
    esposte, non alla superficie del territorio.

    Args:
        df_settimana: DataFrame filtrato per settimana e ATS,
                      con colonne 'comune' e 'valore'
        ats_label:    stringa ATS per recuperare min_comuni
        pesi:         dict {comune: popolazione} per la ponderazione
        min_comuni:   soglia minima di comuni validi

    Returns:
        dict con le statistiche aggregate della settimana
    """
    # Calcola la media settimanale per ogni comune
    # (media dei valori giornalieri di quella settimana)
    media_per_comune = (
        df_settimana
        .groupby("comune")["valore"]
        .mean()              # media dei giorni nella settimana
        .dropna()
    )

    n_comuni = len(media_per_comune)

    # Se troppo pochi comuni hanno dati, restituisce NaN
    if n_comuni < min_comuni:
        return {
            "n_comuni":       n_comuni,
            "media_comunale": np.nan,
            "std_comunale":   np.nan,
            "min_comunale":   np.nan,
            "max_comunale":   np.nan,
            "pop_weight_media": np.nan,
        }

    # ── Calcolo della media spaziale ─────────────────────────
    valori = media_per_comune.values
    comuni = media_per_comune.index.tolist()

    media_semplice = float(np.mean(valori))
    std_comunale   = float(np.std(valori, ddof=1)) if n_comuni > 1 else np.nan
    min_comunale   = float(np.min(valori))
    max_comunale   = float(np.max(valori))

    # Media ponderata per popolazione (se pesi disponibili)
    pop_weight_media = np.nan
    if USE_POPULATION_WEIGHTS and pesi:
        pesi_comuni = np.array([pesi.get(c, 0) for c in comuni], dtype=float)
        if pesi_comuni.sum() > 0:
            pop_weight_media = float(
                np.average(valori, weights=pesi_comuni)
            )
        # Se alcuni comuni non hanno peso ISTAT, usiamo la media
        # semplice per quei comuni (peso = 1) e avvisiamo.
        n_senza_peso = sum(1 for c in comuni if c not in pesi)
        if n_senza_peso > 0:
            # Non stampiamo ogni volta per non saturare il log
            pass

    return {
        "n_comuni":         n_comuni,
        "media_comunale":   media_semplice,
        "std_comunale":     std_comunale,
        "min_comunale":     min_comunale,
        "max_comunale":     max_comunale,
        "pop_weight_media": pop_weight_media,
    }


def processa_stagione_ats_parametro(
    df_arpa:       pd.DataFrame,
    stagione_label: str,
    anno_inizio:   int,
    anno_fine:     int,
    ats_label:     str,
    parametro:     str,
    parametro_short: str,
) -> pd.DataFrame:
    """
    Produce il CSV settimanale per una combinazione
    (stagione, ATS, parametro).

    Flusso:
        1. Calcola le settimane ISO della stagione.
        2. Filtra df_arpa per ATS e parametro.
        3. Aggiunge colonne settimana ISO al subset filtrato.
        4. Per ogni settimana della stagione, aggrega i comuni.
        5. Restituisce un DataFrame ordinato per ORDINE.

    Trattamento delle settimane mancanti:
        Se per una settimana non ci sono dati (nessun comune
        ha misure in quella settimana), la riga viene comunque
        inclusa nel CSV con tutti i valori NaN. Questo garantisce
        che tutte le stagioni abbiano lo stesso numero di righe,
        facilitando il join con i dati sanitari.

    Args:
        df_arpa:         DataFrame completo con tutti i dati ARPA
        stagione_label:  es. '22-23'
        anno_inizio:     anno della W46 di inizio stagione
        anno_fine:       anno della W15 di fine stagione
        ats_label:       'ATS_Bergamo' o 'ATS_Montagna'
        parametro:       nome parametro nel dataset ARPA
        parametro_short: etichetta breve per il file output

    Returns:
        pd.DataFrame con le statistiche settimanali aggregate
    """
    # ── Step 1: Calendario della stagione ───────────────────
    df_settimane_stagione = calcola_settimane_stagione(anno_inizio, anno_fine)

    # ── Step 2: Filtraggio per ATS e parametro ──────────────
    # La colonna 'ats' nel dataset usa 'ATS_Bergamo' o 'ATS_Montagna'
    df_subset = df_arpa[
        (df_arpa["ats"] == ats_label) &
        (df_arpa["parametro"] == parametro)
    ].copy()

    if df_subset.empty:
        print(f"    ⚠ Nessun dato per {ats_label} / {parametro}")
        # Restituisce un DataFrame con tutte le settimane ma valori NaN
        df_vuoto = df_settimane_stagione.copy()
        df_vuoto.insert(0, "stagione", stagione_label)
        df_vuoto.insert(0, "ats", ats_label)
        df_vuoto["parametro"] = parametro_short
        for col in ["n_comuni", "media_comunale", "std_comunale",
                    "min_comunale", "max_comunale", "pop_weight_media"]:
            df_vuoto[col] = np.nan
        return df_vuoto

    # ── Step 3: Assegna settimana ISO al subset ──────────────
    df_subset = assegna_settimana_iso(df_subset)

    # ── Step 4: Carica pesi demografici ─────────────────────
    # Usiamo l'anno della parte primaverile come riferimento ISTAT
    # (es. per stagione 22-23 → anno ISTAT 2023)
    pesi = {}
    if USE_POPULATION_WEIGHTS:
        pesi = carica_pesi_popolazione(ats_label, anno_fine)
        if not pesi:
            print(f"    ⚠ Pesi demografici non disponibili per {ats_label} {anno_fine}."
                  f" Uso media semplice.")

    # ── Step 5: Aggregazione per ogni settimana ──────────────
    min_comuni = MIN_COMUNI_PER_ATS.get(ats_label, 3)
    righe = []

    for _, row_sett in df_settimane_stagione.iterrows():
        anno_iso = int(row_sett["anno_iso"])
        sett_iso = int(row_sett["settimana_iso"])
        ordine   = int(row_sett["ordine"])
        wlabel   = row_sett["week_label"]

        # Filtra i dati per la settimana ISO corrente
        mask_sett = (
            (df_subset["anno_iso"]      == anno_iso) &
            (df_subset["settimana_iso"] == sett_iso)
        )
        df_sett = df_subset[mask_sett]

        # Aggrega
        stats = aggrega_settimana_ats(df_sett, ats_label, pesi, min_comuni)

        righe.append({
            "stagione":         stagione_label,
            "ats":              ats_label,
            "parametro":        parametro_short,
            "anno_iso":         anno_iso,
            "settimana_iso":    sett_iso,
            "week_label":       wlabel,
            "ordine":           ordine,
            **stats,
        })

    df_out = pd.DataFrame(righe)

    # Colonne in ordine logico per il CSV
    cols_base = [
        "stagione", "ats", "parametro",
        "anno_iso", "settimana_iso", "week_label", "ordine",
        "n_comuni", "media_comunale", "std_comunale",
        "min_comunale", "max_comunale",
    ]
    if USE_POPULATION_WEIGHTS:
        cols_base.append("pop_weight_media")

    df_out = df_out[[c for c in cols_base if c in df_out.columns]]

    return df_out


# ═══════════════════════════════════════════════════════════
# SEZIONE 3 — MAIN
# ═══════════════════════════════════════════════════════════

def main():
    print("\n" + "=" * 65)
    print("02_Seasons.py — Aggregazione Stagionale ARPA per ATS")
    print("=" * 65)

    # ── Verifica cartella input ──────────────────────────────
    if not os.path.isdir(INPUT_DIR):
        print(f"\n  ERRORE: cartella input non trovata: '{INPUT_DIR}'")
        print(f"  Assicurati di eseguire lo script dalla cartella ILI/ARPA_COMUNI/")
        print(f"  e che 01_download_arpa.py sia già stato eseguito.")
        sys.exit(1)

    # ── Creazione cartelle di output ─────────────────────────
    out_bg = os.path.join(OUTPUT_DIR, "ATS_BERGAMO")
    out_mt = os.path.join(OUTPUT_DIR, "ATS_MONTAGNA")
    out_cb = os.path.join(OUTPUT_DIR, "COMBINED")
    for folder in [out_bg, out_mt, out_cb]:
        os.makedirs(folder, exist_ok=True)

    # ── Caricamento dati ARPA ────────────────────────────────
    print("\n" + "─" * 65)
    print("STEP 1: Caricamento dei dati ARPA")
    print("─" * 65)

    df_arpa = carica_tutti_i_dati(INPUT_DIR)

    if df_arpa.empty:
        print("\n  ERRORE: nessun dato caricato. Verificare i file di input.")
        sys.exit(1)

    # Verifica che la colonna 'ats' sia presente
    if "ats" not in df_arpa.columns:
        print("\n  ERRORE: colonna 'ats' mancante nel dataset.")
        print("  Verifica che 01_download_arpa.py abbia prodotto questa colonna.")
        sys.exit(1)

    # ── Aggiunta settimane ISO al dataset completo ───────────
    # Calcoliamo le settimane una volta sola su tutto il dataset
    # (è più efficiente che farlo per ogni subset).
    df_arpa = assegna_settimana_iso(df_arpa)

    # ── Riepilogo disponibilità dati ─────────────────────────
    print("\n" + "─" * 65)
    print("STEP 2: Riepilogo disponibilità dati per ATS")
    print("─" * 65)

    for ats_lbl in ["ATS_Bergamo", "ATS_Montagna"]:
        df_ats = df_arpa[df_arpa["ats"] == ats_lbl]
        if df_ats.empty:
            print(f"  ⚠ {ats_lbl}: NESSUN DATO TROVATO")
            continue
        print(f"\n  {ats_lbl}:")
        print(f"    Righe totali: {len(df_ats):,}")
        print(f"    Periodo:      {df_ats['data'].min().date()} → {df_ats['data'].max().date()}")
        print(f"    Comuni unici: {df_ats['comune'].nunique()}")
        for param in sorted(df_ats["parametro"].unique()):
            n = df_ats[df_ats["parametro"] == param]["comune"].nunique()
            print(f"    {param}: {n} comuni")

    # ── Loop principale: stagioni × ATS × parametri ─────────
    print("\n" + "─" * 65)
    print("STEP 3: Aggregazione stagionale")
    print("─" * 65)

    # Raccolta di tutti i risultati per i file COMBINED
    all_results_bg = []
    all_results_mt = []

    for stagione_label, (anno_inizio, anno_fine) in STAGIONI.items():
        print(f"\n  ── Stagione {stagione_label} "
              f"(W46/{anno_inizio} → W15/{anno_fine}) ──")

        # Calcola la finestra date della stagione per report
        df_cal = calcola_settimane_stagione(anno_inizio, anno_fine)
        print(f"     Settimane previste: {len(df_cal)} "
              f"(dalla W{df_cal['settimana_iso'].iloc[0]:02d}/{df_cal['anno_iso'].iloc[0]} "
              f"alla W{df_cal['settimana_iso'].iloc[-1]:02d}/{df_cal['anno_iso'].iloc[-1]})")

        for ats_label, ats_dir_label in ATS_LABELS.items():
            out_folder = out_bg if "BERGAMO" in ats_dir_label else out_mt

            for param_full, param_short in PARAMETRI_MAP.items():

                df_stagione = processa_stagione_ats_parametro(
                    df_arpa         = df_arpa,
                    stagione_label  = stagione_label,
                    anno_inizio     = anno_inizio,
                    anno_fine       = anno_fine,
                    ats_label       = ats_label,
                    parametro       = param_full,
                    parametro_short = param_short,
                )

                # Conteggio settimane con dati validi
                n_valid = df_stagione["media_comunale"].notna().sum()
                n_total = len(df_stagione)
                print(f"     {ats_label} / {param_short}: "
                      f"{n_valid}/{n_total} settimane con dati "
                      f"(min comuni threshold: {MIN_COMUNI_PER_ATS.get(ats_label, 3)})")

                # Salvataggio CSV per stagione
                fname = f"{param_short}_{ats_dir_label}_stagione_{stagione_label}.csv"
                fpath = os.path.join(out_folder, fname)
                df_stagione.to_csv(fpath, index=False, float_format="%.4f")

                # Accumula per i file COMBINED
                if "BERGAMO" in ats_dir_label:
                    all_results_bg.append(df_stagione)
                else:
                    all_results_mt.append(df_stagione)

    # ── File COMBINED (tutte le stagioni e parametri) ────────
    print("\n" + "─" * 65)
    print("STEP 4: Creazione file COMBINED")
    print("─" * 65)

    for label, all_results in [("ATS_Bergamo", all_results_bg),
                                ("ATS_Montagna", all_results_mt)]:
        if not all_results:
            continue
        df_combined = pd.concat(all_results, ignore_index=True)

        # Ordina per parametro, stagione, ordine (utile per analisi)
        df_combined = df_combined.sort_values(
            ["parametro", "stagione", "ordine"]
        ).reset_index(drop=True)

        lbl_short = label.replace("ATS_", "").lower()
        fname = f"tutti_parametri_{lbl_short}.csv"
        fpath = os.path.join(out_cb, fname)
        df_combined.to_csv(fpath, index=False, float_format="%.4f")
        print(f"  ✓ {fname}: {len(df_combined):,} righe")

    # ── Riepilogo finale ─────────────────────────────────────
    print("\n" + "=" * 65)
    print("✅ 02_Seasons.py COMPLETATO")
    print("=" * 65)
    print(f"\nOutput salvato in: {os.path.abspath(OUTPUT_DIR)}/")

    for folder, label in [(out_bg, "ATS_BERGAMO"),
                          (out_mt, "ATS_MONTAGNA"),
                          (out_cb, "COMBINED")]:
        files = sorted(os.listdir(folder)) if os.path.isdir(folder) else []
        print(f"\n  {label}/ ({len(files)} file):")
        for f in files:
            fpath = os.path.join(folder, f)
            size_kb = os.path.getsize(fpath) / 1024
            print(f"    └─ {f}  ({size_kb:.1f} KB)")

    print("\n" + "─" * 65)
    print("NOTE PER L'ANALISI DOWNSTREAM:")
    print("─" * 65)
    print("""
  1. I file COMBINED/ contengono tutte le stagioni concatenate.
     Per il join con i dati ILI, usa:
         df.merge(df_ili, on=['stagione', 'week_label'])

  2. La colonna 'ordine' (1, 2, ...) identifica la posizione
     progressiva della settimana nella stagione. Usa questa
     colonna per l'asse X nei grafici.

  3. Le righe con n_comuni < MIN_COMUNI_PER_ATS hanno
     media_comunale = NaN. Prima dell'analisi, valuta se
     imputare (es. interpolazione lineare) o escludere queste
     settimane.

  4. Prima di calcolare correlazioni con i dati ILI, considera
     di de-stagionalizzare entrambe le serie o di usare modelli
     che includano la settimana dell'anno come covariate
     (per evitare correlazioni spurie da stagionalità comune).

  5. BIAS ECOLOGICO: tutte le inferenze si applicano a livello
     di ATS, NON individuale. Non estrarre conclusioni causali
     a livello di singolo paziente.
  """)


# ═══════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()