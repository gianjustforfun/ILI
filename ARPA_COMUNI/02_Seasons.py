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

Per la PARTE SPAZIALE sono implementati tre approcci:

    1. MEDIA SEMPLICE (sempre calcolata):
       Ogni comune pesa uguale indipendentemente da dimensione
       o popolazione. Semplice e robusta, ma sensibile alla
       distribuzione non uniforme dei sensori ARPA.

    2. MEDIA PONDERATA PER POPOLAZIONE (USE_POPULATION_WEIGHTS):
       Ogni comune pesa in proporzione alla sua popolazione
       residente (dati ISTAT). Preferibile per outcome di
       salute pubblica perché stima l'esposizione effettiva
       della popolazione. Richiede i CSV ISTAT per comune.

    3. MEDIA PONDERATA PER AREA (USE_AREA_WEIGHTS):
       Ogni comune pesa in proporzione alla sua superficie
       geografica (da shapefile ISTAT Com01012026_g_WGS84).
       Stima la concentrazione media sul territorio dell'ATS.
       Appropriata per analisi ambientali territoriali.
       ATTENZIONE: in aree con forte gradiente
       urbano/montano (es. ATS Bergamo), tende a sotto-stimare
       l'esposizione della popolazione rispetto alla media
       pop-ponderata (i grandi comuni montani pesano molto
       ma hanno poca popolazione).

    CONFRONTO TRA APPROCCI:
    Confrontare le tre medie permette di quantificare
    l'eterogeneità spaziale dell'esposizione nell'ATS.
    Se le tre convergono, la distribuzione è uniforme.
    Se divergono, esiste un gradiente geografico rilevante
    (es. pianura più inquinata della montagna).

=============================================================
BIAS E LIMITAZIONI — LEGGERE PRIMA DI USARE I RISULTATI
=============================================================

1. BIAS ECOLOGICO (aggregation bias):
   I dati sono aggregati a livello di ATS, NON individuale.
   Non è possibile trarre conclusioni sulle associazioni a
   livello individuale (ecological fallacy di Robinson, 1950).

2. COPERTURA SPAZIALE ETEROGENEA DEI SENSORI ARPA:
   Non tutti i comuni hanno un sensore attivo. La media viene
   calcolata solo sui comuni con almeno una misura valida.

3. FINESTRA TEMPORALE BREVE:
   Massimo 5 stagioni (2021-2026). Correlazioni con p<0.05
   devono essere interpretate con estrema cautela.

4. STAGIONALITÀ CONFONDENTE:
   Sia PM/NO2 sia ILI hanno un forte pattern stagionale
   invernale. De-stagionalizzare o controllare per settimana.

5. MISSING DATA:
   Settimane con meno di MIN_COMUNI_PER_ATS comuni validi
   vengono marcate come NaN nel CSV di output.

=============================================================
REQUISITI
=============================================================

    pip install pandas numpy geopandas dbfread

geopandas e dbfread sono necessari solo se USE_AREA_WEIGHTS=True.

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

INPUT_DIR  = "dati_arpa_output"
OUTPUT_DIR = "seasons_output"

ISTAT_DIR_MONTAGNA = "../ISTAT/ATS MONTAGNA"
ISTAT_DIR_BERGAMO  = "../ISTAT/ATS BERGAMO"

# Percorso allo shapefile ISTAT dei comuni italiani.
# Relativo alla cartella ILI/ARPA_COMUNI/ (da cui gira lo script).
SHAPEFILE_COMUNI_PATH = "../COPERNICUS/data/raw/Com01012026_g_WGS84.shp"

# ── Parametri di aggregazione ────────────────────────────────

# True  → media ponderata per popolazione (richiede dati ISTAT)
# False → salta (default robusto)
USE_POPULATION_WEIGHTS = True

# True  → aggiunge colonna 'area_weight_media' (richiede shapefile)
# False → salta
USE_AREA_WEIGHTS = True

# Numero minimo di comuni con dati validi per considerare
# affidabile la media settimanale di una ATS.
MIN_COMUNI_PER_ATS = {
    "ATS_Bergamo":  5,
    "ATS_Montagna": 3,
}

# ── Definizione delle stagioni influenzali ────────────────────

STAGIONI = {
    "21-22": (2021, 2022),
    "22-23": (2022, 2023),
    "23-24": (2023, 2024),
    "24-25": (2024, 2025),
    "25-26": (2025, 2026),
}

# ── Mapping nomi parametri → etichette file ───────────────────

PARAMETRI_MAP = {
    "PM10":                       "PM10",
    "Particelle sospese PM2.5":   "PM25",
    "Biossido di Azoto":          "NO2",
}

# ── Etichette ATS ────────────────────────────────────────────

ATS_LABELS = {
    "ATS_Bergamo":  "ATS_BERGAMO",
    "ATS_Montagna": "ATS_MONTAGNA",
}

# Mappatura ATS → codici provincia ISTAT (COD_PROV nel DBF).
# ATS Montagna = None perché comprende subset di più province;
# il filtraggio avviene sul nome comune già etichettato nel
# dataset ARPA come 'ATS_Montagna'.
ATS_PROV_CODES = {
    "ATS_Bergamo":  [16],   # BG
    "ATS_Montagna": None,   # SO(14) + subset BS(17) + subset CO(13)
}


# ═══════════════════════════════════════════════════════════
# SEZIONE 2 — CACHE GLOBALE SHAPEFILE
# ═══════════════════════════════════════════════════════════

# Il shapefile viene caricato una sola volta e tenuto in cache
# per evitare I/O ripetuti nel loop stagioni × ATS × parametri.
_SHAPEFILE_CACHE = {}


# ═══════════════════════════════════════════════════════════
# SEZIONE 3 — FUNZIONI DI SUPPORTO
# ═══════════════════════════════════════════════════════════

def carica_tutti_i_dati(input_dir: str) -> pd.DataFrame:
    """
    Carica e unisce tutti i file CSV delle stime comunali ARPA.

    Cerca tutti i file .csv nella cartella input_dir il cui
    nome inizia con 'stime_comunali_', li concatena in un
    unico DataFrame e normalizza le colonne principali.

    Returns:
        pd.DataFrame con tutti i dati unificati, oppure
        DataFrame vuoto se nessun file è trovato.
    """
    pattern = "stime_comunali_"
    files = sorted([
        f for f in os.listdir(input_dir)
        if f.endswith(".csv") and f.startswith(pattern)
        and "FINALE" not in f
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

    df_all = pd.concat(dfs, ignore_index=True)

    # ── Normalizzazione ──────────────────────────────────────
    df_all["data"]   = pd.to_datetime(df_all["data"], errors="coerce")
    df_all["valore"] = pd.to_numeric(df_all["valore"], errors="coerce")
    df_all.loc[df_all["valore"] < 0, "valore"] = np.nan

    df_all["parametro"] = df_all["parametro"].astype(str).str.strip()
    if "ats" in df_all.columns:
        df_all["ats"] = df_all["ats"].astype(str).str.strip()
    if "comune" in df_all.columns:
        df_all["comune"] = df_all["comune"].astype(str).str.strip().str.title()
    if "provincia" in df_all.columns:
        df_all["provincia"] = df_all["provincia"].astype(str).str.strip().str.upper()

    n_prima = len(df_all)
    df_all = df_all.dropna(subset=["data", "valore"])
    n_dopo = len(df_all)
    if n_prima > n_dopo:
        print(f"  Rimosse {n_prima - n_dopo:,} righe con data/valore nullo.")

    n_prima = len(df_all)
    df_all = df_all.drop_duplicates(subset=["data", "comune", "parametro"], keep="first")
    n_dopo = len(df_all)
    if n_prima > n_dopo:
        print(f"  Rimossi {n_prima - n_dopo:,} duplicati.")

    print(f"\n  Dataset unificato: {len(df_all):,} righe")
    print(f"  Periodo: {df_all['data'].min().date()} → {df_all['data'].max().date()}")
    print(f"  Parametri: {sorted(df_all['parametro'].unique())}")
    if "ats" in df_all.columns:
        print(f"  ATS: {sorted(df_all['ats'].unique())}")

    return df_all


def assegna_settimana_iso(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggiunge colonne settimana ISO al DataFrame.

    Le settimane ISO (ISO 8601) sono il riferimento standard
    nei sistemi di sorveglianza epidemiologica (InfluNet, ECDC).
    ATTENZIONE: l'anno ISO può differire dall'anno solare
    (es. 2022-01-01 → anno ISO 2021, settimana 52).

    Colonne aggiunte:
        - anno_iso:      anno ISO della settimana
        - settimana_iso: numero settimana ISO (1-53)
        - week_label:    etichetta 'WXX/YYYY' (es. 'W46/2021')
    """
    df = df.copy()
    df["anno_iso"]      = df["data"].dt.isocalendar().year.astype(int)
    df["settimana_iso"] = df["data"].dt.isocalendar().week.astype(int)
    df["week_label"] = (
        "W" + df["settimana_iso"].astype(str).str.zfill(2)
        + "/" + df["anno_iso"].astype(str)
    )
    return df


def calcola_settimane_stagione(anno_inizio: int, anno_fine: int) -> pd.DataFrame:
    """
    Costruisce la sequenza ordinata di settimane ISO di una stagione.

    Definizione stagione influenzale:
        W46 dell'anno_inizio → W15 dell'anno_fine (inclusi).

    Returns:
        pd.DataFrame con colonne:
            anno_iso, settimana_iso, week_label, ordine
    """
    date_inizio = pd.Timestamp(f"{anno_inizio}-11-01")
    date_fine   = pd.Timestamp(f"{anno_fine}-04-30")

    all_days = pd.date_range(date_inizio, date_fine, freq="D")
    df_days  = pd.DataFrame({"data": all_days})
    df_days["anno_iso"]      = df_days["data"].dt.isocalendar().year.astype(int)
    df_days["settimana_iso"] = df_days["data"].dt.isocalendar().week.astype(int)
    df_days["week_label"] = (
        "W" + df_days["settimana_iso"].astype(str).str.zfill(2)
        + "/" + df_days["anno_iso"].astype(str)
    )

    mask_inizio = (df_days["anno_iso"] == anno_inizio) & (df_days["settimana_iso"] >= 46)
    mask_fine   = (df_days["anno_iso"] == anno_fine)   & (df_days["settimana_iso"] <= 15)
    df_stagione = df_days[mask_inizio | mask_fine].copy()

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
    Carica i pesi demografici comunali dai CSV ISTAT.

    Struttura attesa su disco:
        ATS BERGAMO/
            Popolazione residente_ATS_Bergamo_XXXX.csv
        ATS MONTAGNA/
            BRESCIA/   → CSV con comuni BS
            COMO/      → CSV con comuni CO
            SONDRIO/   → CSV con comuni SO

    Colonne attese nei CSV: 'Comune', 'Totale'
    """
    pesi = {}

    if ats_label == "ATS_Bergamo":
        folder = ISTAT_DIR_BERGAMO
        if not os.path.isdir(folder):
            print(f"  ⚠ Cartella ISTAT Bergamo non trovata: {folder}")
            return pesi

        # Cerca il file che contiene l'anno nel nome
        fpath = None
        for fname in os.listdir(folder):
            if fname.endswith(".csv") and str(anno) in fname:
                fpath = os.path.join(folder, fname)
                break

        if fpath is None:
            print(f"  ⚠ Nessun file ISTAT Bergamo per anno {anno}")
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
            print(f"    Pop-weights Bergamo: {len(pesi)} comuni da {os.path.basename(fpath)}")
        except Exception as e:
            print(f"  ⚠ Errore nel caricare ISTAT Bergamo: {e}")

    elif ats_label == "ATS_Montagna":
        # ATS Montagna ha tre sottocartelle: BRESCIA, COMO, SONDRIO
        # Le leggiamo tutte e uniamo i dizionari
        sottocartelle = ["BRESCIA", "COMO", "SONDRIO"]
        tot_trovati = 0

        for sub in sottocartelle:
            folder_sub = os.path.join(ISTAT_DIR_MONTAGNA, sub)
            if not os.path.isdir(folder_sub):
                print(f"  ⚠ Sottocartella non trovata: {folder_sub}")
                continue

            # Cerca il file con l'anno nel nome
            fpath = None
            for fname in os.listdir(folder_sub):
                if fname.endswith(".csv") and str(anno) in fname:
                    fpath = os.path.join(folder_sub, fname)
                    break

            if fpath is None:
                print(f"  ⚠ Nessun file ISTAT {sub} per anno {anno}")
                continue

            try:
                df_pop = pd.read_csv(fpath)
                if "Comune" not in df_pop.columns or "Totale" not in df_pop.columns:
                    print(f"  ⚠ Colonne 'Comune'/'Totale' mancanti in {fpath}")
                    continue
                n_prima = len(pesi)
                for _, row in df_pop.iterrows():
                    nome = str(row["Comune"]).strip().title()
                    pop  = row["Totale"]
                    if pd.notna(pop) and pop > 0:
                        pesi[nome] = int(pop)
                tot_trovati += len(pesi) - n_prima
                print(f"    Pop-weights {sub}: {len(pesi) - n_prima} comuni da {os.path.basename(fpath)}")
            except Exception as e:
                print(f"  ⚠ Errore nel caricare ISTAT {sub}: {e}")

        print(f"    Pop-weights Montagna totale: {len(pesi)} comuni (BS+CO+SO)")

    return pesi


def _normalizza_nome_comune(nome: str) -> str:
    """
    Normalizza un nome comune per il join tra dataset ARPA e shapefile ISTAT.

    Gestisce differenze tipografiche comuni:
        - maiuscole/minuscole
        - apostrofi e accenti
        - trattini
        - spazi multipli

    Esempio:
        "Calusco D'Adda" → "calusco d adda"
        "Ala di Stura"   → "ala di stura"
    """
    return (
        str(nome)
        .strip()
        .lower()
        .replace("'", " ")
        .replace("`", " ")
        .replace("-", " ")
        .replace("  ", " ")
        .strip()
    )


def carica_pesi_area(ats_label: str, comuni_nel_dataset: list) -> dict:
    """
    Estrae i pesi per area comunale dallo shapefile ISTAT.

    Per ogni comune presente nel dataset ARPA per quella ATS,
    recupera l'area in m² dal campo Shape_Area dello shapefile.

    FORMULA USATA IN aggrega_settimana_ats():
        area_weight_media = Σ_c(valore_c × Area_c) / Σ_c(Area_c)

    Equivalente all'equazione 8.6 di Gianquintieri (APHREH-ADSMap):
        EX_BSA = Σ_c(EX_c · Area_c∩BSA) / Area_BSA
    dove si assume che l'intera area di ogni comune rientri
    nell'ATS (assunzione valida per ATS Bergamo = provincia BG
    e per ATS Montagna con i comuni già etichettati nel dataset).

    STRATEGIA DI JOIN:
    Join su nome normalizzato (lower, strip, no apostrofi).
    Se un comune non viene trovato in modo esatto, si tenta
    un match parziale (nome ARPA contenuto nel nome ISTAT o
    viceversa). I comuni non trovati ricevono peso 0
    (esclusi dalla media) e vengono segnalati con un avviso.

    NOTA SUL BIAS:
    La ponderazione per area è meno appropriata della
    ponderazione per popolazione per outcome di salute pubblica,
    perché i grandi comuni montani (poca popolazione, molta area)
    possono distorcere la media. Confrontare sempre le due
    colonne (area_weight_media vs pop_weight_media) per
    quantificare questo effetto.

    Args:
        ats_label:          'ATS_Bergamo' o 'ATS_Montagna'
        comuni_nel_dataset: lista nomi comuni dal dataset ARPA

    Returns:
        dict {nome_comune: area_m2} oppure {} se non disponibile.
    """
    global _SHAPEFILE_CACHE

    if not USE_AREA_WEIGHTS:
        return {}

    shp_path = SHAPEFILE_COMUNI_PATH
    if not os.path.exists(shp_path):
        print(f"  ⚠ Shapefile non trovato: {shp_path}")
        print(f"    Aggiorna SHAPEFILE_COMUNI_PATH in questo script.")
        return {}

    # ── Carica shapefile con cache ───────────────────────────
    if shp_path not in _SHAPEFILE_CACHE:
        try:
            import geopandas as gpd
        except ImportError:
            print("  ⚠ geopandas non installato. Eseguire:")
            print("    pip install geopandas --break-system-packages")
            return {}

        try:
            gdf = gpd.read_file(shp_path, encoding='utf-8')

            # Se geopandas non carica gli attributi dal DBF,
            # li legge separatamente con dbfread
            required = ['COMUNE', 'Shape_Area']
            missing_cols = [c for c in required if c not in gdf.columns]
            if missing_cols:
                try:
                    from dbfread import DBF
                    dbf_path = shp_path.replace('.shp', '.dbf')
                    df_attr = pd.DataFrame(iter(DBF(dbf_path, encoding='utf-8')))
                    for col in required:
                        if col in df_attr.columns:
                            gdf[col] = df_attr[col].values
                    missing_cols = [c for c in required if c not in gdf.columns]
                except Exception as e:
                    print(f"  ⚠ Errore lettura DBF con dbfread: {e}")

            if missing_cols:
                print(f"  ⚠ Colonne mancanti nel shapefile: {missing_cols}")
                return {}

            _SHAPEFILE_CACHE[shp_path] = gdf
            print(f"  ✓ Shapefile caricato: {len(gdf):,} comuni italiani")

        except Exception as e:
            print(f"  ⚠ Errore nel caricare lo shapefile: {e}")
            return {}

    gdf = _SHAPEFILE_CACHE[shp_path]

    # ── Filtra per provincia se disponibile ─────────────────
    prov_codes = ATS_PROV_CODES.get(ats_label)
    if prov_codes is not None and 'COD_PROV' in gdf.columns:
        gdf_ats = gdf[gdf['COD_PROV'].isin(prov_codes)].copy()
    else:
        # ATS Montagna: nessun filtro su provincia, si usa il join
        # per nome sui comuni già etichettati nel dataset ARPA
        gdf_ats = gdf.copy()

    # ── Dizionario nome_normalizzato → area ──────────────────
    gdf_ats = gdf_ats.copy()
    gdf_ats['_nome_norm'] = gdf_ats['COMUNE'].apply(_normalizza_nome_comune)
    istat_area = dict(zip(gdf_ats['_nome_norm'], gdf_ats['Shape_Area']))

    # ── Join con i comuni del dataset ARPA ───────────────────
    pesi = {}
    non_trovati = []

    for comune in comuni_nel_dataset:
        nome_norm = _normalizza_nome_comune(comune)
        area = istat_area.get(nome_norm)

        if area is None or area <= 0:
            # Tenta match parziale
            candidati = [
                (k, v) for k, v in istat_area.items()
                if nome_norm in k or k in nome_norm
            ]
            if len(candidati) == 1:
                area = candidati[0][1]
            else:
                non_trovati.append(comune)
                continue

        pesi[comune] = float(area)

    if non_trovati:
        print(f"    ⚠ {len(non_trovati)} comuni ARPA non trovati nello shapefile "
              f"(esclusi da area_weight_media):")
        for c in non_trovati[:10]:
            print(f"        '{c}'")
        if len(non_trovati) > 10:
            print(f"        ... e altri {len(non_trovati)-10}")
        print(f"    → Aggiungere mapping manuale in NOME_ARPA_TO_ISTAT se necessario.")

    print(f"    Area-weights: {len(pesi)}/{len(comuni_nel_dataset)} comuni "
          f"con area da shapefile ISTAT")
    return pesi


def aggrega_settimana_ats(
    df_settimana: pd.DataFrame,
    ats_label:    str,
    pesi_pop:     dict,
    pesi_area:    dict,
    min_comuni:   int
) -> dict:
    """
    Calcola la concentrazione media a livello ATS per una
    singola settimana e un singolo parametro.

    APPROCCIO METODOLOGICO — Media Spaziale (two-step averaging):

        STEP 1: Media giornaliera per comune (già nei dati ARPA).
        STEP 2: Media settimanale per comune (media dei giorni).
        STEP 3: Aggregazione tra comuni (tre varianti):
            a) Media semplice: tutti i comuni pesano uguale.
            b) Pop-weighted:   peso = popolazione residente.
            c) Area-weighted:  peso = superficie geografica (m²).

    Args:
        df_settimana: DataFrame filtrato per settimana e ATS,
                      con colonne 'comune' e 'valore'
        ats_label:    stringa ATS (per log)
        pesi_pop:     dict {comune: popolazione} (può essere {})
        pesi_area:    dict {comune: area_m2}     (può essere {})
        min_comuni:   soglia minima comuni validi

    Returns:
        dict con statistiche aggregate della settimana.
    """
    # Media settimanale per comune
    media_per_comune = (
        df_settimana
        .groupby("comune")["valore"]
        .mean()
        .dropna()
    )

    n_comuni = len(media_per_comune)

    if n_comuni < min_comuni:
        return {
            "n_comuni":          n_comuni,
            "media_comunale":    np.nan,
            "std_comunale":      np.nan,
            "min_comunale":      np.nan,
            "max_comunale":      np.nan,
            "pop_weight_media":  np.nan,
            "area_weight_media": np.nan,
        }

    valori = media_per_comune.values
    comuni = media_per_comune.index.tolist()

    media_semplice = float(np.mean(valori))
    std_comunale   = float(np.std(valori, ddof=1)) if n_comuni > 1 else np.nan
    min_comunale   = float(np.min(valori))
    max_comunale   = float(np.max(valori))

    # ── Media ponderata per popolazione ──────────────────────
    pop_weight_media = np.nan
    if USE_POPULATION_WEIGHTS and pesi_pop:
        pesi_v = np.array([pesi_pop.get(c, 0) for c in comuni], dtype=float)
        if pesi_v.sum() > 0:
            pop_weight_media = float(np.average(valori, weights=pesi_v))

    # ── Media ponderata per area ──────────────────────────────
    #
    # FORMULA:
    #   area_weight_media = Σ_c(valore_c × Area_c) / Σ_c(Area_c)
    #
    # I comuni senza area nel dizionario ricevono peso 0
    # e sono esclusi dalla media (avviso già stampato da
    # carica_pesi_area() a inizio stagione, non ripetiamo qui).
    #
    area_weight_media = np.nan
    if USE_AREA_WEIGHTS and pesi_area:
        pesi_a = np.array([pesi_area.get(c, 0) for c in comuni], dtype=float)
        if pesi_a.sum() > 0:
            area_weight_media = float(np.average(valori, weights=pesi_a))

    return {
        "n_comuni":          n_comuni,
        "media_comunale":    media_semplice,
        "std_comunale":      std_comunale,
        "min_comunale":      min_comunale,
        "max_comunale":      max_comunale,
        "pop_weight_media":  pop_weight_media,
        "area_weight_media": area_weight_media,
    }


def processa_stagione_ats_parametro(
    df_arpa:         pd.DataFrame,
    stagione_label:  str,
    anno_inizio:     int,
    anno_fine:       int,
    ats_label:       str,
    parametro:       str,
    parametro_short: str,
) -> pd.DataFrame:
    """
    Produce il DataFrame settimanale per una combinazione
    (stagione, ATS, parametro).

    Flusso:
        1. Calcola le settimane ISO della stagione.
        2. Filtra df_arpa per ATS e parametro.
        3. Assegna settimana ISO al subset filtrato.
        4. Carica pesi demografici (se USE_POPULATION_WEIGHTS).
        5. Carica pesi area (se USE_AREA_WEIGHTS).
        6. Per ogni settimana della stagione, aggrega i comuni.
        7. Restituisce DataFrame ordinato per ORDINE.

    Le settimane senza dati vengono incluse con valori NaN
    per mantenere la stessa struttura in tutte le stagioni
    (facilita il join con i dati sanitari ILI).
    """
    # Step 1: Calendario della stagione
    df_settimane_stagione = calcola_settimane_stagione(anno_inizio, anno_fine)

    # Step 2: Filtraggio per ATS e parametro
    df_subset = df_arpa[
        (df_arpa["ats"] == ats_label) &
        (df_arpa["parametro"] == parametro)
    ].copy()

    if df_subset.empty:
        print(f"    ⚠ Nessun dato per {ats_label} / {parametro}")
        df_vuoto = df_settimane_stagione.copy()
        df_vuoto.insert(0, "stagione", stagione_label)
        df_vuoto.insert(0, "ats", ats_label)
        df_vuoto["parametro"] = parametro_short
        for col in ["n_comuni", "media_comunale", "std_comunale",
                    "min_comunale", "max_comunale",
                    "pop_weight_media", "area_weight_media"]:
            df_vuoto[col] = np.nan
        return df_vuoto

    # Step 3: Assegna settimana ISO
    df_subset = assegna_settimana_iso(df_subset)

    # Step 4: Carica pesi demografici (popolazione)
    pesi_pop = {}
    if USE_POPULATION_WEIGHTS:
        pesi_pop = carica_pesi_popolazione(ats_label, anno_fine)
        if not pesi_pop:
            print(f"    ⚠ Pesi pop non disponibili per {ats_label} {anno_fine}."
                  f" Uso media semplice.")

    # Step 5: Carica pesi area (shapefile ISTAT)
    pesi_area = {}
    if USE_AREA_WEIGHTS:
        comuni_presenti = df_subset["comune"].unique().tolist()
        pesi_area = carica_pesi_area(ats_label, comuni_presenti)

    # Step 6: Aggregazione per ogni settimana
    min_comuni = MIN_COMUNI_PER_ATS.get(ats_label, 3)
    righe = []

    for _, row_sett in df_settimane_stagione.iterrows():
        anno_iso = int(row_sett["anno_iso"])
        sett_iso = int(row_sett["settimana_iso"])
        ordine   = int(row_sett["ordine"])
        wlabel   = row_sett["week_label"]

        mask_sett = (
            (df_subset["anno_iso"]      == anno_iso) &
            (df_subset["settimana_iso"] == sett_iso)
        )
        df_sett = df_subset[mask_sett]

        stats = aggrega_settimana_ats(
            df_settimana = df_sett,
            ats_label    = ats_label,
            pesi_pop     = pesi_pop,
            pesi_area    = pesi_area,
            min_comuni   = min_comuni,
        )

        righe.append({
            "stagione":      stagione_label,
            "ats":           ats_label,
            "parametro":     parametro_short,
            "anno_iso":      anno_iso,
            "settimana_iso": sett_iso,
            "week_label":    wlabel,
            "ordine":        ordine,
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
    if USE_AREA_WEIGHTS:
        cols_base.append("area_weight_media")

    df_out = df_out[[c for c in cols_base if c in df_out.columns]]

    return df_out


# ═══════════════════════════════════════════════════════════
# SEZIONE 4 — MAIN
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

    # ── Verifica shapefile ───────────────────────────────────
    if USE_AREA_WEIGHTS:
        if os.path.exists(SHAPEFILE_COMUNI_PATH):
            print(f"\n  ✓ Shapefile trovato: {SHAPEFILE_COMUNI_PATH}")
        else:
            print(f"\n  ⚠ Shapefile NON trovato: {SHAPEFILE_COMUNI_PATH}")
            print(f"    USE_AREA_WEIGHTS verrà ignorato per questa esecuzione.")

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

    if "ats" not in df_arpa.columns:
        print("\n  ERRORE: colonna 'ats' mancante nel dataset.")
        print("  Verifica che 01_download_arpa.py abbia prodotto questa colonna.")
        sys.exit(1)

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

    all_results_bg = []
    all_results_mt = []

    for stagione_label, (anno_inizio, anno_fine) in STAGIONI.items():
        print(f"\n  ── Stagione {stagione_label} "
              f"(W46/{anno_inizio} → W15/{anno_fine}) ──")

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

                n_valid = df_stagione["media_comunale"].notna().sum()
                n_total = len(df_stagione)
                print(f"     {ats_label} / {param_short}: "
                      f"{n_valid}/{n_total} settimane con dati")

                fname = f"{param_short}_{ats_dir_label}_stagione_{stagione_label}.csv"
                fpath = os.path.join(out_folder, fname)
                df_stagione.to_csv(fpath, index=False, float_format="%.4f")

                if "BERGAMO" in ats_dir_label:
                    all_results_bg.append(df_stagione)
                else:
                    all_results_mt.append(df_stagione)

    # ── File COMBINED ────────────────────────────────────────
    print("\n" + "─" * 65)
    print("STEP 4: Creazione file COMBINED")
    print("─" * 65)

    for label, all_results in [("ATS_Bergamo", all_results_bg),
                                ("ATS_Montagna", all_results_mt)]:
        if not all_results:
            continue
        df_combined = pd.concat(all_results, ignore_index=True)
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
    print("COLONNE NEI CSV DI OUTPUT:")
    print("─" * 65)
    print("""
  - stagione:          etichetta stagione (es. '22-23')
  - ats:               'ATS_Bergamo' o 'ATS_Montagna'
  - parametro:         'PM10', 'PM25', 'NO2'
  - anno_iso:          anno ISO della settimana
  - settimana_iso:     numero settimana ISO (1-53)
  - week_label:        etichetta leggibile (es. 'W46/2021')
  - ordine:            posizione progressiva nella stagione (1, 2, ...)
  - n_comuni:          comuni con dati validi nella settimana
  - media_comunale:    media semplice tra comuni (µg/m³)
  - std_comunale:      deviazione standard tra comuni (variabilità spaziale)
  - min_comunale:      minimo tra i comuni
  - max_comunale:      massimo tra i comuni
  - area_weight_media: media ponderata per area comunale (µg/m³)
                       [solo se USE_AREA_WEIGHTS=True]
  - pop_weight_media:  media ponderata per popolazione (µg/m³)
                       [solo se USE_POPULATION_WEIGHTS=True]

  NOTE PER L'ANALISI:
  - Usa 'ordine' come asse X nei grafici per confrontare stagioni.
  - Join con dati ILI: df.merge(df_ili, on=['stagione', 'week_label'])
  - Righe con n_comuni < MIN_COMUNI_PER_ATS → media_comunale = NaN.
  - Confronta media_comunale vs area_weight_media per quantificare
    l'eterogeneità spaziale dell'esposizione nell'ATS.
  - BIAS ECOLOGICO: inferenze solo a livello ATS, mai individuale.
  """)


# ═══════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()