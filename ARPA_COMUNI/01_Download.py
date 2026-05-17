"""
=============================================================
SCRIPT — DOWNLOAD STIME COMUNALI ARPA LOMBARDIA
=============================================================

COSA FA QUESTO SCRIPT:
    Scarica le stime giornaliere comunali di qualità dell'aria
    (PM10, PM2.5, NO2) dall'API di ARPA Lombardia (portale
    dati.lombardia.it / Socrata) per i territori delle ATS di
    interesse, le filtra e le salva in CSV annuali pronti per
    le analisi successive.

    Questo è il PRIMO PASSO per costruire il dataset ambientale
    da correlare con i dati ILI delle ATS.

TERRITORI COPERTI:
    ┌──────────────────┬───────────────────────────────────────┐
    │ ATS              │ Territori                             │
    ├──────────────────┼───────────────────────────────────────┤
    │ ATS Bergamo      │ Intera provincia di Bergamo (BG)      │
    │                  │ 243 comuni                            │
    ├──────────────────┼───────────────────────────────────────┤
    │ ATS Brianza      │ Intera provincia di Monza-Brianza (MB)│
    │  [NUOVO]         │ + Intera provincia di Lecco (LC)      │
    ├──────────────────┼───────────────────────────────────────┤
    │ ATS Montagna     │ Intera provincia di Sondrio (SO)      │
    │                  │ + 41 comuni selezionati di Brescia    │
    │                  │ + 16 comuni selezionati di Como       │
    └──────────────────┴───────────────────────────────────────┘

PARAMETRI SCARICATI:
    - PM10                        (particolato fine)
    - Particelle sospese PM2.5    (particolato ultrafine)
    - Biossido di Azoto (NO2)     (inquinante combustione)

    Per ciascun parametro viene scaricato solo l'operatore
    idoperatore=1 (media giornaliera comunale).

ANNI COPERTI:
    2021, 2022, 2023, 2024, 2025-2026 (dataset corrente)

STRATEGIA DI DOWNLOAD (perché è fatta così):
    L'API Socrata di dati.lombardia.it ha un bug/limitazione:
    il filtro $where su 'idsensore' produce un errore di tipo
    ("Type mismatch") sia con IN() sia con =, sia passando il
    valore come numero sia come stringa. Tentare di filtrare
    lato server rompe la query.

    Soluzione adottata: download completo pagina per pagina
    (PAGE_SIZE = 50.000 righe / richiesta), filtro immediato
    in pandas prima di accumulare i dati. In questo modo:
      - Zero problemi di tipo con l'API
      - Memoria controllata (le righe inutili vengono scartate
        subito, prima di essere accumulate)
      - Riproducibilità garantita (nessuna dipendenza da
        comportamenti API non documentati)

    Volume stimato da scaricare: ~13M righe per 4 anni
    (~300-500 MB), poi ridotti a ~500k righe (~15 MB) dopo
    il filtro territoriale. Tempo stimato: 10-20 min/anno.

FILE DI OUTPUT (cartella dati_arpa_output/):
    stime_comunali_2021.csv
    stime_comunali_2022.csv
    stime_comunali_2023.csv
    stime_comunali_2024.csv
    stime_comunali_2025-2026.csv
    anagrafica_sensori.csv       (cache anagrafica, scaricata una volta)
    check_comuni_giorni_XXXX.csv (report di completezza per anno)

STRUTTURA DELLE COLONNE NEI FILE DI OUTPUT:
    data          → data di riferimento (datetime)
    idsensore     → ID sensore ARPA originale
    valore        → concentrazione media giornaliera (µg/m³)
    idoperatore   → sempre 1 (media giornaliera)
    parametro     → nome del parametro (PM10, PM2.5, NO2)
    unitamisura   → unità di misura
    provincia     → sigla provincia (BG, LC, MB, SO, BS, CO)
    comune        → nome del comune
    ats           → ATS di appartenenza (ATS_Bergamo, ATS_Brianza,
                    ATS_Montagna)
    anno_dataset  → etichetta anno del dataset sorgente

CHECK DI COMPLETEZZA (funzione check_comuni_e_giorni_per_anno):
    Al termine del download verifica:
      - Quanti comuni unici per provincia/parametro sono presenti
        (confrontati con i valori attesi)
      - Se ogni comune ha dati per tutti i giorni del periodo
      - Salva un CSV di dettaglio con la percentuale di completezza
        per ogni comune + parametro

BIAS E LIMITAZIONI DA TENERE A MENTE:
    - Bias ecologico: i dati sono medie comunali (stima areale),
      non misure su stazioni fisse. Le medie areali attenuano i
      picchi locali e non riflettono l'esposizione individuale.
    - Finestra temporale: 5 anni (2021-2026). Correlazioni con
      ILI su serie così corte hanno elevato rischio di spurious
      correlation. Interpretare con cautela.
    - Copertura sensori: non tutti i comuni hanno sensori attivi
      per tutti i parametri; i comuni con dati mancanti appaiono
      nel report di completezza. Prima di qualsiasi analisi,
      verificare la copertura per il proprio territorio.
    - Sensori dismessi: i sensori con storico='S' vengono
      automaticamente esclusi e elencati a schermo. Se un comune
      non appare nel dataset finale, potrebbe avere solo sensori
      dismessi per quel parametro.

REQUISITI:
    pip install requests pandas pyarrow

    Il download è riprendibile: se un file CSV annuale esiste già
    in dati_arpa_output/, l'anno viene saltato. Per riscaricare
    un anno, eliminare il file corrispondente.
=============================================================
"""

import requests
import pandas as pd
import time
import os
from io import StringIO

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURAZIONE
# ──────────────────────────────────────────────────────────────────────────────

APP_TOKEN = "mWl3E57Kd0qdtpuv0Pu5cmF2E"

PARAMETRI_INTERESSE = ["PM10", "Particelle sospese PM2.5", "Biossido di Azoto"]

# idoperatore=1 → media giornaliera comunale (quello che ci serve)
IDOPERATORE_MEDIO = 1

OUTPUT_DIR = "dati_arpa_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Quante righe per richiesta API (massimo Socrata = 50000)
PAGE_SIZE = 50000

# ──────────────────────────────────────────────────────────────────────────────
# TERRITORI
# ──────────────────────────────────────────────────────────────────────────────

# Province scaricate INTEGRALMENTE (tutti i comuni)
# BG = ATS Bergamo (243 comuni)
# SO = ATS Montagna (77 comuni)
# MB = ATS Brianza — Monza-Brianza (55 comuni)
# LC = ATS Brianza — Lecco (84 comuni)
PROVINCE_INTERE = ["BG", "SO", "MB", "LC"]

# Comuni specifici di BS (Brescia) che fanno parte di ATS Montagna
COMUNI_BRESCIA_ATS_MONTAGNA = {
    "Angolo Terme", "Artogne", "Berzo Demo", "Berzo Inferiore", "Bienno",
    "Borno", "Braone", "Breno", "Capo di Ponte", "Cedegolo", "Cerveno",
    "Ceto", "Cevo", "Cimbergo", "Cividate Camuno", "Corteno Golgi",
    "Darfo Boario Terme", "Edolo", "Esine", "Gianico", "Incudine",
    "Losine", "Lozio", "Malegno", "Malonno", "Monno", "Niardo",
    "Ono San Pietro", "Ossimo", "Paisco Loveno", "Paspardo", "Pian Camuno",
    "Piancogno", "Pisogne", "Ponte di Legno", "Saviore dell'Adamello",
    "Sellero", "Sonico", "Temù", "Vezza d'Oglio", "Vione",
}

# Comuni specifici di CO (Como) che fanno parte di ATS Montagna
COMUNI_COMO_ATS_MONTAGNA = {
    "Cremia", "Domaso", "Dongo", "Dosso del Liro", "Garzeno", "Gera Lario",
    "Gravedona ed Uniti", "Livo", "Montemezzo", "Musso", "Peglio",
    "Pianello del Lario", "Sorico", "Stazzona", "Trezzone", "Vercana",
}

# ──────────────────────────────────────────────────────────────────────────────
# DATASET IDS (Socrata / dati.lombardia.it)
# ──────────────────────────────────────────────────────────────────────────────

ANAGRAFICA_ID = "5rep-i3mj"

DATASET_IDS = {
    "2025-2026": "ysm5-jwrn",
    "2024":      "qyg8-q6gd",
    "2023":      "q25y-843e",
    "2022":      "fqaz-7ste",
    "2021":      "7iq4-hq9t",
}

BASE_URL = "https://www.dati.lombardia.it/resource/{dataset_id}.csv"

# ──────────────────────────────────────────────────────────────────────────────
# UTILITY
# ──────────────────────────────────────────────────────────────────────────────

def get_headers() -> dict:
    """Costruisce gli header HTTP per l'API Socrata."""
    h = {"Accept": "text/csv"}
    if APP_TOKEN:
        h["X-App-Token"] = APP_TOKEN
    return h


def api_get_csv(dataset_id: str, params: dict) -> pd.DataFrame:
    """
    Chiamata API semplice senza filtri WHERE.
    Nessun $where → evita il bug di tipo di dati.lombardia.it.
    """
    url = BASE_URL.format(dataset_id=dataset_id)
    try:
        resp = requests.get(url, params=params, headers=get_headers(), timeout=180)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text))
        return df
    except requests.exceptions.HTTPError as e:
        print(f"  ERRORE HTTP {resp.status_code}: {e}")
        print(f"  Risposta: {resp.text[:200]}")
        return pd.DataFrame()
    except Exception as e:
        print(f"  ERRORE: {e}")
        return pd.DataFrame()


# ──────────────────────────────────────────────────────────────────────────────
# STEP 1: CARICA ANAGRAFICA (da cache se disponibile)
# ──────────────────────────────────────────────────────────────────────────────

def carica_anagrafica() -> pd.DataFrame:
    """
    Scarica (o carica da cache) l'anagrafica sensori ARPA.
    L'anagrafica contiene: idsensore, nometiposensore, provincia,
    comune, storico ('N'=attivo, 'S'=dismesso), ecc.
    """
    cache = os.path.join(OUTPUT_DIR, "anagrafica_sensori.csv")

    if os.path.exists(cache):
        print(f"  Anagrafica da cache: {cache}")
        df = pd.read_csv(cache)
    else:
        print("  Scaricamento anagrafica...")
        params = {"$limit": 10000}
        df = api_get_csv(ANAGRAFICA_ID, params)
        if df.empty:
            raise RuntimeError("Impossibile scaricare anagrafica.")
        df.to_csv(cache, index=False)

    df.columns = df.columns.str.lower().str.strip()
    return df


# ──────────────────────────────────────────────────────────────────────────────
# STEP 2: COSTRUISCI SET DI IDSENSORE DA TENERE
# ──────────────────────────────────────────────────────────────────────────────

def costruisci_set_sensori(df_ana: pd.DataFrame) -> tuple:
    """
    Seleziona i sensori attivi (storico='N') per il territorio di
    interesse e i parametri desiderati.

    Logica di inclusione territoriale:
        - BG, SO, MB, LC → province intere
        - BS → solo i comuni di ATS Montagna
        - CO → solo i comuni di ATS Montagna

    Ritorna:
        sensori_int  : set di idsensore (int) — usati per il filtro
        sensori_str  : set di idsensore (str) — fallback se API restituisce str
        df_ana_filt  : sottoinsieme anagrafica sensori selezionati
    """
    df_ana["comune_norm"] = df_ana["comune"].str.strip().str.title()
    comuni_bs = {c.title() for c in COMUNI_BRESCIA_ATS_MONTAGNA}
    comuni_co = {c.title() for c in COMUNI_COMO_ATS_MONTAGNA}

    def includi(row):
        prov   = str(row.get("provincia", "")).strip().upper()
        comune = row.get("comune_norm", "")
        # Province intere (BG, SO, MB, LC)
        if prov in ("BG", "SO", "MB", "LC"): return True
        # Brescia: solo comuni ATS Montagna
        if prov == "BS":                      return comune in comuni_bs
        # Como: solo comuni ATS Montagna
        if prov == "CO":                      return comune in comuni_co
        return False

    mask_t = df_ana.apply(includi, axis=1)
    mask_p = df_ana["nometiposensore"].isin(PARAMETRI_INTERESSE)

    # storico='N' → attivo; 'S' → dismesso (da escludere)
    mask_attivo = df_ana["storico"].str.strip().str.upper() == "N"

    # Stampa sensori dismessi nel territorio di interesse (informativo)
    df_territorio_param = df_ana[mask_t & mask_p].copy()
    df_dismessi = df_territorio_param[~mask_attivo[df_territorio_param.index]]
    if not df_dismessi.empty:
        print(f"\n  ⚠ Sensori DISMESSI (storico='S') esclusi dall'analisi:")
        print(f"  {'Comune (Provincia)':<35} {'Parametro':<30} {'ID'}")
        print(f"  {'-'*75}")
        for _, row in df_dismessi.sort_values(["provincia", "comune"]).iterrows():
            label = f"{row['comune'].title()} ({row['provincia'].upper()})"
            print(f"  {label:<35} {row['nometiposensore']:<30} {row['idsensore']}")
        print(f"\n  Totale sensori dismessi esclusi: {len(df_dismessi)}")

    # Applica tutti e tre i filtri
    df_filt = df_ana[mask_t & mask_p & mask_attivo].copy()

    print(f"\n  Sensori attivi selezionati: {len(df_filt)} "
          f"(su {len(df_ana)} totali nell'anagrafica, "
          f"{len(df_dismessi)} dismessi esclusi)")

    # Manteniamo entrambe le versioni per robustezza nel merge
    df_filt["idsensore_int"] = df_filt["idsensore"].astype(int)
    df_filt["idsensore_str"] = df_filt["idsensore"].astype(str)

    sensori_int = set(df_filt["idsensore_int"].tolist())
    sensori_str = set(df_filt["idsensore_str"].tolist())

    return sensori_int, sensori_str, df_filt


# ──────────────────────────────────────────────────────────────────────────────
# CHECK PRE-STEP 3: comuni unici per provincia nei sensori filtrati
# ──────────────────────────────────────────────────────────────────────────────

def check_comuni_per_provincia(df_ana_filt: pd.DataFrame):
    """
    Verifica che i comuni attesi per ogni provincia siano presenti
    nell'anagrafica filtrata, suddivisi per parametro.

    Valori attesi:
        BG → 243  (ATS Bergamo)
        SO →  77  (ATS Montagna)
        BS →  41  (ATS Montagna, comuni selezionati)
        CO →  16  (ATS Montagna, comuni selezionati)
        MB →  55 (ATS Brianza, tutta la provincia)
        LC →  84 (ATS Brianza, tutta la provincia)
    """
    print("\n" + "─" * 80)
    print("CHECK: Comuni unici per provincia e PARAMETRO nei sensori selezionati")
    print("─" * 80)

    # Aggiornare MB e LC dopo il primo run verificando i valori reali
    ATTESI = {"BG": 243, "SO": 77, "BS": 41, "CO": 16, "MB": 55, "LC": 84}
    PARAMETRI = ["Biossido di Azoto", "PM10", "Particelle sospese PM2.5"]

    df_ana_filt["parametro_norm"] = df_ana_filt["nometiposensore"].str.strip()

    for parametro in PARAMETRI:
        print(f"\n{parametro:40s}")
        print("-" * 45)

        df_param = df_ana_filt[df_ana_filt["parametro_norm"] == parametro]
        comuni_per_prov = (
            df_param
            .groupby("provincia")["comune_norm"]
            .nunique()
            .sort_index()
        )

        for prov, count in comuni_per_prov.items():
            prov_upper = prov.upper()
            atteso = ATTESI.get(prov_upper)
            if atteso is not None:
                stato = "✓" if count == atteso else f"✗ (attesi {atteso})"
            else:
                stato = f"(atteso da definire dopo primo run)"
            print(f"  {prov:4s} → {count:4d} comuni  {stato}")

        totale = comuni_per_prov.sum()
        print(f"  TOTALE → {totale:4d} comuni")

    # Riepilogo generale
    print("\n" + "-" * 45)
    print("RIEPILOGO GENERALE (tutti i parametri):")
    comuni_generali = df_ana_filt.groupby("provincia")["comune_norm"].nunique().sort_index()
    for prov, count in comuni_generali.items():
        atteso = ATTESI.get(prov.upper())
        if atteso is not None:
            stato = "✓" if count == atteso else f"✗ (attesi {atteso})"
        else:
            stato = "(atteso da definire dopo primo run)"
        print(f"  {prov:4s} → {count:4d} comuni  {stato}")

    print("─" * 80)


# ──────────────────────────────────────────────────────────────────────────────
# STEP 3: SCARICA DATASET COMPLETO E FILTRA IN PANDAS
# ──────────────────────────────────────────────────────────────────────────────

def scarica_e_filtra(dataset_id: str, anno_label: str,
                     sensori_int: set, sensori_str: set) -> pd.DataFrame:
    """
    Scarica l'intero dataset pagina per pagina (senza filtri API),
    filtra ogni pagina in memoria prima di accumularla.

    Perché non filtriamo lato API:
        Il campo 'idsensore' su dati.lombardia.it genera un errore
        di tipo quando usato in clausole $where (bug Socrata). Il
        filtro locale è la soluzione adottata per tutti gli anni.

    Perché filtriamo pagina per pagina e non alla fine:
        Riduce il picco di memoria: le ~13M righe non vengono mai
        tutte in memoria contemporaneamente. Solo le ~500k righe
        utili vengono accumulate in all_filtered.

    Il download è riprendibile: se il file CSV dell'anno esiste già,
    la funzione restituisce subito il file esistente senza riscaricare.
    """
    print(f"\n{'='*60}")
    print(f"STEP 3: Download + filtro {anno_label} (ID: {dataset_id})")
    print(f"  Strategia: download completo, filtro locale pagina per pagina")
    print(f"{'='*60}")

    cache_anno = os.path.join(OUTPUT_DIR, f"stime_comunali_{anno_label}.csv")
    if os.path.exists(cache_anno):
        print(f"  File già presente: {cache_anno} — salto download.")
        return pd.read_csv(cache_anno, parse_dates=["data"])

    all_filtered = []
    offset = 0
    page_num = 0
    total_downloaded = 0
    total_kept = 0

    while True:
        page_num += 1
        params = {
            "$limit":  PAGE_SIZE,
            "$offset": offset,
            # Nessun $where — scarica tutto, filtra localmente
        }

        df_page = api_get_csv(dataset_id, params)

        if df_page.empty:
            print(f"  Pagina {page_num}: vuota → fine download.")
            break

        df_page.columns = df_page.columns.str.lower().str.strip()
        total_downloaded += len(df_page)

        # ── FILTRO LOCALE ──────────────────────────────────────────────────
        if "idsensore" not in df_page.columns:
            print(f"  ATTENZIONE: colonna 'idsensore' non trovata. "
                  f"Colonne disponibili: {list(df_page.columns)}")
            break

        # Prova int; se fallisce (es. valori stringa non numerici) usa str
        try:
            mask_id = df_page["idsensore"].astype(int).isin(sensori_int)
        except (ValueError, TypeError):
            mask_id = df_page["idsensore"].astype(str).isin(sensori_str)

        # Filtro operatore: solo media giornaliera (idoperatore=1)
        if "idoperatore" in df_page.columns:
            try:
                mask_op = df_page["idoperatore"].astype(int) == IDOPERATORE_MEDIO
            except (ValueError, TypeError):
                mask_op = df_page["idoperatore"].astype(str) == str(IDOPERATORE_MEDIO)
        else:
            mask_op = pd.Series(True, index=df_page.index)

        df_filtered = df_page[mask_id & mask_op].copy()
        total_kept += len(df_filtered)

        if not df_filtered.empty:
            all_filtered.append(df_filtered)

        pct = (total_kept / total_downloaded * 100) if total_downloaded > 0 else 0
        print(f"  Pag.{page_num:3d} | offset={offset:>8,} | "
              f"scaricate={total_downloaded:>8,} | "
              f"tenute={total_kept:>7,} ({pct:.1f}%)",
              flush=True)

        if len(df_page) < PAGE_SIZE:
            print("  → Ultima pagina raggiunta.")
            break

        offset += PAGE_SIZE
        time.sleep(0.1)  # pausa minima di cortesia verso l'API

    print(f"\n  Totale scaricate: {total_downloaded:,} | Tenute: {total_kept:,}")

    if not all_filtered:
        print(f"  ⚠ Nessuna riga dopo il filtro per {anno_label}.")
        return pd.DataFrame()

    df = pd.concat(all_filtered, ignore_index=True)
    return df


# ──────────────────────────────────────────────────────────────────────────────
# STEP 4: MERGE CON ANAGRAFICA E PULIZIA
# ──────────────────────────────────────────────────────────────────────────────

def merge_e_pulisci(df_dati: pd.DataFrame, df_ana_filt: pd.DataFrame,
                    anno_label: str) -> pd.DataFrame:
    """
    Arricchisce i dati scaricati con le informazioni dell'anagrafica
    (parametro, provincia, comune) e assegna l'ATS di appartenenza.

    Mappa ATS:
        BG       → ATS_Bergamo
        MB, LC   → ATS_Brianza   ← AGGIUNTO
        SO, BS, CO → ATS_Montagna
    """
    if df_dati.empty:
        return df_dati

    # Normalizzazione chiave di join: preferisce int, fallback str
    try:
        df_dati["idsensore_join"] = df_dati["idsensore"].astype(int)
    except (ValueError, TypeError):
        df_dati["idsensore_join"] = df_dati["idsensore"].astype(str)

    df_ana = df_ana_filt.copy()
    df_ana["idsensore_join"] = df_ana["idsensore_int"]

    # Conversione tipi
    df_dati["data"]   = pd.to_datetime(df_dati["data"], errors="coerce")
    df_dati["valore"] = pd.to_numeric(df_dati["valore"], errors="coerce")

    # Join con l'anagrafica per aggiungere parametro, provincia, comune
    cols = [c for c in ["idsensore_join", "nometiposensore", "unitamisura",
                         "provincia", "comune"] if c in df_ana.columns]
    df = df_dati.merge(df_ana[cols], on="idsensore_join", how="left")
    df = df.rename(columns={"nometiposensore": "parametro"})
    df = df.drop(columns=["idsensore_join"], errors="ignore")

    # Assegnazione ATS
    def ats(prov):
        if prov == "BG":               return "ATS_Bergamo"
        if prov in ("MB", "LC"):       return "ATS_Brianza"    # AGGIUNTO
        if prov in ("SO", "BS", "CO"): return "ATS_Montagna"
        return "Altro"

    df["ats"] = df["provincia"].apply(ats)
    df["anno_dataset"] = anno_label

    # Rimozione righe con data o valore mancante
    n = len(df)
    df = df.dropna(subset=["data", "valore"])
    if (n - len(df)) > 0:
        print(f"  Rimosse {n - len(df)} righe con data/valore nullo.")

    print(f"  Righe finali {anno_label}: {len(df):,}")
    return df


# ──────────────────────────────────────────────────────────────────────────────
# STEP 5: CHECK FILE FINALI
# ──────────────────────────────────────────────────────────────────────────────

def check_comuni_e_giorni_per_anno(output_dir: str, dataset_labels: dict,
                                   parametri_interesse=None, salva_csv: bool = False):
    """
    Per ogni file finale stime_comunali_{anno}.csv verifica:
      1. Quanti comuni unici per provincia per ciascun parametro
         (confrontati con i valori attesi dove definiti)
      2. Se ogni comune ha dati per tutti i giorni del periodo
         coperto dal file

    I comuni incompleti (giorni_presenti < giorni_attesi) vengono
    segnalati a schermo. Se salva_csv=True, scrive anche un CSV
    con il dettaglio comune x parametro x completezza %.

    Valori attesi per il confronto:
        BG → 243, SO → 77, BS → 41, CO → 16, MB → 55, LC → 84
    """
    if parametri_interesse is None:
        parametri_interesse = PARAMETRI_INTERESSE

    print("\n" + "="*80)
    print("CHECK FINALE PER PARAMETRO: comuni unici e completezza giornaliera")
    print("="*80)

    for anno_label in dataset_labels.keys():
        path = os.path.join(output_dir, f"stime_comunali_{anno_label}.csv")
        if not os.path.isfile(path):
            print(f"  {anno_label}: file finale mancante: {path} — salto.")
            continue

        try:
            df = pd.read_csv(path, parse_dates=["data"])
        except Exception as e:
            print(f"  {anno_label}: impossibile leggere {path}: {e}")
            continue

        if "comune" not in df.columns or "parametro" not in df.columns:
            print(f"  {anno_label}: colonne 'comune' o 'parametro' assenti — salto.")
            continue

        df["comune_norm"] = df["comune"].astype(str).str.strip().str.title()
        df["provincia"]   = df["provincia"].astype(str).str.strip().str.upper()
        df["parametro"]   = df["parametro"].astype(str).str.strip()

        date_min   = df["data"].min().date()
        date_max   = df["data"].max().date()
        total_days = len(pd.date_range(date_min, date_max, freq="D"))

        print(f"\n{anno_label}: periodo {date_min} → {date_max} ({total_days} giorni)")

        rows_out = []

        for parametro in parametri_interesse:
            df_p = df[df["parametro"] == parametro].copy()
            if df_p.empty:
                print(f"  {parametro}: nessun dato trovato in {anno_label}")
                continue

            cnt = df_p.groupby("provincia")["comune_norm"].nunique().to_dict()

            print(f"\n  {parametro}:")
            for prov in sorted(set(list(cnt.keys()) + list(ATTESI.keys()))):
                count  = int(cnt.get(prov, 0))
                atteso = ATTESI.get(prov)
                if atteso is not None:
                    stato = "✓" if count == atteso else f"✗ (attesi {atteso})"
                else:
                    stato = f"(comuni trovati: {count}, atteso da definire)"
                print(f"    {prov:4s} → {count:4d} comuni  {stato}")

            # Verifica completezza giornaliera per ogni comune
            for comune in sorted(df_p["comune_norm"].unique()):
                prov = df_p[df_p["comune_norm"] == comune]["provincia"].iat[0]
                giorni_presenti   = len(
                    df_p[df_p["comune_norm"] == comune]["data"].dt.date.unique()
                )
                completezza_pct = giorni_presenti / total_days * 100 if total_days > 0 else 0

                if giorni_presenti < total_days:
                    print(f"      - INCOMPLETO: {comune} ({prov}) "
                          f"→ {giorni_presenti}/{total_days} giorni "
                          f"({completezza_pct:.1f}%)")

                rows_out.append({
                    "anno_label":       anno_label,
                    "parametro":        parametro,
                    "provincia":        prov,
                    "comune":           comune,
                    "giorni_presenti":  giorni_presenti,
                    "giorni_attesi":    total_days,
                    "completezza_pct":  round(completezza_pct, 1)
                })

        if salva_csv and rows_out:
            out_df   = pd.DataFrame(rows_out)
            out_path = os.path.join(output_dir, f"check_comuni_giorni_{anno_label}.csv")
            out_df.to_csv(out_path, index=False)
            print(f"\n  Dettaglio salvato in: {out_path}")

    print("\n" + "="*80 + "\n")


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "="*60)
    print("DOWNLOAD STIME COMUNALI ARPA LOMBARDIA")
    print("ATS: Bergamo | Brianza (MB+LC) | Montagna (SO+BS+CO)")
    print("="*60)

    # Step 1: Anagrafica sensori
    print("\n[STEP 1] Caricamento anagrafica sensori...")
    df_ana = carica_anagrafica()

    # Step 2: Set sensori da tenere
    print("\n[STEP 2] Selezione sensori per territorio e parametro...")
    sensori_int, sensori_str, df_ana_filt = costruisci_set_sensori(df_ana)
    check_comuni_per_provincia(df_ana_filt)

    all_anni = []

    for anno_label, dataset_id in DATASET_IDS.items():
        out_final = os.path.join(OUTPUT_DIR, f"stime_comunali_{anno_label}.csv")

        # Ripartenza: se il file finale esiste già, salta il download
        if os.path.isfile(out_final):
            print(f"\n{anno_label}: File finale già presente: {out_final} — salto.")
            try:
                df_existing = pd.read_csv(out_final, parse_dates=["data"])
                all_anni.append(df_existing)
            except Exception:
                print(f"  Attenzione: impossibile leggere {out_final}, rielaboro l'anno.")
            continue

        # Step 3: Download paginato + filtro locale
        df_dati = scarica_e_filtra(dataset_id, anno_label, sensori_int, sensori_str)

        # Step 4: Merge con anagrafica + assegnazione ATS
        df_clean = merge_e_pulisci(df_dati, df_ana_filt, anno_label)

        if not df_clean.empty:
            df_clean.to_csv(out_final, index=False)
            print(f"  → Salvato: {out_final}")
            all_anni.append(df_clean)

    # Riepilogo dataset completo
    if all_anni:
        df_finale = pd.concat(all_anni, ignore_index=True)

        print(f"\n{'='*60}")
        print("RIEPILOGO FINALE")
        print(f"{'='*60}")
        print(f"Righe totali:   {len(df_finale):,}")
        if "data" in df_finale.columns and not df_finale["data"].isna().all():
            print(f"Periodo:        {df_finale['data'].min().date()} "
                  f"→ {df_finale['data'].max().date()}")
        print(f"ATS:            {sorted(df_finale['ats'].unique())}")
        print(f"Parametri:      {sorted(df_finale['parametro'].dropna().unique())}")
        print(f"Province:       {sorted(df_finale['provincia'].dropna().unique())}")
        if "comune" in df_finale.columns:
            print(f"Comuni unici:   {df_finale['comune'].nunique()}")
    else:
        print("\n⚠ Nessun dato raccolto. Verificare connessione e dataset IDs.")

    # Step 5: Check completezza finale per ogni anno e parametro
    check_comuni_e_giorni_per_anno(
        OUTPUT_DIR,
        DATASET_IDS,
        parametri_interesse=PARAMETRI_INTERESSE,
        salva_csv=True
    )


if __name__ == "__main__":
    main()