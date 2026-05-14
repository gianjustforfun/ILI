"""
download_arpa_stime_comunali.py
====================================
STRATEGIA FINALE: scarica l'intero dataset con paginazione semplice
(nessun $where), poi filtra in pandas per idsensore.

PERCHÉ abbiamo abbandonato il filtraggio via API:
    Il campo 'idsensore' nel dataset DATI causa un errore di tipo
    ("Type mismatch") sia con IN() che con =, sia come numero che
    come stringa. Questo è un bug/limitazione della configurazione
    Socrata specifica di dati.lombardia.it per questi dataset.

    La soluzione è scaricare tutto e filtrare localmente.

Ci sono 243 comuni in provincia di Bergamo, 77 in provincia di Sondrio,
41 in provincia di Brescia facenti parte dell'ATS Montagna e 16 in
provincia di Como sempre facenti parte di ATS Montagna. All'interno
dell'anagrafica sensori ci sono città che fanno parte dello storico
denominate con la lettera S, che quindi non vengono considerate.

STIMA VOLUME DATI:
    - Lombardia ha ~1500 comuni × 3 parametri × 2 operatori = ~9000 sensori
    - Ogni sensore ha ~365 righe/anno
    - Totale: ~3.3M righe/anno
    - 4 anni: ~13M righe → CSV di circa 300-500 MB
    - Dopo filtro (BG+SO+BS+CO specifici): ~500k righe → CSV ~15 MB

    Il download richiede ~10-20 minuti per anno con connessione normale.

Dipendenze:
    pip install requests pandas pyarrow
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

PROVINCE_INTERE = ["BG", "SO"]

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

COMUNI_COMO_ATS_MONTAGNA = {
    "Cremia", "Domaso", "Dongo", "Dosso del Liro", "Garzeno", "Gera Lario",
    "Gravedona ed Uniti", "Livo", "Montemezzo", "Musso", "Peglio",
    "Pianello del Lario", "Sorico", "Stazzona", "Trezzone", "Vercana",
}

# ──────────────────────────────────────────────────────────────────────────────
# DATASET IDS
# ──────────────────────────────────────────────────────────────────────────────

ANAGRAFICA_ID = "5rep-i3mj"

DATASET_IDS = {
    "2025-2026": "ysm5-jwrn",
    "2024": "qyg8-q6gd",
    "2023":      "q25y-843e",
    "2022":      "fqaz-7ste",
    "2021":      "7iq4-hq9t",
}

BASE_URL = "https://www.dati.lombardia.it/resource/{dataset_id}.csv"

# ──────────────────────────────────────────────────────────────────────────────
# UTILITY
# ──────────────────────────────────────────────────────────────────────────────

def get_headers() -> dict:
    h = {"Accept": "text/csv"}
    if APP_TOKEN:
        h["X-App-Token"] = APP_TOKEN
    return h


def api_get_csv(dataset_id: str, params: dict) -> pd.DataFrame:
    """Chiamata API semplice senza filtri WHERE."""
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
    Ritorna:
        - sensori_int: set di idsensore (int) da tenere
        - sensori_str: set di idsensore (str) da tenere
        - df_ana_filt: sottoinsieme dell'anagrafica con le colonne utili

    Filtri applicati:
        1. Territorio (provincia/comuni specifici)
        2. Parametro (PM10, PM2.5, NO2)
        3. storico == 'N' (sensori attivi; 'S' = dismessi)
    """
    df_ana["comune_norm"] = df_ana["comune"].str.strip().str.title()
    comuni_bs = {c.title() for c in COMUNI_BRESCIA_ATS_MONTAGNA}
    comuni_co = {c.title() for c in COMUNI_COMO_ATS_MONTAGNA}

    def includi(row):
        prov   = str(row.get("provincia", "")).strip().upper()
        comune = row.get("comune_norm", "")
        if prov in ("BG", "SO"):   return True
        if prov == "BS":           return comune in comuni_bs
        if prov == "CO":           return comune in comuni_co
        return False

    mask_t = df_ana.apply(includi, axis=1)
    mask_p = df_ana["nometiposensore"].isin(PARAMETRI_INTERESSE)

    # Filtro storico: 'N' = attivo, 'S' = dismesso
    # Normalizziamo per sicurezza (strip + upper) nel caso ci siano spazi
    mask_attivo = df_ana["storico"].str.strip().str.upper() == "N"

    # Prima applichiamo territorio + parametro (senza filtro storico)
    # così possiamo stampare i dismessi in modo informativo
    df_territorio_param = df_ana[mask_t & mask_p].copy()

    # Sensori dismessi nel nostro territorio di interesse
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

    # Teniamo entrambi i tipi per robustezza nel merge
    df_filt["idsensore_int"] = df_filt["idsensore"].astype(int)
    df_filt["idsensore_str"] = df_filt["idsensore"].astype(str)

    sensori_int = set(df_filt["idsensore_int"].tolist())
    sensori_str = set(df_filt["idsensore_str"].tolist())

    return sensori_int, sensori_str, df_filt

# ──────────────────────────────────────────────────────────────────────────────
# CHECK PRE-STEP 3: comuni unici per provincia nei sensori filtrati
# ──────────────────────────────────────────────────────────────────────────────

def check_comuni_per_provincia(df_ana_filt: pd.DataFrame):
    print("\n" + "─" * 80)
    print("CHECK: Comuni unici per provincia e PARAMETRO nei sensori selezionati")
    print("─" * 80)

    ATTESI = {"BG": 243, "SO": 77, "BS": 41, "CO": 16}
    PARAMETRI = ["Biossido di Azoto", "PM10", "Particelle sospese PM2.5"]

    # Normalizza nometiposensore per sicurezza
    df_ana_filt["parametro_norm"] = df_ana_filt["nometiposensore"].str.strip()

    for parametro in PARAMETRI:
        print(f"\n{parametro:40s}")
        print("-" * 45)

        # Filtra per parametro e conta comuni unici per provincia
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
                stato = "(non in lista attesi)"
            print(f"  {prov:4s} → {count:4d} comuni  {stato}")

        # Totale per parametro
        totale = comuni_per_prov.sum()
        print(f"  TOTALE → {totale:4d} comuni")

    # Riepilogo generale (tutti i parametri)
    print("\n" + "-" * 45)
    print("RIEPILOGO GENERALE (tutti i parametri):")
    comuni_generali = df_ana_filt.groupby("provincia")["comune_norm"].nunique().sort_index()
    for prov, count in comuni_generali.items():
        atteso = ATTESI.get(prov.upper())
        stato = "✓" if (
                    atteso is not None and count == atteso) else f"✗ (attesi {atteso})" if atteso else "(nessun atteso)"
        print(f"  {prov:4s} → {count:4d} comuni  {stato}")

    print("─" * 80)

# ──────────────────────────────────────────────────────────────────────────────
# STEP 3: SCARICA DATASET COMPLETO E FILTRA IN PANDAS
# ──────────────────────────────────────────────────────────────────────────────

def scarica_e_filtra(dataset_id: str, anno_label: str,
                     sensori_int: set, sensori_str: set) -> pd.DataFrame:
    """
    Scarica l'intero dataset pagina per pagina (nessun filtro API),
    filtra ogni pagina subito in memoria prima di accumularla.

    Vantaggi di filtrare pagina per pagina:
        - Riduce la memoria usata (non teniamo milioni di righe inutili)
        - Più veloce da processare alla fine

    Nessun $where → zero problemi di tipo.
    """
    print(f"\n{'='*60}")
    print(f"STEP 3: Download + filtro {anno_label} (ID: {dataset_id})")
    print(f"  Strategia: download completo, filtro locale pagina per pagina")
    print(f"{'='*60}")

    # Controlla se esiste già il file intermedio (ripartenza dopo interruzione)
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
            # Nessun $where — scarica tutto
            # Ordiniamo solo per idsensore per coerenza (opzionale)
        }

        df_page = api_get_csv(dataset_id, params)

        if df_page.empty:
            print(f"  Pagina {page_num}: vuota → fine download.")
            break

        # Normalizza colonne
        df_page.columns = df_page.columns.str.lower().str.strip()

        total_downloaded += len(df_page)

        # ── FILTRO LOCALE ──────────────────────────────────────────────────
        # Il campo idsensore nel dataset dati potrebbe essere int o str
        # a seconda del dataset. Proviamo entrambi.

        if "idsensore" not in df_page.columns:
            print(f"  ATTENZIONE: colonna 'idsensore' non trovata. "
                  f"Colonne disponibili: {list(df_page.columns)}")
            break

        # Converti idsensore al tipo più adatto per il confronto
        try:
            mask_id = df_page["idsensore"].astype(int).isin(sensori_int)
        except (ValueError, TypeError):
            # Se la conversione a int fallisce, usa stringhe
            mask_id = df_page["idsensore"].astype(str).isin(sensori_str)

        # Filtra per idoperatore = 1 (media giornaliera)
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

        # Progress
        pct = (total_kept / total_downloaded * 100) if total_downloaded > 0 else 0
        print(f"  Pag.{page_num:3d} | offset={offset:>8,} | "
              f"scaricate={total_downloaded:>8,} | "
              f"tenute={total_kept:>7,} ({pct:.1f}%)",
              flush=True)

        if len(df_page) < PAGE_SIZE:
            print("  → Ultima pagina raggiunta.")
            break

        offset += PAGE_SIZE
        time.sleep(0.1)  # pausa minima (il filtro locale è veloce)

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
    if df_dati.empty:
        return df_dati

    # Prepara idsensore per il merge: proviamo int su entrambi i lati
    try:
        df_dati["idsensore_join"] = df_dati["idsensore"].astype(int)
    except (ValueError, TypeError):
        df_dati["idsensore_join"] = df_dati["idsensore"].astype(str)

    df_ana = df_ana_filt.copy()
    df_ana["idsensore_join"] = df_ana["idsensore_int"]

    # Converti data e valore
    df_dati["data"]   = pd.to_datetime(df_dati["data"], errors="coerce")
    df_dati["valore"] = pd.to_numeric(df_dati["valore"], errors="coerce")

    # Colonne utili dall'anagrafica
    cols = [c for c in ["idsensore_join", "nometiposensore", "unitamisura",
                         "provincia", "comune"] if c in df_ana.columns]
    df = df_dati.merge(df_ana[cols], on="idsensore_join", how="left")

    df = df.rename(columns={"nometiposensore": "parametro"})
    df = df.drop(columns=["idsensore_join"], errors="ignore")

    # ATS
    def ats(prov):
        if prov == "BG": return "ATS_Bergamo"
        if prov in ("SO", "BS", "CO"): return "ATS_Montagna"
        return "Altro"
    df["ats"] = df["provincia"].apply(ats)
    df["anno_dataset"] = anno_label

    # Rimozione NaN
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
    Per ogni file finale stime_comunali_{anno}.csv in output_dir:
      - Conta i comuni unici per provincia per ciascun parametro (parametri_interesse)
      - Controlla che per ciascun comune+parametro ci siano dati per tutti i giorni
        dell'intervallo presente nel file (se il dataset copre 2 anni come 2025-2026
        la funzione può essere chiamata indicando '2025-2026' come chiave e farà
        il controllo sui giorni effettivamente presenti).
    Se salva_csv=True scrive output/check_comuni_giorni_{anno}.csv con il dettaglio.
    """
    if parametri_interesse is None:
        parametri_interesse = PARAMETRI_INTERESSE

    ATTESI = {"BG": 243, "SO": 77, "BS": 41, "CO": 16}

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
            print(f"  {anno_label}: colonne 'comune' o 'parametro' assenti in {path} — salto.")
            continue

        # Normalizzazione
        df["comune_norm"] = df["comune"].astype(str).str.strip().str.title()
        df["provincia"] = df["provincia"].astype(str).str.strip().str.upper()
        df["parametro"] = df["parametro"].astype(str).str.strip()

        # Determina intervallo di date effettivo nel file
        date_min = df["data"].min().date()
        date_max = df["data"].max().date()
        giorni_presenti_anno = pd.date_range(date_min, date_max, freq="D")
        total_days = len(giorni_presenti_anno)

        print(f"\n{anno_label}: periodo {date_min} → {date_max} ({total_days} giorni)")

        # Struttura di output per eventualmente salvare
        rows_out = []

        # Loop per parametro
        for parametro in parametri_interesse:
            df_p = df[df["parametro"] == parametro].copy()
            if df_p.empty:
                print(f"  {parametro}: nessun dato trovato per questo parametro in {anno_label}")
                continue

            # Conta comuni unici per provincia (nunique su comune_norm)
            cnt = df_p.groupby("provincia")["comune_norm"].nunique().to_dict()

            print(f"\n  {parametro}:")
            for prov in sorted(set(list(cnt.keys()) + list(ATTESI.keys()))):
                count = int(cnt.get(prov, 0))
                atteso = ATTESI.get(prov)
                stato = "✓" if (atteso is not None and count == atteso) else (f"✗ (attesi {atteso})" if atteso is not None else "(nessun atteso definito)")
                print(f"    {prov:4s} → {count:4d} comuni  {stato}")

            # Per ogni comune nella selezione verifica completezza giornaliera
            comuni = df_p["comune_norm"].unique()
            for comune in sorted(comuni):
                prov = df_p[df_p["comune_norm"] == comune]["provincia"].iat[0]
                # Costruisci serie di date presenti per questo comune+parametro
                dates_comune = pd.to_datetime(df_p[df_p["comune_norm"] == comune]["data"].dt.date.unique())
                # Conta giorni unici presenti
                giorni_presenti = len(dates_comune)
                completezza_pct = giorni_presenti / total_days * 100 if total_days > 0 else 0

                # segnala comuni non completi
                if giorni_presenti < total_days:
                    print(f"      - INCOMPLETO: {comune} ({prov}) → {giorni_presenti}/{total_days} giorni ({completezza_pct:.1f}%)")
                # accumula riga per CSV
                rows_out.append({
                    "anno_label": anno_label,
                    "parametro": parametro,
                    "provincia": prov,
                    "comune": comune,
                    "giorni_presenti": giorni_presenti,
                    "giorni_attesi": total_days,
                    "completezza_pct": round(completezza_pct, 1)
                })

        # Salvataggio opzionale CSV con dettaglio per comune
        if salva_csv and rows_out:
            out_df = pd.DataFrame(rows_out)
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
    print("="*60)

    # Step 1: Anagrafica
    df_ana = carica_anagrafica()

    # Step 2: Set sensori
    sensori_int, sensori_str, df_ana_filt = costruisci_set_sensori(df_ana)

    check_comuni_per_provincia(df_ana_filt)

    all_anni = []

    for anno_label, dataset_id in DATASET_IDS.items():
        out_final = os.path.join(OUTPUT_DIR, f"stime_comunali_{anno_label}.csv")

        if os.path.isfile(out_final):
            print(f"\n{anno_label}: File finale già presente: {out_final} — salto.")
            # Se vuoi puoi anche leggere il file e aggiungerlo a all_anni senza rielaborare:
            try:
                df_existing = pd.read_csv(out_final, parse_dates=["data"])
                all_anni.append(df_existing)
            except Exception:
                # se il file esiste ma non è leggibile, avvisa e continua con il flusso normale
                print(f"  Attenzione: impossibile leggere {out_final}, rielaboro l'anno.")
            continue

        # Step 3: Download + filtro locale
        df_dati = scarica_e_filtra(dataset_id, anno_label, sensori_int, sensori_str)

        # Step 4: Merge + pulizia
        df_clean = merge_e_pulisci(df_dati, df_ana_filt, anno_label)

        if not df_clean.empty:
            out = out_final
            df_clean.to_csv(out, index=False)
            print(f"  → Salvato: {out}")
            all_anni.append(df_clean)

    # Dataset finale
    df_finale = pd.concat(all_anni, ignore_index=True)

    print(f"\n{'='*60}")
    print("RIEPILOGO FINALE")
    print(f"{'='*60}")
    print(f"Righe totali:   {len(df_finale):,}")
    if "data" in df_finale.columns and not df_finale["data"].isna().all():
        print(f"Periodo:        {df_finale['data'].min().date()} → {df_finale['data'].max().date()}")
    print(f"ATS:            {sorted(df_finale['ats'].unique())}")
    print(f"Parametri:      {sorted(df_finale['parametro'].dropna().unique())}")
    print(f"Province:       {sorted(df_finale['provincia'].dropna().unique())}")
    if "comune" in df_finale.columns:
        print(f"Comuni unici:   {df_finale['comune'].nunique()}")

    csv_out = os.path.join(OUTPUT_DIR, "stime_comunali_FINALE.csv")
    df_finale.to_csv(csv_out, index=False)
    print(f"\n→ CSV: {csv_out}")

    try:
        pq_out = os.path.join(OUTPUT_DIR, "stime_comunali_FINALE.parquet")
        df_finale.to_parquet(pq_out, index=False)
        print(f"→ Parquet: {pq_out}")
    except Exception as e:
        print(f"  Parquet: {e} (pip install pyarrow)")

    check_comuni_e_giorni_per_anno(OUTPUT_DIR, DATASET_IDS, parametri_interesse=PARAMETRI_INTERESSE, salva_csv=True)

if __name__ == "__main__":
    main()