"""
SCRIPT 3 — Download e processamento dati ARPA Lombardia
=========================================================
Scarica i dati di qualità dell'aria (NO2, PM2.5, PM10) dal portale
open data di ARPA Lombardia e li aggrega per ATS.

PORTALE ARPA:
  https://www.dati.lombardia.it/
  Sezione "Ambiente" → "Qualità dell'aria"

I dati ARPA sono distribuiti come CSV scaricabili via API Socrata.
Ogni file contiene misure orarie/giornaliere per tutte le stazioni.

LOGICA:
  1. Scarica l'anagrafica delle stazioni (con coordinate)
  2. Identifica quali stazioni sono dentro ATS_Bergamo / ATS_Montagna
  3. Scarica i dati delle misure per quelle stazioni
  4. Aggrega a livello giornaliero e poi per ATS

PREREQUISITI:
  pip install requests pandas geopandas shapely tqdm

NOTA SUI DATI ARPA:
  - PM2.5 e PM10 sono misure giornaliere (medie delle 24h)
  - NO2 è misura oraria → aggregheremo alla media giornaliera
  - Non tutte le stazioni misurano tutti i parametri!
  - In ATS Montagna ci sono pochissime stazioni → segnalare nel report
"""

import requests
import pandas as pd
import geopandas as gpd
import json
from pathlib import Path
from tqdm import tqdm  # barra di avanzamento; se non disponibile, rimuovere


# ---------------------------------------------------------------------------
# CONFIGURAZIONE
# ---------------------------------------------------------------------------

# Anni e mesi da scaricare (devono corrispondere a quelli di ERA5)
ANNI = [2022, 2023, 2024]
MESI_INFLUENZALI = [10, 11, 12, 1, 2, 3, 4]  # ottobre-aprile

# URL API ARPA Lombardia (Socrata Open Data API - SODA)
# Questi URL sono stabili e pubblici, non richiedono autenticazione per uso base
URL_STAZIONI = (
    "https://www.dati.lombardia.it/resource/ib47-atvt.json"
    "?$limit=5000"
)

# Dataset misure qualità aria (sensori fissi, dati validati)
# Il dataset principale delle misure validate ARPA è:
URL_MISURE = "https://www.dati.lombardia.it/resource/nicp-bhqi.csv"

# Parametri di interesse con i codici usati da ARPA
PARAMETRI = {
    'NO2':   'Biossido di Azoto',
    'PM2.5': 'Particolato Fine PM2.5',
    'PM10':  'Particolato PM10',
}

# File shapefile comuni (lo stesso usato nello script 2)
COMUNI_SHP = 'Com01012024_g_WGS84.shp'

# Definizione ATS (identica allo script 2 per coerenza)
ATS_DEFINIZIONE = {
    'ATS_Bergamo': {
        'province_intere': ['016'],
        'comuni_specifici': []
    },
    'ATS_Montagna': {
        'province_intere': ['098'],
        'comuni_specifici': [
            'Angolo Terme', 'Artogne', 'Berzo Demo', 'Berzo Inferiore',
            'Bienno', 'Borno', 'Braone', 'Breno', 'Capo di Ponte',
            'Cedegolo', 'Cerveno', 'Ceto', 'Cevo', 'Cimbergo',
            'Cividate Camuno', 'Corteno Golgi', 'Darfo Boario Terme',
            'Edolo', 'Esine', 'Gianico', 'Incudine', 'Losine', 'Lozio',
            'Malegno', 'Malonno', 'Monno', 'Niardo', 'Ono San Pietro',
            'Ossimo', 'Paisco Loveno', 'Paspardo', 'Pian Camuno',
            'Piancogno', 'Pisogne', 'Ponte di Legno',
            "Saviore dell'Adamello", 'Sellero', 'Sonico', 'Temù',
            "Vezza d'Oglio", 'Vione',
            'Cremia', 'Domaso', 'Dongo', 'Dosso del Liro', 'Garzeno',
            'Gera Lario', 'Gravedona ed Uniti', 'Livo', 'Montemezzo',
            'Musso', 'Peglio', 'Pianello del Lario', 'Sorico', 'Stazzona',
            'Trezzone', 'Vercana',
        ]
    }
}


# ---------------------------------------------------------------------------
# FUNZIONI
# ---------------------------------------------------------------------------

def scarica_anagrafica_stazioni() -> gpd.GeoDataFrame:
    """
    Scarica l'anagrafica di tutte le stazioni ARPA Lombardia.
    Ritorna un GeoDataFrame con coordinate e metadati.
    """
    cache = Path('stazioni_arpa.geojson')
    if cache.exists():
        print("Carico anagrafica stazioni da cache...")
        return gpd.read_file(cache)

    print(f"Scarico anagrafica stazioni da ARPA...")
    resp = requests.get(URL_STAZIONI, timeout=60)
    resp.raise_for_status()
    stazioni = pd.DataFrame(resp.json())

    print(f"  Stazioni trovate: {len(stazioni)}")
    print(f"  Colonne: {stazioni.columns.tolist()}")

    # Le coordinate sono in colonne 'lat' e 'lng' o simili
    # Adattiamo i nomi se necessario
    col_lat = next((c for c in stazioni.columns if 'lat' in c.lower()), None)
    col_lon = next((c for c in stazioni.columns if 'lon' in c.lower() or 'lng' in c.lower()), None)

    if not col_lat or not col_lon:
        raise ValueError(f"Colonne lat/lon non trovate. Colonne disponibili: {stazioni.columns.tolist()}")

    stazioni[col_lat] = pd.to_numeric(stazioni[col_lat], errors='coerce')
    stazioni[col_lon] = pd.to_numeric(stazioni[col_lon], errors='coerce')
    stazioni = stazioni.dropna(subset=[col_lat, col_lon])

    gdf = gpd.GeoDataFrame(
        stazioni,
        geometry=gpd.points_from_xy(stazioni[col_lon], stazioni[col_lat]),
        crs='EPSG:4326'
    )

    gdf.to_file(cache, driver='GeoJSON')
    return gdf


def assegna_ats_a_stazioni(stazioni_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Spatial join: assegna ogni stazione ARPA alla sua ATS.
    Usa il GeoJSON dei confini ATS prodotto dallo script 2.
    """
    confini_file = 'confini_ats.geojson'

    if not Path(confini_file).exists():
        # Fallback: costruisci i confini dai comuni ISTAT
        print(f"confini_ats.geojson non trovato, lo costruisco da {COMUNI_SHP}...")
        comuni = gpd.read_file(COMUNI_SHP)

        # Normalizzazione colonne (stessa logica dello script 2)
        for col in comuni.columns:
            if col.upper() in ('COMUNE', 'DENOMINAZI', 'DENOMVD', 'NOME'):
                comuni = comuni.rename(columns={col: 'COMUNE'})
            if col.upper() in ('COD_PROV', 'COD_PRO', 'CODPRO'):
                comuni = comuni.rename(columns={col: 'COD_PROV'})

        comuni['COD_PROV'] = comuni['COD_PROV'].astype(str).str.zfill(3)

        risultati = []
        for nome_ats, config in ATS_DEFINIZIONE.items():
            mask_prov   = comuni['COD_PROV'].isin(config['province_intere'])
            mask_comuni = comuni['COMUNE'].isin(config['comuni_specifici'])
            sel = comuni[mask_prov | mask_comuni].copy()
            sel['ATS'] = nome_ats
            risultati.append(sel)

        comuni_ats = pd.concat(risultati, ignore_index=True)
        poligoni_ats = comuni_ats.dissolve(by='ATS').reset_index()[['ATS', 'geometry']]
        poligoni_ats.to_file(confini_file, driver='GeoJSON')

    confini = gpd.read_file(confini_file)

    joined = gpd.sjoin(
        stazioni_gdf,
        confini[['ATS', 'geometry']],
        how='left',
        predicate='within'
    )

    stazioni_ats = joined.dropna(subset=['ATS']).copy()
    print(f"\nStazioni nelle ATS di interesse: {len(stazioni_ats)}")
    print(stazioni_ats.groupby('ATS').size().rename('n_stazioni'))

    return stazioni_ats


def scarica_misure_periodo(id_stazioni: list, anno: int, mese: int,
                            parametro: str) -> pd.DataFrame:
    """
    Scarica le misure di un parametro per un gruppo di stazioni,
    in un dato anno/mese, usando l'API Socrata di ARPA.

    Parametro: es. 'Biossido di Azoto'

    L'API SODA permette filtri WHERE via query string.
    Scarichiamo a mese per tenere ogni richiesta leggera.
    """
    # Costruiamo le date di inizio/fine del mese
    import calendar
    ultimo_giorno = calendar.monthrange(anno, mese)[1]
    data_inizio = f"{anno}-{mese:02d}-01T00:00:00"
    data_fine   = f"{anno}-{mese:02d}-{ultimo_giorno:02d}T23:59:59"

    # Codici stazione come stringa per il filtro
    id_str = ','.join([f"'{sid}'" for sid in id_stazioni])

    # Costruisce URL con filtri SODA
    url = (
        f"{URL_MISURE}"
        f"?$where=idoperatore IN ({id_str})"
        f" AND nometiposensore='{parametro}'"
        f" AND datatimemisura>='{data_inizio}'"
        f" AND datatimemisura<='{data_fine}'"
        f"&$limit=100000"
        f"&$order=datatimemisura ASC"
    )

    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    # Leggi CSV dalla risposta
    from io import StringIO
    df = pd.read_csv(StringIO(resp.text))
    return df


def processa_misure(df_raw: pd.DataFrame, parametro: str) -> pd.DataFrame:
    """
    Pulisce e aggrega le misure orarie → media giornaliera per stazione.
    Rimuove valori non validi (negativi o nulli).
    """
    if df_raw.empty:
        return pd.DataFrame()

    # Identifica colonne rilevanti (i nomi ARPA possono variare leggermente)
    col_tempo = next((c for c in df_raw.columns if 'data' in c.lower() or 'time' in c.lower()), None)
    col_valore = next((c for c in df_raw.columns if 'valore' in c.lower() or 'value' in c.lower()), None)
    col_staz = next((c for c in df_raw.columns if 'operatore' in c.lower() or 'stazione' in c.lower()
                     or 'idsensore' in c.lower()), None)

    if not all([col_tempo, col_valore, col_staz]):
        print(f"  ATTENZIONE: colonne non trovate in {df_raw.columns.tolist()}")
        return pd.DataFrame()

    df = df_raw[[col_staz, col_tempo, col_valore]].copy()
    df.columns = ['id_stazione', 'datetime', 'valore']

    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    df['valore']   = pd.to_numeric(df['valore'], errors='coerce')
    df = df.dropna(subset=['datetime', 'valore'])

    # Rimuovi valori non fisici (negativi o > soglia realistica)
    soglie = {'NO2': 500, 'PM2.5': 500, 'PM10': 1000}
    soglia = soglie.get(parametro, 1000)
    df = df[(df['valore'] >= 0) & (df['valore'] <= soglia)]

    # Aggrega a media giornaliera per stazione
    df['data'] = df['datetime'].dt.date
    df_daily = df.groupby(['id_stazione', 'data'])['valore'].mean().reset_index()
    df_daily.columns = ['id_stazione', 'data', parametro]

    return df_daily


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    # --- 1. Anagrafica stazioni ---
    stazioni = scarica_anagrafica_stazioni()

    # --- 2. Filtra stazioni nei nostri ATS ---
    stazioni_ats = assegna_ats_a_stazioni(stazioni)

    if stazioni_ats.empty:
        raise RuntimeError(
            "Nessuna stazione trovata nelle ATS. "
            "Verificate che confini_ats.geojson sia presente "
            "(eseguire prima lo script 02)."
        )

    # Mappa id_stazione → ATS
    col_id = next((c for c in stazioni_ats.columns
                   if 'operatore' in c.lower() or 'idsensore' in c.lower()
                   or 'idstazione' in c.lower()), None)
    if not col_id:
        print(f"Colonne disponibili: {stazioni_ats.columns.tolist()}")
        raise ValueError("Colonna ID stazione non trovata.")

    id_to_ats = dict(zip(stazioni_ats[col_id].astype(str),
                         stazioni_ats['ATS']))
    id_stazioni = list(id_to_ats.keys())

    print(f"\nStazioni selezionate: {len(id_stazioni)}")

    # --- 3. Scarica misure per ogni parametro, anno, mese ---
    tutti_i_dati = {p: [] for p in PARAMETRI}

    for parametro, nome_parametro in PARAMETRI.items():
        print(f"\n{'='*50}")
        print(f"Parametro: {parametro} ({nome_parametro})")

        for anno in ANNI:
            for mese in MESI_INFLUENZALI:
                # Salta mesi non validi per l'anno (es. aprile 2022
                # è prima della stagione 2022/23 se partiamo da ott 2022)
                print(f"  {anno}-{mese:02d}...", end=' ', flush=True)

                try:
                    df_raw = scarica_misure_periodo(
                        id_stazioni, anno, mese, nome_parametro
                    )
                    if df_raw.empty:
                        print("(nessun dato)")
                        continue

                    df_proc = processa_misure(df_raw, parametro)
                    if not df_proc.empty:
                        tutti_i_dati[parametro].append(df_proc)
                        print(f"{len(df_proc)} righe")
                    else:
                        print("(dati non validi)")

                except Exception as e:
                    print(f"ERRORE: {e}")

    # --- 4. Consolida e aggrega per ATS ---
    print(f"\n{'='*50}")
    print("Aggregazione per ATS...")

    df_lista = []

    for parametro, chunks in tutti_i_dati.items():
        if not chunks:
            print(f"  ATTENZIONE: nessun dato per {parametro}!")
            continue

        df_param = pd.concat(chunks, ignore_index=True)
        df_param['id_stazione'] = df_param['id_stazione'].astype(str)
        df_param['ATS'] = df_param['id_stazione'].map(id_to_ats)
        df_param = df_param.dropna(subset=['ATS'])

        # Media per ATS e giorno
        df_ats = df_param.groupby(['data', 'ATS'])[parametro].mean().reset_index()
        df_lista.append(df_ats)

    if not df_lista:
        print("ERRORE: nessun dato scaricato correttamente.")
        return

    # Merge di tutti i parametri
    from functools import reduce
    df_finale = reduce(
        lambda left, right: pd.merge(left, right, on=['data', 'ATS'], how='outer'),
        df_lista
    )

    df_finale['data'] = pd.to_datetime(df_finale['data'])
    df_finale = df_finale.sort_values(['ATS', 'data']).reset_index(drop=True)

    output_file = 'inquinanti_per_ats_giornaliero.csv'
    df_finale.to_csv(output_file, index=False)
    print(f"\nSalvato: {output_file}  ({len(df_finale)} righe)")

    print("\nPrime righe:")
    print(df_finale.head(10).to_string(index=False))

    # --- 5. Report copertura (importante per il report metodologico) ---
    print("\n--- REPORT COPERTURA DATI ---")
    print("(NaN = stazione non disponibile per quel parametro/ATS)")
    print(df_finale.groupby('ATS')[list(PARAMETRI.keys())].apply(
        lambda x: x.notna().mean() * 100
    ).round(1).to_string())

    print("\n⚠️  BIAS DA SEGNALARE NEL REPORT:")
    print("  • ATS Montagna ha pochissime stazioni ARPA → dati inquinanti meno rappresentativi")
    print("  • Le stazioni sono concentrate nei fondovalle, non nei comuni montani isolati")
    print("  • La media per ATS pesa ugualmente ogni stazione (non pesata per popolazione)")


if __name__ == '__main__':
    main()
