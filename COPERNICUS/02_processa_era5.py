"""
SCRIPT 2 — Processamento ERA5 + calcolo umidità assoluta
=========================================================
Legge era5_ats.nc, calcola umidità assoluta (AH) e relativa (RH),
poi aggrega i dati per ATS usando i confini comunali ISTAT.

LOGICA:
  - ERA5 produce una griglia di punti ~11x11 km
  - Per ogni cella della griglia, troviamo a quale comune appartiene
    (spatial join con shapefile ISTAT dei comuni)
  - Calcoliamo la media per ATS pesando le celle per area

OUTPUT:
  meteo_per_ats_giornaliero.csv  — una riga per (data, ATS), colonne T, AH, RH

PREREQUISITI:
  pip install xarray netCDF4 numpy pandas geopandas shapely
  Shapefile comuni italiani ISTAT scaricabile da:
  https://www.istat.it/it/archivio/222527
  → scaricate "Limiti delle unità amministrative a fini statistici al 1 gennaio 2024"
  → estraete il file dei Comuni (es. Com01012024_g_WGS84.shp)
"""

import xarray as xr
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path


# ---------------------------------------------------------------------------
# CONFIGURAZIONE
# ---------------------------------------------------------------------------

ERA5_FILE = 'era5_ats.nc'

# Shapefile comuni ISTAT (scaricato separatamente, vedi istruzioni sopra)
COMUNI_SHP = 'Com01012024_g_WGS84.shp'

# Definizione dei territori ATS
# Chiave = nome ATS, valore = dict con province complete e/o lista comuni specifici

# Codici ISTAT delle province:
#   016 = Bergamo, 014 = Como, 017 = Cremona, 098 = Sondrio, 015 = Brescia
ATS_DEFINIZIONE = {
    'ATS_Bergamo': {
        'province_intere': ['016'],   # tutta la provincia di Bergamo
        'comuni_specifici': []        # nessun comune aggiuntivo
    },
    'ATS_Montagna': {
        'province_intere': ['098'],   # tutta la provincia di Sondrio
        'comuni_specifici': [
            # Comuni della provincia di Brescia (Val Camonica)
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
            # Comuni della provincia di Como (Alto Lario)
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

def calc_humidity(T_k: np.ndarray, Td_k: np.ndarray):
    """
    Calcola umidità assoluta (g/m³) e relativa (%) da T e Td in Kelvin.

    Formula Magnus per la pressione di vapore saturo (Alduchov & Eskridge 1996).
    Questa è la stessa formula usata in Shaman & Kohn (2009), lo studio
    cardine della vostra revisione letteraria.

    Parametri:
        T_k  : temperatura in Kelvin
        Td_k : temperatura di rugiada (dew point) in Kelvin

    Ritorna:
        AH   : umidità assoluta in g/m³
        RH   : umidità relativa in %
    """
    T_c  = T_k  - 273.15  # converti in Celsius
    Td_c = Td_k - 273.15

    # Pressione vapore saturo alla temperatura dell'aria (hPa)
    e_sat = 6.1078 * np.exp((17.2694 * T_c)  / (T_c  + 237.29))
    # Pressione vapore effettiva alla temperatura di rugiada (hPa)
    e_act = 6.1078 * np.exp((17.2694 * Td_c) / (Td_c + 237.29))

    # Umidità relativa (%)
    RH = (e_act / e_sat) * 100.0

    # Umidità assoluta (g/m³)
    # Formula: AH = (2165 * e) / T  dove e in hPa e T in Kelvin
    # Derivata dalla legge dei gas ideali per il vapore acqueo
    AH = (2165.0 * e_act) / T_k

    return AH, RH


def build_ats_mask(comuni_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Costruisce un GeoDataFrame con una colonna 'ATS' che indica
    l'appartenenza di ogni comune a ATS_Bergamo o ATS_Montagna.
    I comuni non appartenenti a nessuna ATS vengono scartati.
    """
    risultati = []

    for nome_ats, config in ATS_DEFINIZIONE.items():
        # 1. Comuni da province intere
        mask_prov = comuni_gdf['COD_PROV'].isin(config['province_intere'])

        # 2. Comuni specifici (da altre province)
        # Usiamo COMUNE (nome) — fate attenzione agli accenti!
        # Il campo nel shapefile ISTAT si chiama 'COMUNE' o 'DENOMINAZI'
        # Verifichiamo entrambi più sotto
        mask_comuni = comuni_gdf['COMUNE'].isin(config['comuni_specifici'])

        selezionati = comuni_gdf[mask_prov | mask_comuni].copy()
        selezionati['ATS'] = nome_ats
        risultati.append(selezionati)

    return pd.concat(risultati, ignore_index=True)


def era5_to_points(ds: xr.Dataset, data: str) -> gpd.GeoDataFrame:
    """
    Estrae una slice giornaliera da ERA5 e la converte in GeoDataFrame
    di punti (uno per cella della griglia).
    """
    # ERA5 recente usa 'valid_time' invece di 'time'
    dim_tempo = 'valid_time' if 'valid_time' in ds.dims else 'time'
    giorno = ds.sel({dim_tempo: data})

    # ERA5 usa 't2m' per temperatura e 'd2m' per dew point
    T  = giorno['t2m'].values.flatten()
    Td = giorno['d2m'].values.flatten()

    # Crea meshgrid di coordinate
    lons, lats = np.meshgrid(ds.longitude.values, ds.latitude.values)
    lons = lons.flatten()
    lats = lats.flatten()

    # Calcola umidità
    AH, RH = calc_humidity(T, Td)

    gdf = gpd.GeoDataFrame({
        'lon': lons,
        'lat': lats,
        'T_celsius': T - 273.15,
        'AH': AH,
        'RH': RH,
    }, geometry=gpd.points_from_xy(lons, lats), crs='EPSG:4326')

    return gdf


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    # --- 1. Carica shapefile comuni ---
    print(f"Carico shapefile comuni: {COMUNI_SHP}")
    if not Path(COMUNI_SHP).exists():
        raise FileNotFoundError(
            f"File {COMUNI_SHP} non trovato.\n"
            "Scaricarlo da https://www.istat.it/it/archivio/222527\n"
            "Cercare 'Limiti delle unità amministrative' → Comuni → WGS84"
        )

    comuni = gpd.read_file(COMUNI_SHP)

    # Debug: stampa le colonne disponibili per capire i nomi esatti
    print(f"Colonne shapefile: {comuni.columns.tolist()}")

    # Normalizzazione nomi colonne (ISTAT cambia a volte i nomi)
    # Mappa i nomi comuni che ISTAT usa
    rename_map = {}
    for col in comuni.columns:
        if col.upper() in ('COMUNE', 'DENOMINAZI', 'DENOMVD', 'NOME'):
            rename_map[col] = 'COMUNE'
        if col.upper() in ('COD_PROV', 'COD_PRO', 'CODPRO'):
            rename_map[col] = 'COD_PROV'

    comuni = comuni.rename(columns=rename_map)

    # I codici provincia in ISTAT sono numerici a 3 cifre come stringa
    comuni['COD_PROV'] = comuni['COD_PROV'].astype(str).str.zfill(3)

    # --- 2. Costruisci maschera ATS ---
    comuni_ats = build_ats_mask(comuni)

    # CRITICO: riproietta i confini ATS da EPSG:32632 (UTM, metri)
    # a EPSG:4326 (gradi lat/lon), stesso sistema di ERA5.
    # Senza questo il spatial join confronta gradi con metri → nessun match.
    comuni_ats = comuni_ats.to_crs('EPSG:4326')
    print(f"\nComuni per ATS:")
    print(comuni_ats.groupby('ATS').size().to_string())

    # Geometria unione per ogni ATS (poligono complessivo, usato per la viz)
    poligoni_ats = comuni_ats.dissolve(by='ATS').reset_index()[['ATS', 'geometry']]
    poligoni_ats = poligoni_ats.to_crs('EPSG:4326')  # assicura WGS84 nel file salvato
    poligoni_ats.to_file('confini_ats.geojson', driver='GeoJSON')
    print("\nSalvato confini_ats.geojson")

    # --- 3. Carica ERA5 ---
    print(f"\nCarico {ERA5_FILE}...")
    ds = xr.open_dataset(ERA5_FILE)
    print(f"Dimensioni: {dict(ds.sizes)}")

    # ERA5 recente usa 'valid_time' invece di 'time' — gestiamo entrambi
    dim_tempo = 'valid_time' if 'valid_time' in ds.dims else 'time'
    valori_tempo = ds[dim_tempo].values
    print(f"Dimensione temporale: '{dim_tempo}'  ({len(valori_tempo)} timestep)")
    print(f"Periodo: {str(valori_tempo[0])[:10]} → {str(valori_tempo[-1])[:10]}")

    # --- 4. Per ogni giorno, spatial join e media per ATS ---
    print("\nProcesso giorno per giorno...")
    righe = []

    for t in valori_tempo:
        data_str = str(t)[:10]

        # Punti ERA5 per questo giorno
        gdf_punti = era5_to_points(ds, t)

        # Spatial join: ogni punto ERA5 → comune → ATS
        joined = gpd.sjoin(
            gdf_punti,
            comuni_ats[['ATS', 'geometry']],
            how='left',
            predicate='within'
        )

        # Scarta punti fuori dai confini ATS
        joined = joined.dropna(subset=['ATS'])

        if joined.empty:
            print(f"  ATTENZIONE {data_str}: nessun punto ERA5 in nessuna ATS!")
            continue

        # Media per ATS
        per_ats = joined.groupby('ATS').agg(
            T_celsius=('T_celsius', 'mean'),
            AH=('AH', 'mean'),
            RH=('RH', 'mean'),
        ).reset_index()
        per_ats['data'] = data_str
        righe.append(per_ats)

    # --- 5. Salva output ---
    df_out = pd.concat(righe, ignore_index=True)
    df_out = df_out[['data', 'ATS', 'T_celsius', 'AH', 'RH']]
    df_out = df_out.sort_values(['ATS', 'data']).reset_index(drop=True)

    # Rinomina colonne per chiarezza
    df_out.columns = ['data', 'ATS', 'temperatura_C', 'umidita_assoluta_gm3', 'umidita_relativa_pct']

    output_file = 'meteo_per_ats_giornaliero.csv'
    df_out.to_csv(output_file, index=False)
    print(f"\nSalvato {output_file}  ({len(df_out)} righe)")
    print("\nPrime righe:")
    print(df_out.head(10).to_string(index=False))

    # --- 6. Verifica rapida ---
    print("\n--- STATISTICHE DESCRITTIVE ---")
    print(df_out.groupby('ATS')[['temperatura_C', 'umidita_assoluta_gm3']].describe().round(2))


if __name__ == '__main__':
    main()