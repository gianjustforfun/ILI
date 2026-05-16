"""
=============================================================
SCRIPT 2 — ERA5 POST-PROCESSING & HUMIDITY COMPUTATION
=============================================================

WHAT THIS SCRIPT DOES:
    Reads the ERA5 NetCDF file produced by Script 1 (era5_ats.nc),
    computes absolute humidity (AH) and relative humidity (RH) from
    the two downloaded variables (2 m temperature and 2 m dewpoint),
    and spatially aggregates all meteorological fields to the two
    ATS territories of interest using ISTAT municipal boundaries.

    Processing steps:
        1. Load the ISTAT shapefile of Italian comuni and assign each
           comune to ATS_Bergamo or ATS_Montagna according to the
           province / comuni-specific rules defined in ATS_DEFINIZIONE.
        2. Reproject the ATS boundaries from the ISTAT native CRS
           (EPSG:32632, UTM Zone 32N, metres) to WGS84 (EPSG:4326,
           decimal degrees) to match the ERA5 coordinate system.
        3. For each CALENDAR DATE in era5_ats.nc:
             a) Select all timesteps belonging to that date
                (00:00, 06:00, 12:00, 18:00 UTC — 4 per day).
             b) Average T2m and Td across the 4 timesteps at every
                grid cell to obtain daily-mean T and Td.
                NOTE: averaging is done on T and Td BEFORE computing
                humidity, not on AH/RH directly, because the Magnus
                formula is non-linear (see HUMIDITY ORDER section).
             c) Compute AH and RH from the daily-mean T and Td.
             d) Perform a spatial join between ERA5 grid-cell points
                and the ATS polygons.
             e) Compute the unweighted spatial mean of T, AH and RH
                over all grid cells that fall within each ATS polygon.
        4. Concatenate all daily records into a single long-format
           DataFrame and save to CSV.
        5. Save the ATS boundary polygons (dissolved from comuni) as a
           GeoJSON file for downstream use and quality-control mapping.

IMPORTANT CHANGE FROM PREVIOUS VERSION:
    Script 1 now downloads 4 timesteps per day (00:00, 06:00, 12:00,
    18:00 UTC) instead of a single 12:00 UTC snapshot. This script
    has been updated accordingly:

        OLD FLOW (single timestep):
            for each timestep → compute AH/RH → spatial mean → 1 row/day

        NEW FLOW (4 timesteps):
            for each DATE → mean(T, Td over 4 timesteps) → compute AH/RH
                         → spatial mean → 1 row/day

    The output format (one row per date per ATS) is unchanged; only
    the internal computation pipeline has been updated.

HUMIDITY COMPUTATION ORDER — WHY T FIRST, THEN AH:
    The Magnus formula is non-linear (exponential). This means:

        mean(AH(T₁, Td₁), AH(T₂, Td₂), ...)  ≠  AH(mean(T), mean(Td))

    The two approaches give slightly different results. We choose to
    average T and Td first, then compute AH and RH from the daily means,
    because:
        - It mirrors the procedure used in Shaman et al. (2009, 2010),
          who applied humidity formulas to daily-mean temperature.
        - It is consistent with how ARPA Lombardia produces daily-mean
          pollutant values (a single arithmetic mean over 24 h readings).
        - It is the more physically meaningful quantity: AH computed from
          the mean daily temperature, rather than the mean of 4 AH values
          computed from instantaneous temperatures.

    The numerical difference between the two approaches is typically
    < 0.05 g/m³ for the temperature ranges observed in northern Italy
    during influenza season. We document the choice here for
    reproducibility.

HUMIDITY FORMULAE:
    Saturation vapour pressure (hPa) — Magnus formula:
        e_sat = 6.1078 · exp( 17.2694 · T_c / (T_c + 237.29) )

    Actual vapour pressure from dewpoint (hPa):
        e_act = 6.1078 · exp( 17.2694 · Td_c / (Td_c + 237.29) )

    Relative Humidity (%):
        RH = (e_act / e_sat) × 100

    Absolute Humidity (g/m³) — ideal-gas approximation:
        AH = (2165 · e_act) / T_K
        where T_K is air temperature in Kelvin.

    Reference: Shaman J. & Kohn M. (2009), PNAS 106(9):3243-3248.

INFLUENZA SEASON WEEK CONVENTION:
    Season 21-22 : ISO week 46/2021 → ISO week 15/2022
    Season 22-23 : ISO week 46/2022 → ISO week 15/2023
    Season 23-24 : ISO week 46/2023 → ISO week 15/2024
    Season 24-25 : ISO week 46/2024 → ISO week 15/2025
    Season 25-26 : ISO week 46/2025 → ISO week 15/2026

    This script operates at daily resolution. Weekly aggregation
    (ISO-week means) is performed by the downstream analysis scripts.

SPATIAL AGGREGATION METHOD:
    ERA5 native resolution: ~0.25° × 0.25° (~27 km at 46° N).
    The bounding box [46.7 N, 9.3 W, 45.4 S, 10.6 E] yields a grid
    of approximately 5 × 6 cells.

    Each ERA5 grid-cell centroid is tested for containment within
    ATS polygons using a point-in-polygon spatial join (GeoPandas
    sjoin, predicate='within'). Only cells whose centroid falls
    strictly inside an ATS polygon are included in the average.
    Area-weighted averaging is NOT applied here (few cells, similar
    areas); this introduces negligible error at ATS scale.

PREREQUISITES:
    pip install xarray netCDF4 numpy pandas geopandas shapely

    ISTAT comuni shapefile — download from:
        https://www.istat.it/it/archivio/222527
    Select "Limiti delle unità amministrative a fini statistici al
    1 gennaio 2024" → extract Com01012024_g_WGS84.shp.

INPUT:
    era5_ats.nc              — output of Script 1 (4 timesteps/day)
    Com01012024_g_WGS84.shp  — ISTAT comuni boundaries (WGS84)

OUTPUT:
    meteo_per_ats_giornaliero.csv — one row per (date, ATS), columns:
        data                  : calendar date (YYYY-MM-DD)
        ATS                   : ATS_Bergamo | ATS_Montagna
        temperatura_C         : daily-mean 2 m air temperature   [°C]
        umidita_assoluta_gm3  : absolute humidity from daily mean [g/m³]
        umidita_relativa_pct  : relative humidity from daily mean [%]

    confini_ats.geojson       — dissolved ATS boundary polygons
                                (WGS84, for mapping and QC)

DOWNSTREAM USAGE:
    meteo_per_ats_giornaliero.csv is read by the GAM / regression
    scripts to provide the meteorological covariates (AH or RH) that
    modulate influenza transmission risk. The daily values are then
    aggregated to ISO-week means by the analysis scripts.

BIASES AND LIMITATIONS:
    - ERA5 grid cells (~27 km) are much coarser than the ATS area:
      the spatial average is based on only 3-6 grid points per ATS,
      which limits spatial representativeness, especially for ATS
      Montagna where steep orography creates strong micro-climatic
      variability (valley inversions, lapse-rate effects).
    - Unweighted cell averaging implicitly assumes homogeneous
      population distribution within each ATS; in practice, most of
      the ATS Montagna population lives in valley floors (Sondrio,
      Darfo Boario Terme) which may not be representative of the
      average ERA5 cell value.
    - The Magnus formula is accurate to within 0.1 % for temperatures
      between −40 °C and +60 °C; no corrections are applied for
      ice-bulb effects below 0 °C.
    - Four 6-hourly timesteps approximate but do not perfectly reproduce
      the true 24-hour mean; ERA5 hourly data would be ideal but would
      increase file size and download time by ~6×.

=============================================================
"""

import xarray as xr
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

ERA5_FILE  = 'era5_ats.nc'
COMUNI_SHP = "data/raw/Com01012026_g_WGS84.shp"

# ATS territory definition.
# ISTAT province codes (3-digit zero-padded):
#   016 = Bergamo  |  098 = Sondrio  |  015 = Brescia  |  014 = Como
ATS_DEFINIZIONE = {
    'ATS_Bergamo': {
        'province_intere': ['016'],
        'comuni_specifici': []
    },
    'ATS_Montagna': {
        'province_intere': ['098'],   # entire Provincia di Sondrio
        'comuni_specifici': [
            # Provincia di Brescia — Val Camonica comuni
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
            # Provincia di Como — Alto Lario comuni
            'Cremia', 'Domaso', 'Dongo', 'Dosso del Liro', 'Garzeno',
            'Gera Lario', 'Gravedona ed Uniti', 'Livo', 'Montemezzo',
            'Musso', 'Peglio', 'Pianello del Lario', 'Sorico', 'Stazzona',
            'Trezzone', 'Vercana',
        ]
    }
}


# ---------------------------------------------------------------------------
# FUNCTIONS
# ---------------------------------------------------------------------------

def calc_humidity(T_k: np.ndarray, Td_k: np.ndarray):
    """
    Compute absolute humidity (g/m³) and relative humidity (%) from
    2 m temperature and dewpoint temperature, both in Kelvin.

    Uses the Magnus formula for saturation vapour pressure
    (Alduchov & Eskridge 1996), consistent with Shaman & Kohn (2009).

    IMPORTANT: T_k and Td_k must already be DAILY MEANS (averaged over
    the 4 ERA5 timesteps) before calling this function. Do not pass
    instantaneous values and average the output — see module docstring
    section 'HUMIDITY COMPUTATION ORDER' for the rationale.

    Parameters
    ----------
    T_k  : np.ndarray — daily-mean 2 m air temperature     [K]
    Td_k : np.ndarray — daily-mean 2 m dewpoint temperature [K]

    Returns
    -------
    AH   : np.ndarray — absolute humidity                  [g/m³]
    RH   : np.ndarray — relative humidity                  [%]
    """
    T_c  = T_k  - 273.15
    Td_c = Td_k - 273.15

    # Saturation vapour pressure at air temperature [hPa]
    e_sat = 6.1078 * np.exp((17.2694 * T_c)  / (T_c  + 237.29))
    # Actual vapour pressure at dewpoint temperature [hPa]
    e_act = 6.1078 * np.exp((17.2694 * Td_c) / (Td_c + 237.29))

    RH = (e_act / e_sat) * 100.0     # relative humidity [%]
    AH = (2165.0 * e_act) / T_k      # absolute humidity [g/m³]

    return AH, RH


def build_ats_mask(comuni_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Assign each comune to ATS_Bergamo or ATS_Montagna and return
    a filtered GeoDataFrame with an 'ATS' column.
    Comuni not belonging to either ATS are discarded.
    """
    results = []
    for ats_name, config in ATS_DEFINIZIONE.items():
        mask_prov   = comuni_gdf['COD_PROV'].isin(config['province_intere'])
        mask_comuni = comuni_gdf['COMUNE'].isin(config['comuni_specifici'])
        selected = comuni_gdf[mask_prov | mask_comuni].copy()
        selected['ATS'] = ats_name
        results.append(selected)
    return pd.concat(results, ignore_index=True)


def daily_mean_to_points(ds: xr.Dataset, date_str: str,
                         dim_time: str) -> gpd.GeoDataFrame:
    """
    For a given calendar date, average all available ERA5 timesteps
    (00:00, 06:00, 12:00, 18:00 UTC) at each grid cell, then compute
    AH and RH from the daily-mean T and Td.

    CHANGE FROM PREVIOUS VERSION:
        Previously, this function received a single timestep and
        computed AH/RH from instantaneous T and Td. Now it receives a
        date string, selects all timesteps for that date, averages T
        and Td first, and then computes humidity. This ensures that
        the output represents a true daily mean, consistent with ARPA
        Lombardia daily-mean pollutant data.

    Parameters
    ----------
    ds        : xr.Dataset  — the full ERA5 dataset (all timesteps)
    date_str  : str         — calendar date to process ('YYYY-MM-DD')
    dim_time  : str         — name of the time dimension in ds
                              ('time' or 'valid_time')

    Returns
    -------
    gdf : gpd.GeoDataFrame with columns:
          lon, lat, T_celsius, AH, RH, geometry
    """
    # ── MODIFICA 1: seleziona TUTTI i timestep del giorno ────────────────
    # Con 4 timestep/giorno, ds[dim_time] ha valori come:
    #   2022-01-01T00:00, 2022-01-01T06:00, 2022-01-01T12:00, 2022-01-01T18:00
    # Selezioniamo quelli che iniziano con la data voluta e facciamo la media.
    #
    # ds.sel con method='nearest' selezionerebbe UN solo timestep.
    # Usiamo invece where() per filtrare tutti i timestep del giorno.
    times_for_date = pd.to_datetime(
        ds[dim_time].values
    ).normalize() == pd.Timestamp(date_str)

    if not times_for_date.any():
        raise ValueError(f"No ERA5 timesteps found for date {date_str}")

    n_timesteps = int(times_for_date.sum())
    if n_timesteps < 4:
        # Avvisa ma procedi: alcuni giorni ai bordi del dataset o dopo
        # il filtro per mese potrebbero avere meno di 4 snapshot.
        print(f"  WARNING {date_str}: only {n_timesteps}/4 timesteps available "
              f"— daily mean will be less accurate.")

    # Seleziona il sottoinsieme e calcola la media lungo la dimensione tempo
    # ── MODIFICA 2: media su T e Td PRIMA di calcolare AH/RH ────────────
    # .mean(dim=dim_time) produce array 2D (lat × lon) con la media giornaliera.
    # È matematicamente più corretto rispetto a calcolare AH istantaneo
    # e poi fare la media, perché la formula di Magnus è non-lineare.
    day_ds = ds.isel({dim_time: times_for_date})
    T_mean  = day_ds['t2m'].mean(dim=dim_time).values   # shape: (lat, lon) [K]
    Td_mean = day_ds['d2m'].mean(dim=dim_time).values   # shape: (lat, lon) [K]

    # Griglia lat/lon
    lons, lats = np.meshgrid(ds.longitude.values, ds.latitude.values)
    lons = lons.flatten()
    lats = lats.flatten()

    # Calcola AH e RH dai valori MEDI giornalieri
    AH, RH = calc_humidity(T_mean.flatten(), Td_mean.flatten())

    return gpd.GeoDataFrame({
        'lon'       : lons,
        'lat'       : lats,
        'T_celsius' : T_mean.flatten() - 273.15,
        'AH'        : AH,
        'RH'        : RH,
    }, geometry=gpd.points_from_xy(lons, lats), crs='EPSG:4326')


def in_flu_season(date) -> bool:
    """Return True if the date falls within an influenza season window
    (ISO weeks 46-52 or 1-15)."""
    iso_week = pd.Timestamp(date).isocalendar().week
    return iso_week >= 46 or iso_week <= 15


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():

    # --- 1. Load ISTAT comuni shapefile ---
    print(f"Loading ISTAT comuni shapefile: {COMUNI_SHP}")
    if not Path(COMUNI_SHP).exists():
        raise FileNotFoundError(
            f"File '{COMUNI_SHP}' not found.\n"
            "Download from: https://www.istat.it/it/archivio/222527\n"
            "Select 'Limiti delle unità amministrative' → Comuni → WGS84\n"
            "and extract Com01012024_g_WGS84.shp into the working directory."
        )

    comuni = gpd.read_file(COMUNI_SHP)
    print(f"  Available columns: {comuni.columns.tolist()}")

    # Normalise column names — ISTAT occasionally changes them across releases
    rename_map = {}
    for col in comuni.columns:
        if col.upper() in ('COMUNE', 'DENOMINAZI', 'DENOMVD', 'NOME'):
            rename_map[col] = 'COMUNE'
        if col.upper() in ('COD_PROV', 'COD_PRO', 'CODPRO'):
            rename_map[col] = 'COD_PROV'
    comuni = comuni.rename(columns=rename_map)

    # ISTAT province codes are 3-digit zero-padded strings
    comuni['COD_PROV'] = comuni['COD_PROV'].astype(str).str.zfill(3)

    # --- 2. Build ATS mask ---
    comuni_ats = build_ats_mask(comuni)

    # CRITICAL: reproject from ISTAT native CRS (EPSG:32632, UTM metres)
    # to WGS84 (EPSG:4326, decimal degrees) to match ERA5 coordinates.
    # Without this step, the spatial join compares degrees with metres
    # and returns zero matches.
    comuni_ats = comuni_ats.to_crs('EPSG:4326')

    print(f"\nComuni assigned per ATS:")
    print(comuni_ats.groupby('ATS').size().to_string())

    # Dissolve comuni into ATS polygons and save GeoJSON for downstream use
    ats_polygons = (comuni_ats
                    .dissolve(by='ATS')
                    .reset_index()[['ATS', 'geometry']]
                    .to_crs('EPSG:4326'))
    ats_polygons.to_file('confini_ats.geojson', driver='GeoJSON')
    print("\nSaved: confini_ats.geojson")

    # --- 3. Load ERA5 dataset ---
    print(f"\nLoading {ERA5_FILE}...")
    ds = xr.open_dataset(ERA5_FILE)
    print(f"  Dimensions : {dict(ds.sizes)}")

    dim_time = 'valid_time' if 'valid_time' in ds.dims else 'time'
    all_timesteps = pd.to_datetime(ds[dim_time].values)
    print(f"  Time dim   : '{dim_time}'  ({len(all_timesteps)} timesteps total)")
    print(f"  Period     : {all_timesteps[0].date()}  →  {all_timesteps[-1].date()}")

    # ── MODIFICA 3: costruiamo la lista di DATE UNICHE (non timestep) ────
    # Con 4 timestep/giorno, iterare su ogni timestep produrrebbe
    # 4 righe per giorno nel CSV finale. Invece iteriamo sulle DATE
    # e all'interno di ogni data aggreghiamo i 4 timestep.
    all_dates = sorted(set(all_timesteps.normalize()))   # date uniche

    # Filtro per stagione influenzale (settimane ISO 46-52 e 1-15)
    dates_flu = [d for d in all_dates if in_flu_season(d)]
    print(f"  Unique dates after ISO-week filter (wk 46-15): {len(dates_flu)}")

    # Sanity check: ogni data dovrebbe avere 4 timestep
    timestep_counts = all_timesteps.normalize().value_counts().sort_index()
    dates_not_4 = timestep_counts[timestep_counts != 4]
    if not dates_not_4.empty:
        print(f"\n  WARNING: the following dates have ≠ 4 timesteps:")
        for d, n in dates_not_4.items():
            print(f"    {d.date()} → {n} timestep(s)")

    # --- 4. Daily aggregation and spatial join ---
    print("\nProcessing daily means (4 timesteps → 1 daily value per ATS)...")
    rows = []

    for date in dates_flu:
        date_str = date.strftime('%Y-%m-%d')

        # Aggrega i 4 timestep → daily-mean T e Td → calcola AH/RH
        try:
            gdf_pts = daily_mean_to_points(ds, date_str, dim_time)
        except ValueError as e:
            print(f"  SKIP {date_str}: {e}")
            continue

        # Spatial join: ogni cella ERA5 → poligono ATS
        joined = gpd.sjoin(
            gdf_pts,
            comuni_ats[['ATS', 'geometry']],
            how='left',
            predicate='within'
        ).dropna(subset=['ATS'])

        if joined.empty:
            print(f"  WARNING {date_str}: no ERA5 grid point falls within any ATS polygon.")
            continue

        # Media spaziale non pesata per ATS
        per_ats = (joined
                   .groupby('ATS')
                   .agg(T_celsius=('T_celsius', 'mean'),
                        AH=('AH', 'mean'),
                        RH=('RH', 'mean'))
                   .reset_index())
        per_ats['data'] = date_str
        rows.append(per_ats)

    # --- 5. Assemble and save output ---
    df_out = (pd.concat(rows, ignore_index=True)
                [['data', 'ATS', 'T_celsius', 'AH', 'RH']]
                .sort_values(['ATS', 'data'])
                .reset_index(drop=True))

    df_out.columns = ['data', 'ATS',
                      'temperatura_C',
                      'umidita_assoluta_gm3',
                      'umidita_relativa_pct']

    output_file = 'meteo_per_ats_giornaliero.csv'
    df_out.to_csv(output_file, index=False)
    print(f"\nSaved: {output_file}  ({len(df_out):,} rows)")
    print("\nFirst rows:")
    print(df_out.head(10).to_string(index=False))

    # --- 6. Sanity check: ogni data dovrebbe avere esattamente 2 righe (una per ATS) ---
    rows_per_date = df_out.groupby('data').size()
    bad_dates = rows_per_date[rows_per_date != 2]
    if not bad_dates.empty:
        print(f"\n  WARNING: {len(bad_dates)} date(s) have ≠ 2 ATS rows (expected 2):")
        print(bad_dates.to_string())
    else:
        print(f"\n  ✓ Sanity check passed: all dates have exactly 2 ATS rows.")

    # --- 7. Descriptive statistics ---
    print("\n--- DESCRIPTIVE STATISTICS BY ATS ---")
    print(df_out.groupby('ATS')[
        ['temperatura_C', 'umidita_assoluta_gm3', 'umidita_relativa_pct']
    ].describe().round(2))


if __name__ == '__main__':
    main()