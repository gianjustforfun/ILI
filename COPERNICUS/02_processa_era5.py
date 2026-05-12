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
        3. For each daily timestep in era5_ats.nc:
             a) Extract a 2-D grid snapshot of T2m and Td.
             b) Compute AH and RH at every grid cell using the Magnus
                formula (Alduchov & Eskridge 1996) — the same
                formulation adopted by Shaman & Kohn (2009).
             c) Perform a spatial join between ERA5 grid-cell points
                and the ATS polygons.
             d) Compute the unweighted spatial mean of T, AH and RH
                over all grid cells that fall within each ATS polygon.
        4. Concatenate all daily records into a single long-format
           DataFrame and save to CSV.
        5. Save the ATS boundary polygons (dissolved from comuni) as a
           GeoJSON file for downstream use and quality-control mapping.

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
    era5_ats.nc              — output of Script 1
    Com01012024_g_WGS84.shp  — ISTAT comuni boundaries (WGS84)

OUTPUT:
    meteo_per_ats_giornaliero.csv — one row per (date, ATS), columns:
        data                  : calendar date (YYYY-MM-DD)
        ATS                   : ATS_Bergamo | ATS_Montagna
        temperatura_C         : mean 2 m air temperature        [°C]
        umidita_assoluta_gm3  : mean absolute humidity          [g/m³]
        umidita_relativa_pct  : mean relative humidity          [%]

    confini_ats.geojson       — dissolved ATS boundary polygons
                                (WGS84, for mapping and QC)

DOWNSTREAM USAGE:
    meteo_per_ats_giornaliero.csv is read by the GAM / regression
    scripts to provide the meteorological covariates (AH or RH) that
    modulate influenza transmission risk.

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
    - Only the 12:00 UTC snapshot is used (inherited from Script 1);
      daily-mean AH/RH would require averaging over all four standard
      synoptic hours (00, 06, 12, 18 UTC).

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
COMUNI_SHP = 'Com01012024_g_WGS84.shp'

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

    Parameters
    ----------
    T_k  : np.ndarray — 2 m air temperature         [K]
    Td_k : np.ndarray — 2 m dewpoint temperature     [K]

    Returns
    -------
    AH   : np.ndarray — absolute humidity            [g/m³]
    RH   : np.ndarray — relative humidity            [%]
    """
    T_c  = T_k  - 273.15
    Td_c = Td_k - 273.15

    # Saturation vapour pressure at air temperature [hPa]
    e_sat = 6.1078 * np.exp((17.2694 * T_c)  / (T_c  + 237.29))
    # Actual vapour pressure at dewpoint temperature [hPa]
    e_act = 6.1078 * np.exp((17.2694 * Td_c) / (Td_c + 237.29))

    RH = (e_act / e_sat) * 100.0                  # relative humidity [%]
    AH = (2165.0 * e_act) / T_k                   # absolute humidity [g/m³]

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


def era5_to_points(ds: xr.Dataset, timestep) -> gpd.GeoDataFrame:
    """
    Extract a single daily snapshot from the ERA5 dataset and convert
    the 2-D grid to a GeoDataFrame of point geometries (one per cell).

    Handles both 'time' and 'valid_time' dimension names, which differ
    between ERA5 legacy and recent CDS downloads.

    Parameters
    ----------
    ds        : xr.Dataset — the full ERA5 dataset
    timestep  : numpy datetime64 — the specific timestep to extract

    Returns
    -------
    gdf : gpd.GeoDataFrame with columns:
          lon, lat, T_celsius, AH, RH, geometry
    """
    dim_time = 'valid_time' if 'valid_time' in ds.dims else 'time'
    day_slice = ds.sel({dim_time: timestep})

    T  = day_slice['t2m'].values.flatten()
    Td = day_slice['d2m'].values.flatten()

    lons, lats = np.meshgrid(ds.longitude.values, ds.latitude.values)
    lons = lons.flatten()
    lats = lats.flatten()

    AH, RH = calc_humidity(T, Td)

    return gpd.GeoDataFrame({
        'lon'       : lons,
        'lat'       : lats,
        'T_celsius' : T - 273.15,
        'AH'        : AH,
        'RH'        : RH,
    }, geometry=gpd.points_from_xy(lons, lats), crs='EPSG:4326')


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
    timesteps = ds[dim_time].values
    print(f"  Time dim   : '{dim_time}'  ({len(timesteps)} timesteps)")
    print(f"  Period     : {str(timesteps[0])[:10]}  →  {str(timesteps[-1])[:10]}")

    # Filter to influenza-season weeks only (ISO weeks 46-52 and 1-15)
    import datetime
    def in_flu_season(np_dt):
        d = pd.Timestamp(np_dt).date()
        iso_week = d.isocalendar().week
        return iso_week >= 46 or iso_week <= 15

    timesteps_filtered = [t for t in timesteps if in_flu_season(t)]
    print(f"  After ISO-week filter (wk 46-15): {len(timesteps_filtered)} timesteps")

    # --- 4. Daily spatial join and ATS aggregation ---
    print("\nProcessing daily snapshots...")
    rows = []

    for t in timesteps_filtered:
        date_str = str(t)[:10]

        # ERA5 grid → GeoDataFrame of points
        gdf_pts = era5_to_points(ds, t)

        # Spatial join: each ERA5 point → ATS polygon
        joined = gpd.sjoin(
            gdf_pts,
            comuni_ats[['ATS', 'geometry']],
            how='left',
            predicate='within'
        ).dropna(subset=['ATS'])

        if joined.empty:
            print(f"  WARNING {date_str}: no ERA5 grid point falls within any ATS polygon.")
            continue

        # Unweighted spatial mean per ATS
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

    # --- 6. Descriptive statistics ---
    print("\n--- DESCRIPTIVE STATISTICS BY ATS ---")
    print(df_out.groupby('ATS')[
        ['temperatura_C', 'umidita_assoluta_gm3', 'umidita_relativa_pct']
    ].describe().round(2))


if __name__ == '__main__':
    main()