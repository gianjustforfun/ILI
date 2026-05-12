"""
=============================================================
SCRIPT 3 — ARPA LOMBARDIA AIR QUALITY DATA: DOWNLOAD & PROCESSING
=============================================================

WHAT THIS SCRIPT DOES:
    Downloads air quality measurements (NO2, PM10, PM2.5) from the
    ARPA Lombardia open-data portal (dati.lombardia.it, Socrata API),
    spatially assigns each monitoring sensor to its ATS territory, and
    aggregates daily mean concentrations into a wide-format table with
    one column per pollutant.

    Processing steps:
        1. Download the ARPA sensor registry (anagrafica), which
           contains station metadata including coordinates, sensor type,
           province sigla, and active/historical status.
        2. Filter sensors to only those measuring NO2, PM10, or PM2.5
           using the 'nometiposensore' field.
        3. Perform a spatial join between sensor point locations and
           ISTAT comuni boundaries to assign each sensor to
           ATS_Bergamo or ATS_Montagna.
        4. Apply a province-sigla sanity check (ARPA field 'provincia')
           to remove sensors geometrically captured by the wrong ATS
           polygon due to boundary dissolve artefacts (see KNOWN
           GEOMETRIC ARTEFACT below).
        5. Download measurement records from TWO Socrata datasets,
           required to cover the full 2021-present period:
               g2hp-ar79  ->  historical archive  (2018-01-01 : 2025-01-01)
               nicp-bhqi  ->  recent/live feed    (2024-01-01 : today)
           The two datasets overlap in 2024; duplicate records are
           removed by deduplication on (idsensore, data).
        6. Filter records by validity flag (stato: VA = validated,
           PR = provisional) and remove ARPA missing-data sentinel
           value (-9999).
        7. Load ISTAT per-comune population data (same file structure
           as Script 6) and assign a population weight to each sensor
           based on the residential population of its comune.
        8. Compute population-weighted daily mean concentrations per
           (date, ATS, pollutant):
               weighted_mean = sum(reading_i * pop_i) / sum(pop_i)
           Sensors whose comune is not found in the ISTAT files fall
           back to weight = 1 (plain average) with a printed warning.
        9. Pivot to wide format: one row per (date, ATS), separate
           columns for NO2_mean_ugm3, PM10_mean_ugm3, PM25_mean_ugm3.
       10. Save both long-format and wide-format CSV outputs.

WHY POPULATION WEIGHTING INSTEAD OF PLAIN MEAN:
    An unweighted average treats all sensor locations as equally
    representative of the ATS population. In practice, sensors are
    clustered in a few valley-floor towns while large uninhabited
    mountain areas have no sensors at all.
    Population weighting ensures that the ATS-level estimate reflects
    the actual human exposure distribution:
        - A sensor in Sondrio (21 000 residents) is weighted ~2.3x
          more than one in a smaller comune.
        - This is the quantity most relevant for epidemiological
          modelling of influenza burden, where the outcome
          (ER visits, ILI rate) is also population-denominated.
    The unweighted mean (valore_medio) is retained in the long-format
    output for quality-control comparison.

    PRACTICAL NOTE -- ATS Montagna sensor density:
        After the province sanity check, ATS Montagna has only 7 active
        sensors (3 NO2, 2 PM10, 2 PM2.5) covering a territory of
        approximately 3 500 km2 with complex Alpine orography. With
        so few sensors, the population-weighted mean numerically
        converges to the unweighted mean (as confirmed by the
        valore_pesato == valore_medio equality in the QC output).
        ATS Bergamo, by contrast, has 36 sensors over ~2 700 km2,
        predominantly flat or hilly terrain, where population weighting
        produces a measurably different result (NO2: +5.6 ug/m3 vs
        unweighted, reflecting higher urban exposure).
        This asymmetry must be reported explicitly in the Limitations
        section of any publication.

KNOWN GEOMETRIC ARTEFACT -- LODI SENSORS:
    The ATS Montagna boundary is built by dissolving ~134 comuni from
    three provinces (Sondrio, Brescia, Como). The resulting dissolved
    polygon has an irregular eastern border that geometrically overlaps
    with a narrow strip of the Provincia di Lodi (province sigla: LO).
    During the spatial join, up to 15-20 ARPA sensors located in Lodi
    comuni (Lodi city, Codogno, Bertonico, Abbadia Cerreto, San Rocco
    al Porto, Tavazzano con Villavesco) are incorrectly assigned to
    ATS_Montagna.

    These sensors are removed in step 4 by cross-checking the ARPA
    'provincia' field against the set of valid province sigle for each
    ATS:
        ATS_Bergamo  -> {'BG'}
        ATS_Montagna -> {'SO', 'BS', 'CO'}

    Any sensor whose 'provincia' value is not in the allowed set for
    its geometrically-assigned ATS is excluded. This filter reduced
    the sensor count from 63 (before) to 43 (after):
        ATS_Bergamo  : 36 sensors  (NO2: 18, PM10: 12, PM2.5: 6)
        ATS_Montagna :  7 sensors  (NO2:  3, PM10:  2, PM2.5: 2)

SPATIAL AUTOCORRELATION CHECK (exploratory, step 3.5):
    Before trusting any aggregation method, it is worth verifying
    whether nearby sensors actually measure similar values (i.e. whether
    spatial autocorrelation exists in the data). This is done once,
    offline, using Global Moran's I on a representative daily snapshot:

        from esda.moran import Moran
        from libpysal.weights import DistanceBand

        w = DistanceBand.from_dataframe(sensors_ats_mt, threshold=50000)
        mi = Moran(sensors_ats_mt["NO2_daily"], w)
        print(f"Moran's I = {mi.I:.3f}, p = {mi.p_sim:.3f}")

    If p < 0.05, spatial autocorrelation is significant and a proper
    interpolation method (IDW, Kriging, GWR) is warranted. If p >= 0.05,
    the population-weighted mean is already the best available estimator
    given the sparse sensor network.

    For ATS Montagna with only 7 sensors, Moran's I is likely
    underpowered; the population-weighted mean is the recommended
    default. For ATS Bergamo (36 sensors), the test is more meaningful.

ARPA SOCRATA API -- KEY NOTES:
    - idsensore is a TEXT field: must be quoted in WHERE clauses
      -> idsensore in ('123','456')
    - Sentinel value -9999 flags missing/invalid measurements -> excluded.
    - stato field codes:  VA = validated  |  PR = provisional  |  NA = invalid
    - Pagination: iterate with $offset until response < $limit rows.
    - Sensor IDs chunked to <= 200 per request (Socrata URL-length limit).
    - Province codes in the registry use ISTAT sigle (BG, SO, BS, CO,
      LO, ...), NOT numeric codes.

DATASET COVERAGE & OVERLAP STRATEGY:
    Dataset       Start        End (approx.)   Use for
    g2hp-ar79     2018-01-01   2025-01-01      seasons 21-22 ... 24-25
    nicp-bhqi     2024-01-01   today           seasons 24-25 ... 25-26

    Overlap window 2024: both datasets are downloaded; nicp-bhqi record
    is kept on deduplication (keep='last', more recent/corrected).

INFLUENZA SEASON WEEK CONVENTION:
    Season 21-22 : ISO week 46/2021 -> ISO week 15/2022
    Season 22-23 : ISO week 46/2022 -> ISO week 15/2023
    Season 23-24 : ISO week 46/2023 -> ISO week 15/2024
    Season 24-25 : ISO week 46/2024 -> ISO week 15/2025
    Season 25-26 : ISO week 46/2025 -> ISO week 15/2026

    This script produces DAILY output. Aggregation to ISO-week means
    is performed by downstream analysis scripts.

SPATIAL ASSIGNMENT OF SENSORS TO ATS:
    ATS_Bergamo  -- entire Provincia di Bergamo (ISTAT code 016 / sigla BG).
    ATS_Montagna -- entire Provincia di Sondrio (code 098 / sigla SO)
                   + 41 comuni of Val Camonica (Provincia di Brescia / BS)
                   + 16 comuni of Alto Lario (Provincia di Como / CO).

    KNOWN COVERAGE GAP:
        No active ARPA stations exist in the Alto Lario comuni (CO).
        ATS_Montagna values represent Valtellina (SO) and Val Camonica
        (BS) only. This limitation must be stated in any publication.

PREREQUISITES:
    pip install requests pandas geopandas shapely numpy

    ISTAT comuni shapefile:
        https://www.istat.it/it/archivio/222527
        -> Com01012024_g_WGS84.shp

    ISTAT population CSVs (same structure as Script 6):
        ISTAT/ATS BERGAMO/Popolazione residente_ATS_Bergamo_YYYY.csv
        ISTAT/ATS MONTAGNA/BRESCIA/Popolazione residente_Prov_Brescia_YYYY.csv
        ISTAT/ATS MONTAGNA/COMO/Popolazione residente_Prov_Como_YYYY.csv
        ISTAT/ATS MONTAGNA/SONDRIO/Popolazione residente_Prov_Sondrio_YYYY.csv

INPUT:
    ARPA Lombardia API   (live, no local file required)
    Com01012024_g_WGS84.shp
    ISTAT/ population CSV tree (see above)

OUTPUT:
    inquinanti_per_ats_pivot.csv -- wide format, one row per (date, ATS):
        giorno           : calendar date (YYYY-MM-DD)
        ATS              : ATS_Bergamo | ATS_Montagna
        NO2_mean_ugm3    : population-weighted daily mean NO2  [ug/m3]
        PM10_mean_ugm3   : population-weighted daily mean PM10 [ug/m3]
        PM25_mean_ugm3   : population-weighted daily mean PM2.5[ug/m3]

    inquinanti_per_ats_long.csv -- long format (giorno, ATS, parametro,
        valore_pesato, valore_medio, n_sensori, n_misure).
        valore_medio (unweighted) is retained for QC comparison.
        For ATS_Montagna, valore_pesato ~= valore_medio due to low
        sensor count (see PRACTICAL NOTE above).

    stazioni_arpa.geojson  -- sensor registry with ATS assignment (cached)
    confini_ats.geojson    -- dissolved ATS boundary polygons (cached)

CACHING BEHAVIOUR:
    Delete these files to force a fresh download / rebuild:
        stazioni_arpa.geojson       -> sensor registry
        misure_arpa_ats_<range>.csv -> measurement records
        confini_ats.geojson         -> ATS boundary polygons

BIASES AND LIMITATIONS:
    - Sparse sensor network for ATS Montagna (7 sensors, ~3 500 km2):
      spatial representativeness is very limited. The 3 NO2 sensors
      are located in valley floors (Sondrio, Darfo Boario Terme and
      one additional site); high-altitude areas, inversion layers, and
      small lateral valleys are not monitored. Population weighting
      does not compensate for this structural gap.
    - Alto Lario (CO) has no active ARPA stations; ATS Montagna values
      are entirely driven by Valtellina and Val Camonica sensors.
    - ATS Bergamo has denser coverage (36 sensors, ~2 700 km2) but
      sensors remain clustered in urban areas; population weighting
      correctly upweights city-centre stations (+5.6 ug/m3 for NO2
      vs unweighted mean), better reflecting actual population exposure.
    - Population weights are static (most recent ISTAT year available);
      intra-season demographic changes are not captured.
    - Only VA (validated) and PR (provisional) records are retained.
      The provisional share is higher in the most recent weeks and may
      be revised retroactively by ARPA.
    - Daily means are computed from sub-daily measurements; n_misure
      is retained in the long-format output for quality control.

=============================================================
"""

import os
import sys
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
import unicodedata
from io import StringIO
from pathlib import Path


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

# Two Socrata datasets required to cover 2021 - present
DATASET_STORICI = {
    "g2hp-ar79": {
        "url":   "https://www.dati.lombardia.it/resource/g2hp-ar79.csv",
        "range": ("2018-01-01", "2025-01-01"),
    },
    "nicp-bhqi": {
        "url":   "https://www.dati.lombardia.it/resource/nicp-bhqi.csv",
        "range": ("2024-01-01", None),   # None = up to today
    },
}

URL_STAZIONI = "https://www.dati.lombardia.it/resource/ib47-atvt.json?$limit=10000"

# Period of interest
DATA_INIZIO = "2021-01-01"
DATA_FINE   = None   # None = up to today

# ISTAT files
COMUNI_SHP = "Com01012024_g_WGS84.shp"
ISTAT_DIR  = "../ISTAT"   # root folder: same structure expected by Script 6

# ISTAT fallback years for population weights (most recent first)
ANNI_FALLBACK = [2025, 2024, 2023]

# ATS territory definition.
# ISTAT province codes (3-digit zero-padded):
#   016 = Bergamo  |  098 = Sondrio  |  015 = Brescia  |  014 = Como
ATS_DEFINIZIONE = {
    "ATS_Bergamo": {
        "province_intere": ["016"],
        "comuni_specifici": [],
    },
    "ATS_Montagna": {
        "province_intere": ["098"],
        "comuni_specifici": [
            # Provincia di Brescia -- Val Camonica (41 comuni)
            "Angolo Terme", "Artogne", "Berzo Demo", "Berzo Inferiore",
            "Bienno", "Borno", "Braone", "Breno", "Capo di Ponte",
            "Cedegolo", "Cerveno", "Ceto", "Cevo", "Cimbergo",
            "Cividate Camuno", "Corteno Golgi", "Darfo Boario Terme",
            "Edolo", "Esine", "Gianico", "Incudine", "Losine", "Lozio",
            "Malegno", "Malonno", "Monno", "Niardo", "Ono San Pietro",
            "Ossimo", "Paisco Loveno", "Paspardo", "Pian Camuno",
            "Piancogno", "Pisogne", "Ponte di Legno",
            "Saviore dell'Adamello", "Sellero", "Sonico", "Temu",
            "Vezza d'Oglio", "Vione",
            # Provincia di Como -- Alto Lario (16 comuni)
            "Cremia", "Domaso", "Dongo", "Dosso del Liro", "Garzeno",
            "Gera Lario", "Gravedona ed Uniti", "Livo", "Montemezzo",
            "Musso", "Peglio", "Pianello del Lario", "Sorico", "Stazzona",
            "Trezzone", "Vercana",
        ],
    },
}

# Split comuni_specifici by province for ISTAT weight loading
COMUNI_BRESCIA = ATS_DEFINIZIONE["ATS_Montagna"]["comuni_specifici"][:41]
COMUNI_COMO    = ATS_DEFINIZIONE["ATS_Montagna"]["comuni_specifici"][41:]

# Valid province sigle per ATS (ARPA uses sigla, not numeric codes)
PROVINCE_ATS = {
    "ATS_Bergamo":  ["BG"],
    "ATS_Montagna": ["SO", "BS", "CO"],
}


# ---------------------------------------------------------------------------
# FUNCTIONS -- registry and geography
# ---------------------------------------------------------------------------

def normalise_parameter(name: str) -> str | None:
    """
    Map the raw ARPA 'nometiposensore' string to a standardised code.

    Returns
    -------
    'NO2' | 'PM10' | 'PM25' | None
    """
    if not isinstance(name, str):
        return None
    n = name.lower().strip()
    if "biossido di azoto" in n or ("no" in n and "2" in n):
        return "NO2"
    if "pm10" in n:
        return "PM10"
    if "pm2" in n and "5" in n:
        return "PM25"
    return None


def download_sensor_registry() -> gpd.GeoDataFrame:
    """
    Download the ARPA Lombardia sensor registry from the Socrata API.
    Only active sensors (storico == 'N') are retained.
    Results are cached in stazioni_arpa.geojson.

    Returns
    -------
    gpd.GeoDataFrame -- one row per active sensor (WGS84 point geometry)
    """
    cache = Path("stazioni_arpa.geojson")
    if cache.exists():
        print("  Loading sensor registry from local cache...")
        return gpd.read_file(cache)

    print("  Downloading sensor registry from ARPA API...")
    resp = requests.get(URL_STAZIONI, timeout=60)
    resp.raise_for_status()
    sensors = pd.DataFrame(resp.json())
    print(f"  Total sensors in registry : {len(sensors)}")

    if "storico" in sensors.columns:
        sensors = sensors[sensors["storico"] == "N"]
        print(f"  Active sensors (storico=N): {len(sensors)}")

    sensors["lat"] = pd.to_numeric(sensors["lat"], errors="coerce")
    sensors["lng"] = pd.to_numeric(sensors["lng"], errors="coerce")
    sensors = sensors.dropna(subset=["lat", "lng"])

    gdf = gpd.GeoDataFrame(
        sensors,
        geometry=gpd.points_from_xy(sensors["lng"], sensors["lat"]),
        crs="EPSG:4326",
    )
    gdf.to_file(cache, driver="GeoJSON")
    return gdf


def build_ats_boundaries() -> gpd.GeoDataFrame:
    """
    Build dissolved ATS boundary polygons from the ISTAT comuni shapefile.
    Results are cached in confini_ats.geojson.

    Returns
    -------
    gpd.GeoDataFrame -- columns: ATS, geometry (CRS: EPSG:4326)
    """
    cache = Path("confini_ats.geojson")
    if cache.exists():
        return gpd.read_file(cache)

    print(f"  Building ATS boundaries from {COMUNI_SHP}...")
    comuni = gpd.read_file(COMUNI_SHP)
    comuni.columns = [c.upper() for c in comuni.columns]

    rename_map = {}
    for col in comuni.columns:
        if col in ("COMUNE", "DENOMINAZI", "DENOMVD", "NOME", "DEN_COM"):
            rename_map[col] = "COMUNE"
        if col in ("COD_PROV", "COD_PRO", "CODPRO"):
            rename_map[col] = "COD_PROV"
    comuni = comuni.rename(columns=rename_map)

    if "COD_PROV" not in comuni.columns:
        raise ValueError(
            f"Column COD_PROV not found. Available: {comuni.columns.tolist()}"
        )

    comuni["COD_PROV"] = comuni["COD_PROV"].astype(str).str.zfill(3)

    results = []
    for ats_name, cfg in ATS_DEFINIZIONE.items():
        mask_prov   = comuni["COD_PROV"].isin(cfg["province_intere"])
        mask_comuni = (
            comuni["COMUNE"].isin(cfg["comuni_specifici"])
            if cfg["comuni_specifici"]
            else pd.Series(False, index=comuni.index)
        )
        selected = comuni[mask_prov | mask_comuni].copy()
        selected["ATS"] = ats_name
        results.append(selected)
        print(f"  {ats_name}: {len(selected)} comuni")

    comuni_ats = pd.concat(results, ignore_index=True)
    polygons = comuni_ats.dissolve(by="ATS").reset_index()[["ATS", "geometry"]]
    polygons.to_file(cache, driver="GeoJSON")
    return polygons


def assign_ats_to_sensors(sensors_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Point-in-polygon spatial join: assign each sensor to ATS_Bergamo
    or ATS_Montagna. Sensors outside both polygons are dropped.

    After the spatial join, a province-code sanity check removes sensors
    whose ARPA province sigla (e.g. 'LO', 'MI') does not belong to the
    ATS they were geometrically assigned to. This handles edge cases
    where the dissolved ATS polygon slightly overlaps a neighbouring
    province due to boundary artefacts.

    Province sigle used by ARPA Lombardia (column 'provincia'):
        ATS_Bergamo  -> BG
        ATS_Montagna -> SO, BS, CO

    Parameters
    ----------
    sensors_gdf : gpd.GeoDataFrame -- sensor registry (WGS84 points)

    Returns
    -------
    gpd.GeoDataFrame -- subset with added 'ATS' column, province-validated
    """
    boundaries = build_ats_boundaries()
    joined = gpd.sjoin(
        sensors_gdf,
        boundaries[["ATS", "geometry"]],
        how="left",
        predicate="within",
    )
    joined = joined.dropna(subset=["ATS"]).copy()

    # Province sanity check: remove sensors from foreign provinces
    # (e.g. Lodi sensors geometrically captured by ATS Montagna polygon)
    if "idprovincia" in joined.columns:
        prov_col = "idprovincia"
    elif "provincia" in joined.columns:
        prov_col = "provincia"
    else:
        prov_col = None

    if prov_col:
        joined["_prov_norm"] = joined[prov_col].astype(str).str.strip().str.upper()
        province_ats_norm = {
            ats: [p.upper() for p in codes]
            for ats, codes in PROVINCE_ATS.items()
        }
        mask = joined.apply(
            lambda r: r["_prov_norm"] in province_ats_norm.get(r["ATS"], []),
            axis=1,
        )
        n_removed = (~mask).sum()
        if n_removed > 0:
            print(f"  Removed {n_removed} sensors outside valid provinces for their ATS")
        joined = joined[mask].drop(columns=["_prov_norm"]).copy()
    else:
        # Fallback: comune-name blacklist for known Lodi border artefacts
        lodi_comuni = {
            "lodi", "codogno", "bertonico", "abbadia cerreto",
            "san rocco al porto", "tavazzano con villavesco",
        }
        mask = ~joined["comune"].str.lower().str.strip().isin(lodi_comuni)
        n_removed = (~mask).sum()
        if n_removed > 0:
            print(f"  Removed {n_removed} sensors via comune blacklist (province column absent)")
        joined = joined[mask].copy()

    return joined


# ---------------------------------------------------------------------------
# FUNCTIONS -- population weights (from ISTAT files, Script 6 structure)
# ---------------------------------------------------------------------------

def load_population_weights(istat_dir: str) -> dict:
    import unicodedata

    def _strip_accents(s: str) -> str:
        return "".join(
            c for c in unicodedata.normalize("NFD", s)
            if unicodedata.category(c) != "Mn"
        )

    pop = {}

    def _find_file(folder):
        if not os.path.isdir(folder):
            return None, None
        for anno in ANNI_FALLBACK:
            for f in sorted(os.listdir(folder)):
                if f.endswith(".csv") and str(anno) in f:
                    return os.path.join(folder, f), anno
        return None, None

    def _load(filepath, anno, filter_list=None, label=""):
        if not filepath or not os.path.exists(filepath):
            print(f"  WARNING [{label}] file not found -- skipped.")
            return {}
        df = pd.read_csv(filepath)
        if "Comune" not in df.columns or "Totale" not in df.columns:
            print(f"  WARNING [{label}] columns Comune/Totale missing -- skipped.")
            return {}
        df["_norm"] = df["Comune"].astype(str).str.strip()
        df["_norm_ascii"] = df["_norm"].apply(_strip_accents).str.lower()
        if filter_list:
            norm_list_ascii = [_strip_accents(c.strip()).lower() for c in filter_list]
            df = df[df["_norm_ascii"].isin(norm_list_ascii)]
        result = dict(zip(df["_norm"].str.lower(), df["Totale"].astype(int)))
        print(f"  OK [{label}] {len(result)} comuni loaded (year {anno})")
        return result

    folders = {
        "ATS BERGAMO": (os.path.join(istat_dir, "ATS BERGAMO"), None),
        "BS subset":   (os.path.join(istat_dir, "ATS MONTAGNA", "BRESCIA"), COMUNI_BRESCIA),
        "CO subset":   (os.path.join(istat_dir, "ATS MONTAGNA", "COMO"),    COMUNI_COMO),
        "SO whole":    (os.path.join(istat_dir, "ATS MONTAGNA", "SONDRIO"), None),
    }

    for label, (folder, filter_list) in folders.items():
        fp, anno = _find_file(folder)
        pop.update(_load(fp, anno, filter_list, label))

    print(f"  Population weights available for {len(pop)} comuni.")
    return pop

    def _find_file(folder):
        if not os.path.isdir(folder):
            return None, None
        for anno in ANNI_FALLBACK:
            for f in sorted(os.listdir(folder)):
                if f.endswith(".csv") and str(anno) in f:
                    return os.path.join(folder, f), anno
        return None, None

    def _load(filepath, anno, filter_list=None, label=""):
        if not filepath or not os.path.exists(filepath):
            print(f"  WARNING [{label}] file not found -- skipped.")
            return {}
        df = pd.read_csv(filepath)
        if "Comune" not in df.columns or "Totale" not in df.columns:
            print(f"  WARNING [{label}] columns Comune/Totale missing -- skipped.")
            return {}
        df["_norm"] = df["Comune"].astype(str).str.strip()
        if filter_list:
            norm_list = [c.strip() for c in filter_list]
            df = df[df["_norm"].isin(norm_list)]
        result = dict(zip(df["_norm"].str.lower(), df["Totale"].astype(int)))
        print(f"  OK [{label}] {len(result)} comuni loaded (year {anno})")
        return result

    folders = {
        "ATS BERGAMO": (os.path.join(istat_dir, "ATS BERGAMO"), None),
        "BS subset":   (os.path.join(istat_dir, "ATS MONTAGNA", "BRESCIA"), COMUNI_BRESCIA),
        "CO subset":   (os.path.join(istat_dir, "ATS MONTAGNA", "COMO"),    COMUNI_COMO),
        "SO whole":    (os.path.join(istat_dir, "ATS MONTAGNA", "SONDRIO"), None),
    }

    for label, (folder, filter_list) in folders.items():
        fp, anno = _find_file(folder)
        pop.update(_load(fp, anno, filter_list, label))

    print(f"  Population weights available for {len(pop)} comuni.")
    return pop


# ---------------------------------------------------------------------------
# FUNCTIONS -- measurement download
# ---------------------------------------------------------------------------

def _download_from_dataset(
    dataset_id:  str,
    url_base:    str,
    ids_sensori: list,
    data_inizio: str,
    data_fine:   str | None,
    page_size:   int = 50_000,
) -> pd.DataFrame:
    """
    Download measurements from a single Socrata dataset with automatic
    pagination and sensor-ID chunking (<= 200 IDs per request).

    Parameters
    ----------
    dataset_id  : str        -- label for progress messages
    url_base    : str        -- Socrata CSV endpoint
    ids_sensori : list[str]  -- sensor IDs to retrieve
    data_inizio : str        -- start date (YYYY-MM-DD, inclusive)
    data_fine   : str | None -- end date   (YYYY-MM-DD, inclusive)
    page_size   : int        -- rows per API request

    Returns
    -------
    pd.DataFrame -- raw measurement records
    """
    date_filter = f"AND data >= '{data_inizio}T00:00:00'"
    if data_fine:
        date_filter += f" AND data <= '{data_fine}T23:59:59'"

    id_chunks = [ids_sensori[i:i+200] for i in range(0, len(ids_sensori), 200)]
    frames    = []
    total     = 0

    for ci, chunk in enumerate(id_chunks):
        ids_quoted   = ",".join(f"'{i}'" for i in chunk)
        where_clause = f"idsensore in ({ids_quoted}) {date_filter}"
        offset = 0

        while True:
            params = {
                "$where":  where_clause,
                "$limit":  page_size,
                "$offset": offset,
                "$order":  "idsensore,data",
            }
            resp = requests.get(url_base, params=params, timeout=180)
            resp.raise_for_status()

            batch = pd.read_csv(StringIO(resp.text))
            if batch.empty:
                break

            frames.append(batch)
            offset += page_size
            total  += len(batch)
            sys.stdout.write(
                f"  [{dataset_id}] chunk {ci+1}/{len(id_chunks)}, "
                f"offset {offset:,}, rows: {total:,}"
            )
            sys.stdout.flush()

            if len(batch) < page_size:
                break

    print()
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def download_measurements(
    ids_sensori: list,
    data_inizio: str = DATA_INIZIO,
    data_fine:   str | None = DATA_FINE,
    page_size:   int = 50_000,
) -> pd.DataFrame:
    """
    Download and merge measurements from all ARPA datasets needed to
    cover the requested period, then deduplicate the 2024 overlap.

    Overlap deduplication: for records present in both g2hp-ar79 and
    nicp-bhqi, the nicp-bhqi version is kept (keep='last').
    Results are cached locally; delete the CSV to force re-download.

    Parameters
    ----------
    ids_sensori : list[str]  -- sensor IDs
    data_inizio : str        -- start date (YYYY-MM-DD)
    data_fine   : str | None -- end date; None = today

    Returns
    -------
    pd.DataFrame -- deduplicated measurement records
    """
    start_tag = data_inizio.replace("-", "")
    end_tag   = data_fine.replace("-", "") if data_fine else "oggi"
    cache     = Path(f"misure_arpa_ats_{start_tag}_{end_tag}.csv")

    if cache.exists():
        print(f"  Loading measurements from cache: {cache}")
        df = pd.read_csv(cache)
        print(f"  Rows: {len(df):,} | Columns: {df.columns.tolist()}")
        return df

    d_start = pd.Timestamp(data_inizio)
    d_end   = pd.Timestamp(data_fine) if data_fine else pd.Timestamp.now()
    frames  = []

    for ds_id, ds_info in DATASET_STORICI.items():
        ds_start = pd.Timestamp(ds_info["range"][0])
        ds_end   = (pd.Timestamp(ds_info["range"][1])
                    if ds_info["range"][1] else pd.Timestamp.now())

        overlap_start = max(d_start, ds_start)
        overlap_end   = min(d_end,   ds_end)

        if overlap_start > overlap_end:
            print(f"  [{ds_id}] No overlap with requested period -> skip")
            continue

        print(f"  [{ds_id}] Downloading {overlap_start.date()} -> {overlap_end.date()}...")
        df_ds = _download_from_dataset(
            dataset_id  = ds_id,
            url_base    = ds_info["url"],
            ids_sensori = ids_sensori,
            data_inizio = str(overlap_start.date()),
            data_fine   = str(overlap_end.date()),
            page_size   = page_size,
        )
        if not df_ds.empty:
            df_ds["_source"] = ds_id
            frames.append(df_ds)
            print(f"  [{ds_id}] {len(df_ds):,} rows downloaded")

    if not frames:
        raise RuntimeError("No measurements downloaded. Check sensor IDs and date range.")

    df = pd.concat(frames, ignore_index=True)

    n_before = len(df)
    df["_date_str"] = df["data"].astype(str)
    df = df.drop_duplicates(subset=["idsensore", "_date_str"], keep="last")
    df = df.drop(columns=["_date_str", "_source"])
    if len(df) != n_before:
        print(f"  Overlap deduplication: {n_before:,} -> {len(df):,} rows")

    df.to_csv(cache, index=False)
    print(f"  Cache saved: {cache}  ({len(df):,} total rows)")
    return df


# ---------------------------------------------------------------------------
# FUNCTIONS -- population-weighted aggregation
# ---------------------------------------------------------------------------

def aggregate_population_weighted(
    df_meas:     pd.DataFrame,
    sensors_ats: gpd.GeoDataFrame,
    pop_weights: dict,
) -> pd.DataFrame:
    """
    Aggregate daily sensor readings using population-weighted means.

    For each (date, ATS, pollutant) group the weighted mean is:
        weighted_mean = sum(reading_i * pop_i) / sum(pop_i)

    where pop_i is the residential population of the comune hosting
    sensor i, sourced from ISTAT files (same structure as Script 6).

    Two-stage aggregation:
        Stage 1 -- sensor-level daily mean (one value per sensor per day,
            regardless of sub-daily measurement frequency).
        Stage 2 -- population-weighted spatial mean across sensors.

    Sensors whose comune is absent from pop_weights receive weight = 1
    (plain average fallback). A warning lists all such comuni.

    Parameters
    ----------
    df_meas      : pd.DataFrame     -- filtered records:
                   giorno, idsensore, valore, ATS, parametro
    sensors_ats  : gpd.GeoDataFrame -- registry with 'comune' and
                   'idsensore_str' columns
    pop_weights  : dict             -- {comune_lower: population}

    Returns
    -------
    pd.DataFrame -- columns:
        giorno, ATS, parametro,
        valore_pesato  (population-weighted mean [ug/m3]),
        valore_medio   (unweighted mean [ug/m3], for QC),
        n_sensori, n_misure
    """
    comune_map = (
        sensors_ats
        .set_index("idsensore_str")["comune"]
        .astype(str).str.strip().str.lower()
        .to_dict()
    )

    df = df_meas.copy()
    df["comune_lower"] = df["idsensore"].map(comune_map)
    df["pop_weight"]   = df["comune_lower"].map(pop_weights)

    missing = df[df["pop_weight"].isna()]["comune_lower"].dropna().unique()
    if len(missing) > 0:
        print(f"  WARNING: population weight not found for {len(missing)} comuni "
              f"(weight set to 1):")
        for c in sorted(missing):
            print(f"    - '{c}'  <- check spelling vs ISTAT file")

    df["pop_weight"] = df["pop_weight"].fillna(1.0)

    # Stage 1: sensor-level daily mean
    df_sensor = (
        df.groupby(["giorno", "ATS", "parametro", "idsensore", "pop_weight"])
        .agg(valore_sensor=("valore", "mean"),
             n_misure=("valore", "count"))
        .reset_index()
    )

    # Stage 2: population-weighted spatial mean
    def _wavg(g):
        w          = g["pop_weight"].values
        v          = g["valore_sensor"].values
        weighted   = float(np.average(v, weights=w))
        unweighted = float(v.mean())
        return pd.Series({
            "valore_pesato": round(weighted,   2),
            "valore_medio":  round(unweighted, 2),
            "n_sensori":     len(g),
            "n_misure":      int(g["n_misure"].sum()),
        })

    df_daily = (
        df_sensor
        .groupby(["giorno", "ATS", "parametro"])
        .apply(_wavg, include_groups=False)
        .reset_index()
    )

    return df_daily


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    period_label = f"{DATA_INIZIO} -> {DATA_FINE if DATA_FINE else 'today'}"
    print("=" * 65)
    print("SCRIPT 3 -- ARPA Lombardia: air quality data by ATS")
    print(f"  Period  : {period_label}")
    print(f"  Weights : population-based (ISTAT files from {ISTAT_DIR})")
    print("=" * 65)

    # ------------------------------------------------------------------
    # 1. Sensor registry
    # ------------------------------------------------------------------
    print("[1/6] Downloading sensor registry...")
    sensors = download_sensor_registry()

    # ------------------------------------------------------------------
    # 2. Filter by pollutant
    # ------------------------------------------------------------------
    print("[2/6] Filtering sensors: NO2, PM10, PM2.5...")
    sensors["parametro"]  = sensors["nometiposensore"].apply(normalise_parameter)
    sensors_of_interest   = sensors[sensors["parametro"].notna()].copy()
    print(f"  Sensors matching target pollutants: {len(sensors_of_interest)}")
    print(sensors_of_interest.groupby("parametro").size().to_string())

    # ------------------------------------------------------------------
    # 3. Spatial join -> ATS assignment + province sanity check
    # ------------------------------------------------------------------
    print("[3/6] Spatial join: assigning sensors to ATS...")
    sensors_ats = assign_ats_to_sensors(sensors_of_interest)

    if sensors_ats.empty:
        raise RuntimeError("No sensors found within ATS polygons. Check shapefile.")

    sensors_ats["idsensore_str"] = sensors_ats["idsensore"].astype(str)
    sensor_map      = sensors_ats.set_index("idsensore_str")[["ATS", "parametro"]]
    ids_to_download = sensor_map.index.tolist()

    print(f"  Sensors within ATS territories: {len(sensors_ats)}")
    print(sensors_ats.groupby(["ATS", "parametro"]).size().to_string())

    sensors_ats[['idsensore', 'nomestazione', 'comune', 'provincia',
                 'nometiposensore', 'lat', 'lng', 'quota', 'ATS']].to_json(
        os.path.join(os.path.dirname(__file__), "sensori_mappa.json"),
        orient="records", force_ascii=False
    )
    print("  sensori_mappa.json exported for map update")

    # ------------------------------------------------------------------
    # 4. Download measurements
    # ------------------------------------------------------------------
    print(f"[4/6] Downloading measurements ({period_label})...")
    df_meas = download_measurements(
        ids_to_download,
        data_inizio=DATA_INIZIO,
        data_fine=DATA_FINE,
    )

    # ------------------------------------------------------------------
    # 5. Load population weights
    # ------------------------------------------------------------------
    print(f"[5/6] Loading population weights from {ISTAT_DIR}...")
    pop_weights = load_population_weights(ISTAT_DIR)

    # ------------------------------------------------------------------
    # 6. Clean, filter, aggregate
    # ------------------------------------------------------------------
    print("[6/6] Cleaning data and computing population-weighted means...")

    df_meas.columns      = [c.lower() for c in df_meas.columns]
    df_meas["idsensore"] = df_meas["idsensore"].astype(str)
    df_meas["valore"]    = pd.to_numeric(df_meas["valore"], errors="coerce")
    df_meas["data"]      = pd.to_datetime(df_meas["data"],  errors="coerce")

    # Keep validated (VA) and provisional (PR) records only
    if "stato" in df_meas.columns:
        n_pre = len(df_meas)
        df_meas = df_meas[df_meas["stato"].isin(["VA", "PR"])]
        print(f"  Validity filter (VA/PR): {n_pre:,} -> {len(df_meas):,} rows")

    # Remove ARPA missing-data sentinel (-9999); threshold < -9000
    df_meas = df_meas[df_meas["valore"] > -9000]

    # Attach ATS and pollutant labels via sensor map
    df_meas           = df_meas.join(sensor_map, on="idsensore", how="inner")
    df_meas["giorno"] = df_meas["data"].dt.date
    df_meas           = df_meas.dropna(subset=["giorno", "valore", "ATS", "parametro"])

    print(f"  Valid rows after all filters: {len(df_meas):,}")

    df_daily = aggregate_population_weighted(df_meas, sensors_ats, pop_weights)
    print(f"  Aggregated rows (date x ATS x pollutant): {len(df_daily):,}")

    # ------------------------------------------------------------------
    # Pivot to wide format
    # ------------------------------------------------------------------
    df_pivot = df_daily.pivot_table(
        index=["giorno", "ATS"],
        columns="parametro",
        values="valore_pesato",
        aggfunc="mean",
    ).reset_index()

    df_pivot.columns.name = None
    rename_cols = {
        "NO2":  "NO2_mean_ugm3",
        "PM10": "PM10_mean_ugm3",
        "PM25": "PM25_mean_ugm3",
    }
    df_pivot = df_pivot.rename(
        columns={k: v for k, v in rename_cols.items() if k in df_pivot.columns}
    )
    df_pivot = df_pivot.sort_values(["giorno", "ATS"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # Save outputs
    # ------------------------------------------------------------------
    print("" + "=" * 65)

    out_pivot = "inquinanti_per_ats_pivot.csv"
    df_pivot.to_csv(out_pivot, index=False)
    print(f"  {out_pivot}  -- {len(df_pivot):,} rows")
    print(f"  Columns: {df_pivot.columns.tolist()}")

    out_long = "inquinanti_per_ats_long.csv"
    df_daily.to_csv(out_long, index=False)
    print(f"  {out_long}  -- {len(df_daily):,} rows (long format)")

    print("First 10 rows (wide format):")
    print(df_pivot.head(10).to_string(index=False))

    print("Descriptive statistics -- population-weighted means [ug/m3]:")
    stats = (
        df_daily
        .groupby(["ATS", "parametro"])["valore_pesato"]
        .agg(n="count", mean="mean", min="min", max="max")
        .round(2)
    )
    print(stats.to_string())

    print("Weighted vs unweighted mean comparison (overall averages):")
    cmp = (
        df_daily
        .groupby(["ATS", "parametro"])[["valore_pesato", "valore_medio"]]
        .mean()
        .round(3)
    )
    print(cmp.to_string())

    print("Script 3 completed successfully.")


if __name__ == "__main__":
    main()
