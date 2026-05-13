# ==============================================================
# SCRIPT 3 — ARPA Lombardia: air quality data by ATS
# ==============================================================
#
# OBJECTIVE
# --------------------------------------------------------------
# Download and aggregate ARPA Lombardia air quality measurements
# for selected ATS territories using a hierarchical assignment:
#
#     sensor -> ISTAT municipality -> province -> ATS
#
# This avoids direct sensor-to-ATS spatial joins and makes the
# assignment explicit, auditable, and robust to geometry issues.
#
# WORKFLOW
# --------------------------------------------------------------
# 1. Load ISTAT municipality boundaries from the local shapefile.
#    - CRS normalized to EPSG:4326.
#    - Province sigla reconstructed from COD_PROV via lookup.
# 2. Download/load ARPA sensor registry.
#    - Use nometiposensore to identify pollutant type.
# 3. Filter sensors to target pollutants:
#    - Biossido di Azoto
#    - PM10
#    - PM10 (SM2005)
#    - Particelle sospese PM2.5
# 4. Spatial join: sensor coordinates -> municipality polygon.
# 5. Municipality -> province -> ATS mapping.
# 6. Download/load measurements for retained sensors.
# 7. Compute daily means by ATS and pollutant.
# 8. Export long and wide outputs to CSV.
#
# ATS DEFINITIONS
# --------------------------------------------------------------
# ATS_Bergamo:
#     - all municipalities in province BG
#
# ATS_Montagna:
#     - all municipalities in province SO
#     - all municipalities in province CO
#     - all municipalities in province BS
#
# NOTES
# --------------------------------------------------------------
# - The ISTAT shapefile is expected in data/raw/.
# - The working directory is the COPERNICUS folder.
# - The script uses requests with verify=False for ARPA downloads
#   if SSL certificate validation fails on macOS.
# - nometiposensore is the correct field in the registry, not
#   parametro.
#
# ==============================================================

import os
import io
import warnings
import requests
import pandas as pd
import geopandas as gpd
from datetime import datetime
from requests.packages.urllib3.exceptions import InsecureRequestWarning

warnings.filterwarnings("ignore")
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# ==============================================================
# CONFIG
# ==============================================================

START_DATE = "2021-01-01"
TODAY = datetime.today().strftime("%Y-%m-%d")

OUTPUT_DIR = "data/processed"
CACHE_DIR = "data/raw/arpa"
COMUNI_FILE = "data/raw/Com01012026_g_WGS84.shp"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

TARGET_PARAMS = [
    "Biossido di Azoto",
    "PM10",
    "PM10 (SM2005)",
    "Particelle sospese PM2.5",
]

# ==============================================================
# ATS DEFINITIONS (province-based)
# ==============================================================

ATS_MAPPING = {
    "ATS_Bergamo": {"BG"},
    "ATS_Montagna": {"SO", "CO", "BS"},
}

# ==============================================================
# LOOKUP TABLE: COD_PROV -> province sigla
# ==============================================================

COD_PROV_TO_SIGLA = {
    1: "TO", 2: "VC", 3: "NO", 4: "CN", 5: "AT", 6: "AL",
    96: "BI", 103: "VB", 7: "AO", 12: "VA", 13: "CO", 14: "SO",
    15: "MI", 16: "BG", 17: "BS", 18: "PV", 19: "CR", 20: "MN",
    97: "LC", 98: "LO", 108: "MB", 23: "BZ", 22: "TN", 24: "VR",
    25: "VI", 26: "BL", 27: "TV", 28: "VE", 29: "PD", 30: "RO",
    31: "UD", 32: "GO", 33: "TS", 93: "PN", 8: "IM", 9: "SV",
    10: "GE", 11: "SP", 34: "PC", 35: "PR", 36: "RE", 37: "MO",
    38: "BO", 39: "FE", 40: "RA", 41: "FC", 42: "RN", 45: "MS",
    46: "LU", 47: "PT", 48: "FI", 49: "LI", 50: "PI", 51: "AR",
    52: "SI", 53: "GR", 100: "PO", 54: "PG", 55: "TR", 56: "VT",
    57: "RI", 58: "RM", 59: "LT", 60: "FR", 61: "AQ", 62: "TE",
    63: "PE", 68: "CH", 66: "CB", 70: "IS", 67: "CE", 64: "BN",
    65: "NA", 69: "AV", 84: "SA", 71: "FG", 72: "BA", 73: "TA",
    74: "BR", 75: "LE", 76: "PZ", 77: "MT", 78: "CS", 79: "CZ",
    80: "RC", 101: "KR", 102: "VV", 81: "PA", 82: "ME", 83: "AG",
    85: "CL", 86: "EN", 87: "CT", 88: "RG", 89: "SR", 90: "TP",
    91: "SS", 92: "NU", 95: "CA", 104: "OR", 105: "OG", 106: "VS",
    107: "CI", 111: "SU",
}

# ==============================================================
# HELPERS
# ==============================================================

def read_csv_url(url: str) -> pd.DataFrame:
    """
    Download a CSV from URL, bypassing SSL verification if needed.
    """
    r = requests.get(url, verify=False, timeout=120)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))

# ==============================================================
# MAIN
# ==============================================================

print("=" * 65)
print("SCRIPT 3 -- ARPA Lombardia: air quality data by ATS")
print(f"  Period  : {START_DATE} -> {TODAY}")
print("  Method  : sensor -> comune -> provincia -> ATS")
print("=" * 65)

# --------------------------------------------------------------
# [1/6] LOAD ISTAT MUNICIPALITIES
# --------------------------------------------------------------
# Load municipal boundaries, normalize CRS, and reconstruct the
# province sigla from COD_PROV.
# --------------------------------------------------------------

print("[1/6] Loading ISTAT municipalities...")

comuni_gdf = gpd.read_file(COMUNI_FILE)
comuni_gdf = comuni_gdf.to_crs(epsg=4326)

comuni_gdf["comune"] = comuni_gdf["COMUNE"].astype(str).str.strip()
comuni_gdf["provincia"] = comuni_gdf["COD_PROV"].astype(int).map(COD_PROV_TO_SIGLA)

print(f"  Municipalities loaded  : {len(comuni_gdf)}")
print(f"  Unmapped provinces     : {comuni_gdf['provincia'].isna().sum()}")

# --------------------------------------------------------------
# [2/6] DOWNLOAD SENSOR REGISTRY
# --------------------------------------------------------------
# Load the ARPA registry from cache if available, otherwise
# download it from the Lombardia open-data portal.
# --------------------------------------------------------------

print("[2/6] Downloading sensor registry...")

sensor_cache = os.path.join(CACHE_DIR, "sensori_registry.csv")

if os.path.exists(sensor_cache):
    print("  Loading sensor registry from cache...")
    sensori = pd.read_csv(sensor_cache)
else:
    url = "https://www.dati.lombardia.it/resource/ib47-atvt.csv?$limit=50000"
    sensori = read_csv_url(url)
    sensori.to_csv(sensor_cache, index=False)
    print("  Sensor registry downloaded and cached.")

print("Colonne sensori:", sensori.columns.tolist())
print(sensori.head(2))
print("Valori unici nometiposensore:", sorted(sensori["nometiposensore"].dropna().unique()))

# --------------------------------------------------------------
# [3/6] FILTER POLLUTANTS
# --------------------------------------------------------------
# Retain only sensors whose type matches the pollutants of
# interest. This uses the nometiposensore field.
# --------------------------------------------------------------

print("[3/6] Filtering pollutants...")

sensori = sensori[
    sensori["nometiposensore"].isin(TARGET_PARAMS)
].copy()

print(f"  Sensors matching target pollutants: {len(sensori)}")
print(sensori["nometiposensore"].value_counts().sort_index())

# --------------------------------------------------------------
# [4/6] SENSOR -> MUNICIPALITY SPATIAL JOIN
# --------------------------------------------------------------
# Convert sensor coordinates to a GeoDataFrame and assign each
# sensor to the ISTAT municipality polygon containing it.
# --------------------------------------------------------------

print("[4/6] Assigning sensors to municipalities...")

sensori = sensori.dropna(subset=["lng", "lat"])

sensori_gdf = gpd.GeoDataFrame(
    sensori,
    geometry=gpd.points_from_xy(sensori["lng"], sensori["lat"]),
    crs="EPSG:4326"
).to_crs(comuni_gdf.crs)

comuni_layer = comuni_gdf[["comune", "provincia", "geometry"]].rename(
    columns={
        "comune": "comune_istat",
        "provincia": "provincia_istat"
    }
)

joined = gpd.sjoin(
    sensori_gdf,
    comuni_layer,
    how="left",
    predicate="within"
)

outside = joined["comune_istat"].isna().sum()
print(f"  Sensors outside municipalities: {outside}")

joined = joined.dropna(subset=["comune_istat"]).copy()

# --------------------------------------------------------------
# [5/6] ASSIGN ATS VIA PROVINCE MAPPING
# --------------------------------------------------------------
# Map province_istat to ATS using explicit province->ATS rules.
# --------------------------------------------------------------

print("[5/6] Assigning ATS via province mapping...")

province_to_ats = {
    prov: ats
    for ats, provinces in ATS_MAPPING.items()
    for prov in provinces
}

joined["ATS"] = (
    joined["provincia_istat"]
    .astype(str)
    .str.upper()
    .map(province_to_ats)
)

before_filter = len(joined)
joined = joined.dropna(subset=["ATS"]).copy()
after_filter = len(joined)

print(f"  Sensors retained after ATS filter : {after_filter}")
print(f"  Sensors removed (outside ATS)     : {before_filter - after_filter}")

print("\n[DEBUG] Sensors per ATS and pollutant:\n")
print(
    joined.groupby(["ATS", "nometiposensore"])
    .size()
    .to_string()
)

# --------------------------------------------------------------
# EXPORT SENSOR MAP
# --------------------------------------------------------------
# Save an auditable map of sensor -> municipality -> province -> ATS.
# --------------------------------------------------------------

sensor_map = joined[[
    "idsensore",
    "nomestazione",
    "comune",
    "provincia",
    "ATS",
    "nometiposensore",
    "lat",
    "lng",
]].copy()

sensor_map.to_json(
    os.path.join(OUTPUT_DIR, "sensori_mappa.json"),
    orient="records",
    indent=2
)

print("\n  sensori_mappa.json exported.")

print("[6/6] Downloading measurements...")

measurement_cache = os.path.join(CACHE_DIR, "misure_arpa.csv")

if os.path.exists(measurement_cache):
    print("  Loading measurements from cache...")
    misure = pd.read_csv(measurement_cache)
else:
    sensor_ids = joined["idsensore"].astype(str).unique().tolist()

    def chunk_list(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    parts = []
    for idx, chunk in enumerate(chunk_list(sensor_ids, 10), start=1):
        ids_str = ", ".join(f"'{x}'" for x in chunk)
        where = f"idsensore in ({ids_str})"
        url = (
            "https://www.dati.lombardia.it/resource/nicp-bhqi.csv?"
            f"$where={where}"
            "&$limit=5000000"
        )
        print(f"  Downloading chunk {idx} with {len(chunk)} sensors...")
        df_part = read_csv_url(url)
        parts.append(df_part)

    misure = pd.concat(parts, ignore_index=True)
    misure.to_csv(measurement_cache, index=False)

print(f"  Rows loaded: {len(misure):,}")

# --------------------------------------------------------------
# CLEANING
# --------------------------------------------------------------

misure["data"] = pd.to_datetime(misure["data"])
misure = misure[misure["data"] >= START_DATE].copy()
misure["valore"] = pd.to_numeric(misure["valore"], errors="coerce")
misure = misure.dropna(subset=["valore"])

# --------------------------------------------------------------
# MERGE SENSOR INFO
# --------------------------------------------------------------

misure = misure.merge(
    joined[["idsensore", "ATS", "nometiposensore"]],
    on="idsensore",
    how="inner"
)

# --------------------------------------------------------------
# DAILY AGGREGATION
# --------------------------------------------------------------

misure["giorno"] = misure["data"].dt.date

daily = (
    misure
    .groupby(["giorno", "ATS", "nometiposensore"])["valore"]
    .mean()
    .reset_index()
    .rename(columns={"valore": "valore_medio"})
)

# --------------------------------------------------------------
# WIDE FORMAT
# --------------------------------------------------------------

pivot = (
    daily
    .pivot_table(
        index=["giorno", "ATS"],
        columns="nometiposensore",
        values="valore_medio"
    )
    .reset_index()
)

pivot.columns.name = None

pivot = pivot.rename(columns={
    "Biossido di Azoto": "NO2_mean_ugm3",
    "PM10": "PM10_mean_ugm3",
    "PM10 (SM2005)": "PM10_SM2005_mean_ugm3",
    "Particelle sospese PM2.5": "PM25_mean_ugm3",
})

# --------------------------------------------------------------
# EXPORT
# --------------------------------------------------------------

pivot.to_csv(
    os.path.join(OUTPUT_DIR, "inquinanti_per_ats_pivot.csv"),
    index=False
)

daily.to_csv(
    os.path.join(OUTPUT_DIR, "inquinanti_per_ats_long.csv"),
    index=False
)

# --------------------------------------------------------------
# SUMMARY
# --------------------------------------------------------------

print("=" * 65)
print(f"  inquinanti_per_ats_pivot.csv -- {len(pivot):,} rows")
print(f"  inquinanti_per_ats_long.csv  -- {len(daily):,} rows")

print("\nFirst rows:\n")
print(pivot.head(10).to_string(index=False))

print("\nDescriptive statistics:\n")
stats = (
    daily
    .groupby(["ATS", "nometiposensore"])["valore_medio"]
    .agg(["count", "mean", "min", "max"])
    .round(2)
)
print(stats)

print("\nScript completed successfully.")
print("=" * 65)