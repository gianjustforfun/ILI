
"""
=============================================================
SCRIPT 1 — ERA5 METEOROLOGICAL DATA DOWNLOAD (Copernicus CDS)
=============================================================

WHAT THIS SCRIPT DOES:
    Downloads 2-metre temperature (T2m) and 2-metre dewpoint temperature
    (Td) from the ERA5 reanalysis (ECMWF / Copernicus Climate Data Store)
    for the geographic area covering:
        • ATS Bergamo    — Provincia di Bergamo (entire province)
        • ATS Montagna   — Provincia di Sondrio (entire province)
                         + mountain comuni of Brescia (Val Camonica,
                           up to Ponte di Legno)
                         + mountain comuni of Como (Alto Lario,
                           Gravedona area)

    The two variables are the minimum set required to derive:
        • Relative Humidity (RH)
        • Absolute Humidity (AH, g/m³)
    both of which are used as meteorological covariates in subsequent
    epidemiological models (GAM / regression).

TEMPORAL COVERAGE:
    Influenza seasons 2021/22 through 2025/26.

    Season convention — each influenza season spans from
    ISO week 46 of year Y to ISO week 15 of year Y+1:

        Season 21-22 : week 46/2021 → week 15/2022
        Season 22-23 : week 46/2022 → week 15/2023
        Season 23-24 : week 46/2023 → week 15/2024
        Season 24-25 : week 46/2024 → week 15/2025
        Season 25-26 : week 46/2025 → week 15/2026

    The download therefore covers the calendar date range:
        2021-11-13  (Mon of week 46/2021)  →  2026-04-12  (Sun of week 15/2026)

    Only the months that can contain weeks 46-52 or 1-15 are included:
        January (01), February (02), March (03), April (04),
        November (11), December (12)

    NOTE: October is excluded because week 46 never starts before
    mid-November in the Gregorian calendar.

DOWNLOAD OPTIMISATIONS (to minimise file size):
    1. Tight bounding box restricted to the ATS territories of interest
       (no national or regional download).
    2. Only the 2 strictly necessary variables (T2m + Td).
    3. Single daily timestep at 12:00 UTC (representative of afternoon
       temperature in Italy; saves ~75 % vs. hourly download).
    4. Only the 6 months that fall within the influenza season window
       (Jan–Apr, Nov–Dec); summer months are excluded entirely.
    5. NetCDF output format (binary, compact, directly readable by
       xarray / scipy).

BOUNDING BOX:
    [North, West, South, East] = [46.7, 9.3, 45.4, 10.6]

    This covers:
        North : northern tip of Sondrio / Swiss border
        West  : western Lake Como shore
        South : southern boundary of Provincia di Bergamo
        East  : Val Camonica eastern ridge
    A 0.1° margin is added on all sides to avoid clipping ERA5 grid
    cells at the boundary.

    ERA5 native resolution is ~31 km (0.25° × 0.25°); the bounding box
    yields a small grid (~5 × 5 cells) sufficient for ATS-level spatial
    averages.

PREREQUISITES:
    pip install cdsapi xarray netCDF4 numpy pandas

    Copernicus CDS credentials must be stored in ~/.cdsapirc:
        url: https://cds.climate.copernicus.eu/api/v2
        key: <UID>:<API-KEY>
    Register at https://cds.climate.copernicus.eu to obtain credentials.

INPUT:
    None (data are fetched directly from the Copernicus CDS API).

OUTPUT:
    era5_ats.nc — NetCDF4 file containing:
        • t2m  : 2-metre air temperature          [K]
        • d2m  : 2-metre dewpoint temperature      [K]
        Dimensions: time × latitude × longitude
        Coverage : 2021-11-13 → 2026-04-12 (influenza-season days only)

DOWNSTREAM USAGE:
    The output file era5_ats.nc is read by Script 2, which:
        1. Converts T2m and Td from Kelvin to Celsius.
        2. Computes RH using the Magnus formula.
        3. Computes AH using the ideal-gas approximation.
        4. Spatially averages over each ATS polygon.
        5. Aggregates to ISO-week means and saves per-ATS CSV files.

BIASES AND LIMITATIONS:
    - ERA5 is a reanalysis product, not direct observation; grid-cell
      values represent spatial averages over ~31 km × 31 km cells.
    - Mountainous terrain introduces significant sub-grid variability
      (lapse rate, valley inversions) that ERA5 cannot fully resolve.
    - Using 12:00 UTC as the sole daily timestep underrepresents
      nocturnal temperature minima, which are relevant for cold
      exposure. Users requiring daily min/max should download
      additional timesteps (00:00, 06:00, 18:00 UTC).
    - The download does NOT cover October; if the season definition
      is changed to start at week 40 (common in some flu surveillance
      systems), MESI must be updated to include month 10.

NOTES ON CDS QUEUE:
    The request spans 6 years × 6 months and may be queued by the CDS
    broker. Typical waiting time is 5–30 minutes depending on server
    load. The cdsapi client will print a live status update.
    Do not interrupt the process; the client handles reconnection
    automatically.

=============================================================
"""

import cdsapi
import os
import datetime

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

# Calendar years spanned by influenza seasons 2021/22 through 2025/26.
# Season 21/22 starts in week 46/2021 (November 2021) and ends in
# week 15/2022 (April 2022); therefore year 2021 is needed for November
# and December only.
YEARS = ['2021', '2022', '2023', '2024', '2025', '2026']

# Months that can contain ISO weeks 46-52 or 1-15.
# Week 46 begins in mid-November at the earliest → October excluded.
# Week 15 ends in mid-April at the latest → May excluded.
MONTHS = ['01', '02', '03', '04', '11', '12']

# Single daily timestep: 12:00 UTC = ~13:00-14:00 local Italian time.
# Saves ~75 % of data volume vs. hourly download while preserving
# the diurnal peak temperature, which drives RH and AH estimates.
TIME = ['12:00']

# Bounding box [North, West, South, East] in decimal degrees.
# Covers ATS Bergamo + ATS Montagna (Sondrio, Val Camonica, Alto Lario)
# with a 0.1° buffer on all sides to include ERA5 border grid cells.
BBOX = [46.7,   # North — Swiss border / northern Sondrio
        9.3,    # West  — western Lake Como
        45.4,   # South — southern boundary of Provincia di Bergamo
        10.6]   # East  — Val Camonica eastern ridge

OUTPUT_FILE = 'era5_ats.nc'


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def get_season_date_range():
    """
    Returns the exact calendar start/end dates for the download,
    based on the ISO week 46 → week 15 season definition.

    Season 2021/22 : 2021-W46-Mon → 2022-W15-Sun
    Season 2025/26 : 2025-W46-Mon → 2026-W15-Sun
    """
    def iso_week_monday(year, week):
        return datetime.date.fromisocalendar(year, week, 1)

    def iso_week_sunday(year, week):
        return datetime.date.fromisocalendar(year, week, 7)

    start = iso_week_monday(2021, 46)   # 2021-11-15
    end   = iso_week_sunday(2026, 15)   # 2026-04-12
    return start, end


# ---------------------------------------------------------------------------
# DOWNLOAD
# ---------------------------------------------------------------------------

def main():
    if os.path.exists(OUTPUT_FILE):
        print(f"File '{OUTPUT_FILE}' already exists. Delete it to re-download.")
        return

    start_date, end_date = get_season_date_range()

    print("Connecting to Copernicus Climate Data Store (CDS)...")
    c = cdsapi.Client()

    print("\nERA5 download request summary:")
    print(f"  Variables   : 2m_temperature, 2m_dewpoint_temperature")
    print(f"  Years       : {YEARS}")
    print(f"  Months      : {MONTHS}  (influenza season window: Nov-Apr)")
    print(f"  Time        : {TIME} UTC  (single daily snapshot)")
    print(f"  Bounding box: N={BBOX[0]}° W={BBOX[1]}° S={BBOX[2]}° E={BBOX[3]}°")
    print(f"  Season range: {start_date}  →  {end_date}")
    print(f"  Output      : {OUTPUT_FILE}")
    print()
    print("  NOTE: The CDS may queue this request. Typical wait: 5-30 min.")
    print("        Do not interrupt — the client reconnects automatically.\n")

    c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'variable': [
                '2m_temperature',           # t2m  [K] → convert to °C downstream
                '2m_dewpoint_temperature',  # d2m  [K] → used for RH and AH
            ],
            'year' : YEARS,
            'month': MONTHS,
            # All possible days (1-31); the CDS silently ignores
            # day 31 for months that have fewer days.
            'day'  : [str(d).zfill(2) for d in range(1, 32)],
            'time' : TIME,
            'format': 'netcdf',
            # [North, West, South, East]
            'area' : BBOX,
        },
        OUTPUT_FILE
    )

    size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"\n✓ Download complete: {OUTPUT_FILE}  ({size_mb:.1f} MB)")
    print(f"  Start : {start_date}")
    print(f"  End   : {end_date}")
    print(f"  Next  : run Script 2 to compute RH, AH and aggregate by ISO week.")


if __name__ == '__main__':
    main()
