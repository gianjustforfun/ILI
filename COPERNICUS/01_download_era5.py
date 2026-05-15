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

DOWNLOAD STRATEGY — 4 DAILY TIMESTEPS:
    This script downloads four 6-hourly snapshots per day:
        00:00 UTC  (≈ 01:00-02:00 local time — nocturnal minimum)
        06:00 UTC  (≈ 07:00-08:00 local time — early morning)
        12:00 UTC  (≈ 13:00-14:00 local time — afternoon peak)
        18:00 UTC  (≈ 19:00-20:00 local time — evening cooling)

    WHY FOUR TIMESTEPS INSTEAD OF ONE:

        (1) TEMPERATURE: a single afternoon value overestimates the
            daily mean compared to the true 24-hour average. For cold
            exposure studies, nocturnal minima (00:00 UTC) are
            particularly relevant, as they drive mucosa drying and
            immune suppression effects described in the ILI literature
            (see ILI_fattori_ambientali.docx §9, §2.1).

        (2) HUMIDITY: absolute humidity (AH) peaks in the early
            morning and is lowest in the afternoon (anti-correlated
            with temperature due to the vapour pressure curve). Using
            only 12:00 UTC consistently underestimates the daily mean
            AH, which is the key meteorological predictor for ILI
            (Shaman & Kohn 2009; Shaman et al. 2010). This would
            introduce a systematic bias in the direction of lower AH
            estimates → inflated apparent association with ILI.

        Averaging the four 6-hourly values approximates the 24-hour
        mean much more faithfully (trapezoidal rule over 6-hour
        intervals). The resulting daily mean T2m and Td can then be
        used to compute RH and AH that are methodologically consistent
        with the daily-mean pollutant concentrations from ARPA Lombardia
        (idoperatore=1 in the ARPA dataset, which is also a 24-hour mean).

    TRADE-OFF:
        File size increases by a factor of ~4 relative to the single-
        timestep version (~10-15 MB for the seasonal window considered
        here, still very manageable). Download time increases proportionally.

DOWNLOAD OPTIMISATIONS (to minimise file size):
    1. Tight bounding box restricted to the ATS territories of interest
       (no national or regional download).
    2. Only the 2 strictly necessary variables (T2m + Td).
    3. Four daily timesteps (00:00, 06:00, 12:00, 18:00 UTC) to compute
       a representative daily mean; see reasoning above.
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
        Timesteps : 00:00, 06:00, 12:00, 18:00 UTC per day

DOWNSTREAM USAGE:
    The output file era5_ats.nc is read by Script 2, which:
        1. Groups by calendar date and averages the 4 daily timesteps
           → produces one daily-mean value per grid cell.
        2. Converts T2m and Td from Kelvin to Celsius.
        3. Computes RH using the Magnus formula applied to daily means.
        4. Computes AH using the ideal-gas approximation.
        5. Spatially averages over each ATS polygon.
        6. Aggregates to ISO-week means and saves per-ATS CSV files.

    NOTE FOR SCRIPT 2 DEVELOPERS:
        With 4 timesteps per day, the time dimension in the NetCDF has
        4× more entries than the single-snapshot version. The daily
        aggregation step (groupby date → mean) must be performed BEFORE
        computing RH and AH, because averaging T and Td first and then
        applying the Magnus formula is mathematically more accurate than
        averaging RH values directly (nonlinearity of the exponential).

BIASES AND LIMITATIONS:
    - ERA5 is a reanalysis product, not direct observation; grid-cell
      values represent spatial averages over ~31 km × 31 km cells.
    - Mountainous terrain introduces significant sub-grid variability
      (lapse rate, valley inversions) that ERA5 cannot fully resolve.
      This is a structural limitation for ATS Montagna (Sondrio,
      Val Camonica) and should be declared as such in any analysis.
    - Four 6-hourly timesteps approximate but do not perfectly reproduce
      the true 24-hour mean; ERA5 hourly data would be ideal but would
      increase file size by 6× relative to this version.
    - The download does NOT cover October; if the season definition
      is changed to start at week 40 (common in some flu surveillance
      systems), MESI must be updated to include month 10.
    - ERA5 data are provided on a regular lat/lon grid; the spatial
      average over an ATS polygon requires weighting by cos(latitude)
      to account for the convergence of meridians at higher latitudes.
      Script 2 should implement this correction.

NOTES ON CDS QUEUE:
    The request spans 6 years × 6 months × 4 timesteps and may be
    queued by the CDS broker. Typical waiting time is 10–45 minutes
    depending on server load. The cdsapi client will print a live
    status update. Do not interrupt the process; the client handles
    reconnection automatically.

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

# Four 6-hourly snapshots per day to compute a representative daily mean.
#
# 00:00 UTC → ~01:00-02:00 local Italian time (nocturnal minimum)
# 06:00 UTC → ~07:00-08:00 local Italian time (early morning)
# 12:00 UTC → ~13:00-14:00 local Italian time (afternoon peak)
# 18:00 UTC → ~19:00-20:00 local Italian time (evening cooling)
#
# Averaging these four values approximates the 24-hour mean via a
# trapezoidal rule over 6-hour intervals. This is methodologically
# consistent with ARPA Lombardia daily-mean pollutant data (idoperatore=1).
#
# See the 'DOWNLOAD STRATEGY' section of the module docstring for the
# full rationale and comparison with the previous single-timestep approach.
TIME = ['00:00', '06:00', '12:00', '18:00']

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


def estimate_file_size_mb():
    """
    Rough estimate of the output NetCDF size in MB.

    ERA5 single-level NetCDF: ~1 KB per grid cell per timestep.
    Grid cells in bounding box: ~5 × 5 = 25 cells.
    Variables: 2 (t2m, d2m).
    Timesteps: ~6 months × 30 days × 4 snapshots × 6 years ≈ 4320.
    Estimate: 25 × 2 × 4320 × 2 bytes (float16) ≈ ~0.4 MB.
    In practice ERA5 NetCDF overhead is larger: expect 10-30 MB.
    """
    n_cells = 25          # ~5 lat × 5 lon grid cells in BBOX
    n_vars = 2            # t2m + d2m
    n_days = 30 * 6 * 6  # ~6 months × 30 days × 6 years
    n_timesteps = 4       # 00:00, 06:00, 12:00, 18:00 UTC
    bytes_per_value = 4   # float32
    overhead_factor = 2.5 # NetCDF metadata + compression overhead
    size_bytes = n_cells * n_vars * n_days * n_timesteps * bytes_per_value * overhead_factor
    return size_bytes / (1024 ** 2)


# ---------------------------------------------------------------------------
# DOWNLOAD
# ---------------------------------------------------------------------------

def main():
    if os.path.exists(OUTPUT_FILE):
        print(f"File '{OUTPUT_FILE}' already exists. Delete it to re-download.")
        return

    start_date, end_date = get_season_date_range()
    est_size = estimate_file_size_mb()

    print("Connecting to Copernicus Climate Data Store (CDS)...")
    c = cdsapi.Client()

    print("\nERA5 download request summary:")
    print(f"  Variables    : 2m_temperature, 2m_dewpoint_temperature")
    print(f"  Years        : {YEARS}")
    print(f"  Months       : {MONTHS}  (influenza season window: Nov-Apr)")
    print(f"  Timesteps    : {TIME} UTC  (4 snapshots/day → daily mean in Script 2)")
    print(f"  Bounding box : N={BBOX[0]}° W={BBOX[1]}° S={BBOX[2]}° E={BBOX[3]}°")
    print(f"  Season range : {start_date}  →  {end_date}")
    print(f"  Output       : {OUTPUT_FILE}")
    print(f"  Est. size    : ~{est_size:.0f} MB (4× larger than single-timestep version)")
    print()
    print("  WHY 4 TIMESTEPS: daily mean T and Td computed here will be")
    print("  methodologically consistent with ARPA daily-mean pollutants.")
    print("  Single 12:00 UTC snapshot would underestimate daily mean AH,")
    print("  introducing a systematic bias in the ILI association models.")
    print()
    print("  NOTE: The CDS may queue this request. Typical wait: 10-45 min.")
    print("        Do not interrupt — the client reconnects automatically.\n")

    c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'variable': [
                '2m_temperature',           # t2m [K] → convert to °C in Script 2
                '2m_dewpoint_temperature',  # d2m [K] → used for RH and AH in Script 2
            ],
            'year' : YEARS,
            'month': MONTHS,
            # All possible days (1-31); the CDS silently ignores
            # day 31 for months that have fewer days.
            'day'  : [str(d).zfill(2) for d in range(1, 32)],
            # Four 6-hourly timesteps per day.
            # Script 2 must group by date and average these before
            # computing RH and AH (average T first, then apply Magnus).
            'time' : TIME,
            'format': 'netcdf',
            # [North, West, South, East]
            'area' : BBOX,
        },
        OUTPUT_FILE
    )

    size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"\n✓ Download complete: {OUTPUT_FILE}  ({size_mb:.1f} MB)")
    print(f"  Start      : {start_date}")
    print(f"  End        : {end_date}")
    print(f"  Timesteps  : {len(TIME)} per day ({', '.join(TIME)} UTC)")
    print()
    print("  IMPORTANT — next steps in Script 2:")
    print("    1. Group by calendar date, average the 4 timesteps → daily mean T, Td")
    print("    2. Convert K → °C")
    print("    3. Compute RH and AH from daily mean T and Td (not from hourly values)")
    print("    4. Spatially average over each ATS polygon (weight by cos(lat))")
    print("    5. Aggregate to ISO-week means and save per-ATS CSV files.")


if __name__ == '__main__':
    main()