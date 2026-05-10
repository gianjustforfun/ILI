"""
=============================================================
SCRIPT 6 — ISTAT POPULATION DATA ANALYSIS
=============================================================

WHAT THIS SCRIPT DOES:
    1. Loads ISTAT population CSV files from the ISTAT/ subfolder of the
       project directory (one file per ATS/province per year).
    2. Extracts the population total from each file (last row, column
       'Totale').
    3. Builds two population datasets:
         a) ATS Bergamo — directly from ATS BERGAMO/ files.
         b) ATS Montagna — sum of:
              • Selected comuni of Provincia di Brescia
              • Selected comuni of Provincia di Como
              • Entire Provincia di Sondrio
    4. Saves one CSV per influenza season per ATS into:
         output/stagioni/ATS_BERGAMO/
         output/stagioni/ATS_MONTAGNA/
    5. Produces one plot per influenza season (saved to output/grafici/)
       showing — for both ATS Bergamo and ATS Montagna — two overlaid
       curves:
         • Total ER visits / population  (all-cause rate)
         • ILI ER visits  / population   (ILI-specific rate)

INPUT DIRECTORY STRUCTURE EXPECTED:
    ISTAT/
    ├── ATS BERGAMO/
    │   ├── Popolazione residente_ATS_Bergamo_2022.csv
    │   ├── Popolazione residente_ATS_Bergamo_2023.csv
    │   ├── Popolazione residente_ATS_Bergamo_2024.csv
    │   ├── Popolazione residente_ATS_Bergamo_2025.csv
    │   └── Popolazione residente_ATS_Bergamo_2026.csv
    └── ATS MONTAGNA/
        ├── BRESCIA/
        │   ├── Popolazione residente_Prov_Brescia_2022.csv
        │   └── ...
        ├── COMO/
        │   ├── Popolazione residente_Prov_Como_2022.csv
        │   └── ...
        └── SONDRIO/
            ├── Popolazione residente_Prov_Sondrio_2022.csv
            └── ...

    ⚠ NOTE: The Brescia and Como CSVs from ISTAT contain data at
      COMUNE level, not provincia level. This script filters only the
      comuni that fall within the ATS Montagna catchment area (see
      COMUNI_BRESCIA and COMUNI_COMO constants below). The Sondrio
      CSV is taken in full (all comuni in Sondrio belong to ATS
      Montagna). ATS Bergamo is a single-file dataset (province = ATS).

INFLUENZA SEASON CONVENTION (same as Script 1):
    Season 21-22 : weeks 1-15 of 2022 only (weeks 48-52 of 2021 missing)
    Season 22-23 : weeks 48-52/2022  +  weeks 1-15/2023
    Season 23-24 : weeks 48-52/2023  +  weeks 1-15/2024
    Season 24-25 : weeks 48-52/2024  +  weeks 1-15/2025
    Season 25-26 : weeks 48-52/2025  +  weeks 1-15/2026

    Population assignment per season:
        We use the ISTAT census file for the LATER calendar year of the
        season (e.g. season 22-23 → 2023 file). This is a conservative
        choice: the population denominator reflects the state at the
        mid-point of the ILI winter period (January–April).

ILI DATA DEPENDENCY:
    This script reads the CSV output produced by Script 1:
        ../SORVEGLIANZA ACCESSI PS/output/ATS_BERGAMO/access_tot_bergamo_stagionale.csv
        ../SORVEGLIANZA ACCESSI PS/output/ATS_BERGAMO/ili_ats_bergamo_stagionale.csv
        ../SORVEGLIANZA ACCESSI PS/output/ATS_MONTAGNA/access_tot_montagna_stagionale.csv
        ../SORVEGLIANZA ACCESSI PS/output/ATS_MONTAGNA/ili_ats_montagna_stagionale.csv
    ➜ Run Script 1 before this script.

OUTPUT STRUCTURE:
    output/
    ├── stagioni/
    │   ├── ATS_BERGAMO/
    │   │   ├── popolazione_bergamo_21-22.csv
    │   │   ├── popolazione_bergamo_22-23.csv
    │   │   ├── popolazione_bergamo_23-24.csv
    │   │   ├── popolazione_bergamo_24-25.csv
    │   │   └── popolazione_bergamo_25-26.csv
    │   └── ATS_MONTAGNA/
    │       ├── popolazione_montagna_21-22.csv
    │       └── ...
    └── grafici/
        ├── ratio_ili_population_21-22.png
        ├── ratio_ili_population_22-23.png
        ├── ratio_ili_population_23-24.png
        ├── ratio_ili_population_24-25.png
        └── ratio_ili_population_25-26.png

BIASES AND LIMITATIONS:
    - Ecological bias: all data are aggregated at ATS level. No
      individual-level inference is possible.
    - Population figures are annual ISTAT estimates; they do not capture
      intra-year variation (births, deaths, migration).
    - Using the same population figure for all weeks of a season
      introduces minor approximation error, acceptable at this scale.

REQUIREMENTS:
    pip install pandas matplotlib
=============================================================
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import warnings

warnings.filterwarnings("ignore")


# -------------------------------------------------------
# SECTION 1 — CONFIGURATION
# -------------------------------------------------------

# Root directory of ISTAT files (relative to where you run the script)
ISTAT_DIR = "."  # lo script gira da dentro ISTAT/, quindi "." è la root corretta

# Output directories
OUT_STAGIONI_BG = "output/istat_ats_bergamo"
OUT_STAGIONI_MT = "output/istat_ats_montagna"
OUT_GRAFICI     = "output/grafici"

# Script 1 CSV outputs (read as ILI input)
# Path relative to where you run the script (project root = ILI/)
ILI_DIR_BG = "../SORVEGLIANZA ACCESSI PS/output/ATS_BERGAMO"
ILI_DIR_MT = "../SORVEGLIANZA ACCESSI PS/output/ATS_MONTAGNA"

# Years available in the ISTAT files
# Modify this list if you add or remove yearly files
ANNI_DISPONIBILI = [2022, 2023, 2024, 2025, 2026]

# Season → ISTAT year mapping (we use the later calendar year of each season)
# If the 2026 file does not exist yet, the script falls back to 2025
STAGIONI = {
    "21-22": 2022,
    "22-23": 2023,
    "23-24": 2024,
    "24-25": 2025,
    "25-26": 2026,   # fallback to 2025 if 2026 not available
}

# -------------------------------------------------------
# ATS MONTAGNA — comuni that belong to this ATS
# Source: official ATS Montagna catchment area
#
# The ISTAT files now have ONE ROW PER COMUNE (column 'Comune'),
# so we can filter exactly — no approximation needed.
# Sondrio: read in full (entire province = ATS Montagna).
# -------------------------------------------------------

COMUNI_BRESCIA = [
    "Angolo Terme", "Artogne", "Berzo Demo", "Berzo Inferiore", "Bienno",
    "Borno", "Braone", "Breno", "Capo di Ponte", "Cedegolo", "Cerveno",
    "Ceto", "Cevo", "Cimbergo", "Cividate Camuno", "Corteno Golgi",
    "Darfo Boario Terme", "Edolo", "Esine", "Gianico", "Incudine",
    "Losine", "Lozio", "Malegno", "Malonno", "Monno", "Niardo",
    "Ono San Pietro", "Ossimo", "Paisco Loveno", "Paspardo", "Pian Camuno",
    "Piancogno", "Pisogne", "Ponte di Legno", "Saviore dell'Adamello",
    "Sellero", "Sonico", "Temù", "Vezza d'Oglio", "Vione",
]

COMUNI_COMO = [
    "Cremia", "Domaso", "Dongo", "Dosso del Liro", "Garzeno", "Gera Lario",
    "Gravedona ed Uniti", "Livo", "Montemezzo", "Musso", "Peglio",
    "Pianello del Lario", "Sorico", "Stazzona", "Trezzone", "Vercana",
]

# Sondrio: entire province → no filter needed


# -------------------------------------------------------
# SECTION 2 — HELPER FUNCTIONS
# -------------------------------------------------------

def get_population_total(filepath):
    """
    Reads an ISTAT population CSV and returns the total resident
    population as an integer.

    Handles TWO formats automatically:

    Format A — per-comune (new format):
        Each row = one comune. Columns include 'Comune' and 'Totale'.
        No aggregate row. Total = sum of all 'Totale' values.
        Used for: ATS Bergamo (all comuni of the province),
                  Sondrio (all comuni of the province).

    Format B — per-età (old format):
        Each row = one age band. Last row has Età = 'Totale'.
        Total = value in the 'Totale' column of that last row.

    Parameters:
        filepath (str): path to the ISTAT CSV file

    Returns:
        int: total population, or None if the file cannot be read
    """
    if not os.path.exists(filepath):
        return None
    try:
        df = pd.read_csv(filepath)
        # Detect format: if there is a 'Comune' column → per-comune format
        if 'Comune' in df.columns and 'Totale' in df.columns:
            return int(df['Totale'].sum())
        # Otherwise: per-età format — find the 'Totale' summary row
        totale_row = df[df['Età'].astype(str).str.strip() == 'Totale']
        if totale_row.empty:
            totale_row = df.tail(1)
        return int(totale_row['Totale'].values[0])
    except Exception as e:
        print(f"  ⚠ Error reading {filepath}: {e}")
        return None


def get_population_comuni(filepath, comuni_list):
    """
    Reads an ISTAT CSV where EACH ROW IS ONE COMUNE (new format from
    demo.istat.it with columns: 'Codice comune', 'Comune', marital
    status columns, 'Totale maschi', 'Totale femmine', 'Totale').

    Filters to the comuni in comuni_list and sums their 'Totale' column.

    This is the EXACT and CORRECT approach: no approximation needed
    because the file already has one row per comune with its population.

    Matching strategy:
        Exact string match after stripping leading/trailing whitespace
        from both the CSV values and the comuni_list entries.
        If a comune is not found, a warning is printed and it is skipped
        (contributes 0 to the sum). Check warnings carefully — a mismatch
        is usually due to accents, apostrophes, or capitalisation differences
        between how the comune is named in COMUNI_BRESCIA/COMUNI_COMO and
        how ISTAT spells it in the file.

    Parameters:
        filepath (str):      path to the ISTAT per-comune CSV
        comuni_list (list):  names of comuni to include

    Returns:
        int: sum of 'Totale' for matched comuni, or 0 on error
    """
    if not os.path.exists(filepath):
        print(f"  ⚠ File not found: {filepath}")
        return 0
    try:
        df = pd.read_csv(filepath)
        if 'Comune' not in df.columns or 'Totale' not in df.columns:
            print(f"  ⚠ Expected columns 'Comune' and 'Totale' not found in {filepath}")
            print(f"     Columns found: {df.columns.tolist()}")
            return 0

        # Normalise whitespace in both sides for robust matching
        df['_comune_norm'] = df['Comune'].astype(str).str.strip()
        comuni_norm = [c.strip() for c in comuni_list]

        matched   = df[df['_comune_norm'].isin(comuni_norm)]
        not_found = [c for c in comuni_norm if c not in df['_comune_norm'].values]

        if not_found:
            print(f"  ⚠ {len(not_found)} comuni NOT FOUND in {os.path.basename(filepath)}:")
            for c in not_found:
                print(f"       - '{c}'")
            print(f"     Check spelling vs ISTAT names in the file.")

        total = int(matched['Totale'].sum())
        print(f"     {len(matched)}/{len(comuni_norm)} comuni matched → pop = {total:,}")
        return total
    except Exception as e:
        print(f"  ⚠ Error reading {filepath}: {e}")
        return 0



def _find_year_file(folder, anno):
    """
    Finds the CSV file for a given year inside a folder by looking for
    files whose name ends with '_YYYY.csv' or contains 'YYYY'.

    Parameters:
        folder (str): directory to search
        anno (int):   year to find

    Returns:
        str: full path to the file, or None if not found
    """
    if not os.path.isdir(folder):
        return None
    for fname in os.listdir(folder):
        if fname.endswith(".csv") and str(anno) in fname:
            return os.path.join(folder, fname)
    return None


def load_ili_csv(filepath, value_col):
    """
    Loads a seasonalised ILI CSV produced by Script 1 and returns it
    as a DataFrame.

    Parameters:
        filepath (str):   path to the CSV
        value_col (str):  name of the value column to keep

    Returns:
        pd.DataFrame with columns [STAGIONE, WEEK, ORDINE, value_col]
        or empty DataFrame if file not found
    """
    if not os.path.exists(filepath):
        print(f"  ⚠ ILI file not found: {filepath} — run Script 1 first.")
        return pd.DataFrame()
    df = pd.read_csv(filepath)
    if value_col not in df.columns:
        print(f"  ⚠ Column '{value_col}' not found in {filepath}.")
        return pd.DataFrame()
    return df


def salva_csv_stagione(pop_value, anno_istat, stagione, ats_label, output_dir):
    """
    Saves a one-row CSV summarising the population figure used for a
    given influenza season.

    WHY save a per-season CSV:
        Keeping the population denominator alongside the ILI counts
        makes the dataset self-contained for downstream analyses and
        ensures reproducibility — anyone running a later script can
        verify which population figure was used without re-running
        this script.

    Parameters:
        pop_value (int):    population count
        anno_istat (int):   ISTAT year used
        stagione (str):     e.g. '22-23'
        ats_label (str):    e.g. 'ATS_BERGAMO'
        output_dir (str):   where to save
    """
    os.makedirs(output_dir, exist_ok=True)
    prefix = "bergamo" if "BERGAMO" in ats_label else "montagna"
    fname  = f"popolazione_{prefix}_{stagione}.csv"
    fpath  = os.path.join(output_dir, fname)

    df_out = pd.DataFrame([{
        "STAGIONE":         stagione,
        "ATS":              ats_label,
        "ANNO_ISTAT":       anno_istat,
        "POPOLAZIONE_TOT":  pop_value if pop_value is not None else "N/A",
    }])
    df_out.to_csv(fpath, index=False)
    print(f"  ✓ Saved: {fpath}")


# -------------------------------------------------------
# SECTION 3 — LOAD ISTAT DATA
# -------------------------------------------------------

print("\n" + "=" * 65)
print("SECTION 3: LOADING ISTAT POPULATION DATA")
print("=" * 65)

# --- 3a: ATS BERGAMO ---
# One file per year, covering the whole ATS (province = ATS)
print("\n[3a] ATS BERGAMO — whole-ATS files")

folder_bg = os.path.join(ISTAT_DIR, "ATS BERGAMO")
pop_bergamo = {}   # {stagione: population}

for stagione, anno in STAGIONI.items():
    filepath = _find_year_file(folder_bg, anno)
    if filepath is None and anno == 2026:
        filepath = _find_year_file(folder_bg, 2025)
        if filepath:
            print(f"  ℹ Season {stagione}: 2026 not found, using 2025.")
    if filepath is None:
        print(f"  ⚠ No file for year {anno} in {folder_bg}")
        pop_bergamo[stagione] = None
        continue
    pop = get_population_total(filepath)
    pop_bergamo[stagione] = pop
    disp = f"{pop:,}" if pop else "N/A"
    print(f"  ✓ {stagione} ({anno}): population = {disp}  [{os.path.basename(filepath)}]")

# Save per-season CSVs
print("\n  Saving per-season CSVs (ATS Bergamo)...")
for stagione, pop in pop_bergamo.items():
    anno_usato = STAGIONI[stagione]
    # If 2026 was unavailable, we used 2025
    if anno_usato == 2026 and pop is not None:
        # Check which file we actually found
        if _find_year_file(folder_bg, 2026) is None:
            anno_usato = 2025
    salva_csv_stagione(pop, anno_usato, stagione, "ATS_BERGAMO", OUT_STAGIONI_BG)

# --- 3b: ATS MONTAGNA — BRESCIA subset ---
# The Brescia folder likely contains one file per year for the whole
# province. We attempt to filter to the ATS Montagna comuni.
print("\n[3b] ATS MONTAGNA — Brescia subset")

folder_bs = os.path.join(ISTAT_DIR, "ATS MONTAGNA", "BRESCIA")
pop_brescia = {}

for stagione, anno in STAGIONI.items():
    filepath = _find_year_file(folder_bs, anno)
    if filepath is None and anno == 2026:
        filepath = _find_year_file(folder_bs, 2025)
        if filepath:
            print(f"  ℹ Season {stagione}: 2026 not found, using 2025.")
    if filepath is None:
        print(f"  ⚠ No file for year {anno} in {folder_bs}")
        pop_brescia[stagione] = 0
        continue
    pop = get_population_comuni(filepath, COMUNI_BRESCIA)
    pop_brescia[stagione] = pop
    print(f"  ✓ {stagione} ({anno}): Brescia subset pop = {pop:,}")

# --- 3c: ATS MONTAGNA — COMO subset ---
print("\n[3c] ATS MONTAGNA — Como subset")

folder_co = os.path.join(ISTAT_DIR, "ATS MONTAGNA", "COMO")
pop_como = {}

for stagione, anno in STAGIONI.items():
    filepath = _find_year_file(folder_co, anno)
    if filepath is None and anno == 2026:
        filepath = _find_year_file(folder_co, 2025)
        if filepath:
            print(f"  ℹ Season {stagione}: 2026 not found, using 2025.")
    if filepath is None:
        print(f"  ⚠ No file for year {anno} in {folder_co}")
        pop_como[stagione] = 0
        continue
    pop = get_population_comuni(filepath, COMUNI_COMO)
    pop_como[stagione] = pop
    print(f"  ✓ {stagione} ({anno}): Como subset pop = {pop:,}")

# --- 3d: ATS MONTAGNA — SONDRIO (whole province) ---
print("\n[3d] ATS MONTAGNA — Sondrio (whole province)")

folder_so = os.path.join(ISTAT_DIR, "ATS MONTAGNA", "SONDRIO")
pop_sondrio = {}

for stagione, anno in STAGIONI.items():
    filepath = _find_year_file(folder_so, anno)
    if filepath is None and anno == 2026:
        filepath = _find_year_file(folder_so, 2025)
        if filepath:
            print(f"  ℹ Season {stagione}: 2026 not found, using 2025.")
    if filepath is None:
        print(f"  ⚠ No file for year {anno} in {folder_so}")
        pop_sondrio[stagione] = 0
        continue
    # Sondrio: entire province = ATS Montagna → sum all comuni
    # get_population_total() auto-detects per-comune vs per-età format
    pop = get_population_total(filepath)
    pop_sondrio[stagione] = pop if pop else 0
    print(f"  ✓ {stagione} ({anno}): Sondrio population = {pop_sondrio[stagione]:,}")

# --- 3e: SUM → ATS Montagna total ---
print("\n[3e] ATS MONTAGNA — total (Brescia + Como + Sondrio subsets)")

pop_montagna = {}
for stagione in STAGIONI:
    bs  = pop_brescia.get(stagione, 0) or 0
    co  = pop_como.get(stagione, 0) or 0
    so  = pop_sondrio.get(stagione, 0) or 0
    tot = bs + co + so
    pop_montagna[stagione] = tot if tot > 0 else None
    if tot > 0:
        print(f"  ✓ {stagione}: {bs:,} (BS) + {co:,} (CO) + {so:,} (SO) = {tot:,}")
    else:
        print(f"  ⚠ {stagione}: no data available")

# Save per-season CSVs for ATS Montagna
print("\n  Saving per-season CSVs (ATS Montagna)...")
for stagione, pop in pop_montagna.items():
    anno_usato = STAGIONI[stagione]
    if anno_usato == 2026:
        # Check if 2026 was available in at least one folder
        has_2026 = any([
            _find_year_file(folder_bs, 2026),
            _find_year_file(folder_co, 2026),
            _find_year_file(folder_so, 2026),
        ])
        if not has_2026:
            anno_usato = 2025

    os.makedirs(OUT_STAGIONI_MT, exist_ok=True)
    fname = f"popolazione_montagna_{stagione}.csv"
    fpath = os.path.join(OUT_STAGIONI_MT, fname)
    df_out = pd.DataFrame([{
        "STAGIONE":          stagione,
        "ATS":               "ATS_MONTAGNA",
        "ANNO_ISTAT":        anno_usato,
        "POP_BRESCIA_SUBSET": pop_brescia.get(stagione, "N/A"),
        "POP_COMO_SUBSET":   pop_como.get(stagione, "N/A"),
        "POP_SONDRIO_TOT":   pop_sondrio.get(stagione, "N/A"),
        "POPOLAZIONE_TOT":   pop if pop is not None else "N/A",
    }])
    df_out.to_csv(fpath, index=False)
    print(f"  ✓ Saved: {fpath}")


# -------------------------------------------------------
# SECTION 4 — LOAD ILI DATA (from Script 1 output)
# -------------------------------------------------------

print("\n" + "=" * 65)
print("SECTION 4: LOADING ILI DATA (Script 1 output)")
print("=" * 65)

# --- DEBUG: stampa working directory e verifica i path ---
# Questo blocco aiuta a diagnosticare problemi di path.
# Commentalo una volta che lo script gira correttamente.
print(f"\n  [DEBUG] Working directory     : {os.getcwd()}")
print(f"  [DEBUG] ILI_DIR_BG (assoluto) : {os.path.abspath(ILI_DIR_BG)}")
print(f"  [DEBUG] ILI_DIR_MT (assoluto) : {os.path.abspath(ILI_DIR_MT)}")
print(f"  [DEBUG] Dir BG esiste         : {os.path.isdir(ILI_DIR_BG)}")
print(f"  [DEBUG] Dir MT esiste         : {os.path.isdir(ILI_DIR_MT)}")
for _lbl, _d in [("BG", ILI_DIR_BG), ("MT", ILI_DIR_MT)]:
    if os.path.isdir(_d):
        print(f"  [DEBUG] Contenuto {_lbl}           : {sorted(os.listdir(_d))}")
    else:
        print(f"  [DEBUG] '{_d}' non trovata. Cerco candidate...")
        _found = False
        for _root, _dirs, _files in os.walk("."):
            for _dn in _dirs:
                if "ATS_BERGAMO" in _dn or "ATS_MONTAGNA" in _dn:
                    print(f"           -> Trovata: {os.path.join(_root, _dn)}")
                    _found = True
        if not _found:
            print(f"           -> Nessuna cartella trovata sotto la cwd.")
print()


# ATS Bergamo — total ER and ILI visits
df_tot_bg  = load_ili_csv(
    os.path.join(ILI_DIR_BG, "access_tot_bergamo_stagionale.csv"),
    "ACCESSI_TOTALI_ER_BERGAMO"
)
df_ili_bg  = load_ili_csv(
    os.path.join(ILI_DIR_BG, "ili_ats_bergamo_stagionale.csv"),
    "ACCESSI_ILI_ATS_BERGAMO"
)

# ATS Montagna — total ER and ILI visits
df_tot_mt  = load_ili_csv(
    os.path.join(ILI_DIR_MT, "access_tot_montagna_stagionale.csv"),
    "ACCESSI_TOTALI_ER_MONTAGNA"
)
df_ili_mt  = load_ili_csv(
    os.path.join(ILI_DIR_MT, "ili_ats_montagna_stagionale.csv"),
    "ACCESSI_ILI_ATS_MONTAGNA"
)

print("  ✓ ILI dataframes loaded.")


# -------------------------------------------------------
# SECTION 5 — COMPUTE RATES AND PLOT
# -------------------------------------------------------

print("\n" + "=" * 65)
print("SECTION 5: COMPUTING RATES AND GENERATING PLOTS")
print("=" * 65)

"""
WHAT WE PLOT:
    For each influenza season, one figure with two subplots side by side
    (one per ATS), each showing:

        Rate 1 — Total ER / Population:
            = (total ER visits in week W) / population
            Interpreted as: weekly ER utilisation rate (all causes)

        Rate 2 — ILI ER / Population:
            = (ILI ER visits in week W) / population
            Interpreted as: weekly ILI attack rate seen through the ER

    WHY rates and not absolute counts?
        ATS Bergamo has ~1.1 million residents; ATS Montagna has far
        fewer (~200–250k). Plotting raw counts on the same axes would
        make Montagna's curve nearly invisible. Dividing by population
        puts both on a comparable scale (visits per person), enabling
        direct visual comparison.

    WHY not use %ILI (like Script 1's percentage plot)?
        %ILI = ILI / total ER tells us "of all ER users, what fraction
        had ILI?". The rate = ILI / population tells us "of all
        residents, what fraction accessed the ER for ILI?". The latter
        is the epidemiologically meaningful attack rate analogue.
        Both are useful; this script prioritises the population-based
        rate. Script 1's %ILI plot remains available for comparison.

    SCALE NOTE:
        Rates will be small numbers (e.g. 0.002 = 0.2% per week). The
        Y-axis shows the rate as a decimal. If you prefer per-1000
        inhabitants, multiply by 1000 and change the axis label.
"""

os.makedirs(OUT_GRAFICI, exist_ok=True)

for stagione in STAGIONI:

    # --- Retrieve population denominators ---
    pop_bg = pop_bergamo.get(stagione)
    pop_mt = pop_montagna.get(stagione)

    if pop_bg is None and pop_mt is None:
        print(f"  ⚠ Season {stagione}: no population data — plot skipped.")
        continue

    # --- Filter ILI dataframes to this season ---
    def season_subset(df, stagione):
        """Returns rows for a given season, sorted by ORDINE."""
        if df.empty or 'STAGIONE' not in df.columns:
            return pd.DataFrame()
        return df[df['STAGIONE'] == stagione].sort_values('ORDINE').reset_index(drop=True)

    tot_bg_s = season_subset(df_tot_bg, stagione)
    ili_bg_s = season_subset(df_ili_bg, stagione)
    tot_mt_s = season_subset(df_tot_mt, stagione)
    ili_mt_s = season_subset(df_ili_mt, stagione)

    # --- Compute rates ---
    def compute_rate(df_visits, col_visits, population):
        """
        Computes visit_rate = visits / population for each week.
        Returns a DataFrame with columns [ORDINE, WEEK, RATE].
        Returns empty DataFrame if inputs are invalid.
        """
        if df_visits.empty or population is None or population == 0:
            return pd.DataFrame()
        df = df_visits[['ORDINE', 'WEEK', col_visits]].copy()
        df = df.dropna(subset=[col_visits])
        df['RATE'] = df[col_visits].astype(float) / float(population)
        return df[['ORDINE', 'WEEK', 'RATE']]

    rate_tot_bg = compute_rate(tot_bg_s, 'ACCESSI_TOTALI_ER_BERGAMO', pop_bg)
    rate_ili_bg = compute_rate(ili_bg_s, 'ACCESSI_ILI_ATS_BERGAMO',   pop_bg)
    rate_tot_mt = compute_rate(tot_mt_s, 'ACCESSI_TOTALI_ER_MONTAGNA', pop_mt)
    rate_ili_mt = compute_rate(ili_mt_s, 'ACCESSI_ILI_ATS_MONTAGNA',  pop_mt)

    # --- Check if we have at least one ATS with data ---
    has_bg = not rate_tot_bg.empty or not rate_ili_bg.empty
    has_mt = not rate_tot_mt.empty or not rate_ili_mt.empty
    if not has_bg and not has_mt:
        print(f"  ⚠ Season {stagione}: no rate data computable — plot skipped.")
        continue

    # --- Build common x-axis tick map (week → ordine) ---
    all_ticks = pd.concat([
        rate_tot_bg[['ORDINE', 'WEEK']] if not rate_tot_bg.empty else pd.DataFrame(),
        rate_ili_bg[['ORDINE', 'WEEK']] if not rate_ili_bg.empty else pd.DataFrame(),
        rate_tot_mt[['ORDINE', 'WEEK']] if not rate_tot_mt.empty else pd.DataFrame(),
        rate_ili_mt[['ORDINE', 'WEEK']] if not rate_ili_mt.empty else pd.DataFrame(),
    ]).drop_duplicates().sort_values('ORDINE') if any([
        not rate_tot_bg.empty, not rate_ili_bg.empty,
        not rate_tot_mt.empty, not rate_ili_mt.empty
    ]) else pd.DataFrame()

    # --- Draw figure ---
    fig, axes = plt.subplots(1, 2, figsize=(16, 6), sharey=False)
    fig.suptitle(
        f"ER Visits / Population — Influenza Season {stagione}\n"
        f"(solid = total ER rate, dashed = ILI-specific rate)",
        fontsize=13, fontweight='bold'
    )

    # --- Helper: plot one ATS panel ---
    def plot_panel(ax, rate_tot, rate_ili, label_ats, pop_val, tick_map):
        """
        Draws the total ER rate and the ILI rate on a single axis.

        Parameters:
            ax:        matplotlib Axes object
            rate_tot:  DataFrame with total ER rates
            rate_ili:  DataFrame with ILI rates
            label_ats: ATS name string for title
            pop_val:   population denominator (int)
            tick_map:  DataFrame [ORDINE, WEEK] for x-axis labels
        """
        pop_str = f"{pop_val:,}" if pop_val else "N/A"

        if not rate_tot.empty:
            ax.plot(
                rate_tot['ORDINE'], rate_tot['RATE'],
                color='steelblue', linewidth=2, marker='o', markersize=5,
                label=f"Total ER / pop"
            )
        if not rate_ili.empty:
            ax.plot(
                rate_ili['ORDINE'], rate_ili['RATE'],
                color='firebrick', linewidth=2, linestyle='--',
                marker='s', markersize=5,
                label=f"ILI ER / pop"
            )

        # X-axis ticks
        if not tick_map.empty:
            ax.set_xticks(tick_map['ORDINE'])
            ax.set_xticklabels(tick_map['WEEK'], rotation=45, fontsize=9)

        ax.set_title(f"{label_ats}\n(pop: {pop_str})", fontsize=11)
        ax.set_xlabel("Epidemiological Week", fontsize=10)
        ax.set_ylabel("Visits per Resident", fontsize=10)
        ax.legend(fontsize=9)
        ax.grid(True, linestyle='--', alpha=0.6)

        # Annotate if no data available
        if rate_tot.empty and rate_ili.empty:
            ax.text(0.5, 0.5, "No data available",
                    transform=ax.transAxes, ha='center', va='center',
                    fontsize=12, color='gray')

    plot_panel(axes[0], rate_tot_bg, rate_ili_bg,
               "ATS Bergamo", pop_bg, all_ticks)
    plot_panel(axes[1], rate_tot_mt, rate_ili_mt,
               "ATS Montagna", pop_mt, all_ticks)

    plt.tight_layout()
    fname_out = f"ratio_ili_population_{stagione}.png"
    fpath_out = os.path.join(OUT_GRAFICI, fname_out)
    plt.savefig(fpath_out, dpi=150)
    plt.close()
    print(f"  ✓ Plot saved: {fpath_out}")


# -------------------------------------------------------
# SECTION 6 — FINAL SUMMARY
# -------------------------------------------------------

print("\n" + "=" * 65)
print("✅ SCRIPT 6 COMPLETE!")
print()
print("CSV files produced:")
for folder in [OUT_STAGIONI_BG, OUT_STAGIONI_MT]:
    if os.path.isdir(folder):
        files = sorted(os.listdir(folder))
        print(f"  {folder}/")
        for f in files:
            print(f"    └─ {f}")

print()
print("Plots saved to: output/grafici/")
if os.path.isdir(OUT_GRAFICI):
    for f in sorted(os.listdir(OUT_GRAFICI)):
        if f.startswith("ratio_ili_population"):
            print(f"  └─ {f}")