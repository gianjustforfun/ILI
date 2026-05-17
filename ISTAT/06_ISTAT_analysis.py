"""
=============================================================
SCRIPT 6 — ISTAT POPULATION DATA ANALYSIS
=============================================================

WHAT THIS SCRIPT DOES:
    1. Loads ISTAT population CSV files from the ISTAT/ subfolder of the
       project directory (one file per ATS/province per year).
    2. Extracts the population total from each file (last row, column
       'Totale').
    3. Builds three population datasets:
         a) ATS Bergamo  — directly from ATS BERGAMO/ files.
         b) ATS Montagna — sum of:
              • Selected comuni of Provincia di Brescia
              • Selected comuni of Provincia di Como
              • Entire Provincia di Sondrio
         c) ATS Brianza  — sum of:              ← NUOVO
              • Entire Provincia di Lecco
              • Entire Provincia di Monza e Brianza
    4. Saves one CSV per influenza season per ATS into:
         output/stagioni/ATS_BERGAMO/
         output/stagioni/ATS_MONTAGNA/
         output/stagioni/ATS_BRIANZA/            ← NUOVO
    5. Produces one plot per influenza season (saved to output/grafici/)
       showing — for ATS Bergamo, ATS Montagna, and ATS Brianza — two
       overlaid curves:
         • Total ER visits / population  (all-cause rate)
         • ILI ER visits  / population   (ILI-specific rate)

INPUT DIRECTORY STRUCTURE EXPECTED:
    ISTAT/
    ├── ATS BERGAMO/
    │   ├── Popolazione residente_ATS_Bergamo_2022.csv
    │   └── ...
    ├── ATS MONTAGNA/
    │   ├── BRESCIA/
    │   ├── COMO/
    │   └── SONDRIO/
    └── ATS BRIANZA/                             ← NUOVO
        ├── LECCO/
        │   ├── Popolazione residente_Prov_Lecco_2022.csv
        │   └── ...
        └── MONZA/
            ├── Popolazione residente_Prov_Monza_2022.csv
            └── ...

    ⚠ NOTE — ATS Brianza coverage:
      • Lecco   : intera provincia → nessun filtro necessario
      • Monza   : intera provincia (Monza e Brianza) → nessun filtro necessario
      La logica è identica a quella usata per la Provincia di Sondrio in
      ATS Montagna: get_population_total() somma tutti i comuni del file.

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
        ../SORVEGLIANZA ACCESSI PS/output/ATS_BRIANZA/access_tot_brianza_stagionale.csv   ← NUOVO
        ../SORVEGLIANZA ACCESSI PS/output/ATS_BRIANZA/ili_ats_brianza_stagionale.csv      ← NUOVO
    ➜ Run Script 1 before this script.

OUTPUT STRUCTURE:
    output/
    ├── stagioni/
    │   ├── ATS_BERGAMO/
    │   ├── ATS_MONTAGNA/
    │   └── ATS_BRIANZA/                         ← NUOVO
    │       ├── popolazione_brianza_21-22.csv
    │       └── ...
    └── grafici/
        ├── ratio_ili_population_21-22.png
        └── ...

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
OUT_STAGIONI_BR = "output/istat_ats_brianza"          # ← NUOVO
OUT_MONTAGNA_ALL = "output/istat_ats_montagna/whole"
OUT_GRAFICI     = "output/grafici"

# Script 1 CSV outputs (read as ILI input)
ILI_DIR_BG = "../SORVEGLIANZA ACCESSI PS/output/ATS_BERGAMO"
ILI_DIR_MT = "../SORVEGLIANZA ACCESSI PS/output/ATS_MONTAGNA"
ILI_DIR_BR = "../SORVEGLIANZA ACCESSI PS/output/ATS_BRIANZA"  # ← NUOVO

# Years available in the ISTAT files
ANNI_DISPONIBILI = [2022, 2023, 2024, 2025, 2026]

# Season → ISTAT year mapping (we use the later calendar year of each season)
STAGIONI = {
    "21-22": 2022,
    "22-23": 2023,
    "23-24": 2024,
    "24-25": 2025,
    "25-26": 2026,   # fallback to 2025 if 2026 not available
}

# -------------------------------------------------------
# ATS MONTAGNA — comuni that belong to this ATS
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
# Lecco:   entire province → no filter needed  (ATS Brianza)
# Monza:   entire province → no filter needed  (ATS Brianza)


# -------------------------------------------------------
# SECTION 2 — HELPER FUNCTIONS
# -------------------------------------------------------

def get_population_total(filepath):
    """
    Reads an ISTAT per-comune CSV (one row per comune, columns include
    'Comune' and 'Totale') and returns the sum of 'Totale' across all
    comuni — i.e. the total population of the entire territory covered
    by the file.

    Used for:
        ATS Bergamo  — all 243 comuni of the province
        Sondrio      — all comuni of the province (= entire ATS Montagna)
        Lecco        — all comuni of the province (= part of ATS Brianza)
        Monza        — all comuni of the province (= part of ATS Brianza)

    Parameters:
        filepath (str): path to the ISTAT CSV file

    Returns:
        int: total population, or None if the file cannot be read
    """
    if not os.path.exists(filepath):
        return None
    try:
        df = pd.read_csv(filepath)
        if 'Totale' not in df.columns:
            print(f"  ⚠ Column 'Totale' not found in {filepath}")
            return None
        return int(df['Totale'].sum())
    except Exception as e:
        print(f"  ⚠ Error reading {filepath}: {e}")
        return None


def get_population_comuni(filepath, comuni_list):
    """
    Reads an ISTAT CSV where EACH ROW IS ONE COMUNE and filters to the
    comuni in comuni_list, returning the sum of their 'Totale' column.

    Used for:
        ATS Montagna — Brescia subset and Como subset

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


def salva_csv_comuni_montagna(filepath_bs, filepath_co, filepath_so,
                              anno, comuni_bs, comuni_co, output_dir):
    """
    Builds a single per-comune CSV for ATS Montagna for a given year.
    Unchanged from the original script.
    """
    os.makedirs(output_dir, exist_ok=True)
    parts = []

    def _load_filter(filepath, comuni_list, provincia_label):
        if filepath is None or not os.path.exists(filepath):
            print(f"  ⚠ [{provincia_label}] file not found — skipped.")
            return pd.DataFrame()
        df = pd.read_csv(filepath)
        if 'Comune' not in df.columns or 'Totale' not in df.columns:
            print(f"  ⚠ [{provincia_label}] unexpected columns — skipped.")
            return pd.DataFrame()
        if comuni_list is not None:
            norm = [c.strip() for c in comuni_list]
            df['_norm'] = df['Comune'].astype(str).str.strip()
            df = df[df['_norm'].isin(norm)].drop(columns='_norm')
        df = df[['Comune', 'Totale']].copy()
        df.insert(0, 'PROVINCIA', provincia_label)
        return df

    parts.append(_load_filter(filepath_bs, comuni_bs, 'Brescia'))
    parts.append(_load_filter(filepath_co, comuni_co, 'Como'))
    parts.append(_load_filter(filepath_so, None,      'Sondrio'))

    result = pd.concat([p for p in parts if not p.empty], ignore_index=True)

    if result.empty:
        print(f"  ⚠ No data assembled for ATS Montagna {anno} — CSV not saved.")
        return

    fname = f"ats_montagna_comuni_{anno}.csv"
    fpath = os.path.join(output_dir, fname)
    result.to_csv(fpath, index=False)
    tot   = result['Totale'].sum()
    print(f"  ✓ Saved: {fpath}  ({len(result)} comuni, pop = {tot:,})")


def _find_year_file(folder, anno):
    """
    Finds the CSV file for a given year inside a folder by looking for
    files whose name contains 'YYYY'.

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
    Loads a seasonalised ILI CSV produced by Script 1.
    Returns empty DataFrame if file not found.
    """
    if not os.path.exists(filepath):
        print(f"  ⚠ ILI file not found: {filepath} — run Script 1 first.")
        return pd.DataFrame()
    df = pd.read_csv(filepath)
    if value_col not in df.columns:
        print(f"  ⚠ Column '{value_col}' not found in {filepath}.")
        return pd.DataFrame()
    return df


def salva_csv_stagione(pop_value, anno_istat, stagione, ats_label, output_dir,
                       extra_cols=None):
    """
    Saves a one-row CSV summarising the population figure used for a
    given influenza season.

    Parameters:
        pop_value (int):    population count
        anno_istat (int):   ISTAT year used
        stagione (str):     e.g. '22-23'
        ats_label (str):    e.g. 'ATS_BERGAMO'
        output_dir (str):   where to save
        extra_cols (dict):  additional columns to include in the CSV
                            (used by ATS Montagna and ATS Brianza to
                            record sub-territory breakdown)
    """
    os.makedirs(output_dir, exist_ok=True)

    # Derive filename prefix from ATS label
    if "BERGAMO" in ats_label:
        prefix = "bergamo"
    elif "MONTAGNA" in ats_label:
        prefix = "montagna"
    elif "BRIANZA" in ats_label:
        prefix = "brianza"
    else:
        prefix = ats_label.lower().replace(" ", "_")

    fname  = f"popolazione_{prefix}_{stagione}.csv"
    fpath  = os.path.join(output_dir, fname)

    row = {
        "STAGIONE":         stagione,
        "ATS":              ats_label,
        "ANNO_ISTAT":       anno_istat,
        "POPOLAZIONE_TOT":  pop_value if pop_value is not None else "N/A",
    }
    if extra_cols:
        row.update(extra_cols)

    df_out = pd.DataFrame([row])
    df_out.to_csv(fpath, index=False)
    print(f"  ✓ Saved: {fpath}")


# -------------------------------------------------------
# SECTION 3 — LOAD ISTAT DATA
# -------------------------------------------------------

print("\n" + "=" * 65)
print("SECTION 3: LOADING ISTAT POPULATION DATA")
print("=" * 65)

# --- 3a: ATS BERGAMO ---
print("\n[3a] ATS BERGAMO — whole-ATS files")

folder_bg = os.path.join(ISTAT_DIR, "ATS BERGAMO")
pop_bergamo = {}

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
    print(f"  ✓ {stagione} ({anno}): population = {pop:,}  [{os.path.basename(filepath)}]")

print("\n  Saving per-season CSVs (ATS Bergamo)...")
for stagione, pop in pop_bergamo.items():
    anno_usato = STAGIONI[stagione]
    if anno_usato == 2026 and pop is not None:
        if _find_year_file(folder_bg, 2026) is None:
            anno_usato = 2025
    salva_csv_stagione(pop, anno_usato, stagione, "ATS_BERGAMO", OUT_STAGIONI_BG)

# --- 3b: ATS MONTAGNA — BRESCIA subset ---
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

print("\n  Saving per-season CSVs (ATS Montagna)...")
for stagione, pop in pop_montagna.items():
    anno_usato = STAGIONI[stagione]
    if anno_usato == 2026:
        has_2026 = any([
            _find_year_file(folder_bs, 2026),
            _find_year_file(folder_co, 2026),
            _find_year_file(folder_so, 2026),
        ])
        if not has_2026:
            anno_usato = 2025

    salva_csv_stagione(
        pop, anno_usato, stagione, "ATS_MONTAGNA", OUT_STAGIONI_MT,
        extra_cols={
            "POP_BRESCIA_SUBSET": pop_brescia.get(stagione, "N/A"),
            "POP_COMO_SUBSET":   pop_como.get(stagione, "N/A"),
            "POP_SONDRIO_TOT":   pop_sondrio.get(stagione, "N/A"),
        }
    )

# --- 3f: SAVE WHOLE ATS MONTAGNA PER-COMUNE CSV ---
print("\n[3f] ATS MONTAGNA — per-comune CSV (whole territory, one per year)")
print("     Output folder: " + OUT_MONTAGNA_ALL)

for anno in ANNI_DISPONIBILI:
    fp_bs = _find_year_file(folder_bs, anno)
    fp_co = _find_year_file(folder_co, anno)
    fp_so = _find_year_file(folder_so, anno)

    if fp_bs is None and fp_co is None and fp_so is None:
        print(f"  ⚠ No source files found for year {anno} — skipped.")
        continue

    salva_csv_comuni_montagna(
        filepath_bs = fp_bs,
        filepath_co = fp_co,
        filepath_so = fp_so,
        anno        = anno,
        comuni_bs   = COMUNI_BRESCIA,
        comuni_co   = COMUNI_COMO,
        output_dir  = OUT_MONTAGNA_ALL,
    )

# ==============================================================
# --- 3g: ATS BRIANZA — LECCO (whole province)              ---
# ==============================================================
# PERCHÉ nessun filtro per comuni:
#   La Provincia di Lecco coincide interamente con il territorio
#   dell'ATS Brianza, quindi si somma l'intera colonna 'Totale'
#   del file provinciale. Stessa logica di Sondrio in ATS Montagna.
# ==============================================================
print("\n[3g] ATS BRIANZA — Lecco (whole province)")

folder_lc = os.path.join(ISTAT_DIR, "ATS BRIANZA", "LECCO")
pop_lecco = {}

for stagione, anno in STAGIONI.items():
    filepath = _find_year_file(folder_lc, anno)
    if filepath is None and anno == 2026:
        filepath = _find_year_file(folder_lc, 2025)
        if filepath:
            print(f"  ℹ Season {stagione}: 2026 not found, using 2025.")
    if filepath is None:
        print(f"  ⚠ No file for year {anno} in {folder_lc}")
        pop_lecco[stagione] = 0
        continue
    pop = get_population_total(filepath)
    pop_lecco[stagione] = pop if pop else 0
    print(f"  ✓ {stagione} ({anno}): Lecco population = {pop_lecco[stagione]:,}")

# ==============================================================
# --- 3h: ATS BRIANZA — MONZA (whole province)              ---
# ==============================================================
print("\n[3h] ATS BRIANZA — Monza e Brianza (whole province)")

folder_mb = os.path.join(ISTAT_DIR, "ATS BRIANZA", "MONZA")
pop_monza = {}

for stagione, anno in STAGIONI.items():
    filepath = _find_year_file(folder_mb, anno)
    if filepath is None and anno == 2026:
        filepath = _find_year_file(folder_mb, 2025)
        if filepath:
            print(f"  ℹ Season {stagione}: 2026 not found, using 2025.")
    if filepath is None:
        print(f"  ⚠ No file for year {anno} in {folder_mb}")
        pop_monza[stagione] = 0
        continue
    pop = get_population_total(filepath)
    pop_monza[stagione] = pop if pop else 0
    print(f"  ✓ {stagione} ({anno}): Monza e Brianza population = {pop_monza[stagione]:,}")

# ==============================================================
# --- 3i: SUM → ATS Brianza total (Lecco + Monza)          ---
# ==============================================================
print("\n[3i] ATS BRIANZA — total (Lecco + Monza e Brianza)")

pop_brianza = {}
for stagione in STAGIONI:
    lc  = pop_lecco.get(stagione, 0) or 0
    mb  = pop_monza.get(stagione, 0) or 0
    tot = lc + mb
    pop_brianza[stagione] = tot if tot > 0 else None
    if tot > 0:
        print(f"  ✓ {stagione}: {lc:,} (LC) + {mb:,} (MB) = {tot:,}")
    else:
        print(f"  ⚠ {stagione}: no data available")

print("\n  Saving per-season CSVs (ATS Brianza)...")
for stagione, pop in pop_brianza.items():
    anno_usato = STAGIONI[stagione]
    if anno_usato == 2026:
        has_2026 = any([
            _find_year_file(folder_lc, 2026),
            _find_year_file(folder_mb, 2026),
        ])
        if not has_2026:
            anno_usato = 2025

    salva_csv_stagione(
        pop, anno_usato, stagione, "ATS_BRIANZA", OUT_STAGIONI_BR,
        extra_cols={
            "POP_LECCO_TOT": pop_lecco.get(stagione, "N/A"),
            "POP_MONZA_TOT": pop_monza.get(stagione, "N/A"),
        }
    )


# -------------------------------------------------------
# SECTION 4 — LOAD ILI DATA (from Script 1 output)
# -------------------------------------------------------

print("\n" + "=" * 65)
print("SECTION 4: LOADING ILI DATA (Script 1 output)")
print("=" * 65)

print(f"\n  [DEBUG] Working directory     : {os.getcwd()}")
print(f"  [DEBUG] ILI_DIR_BG (assoluto) : {os.path.abspath(ILI_DIR_BG)}")
print(f"  [DEBUG] ILI_DIR_MT (assoluto) : {os.path.abspath(ILI_DIR_MT)}")
print(f"  [DEBUG] ILI_DIR_BR (assoluto) : {os.path.abspath(ILI_DIR_BR)}")
for _lbl, _d in [("BG", ILI_DIR_BG), ("MT", ILI_DIR_MT), ("BR", ILI_DIR_BR)]:
    if os.path.isdir(_d):
        print(f"  [DEBUG] Contenuto {_lbl}           : {sorted(os.listdir(_d))}")
    else:
        print(f"  [DEBUG] '{_d}' non trovata.")
print()

# ATS Bergamo
df_tot_bg = load_ili_csv(
    os.path.join(ILI_DIR_BG, "access_tot_bergamo_stagionale.csv"),
    "ACCESSI_TOTALI_ER_BERGAMO"
)
df_ili_bg = load_ili_csv(
    os.path.join(ILI_DIR_BG, "ili_ats_bergamo_stagionale.csv"),
    "ACCESSI_ILI_ATS_BERGAMO"
)

# ATS Montagna
df_tot_mt = load_ili_csv(
    os.path.join(ILI_DIR_MT, "access_tot_montagna_stagionale.csv"),
    "ACCESSI_TOTALI_ER_MONTAGNA"
)
df_ili_mt = load_ili_csv(
    os.path.join(ILI_DIR_MT, "ili_ats_montagna_stagionale.csv"),
    "ACCESSI_ILI_ATS_MONTAGNA"
)

# ATS Brianza  ← NUOVO
# Adatta i nomi dei file al pattern usato da Script 1 per questa ATS.
# Se Script 1 usa nomi diversi, aggiorna le stringhe qui sotto.
df_tot_br = load_ili_csv(
    os.path.join(ILI_DIR_BR, "access_tot_brianza_stagionale.csv"),
    "ACCESSI_TOTALI_ER_BRIANZA"
)
df_ili_br = load_ili_csv(
    os.path.join(ILI_DIR_BR, "ili_ats_brianza_stagionale.csv"),
    "ACCESSI_ILI_ATS_BRIANZA"
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
    For each influenza season, one figure with THREE subplots side by
    side (one per ATS: Bergamo, Montagna, Brianza), each showing:

        Rate 1 — Total ER / Population  (solid line)
        Rate 2 — ILI ER  / Population   (dashed line)

    WHY rates and not absolute counts?
        The three ATS have very different population sizes:
            ATS Bergamo  ≈ 1.1 M
            ATS Brianza  ≈ 870 K (LC ~340K + MB ~870K → verify)
            ATS Montagna ≈ 200–250 K
        Plotting raw counts on the same axes would make Montagna's curve
        nearly invisible. Dividing by population puts all three on a
        comparable scale.
"""

os.makedirs(OUT_GRAFICI, exist_ok=True)

for stagione in STAGIONI:

    pop_bg = pop_bergamo.get(stagione)
    pop_mt = pop_montagna.get(stagione)
    pop_br = pop_brianza.get(stagione)

    if pop_bg is None and pop_mt is None and pop_br is None:
        print(f"  ⚠ Season {stagione}: no population data — plot skipped.")
        continue

    def season_subset(df, stagione):
        """Returns rows for a given season, sorted by ORDINE."""
        if df.empty or 'STAGIONE' not in df.columns:
            return pd.DataFrame()
        return df[df['STAGIONE'] == stagione].sort_values('ORDINE').reset_index(drop=True)

    tot_bg_s = season_subset(df_tot_bg, stagione)
    ili_bg_s = season_subset(df_ili_bg, stagione)
    tot_mt_s = season_subset(df_tot_mt, stagione)
    ili_mt_s = season_subset(df_ili_mt, stagione)
    tot_br_s = season_subset(df_tot_br, stagione)   # ← NUOVO
    ili_br_s = season_subset(df_ili_br, stagione)   # ← NUOVO

    def compute_rate(df_visits, col_visits, population):
        """
        Computes visit_rate = visits / population for each week.
        Returns DataFrame with columns [ORDINE, WEEK, RATE].
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
    rate_tot_br = compute_rate(tot_br_s, 'ACCESSI_TOTALI_ER_BRIANZA',  pop_br)  # ← NUOVO
    rate_ili_br = compute_rate(ili_br_s, 'ACCESSI_ILI_ATS_BRIANZA',   pop_br)  # ← NUOVO

    has_bg = not rate_tot_bg.empty or not rate_ili_bg.empty
    has_mt = not rate_tot_mt.empty or not rate_ili_mt.empty
    has_br = not rate_tot_br.empty or not rate_ili_br.empty

    if not has_bg and not has_mt and not has_br:
        print(f"  ⚠ Season {stagione}: no rate data computable — plot skipped.")
        continue

    # Build common x-axis tick map across all three ATS
    all_ticks = pd.concat([
        df[['ORDINE', 'WEEK']] for df in [
            rate_tot_bg, rate_ili_bg,
            rate_tot_mt, rate_ili_mt,
            rate_tot_br, rate_ili_br,
        ] if not df.empty
    ]).drop_duplicates().sort_values('ORDINE') if any(
        [has_bg, has_mt, has_br]
    ) else pd.DataFrame()

    # ── Figure: 1 row × 3 columns (Bergamo | Montagna | Brianza) ──
    fig, axes = plt.subplots(1, 3, figsize=(22, 6), sharey=False)
    fig.suptitle(
        f"ER Visits / Population — Influenza Season {stagione}\n"
        f"(solid = total ER rate, dashed = ILI-specific rate)",
        fontsize=13, fontweight='bold'
    )

    def plot_panel(ax, rate_tot, rate_ili, label_ats, pop_val, tick_map):
        """
        Draws the total ER rate and the ILI rate on a single axis.
        """
        pop_str = f"{pop_val:,}" if pop_val else "N/A"

        if not rate_tot.empty:
            ax.plot(
                rate_tot['ORDINE'], rate_tot['RATE'],
                color='steelblue', linewidth=2, marker='o', markersize=5,
                label="Total ER / pop"
            )
        if not rate_ili.empty:
            ax.plot(
                rate_ili['ORDINE'], rate_ili['RATE'],
                color='firebrick', linewidth=2, linestyle='--',
                marker='s', markersize=5,
                label="ILI ER / pop"
            )

        if not tick_map.empty:
            ax.set_xticks(tick_map['ORDINE'])
            ax.set_xticklabels(tick_map['WEEK'], rotation=45, fontsize=9)

        ax.set_title(f"{label_ats}\n(pop: {pop_str})", fontsize=11)
        ax.set_xlabel("Epidemiological Week", fontsize=10)
        ax.set_ylabel("Visits per Resident", fontsize=10)
        ax.legend(fontsize=9)
        ax.grid(True, linestyle='--', alpha=0.6)

        if rate_tot.empty and rate_ili.empty:
            ax.text(0.5, 0.5, "No data available",
                    transform=ax.transAxes, ha='center', va='center',
                    fontsize=12, color='gray')

    plot_panel(axes[0], rate_tot_bg, rate_ili_bg, "ATS Bergamo",  pop_bg, all_ticks)
    plot_panel(axes[1], rate_tot_mt, rate_ili_mt, "ATS Montagna", pop_mt, all_ticks)
    plot_panel(axes[2], rate_tot_br, rate_ili_br, "ATS Brianza",  pop_br, all_ticks)  # ← NUOVO

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
for folder in [OUT_STAGIONI_BG, OUT_STAGIONI_MT, OUT_STAGIONI_BR, OUT_MONTAGNA_ALL]:
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