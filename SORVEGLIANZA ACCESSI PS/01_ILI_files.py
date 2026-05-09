"""
=============================================================
SCRIPT 1 — ILI DATA EXPLORATION AND CLEANING
=============================================================

WHAT THIS SCRIPT DOES:
    Loads the three Excel files with ILI data (Lombardia/ATS Milano,
    ATS Bergamo, ATS Montagna), corrects values misread by Python
    (thousands separator "." read as decimal separator), reorganises
    the data by INFLUENZA SEASON and saves CSV files into separate
    subfolders per geographic area.

    This is the MANDATORY FIRST STEP before any analysis.

INPUT FILES:
    - Data_ILI.xlsx              → data for Regione Lombardia + ATS Milano
    - Data_ILI_ATS_Bergamo.xlsx  → data for ATS Bergamo
    - Data_ILI_ATS_Montagna.xlsx → data for ATS Montagna (+ Bergamo duplicate)

INFLUENZA SEASON STRUCTURE:
    Seasons run from week 48 (November) to week 15
    (April of the following year). Convention used:

        Season 21-22: weeks 1-15 of 2022 only
                      (the last 4 weeks of 2021 are missing)
        Season 22-23: weeks 48-52 of 2022 + weeks 1-15 of 2023
        Season 23-24: weeks 48-52 of 2023 + weeks 1-15 of 2024
        Season 24-25: weeks 48-52 of 2024 + weeks 1-15 of 2025
        Season 25-26: weeks 48-52 of 2025 + weeks 1-15 of 2026

FILE PROBLEM (and how we fix it):
    The period "." in the Excel file is used as a THOUSANDS SEPARATOR
    (Italian convention: 1.692 = one thousand six hundred and ninety-two).
    Python read "1.692" as the decimal number 1.692 instead of
    the integer 1692.

    SINGLE RULE applied to all sheets:
        If the value read by Python has a non-zero decimal part
            → it was a number with a thousands separator → multiply by 1000
        If the decimal part is 0 (already an integer)
            → it is already correct → leave unchanged

    This works because:
        - Values with a period (e.g. 1.692, 10.459) are read as floats
          with non-zero decimal part → x 1000 → 1692, 10459  ✓
        - Already-integer values (e.g. 82, 766) are read as floats
          with decimal part = 0 → unchanged  ✓

    ⚠ EDGE CASE: a real value that is an exact multiple of 1000 written
      with a period (e.g. 2.000) would be read as 2.0 — zero decimal part —
      and would NOT be corrected (it would stay 2 instead of 2000).
      Check the plots for isolated anomalously low values, which might
      indicate this issue.


OUTPUT STRUCTURE (CSV subfolders):
    output/
    ├── LOMBARDIA/
    │   ├── access_tot_stagionale.csv
    │   ├── access_er_ili_stagionale.csv
    │   ├── admission_after_er_stagionale.csv
    │   └── ili_er_per_age_stagionale.csv
    ├── ATS_MILANO/
    │   ├── access_milano_stagionale.csv
    │   └── ili_ats_milano_stagionale.csv
    ├── ATS_BERGAMO/
    │   ├── access_tot_bergamo_stagionale.csv
    │   └── ili_ats_bergamo_stagionale.csv
    └── ATS_MONTAGNA/
        ├── access_tot_montagna_stagionale.csv
        └── ili_ats_montagna_stagionale.csv

PLOTS PRODUCED (output/grafici/):
    In addition to time-series plots for each variable, for
    ATS Bergamo and ATS Montagna an additional plot is produced
    showing the PERCENTAGE of ILI visits out of total ER visits,
    by influenza season:

        %ILI = (ILI_visits / total_ER_visits) * 100

    This indicator normalises ILI cases relative to the overall
    volume of ER activity, making seasons with different caseloads
    more comparable.

BIASES AND LIMITATIONS TO KEEP IN MIND:
    - Ecological bias: data are aggregated at ATS level, not individual level.
      Aggregated correlations cannot be interpreted as causal relationships
      at the patient level.
    - Short time window (4-5 seasons): increases the risk of spurious
      correlations in subsequent analyses. Interpret with caution.
    - Season 21-22 is incomplete (weeks 48-52 of 2021 are missing).

REQUIREMENTS:
    pip install pandas openpyxl matplotlib
=============================================================
"""

import pandas as pd
import os
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# -------------------------------------------------------
# CONFIGURATION — only modify these paths if necessary
# -------------------------------------------------------
FILE_LOMBARDIA  = "Data_ILI.xlsx"
FILE_BERGAMO    = "Data_ILI_ATS_Bergamo.xlsx"
FILE_MONTAGNA   = "Data_ILI_ATS_Montagna.xlsx"

# Output subfolders
CARTELLE = {
    "LOMBARDIA":    "output/LOMBARDIA",
    "ATS_MILANO":   "output/ATS_MILANO",
    "ATS_BERGAMO":  "output/ATS_BERGAMO",
    "ATS_MONTAGNA": "output/ATS_MONTAGNA",
}
for path in CARTELLE.values():
    os.makedirs(path, exist_ok=True)
os.makedirs("output/grafici", exist_ok=True)

# -------------------------------------------------------
# FUNCTIONS
# -------------------------------------------------------

def correggi_separatore_migliaia(val):
    """
    Corrects values where the thousands-separator period was read by
    Python as a decimal separator.

    Strategy: if the value read has a non-zero decimal part, it means
    the Excel file contained a thousands separator period (e.g. 1.692)
    which Python interpreted as a decimal separator → multiply by 1000.

    If the decimal part is 0 (e.g. 766.0), the value was already a
    correct integer → return unchanged.

    Examples:
        1.692  → decimal part = 0.692 ≠ 0 → x1000 → 1692  ✓
        10.459 → decimal part = 0.459 ≠ 0 → x1000 → 10459  ✓
        766.0  → decimal part = 0.0 = 0   → unchanged → 766  ✓
        82.0   → decimal part = 0.0 = 0   → unchanged → 82   ✓
    """
    if pd.isna(val):
        return val
    num = float(val)
    # math.modf returns (decimal_part, integer_part)
    # if the decimal part is non-zero, there was a thousands separator period
    import math
    parte_decimale, _ = math.modf(num)
    return round(num * 1000) if parte_decimale != 0 else int(num)


def applica_correzione(df, colonne_anni):
    """Applies the correction to all year columns — single function for all sheets."""
    for col in colonne_anni:
        df[col] = df[col].apply(correggi_separatore_migliaia)
        df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')
    return df


def assegna_stagione(week, anno):
    """
    Assigns the influenza season (e.g. '22-23') given week and year.

    Logic:
        - Weeks 48-52: belong to season YEAR — (YEAR+1)
        - Weeks  1-15: belong to season (YEAR-1) — YEAR

    Expected seasons:
        21-22: weeks 1-15 of 2022 only (2021 data missing)
        22-23: weeks 48-52/2022 + weeks 1-15/2023
        23-24: weeks 48-52/2023 + weeks 1-15/2024
        24-25: weeks 48-52/2024 + weeks 1-15/2025
        25-26: weeks 48-52/2025 + weeks 1-15/2026
    """
    if week >= 48:
        y1, y2 = anno, anno + 1
    else:  # weeks 1-15
        y1, y2 = anno - 1, anno
    return f"{str(y1)[-2:]}-{str(y2)[-2:]}"


def ordine_stagionale(week):
    """
    Sequential position within the influenza season.
    Week 48 → 1, week 52 → 5, week 1 → 6, week 15 → 20.
    """
    return week - 47 if week >= 48 else week + 5


def trasforma_in_stagionale(df, colonne_anni, nome_valore, col_gruppo=None):
    """
    Transforms a dataframe from WIDE format (columns = years) to LONG
    format with a SEASON column.

    Parameters:
        df:            dataframe with a WEEK column (and optionally AGE GROUP)
        colonne_anni:  list of year columns (e.g. [2022, 2023, ...])
        nome_valore:   name to assign to the value column
        col_gruppo:    name of an additional grouping column (e.g. 'AGE GROUP'),
                       if present

    Returns a dataframe with columns:
        [col_gruppo?,] STAGIONE, WEEK, ORDINE, <nome_valore>
    """
    righe = []
    id_cols = ['WEEK'] + ([col_gruppo] if col_gruppo else [])

    for _, row in df.iterrows():
        week = row['WEEK']
        gruppo = row[col_gruppo] if col_gruppo else None
        for anno in colonne_anni:
            val = row[anno]
            if pd.isna(val):
                continue
            stagione = assegna_stagione(week, anno)
            ordine   = ordine_stagionale(week)
            entry = {
                'STAGIONE': stagione,
                'WEEK':     week,
                'ORDINE':   ordine,
                nome_valore: val
            }
            if col_gruppo:
                entry[col_gruppo] = gruppo
            righe.append(entry)

    result = pd.DataFrame(righe)
    if result.empty:
        return result  # empty sheet: return empty DataFrame without errors
    if col_gruppo:
        result = result[[col_gruppo, 'STAGIONE', 'WEEK', 'ORDINE', nome_valore]]
        return result.sort_values([col_gruppo, 'STAGIONE', 'ORDINE']).reset_index(drop=True)
    else:
        result = result[['STAGIONE', 'WEEK', 'ORDINE', nome_valore]]
        return result.sort_values(['STAGIONE', 'ORDINE']).reset_index(drop=True)


def salva_csv(df, cartella_key, nome_file):
    """Saves the dataframe to the correct subfolder."""
    path = os.path.join(CARTELLE[cartella_key], nome_file)
    df.to_csv(path, index=False)
    print(f"  ✓ Saved: {path}  ({len(df)} rows)")
    return path


def grafico_stagionale(df, nome_valore, titolo, nome_img, col_gruppo=None):
    """
    Creates a plot by influenza season.
    If col_gruppo is specified, produces one plot per group.
    """
    if col_gruppo:
        for gruppo in df[col_gruppo].unique():
            subset_g = df[df[col_gruppo] == gruppo]
            _disegna_grafico(subset_g, nome_valore,
                             f"{titolo} — {gruppo}",
                             f"{nome_img}_{gruppo.replace(' ', '_')}.png")
    else:
        _disegna_grafico(df, nome_valore, titolo, f"{nome_img}.png")


def _disegna_grafico(df, nome_valore, titolo, nome_file):
    """Draws and saves a single seasonal plot."""
    fig, ax = plt.subplots(figsize=(12, 6))
    stagioni = sorted(df['STAGIONE'].unique())

    for stagione in stagioni:
        subset = df[df['STAGIONE'] == stagione].sort_values('ORDINE')
        valid  = subset.dropna(subset=[nome_valore])
        if not valid.empty:
            ax.plot(valid['ORDINE'], valid[nome_valore],
                    marker='o', label=stagione)

    # X-axis: labels = real weeks in seasonal order
    tick_map = df[['ORDINE', 'WEEK']].drop_duplicates().sort_values('ORDINE')
    ax.set_xticks(tick_map['ORDINE'])
    ax.set_xticklabels(tick_map['WEEK'], rotation=45)

    ax.set_title(titolo)
    ax.set_xlabel("Week")
    ax.set_ylabel(nome_valore)
    ax.legend(title="Season")
    ax.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join("output/grafici", nome_file))
    plt.close()
    print(f"  ✓ Plot: output/grafici/{nome_file}")


def grafico_percentuale_ili(df_totale, col_totale, df_ili, col_ili, titolo, nome_img):
    """
    Computes and plots the percentage of ILI visits out of total ER visits,
    by influenza season.

    WHY this plot is useful:
        The absolute number of ILI visits also depends on the overall
        volume of ER activity (which changes across seasons and years).
        The %ILI ratio normalises this variation and tells us: "of all
        patients who came to the ER that week, how many had an influenza-like
        illness?". It is a more robust indicator for comparing seasons.

    How the merge works:
        The two dataframes (total and ILI) are joined by STAGIONE, WEEK and
        ORDINE. Only rows present in both are used for the calculation.
        Rows without a match are discarded with a warning.

    Parameters:
        df_totale:  seasonalised dataframe of total ER visits
        col_totale: name of the column with total values
        df_ili:     seasonalised dataframe of ILI visits
        col_ili:    name of the column with ILI values
        titolo:     plot title
        nome_img:   image filename (without extension)

    ⚠ Limitation: the ratio may vary for reasons unrelated to influenza
      (e.g. campaigns that shift non-ILI ER visits, changes in triage
      thresholds). Interpret with caution.
    """
    # Inner merge: keep only weeks present in both datasets
    df_merge = pd.merge(
        df_totale[['STAGIONE', 'WEEK', 'ORDINE', col_totale]],
        df_ili[['STAGIONE', 'WEEK', 'ORDINE', col_ili]],
        on=['STAGIONE', 'WEEK', 'ORDINE'],
        how='inner'
    )

    # Report any weeks lost in the merge
    n_totale = len(df_totale)
    n_ili    = len(df_ili)
    n_merge  = len(df_merge)
    if n_merge < max(n_totale, n_ili):
        print(f"  ⚠ Percentage merge: {n_merge} rows used out of "
              f"{n_totale} (total) / {n_ili} (ILI). "
              f"Unmatched weeks discarded.")

    # Compute percentage — handles division by zero with NaN
    df_merge['PCT_ILI'] = (
        df_merge[col_ili].astype(float) /
        df_merge[col_totale].astype(float) * 100
    ).where(df_merge[col_totale] > 0)

    if df_merge['PCT_ILI'].isna().all():
        print(f"  ⚠ No computable values for {nome_img} — plot not produced.")
        return

    # Draw the plot
    fig, ax = plt.subplots(figsize=(12, 6))
    stagioni = sorted(df_merge['STAGIONE'].unique())

    for stagione in stagioni:
        subset = df_merge[df_merge['STAGIONE'] == stagione].sort_values('ORDINE')
        valid  = subset.dropna(subset=['PCT_ILI'])
        if not valid.empty:
            ax.plot(valid['ORDINE'], valid['PCT_ILI'],
                    marker='o', label=stagione)

    # X-axis: labels = real weeks in seasonal order
    tick_map = df_merge[['ORDINE', 'WEEK']].drop_duplicates().sort_values('ORDINE')
    ax.set_xticks(tick_map['ORDINE'])
    ax.set_xticklabels(tick_map['WEEK'], rotation=45)

    ax.set_title(titolo)
    ax.set_xlabel("Week")
    ax.set_ylabel("% ILI visits / total ER visits")
    ax.legend(title="Season")
    ax.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join("output/grafici", f"{nome_img}.png"))
    plt.close()
    print(f"  ✓ Plot: output/grafici/{nome_img}.png")


# -------------------------------------------------------
# STEP 1: LOMBARDIA FILE (Data_ILI.xlsx)
# -------------------------------------------------------
print("\n" + "=" * 65)
print("FILE: Data_ILI.xlsx  →  LOMBARDIA + ATS MILANO")
print("=" * 65)

tutti_fogli_lom = pd.read_excel(FILE_LOMBARDIA, sheet_name=None)
print(f"Sheets found: {list(tutti_fogli_lom.keys())}")

# --- TOTAL ACCESS IN ER (REGION) ---
print("\n[1/6] TOTAL ACCESS IN ER (REGION)")
df = tutti_fogli_lom["TOTAL ACCESS IN ER (REGION)"].copy()
anni = [c for c in df.columns if c != 'WEEK']
df = applica_correzione(df, anni)
df_s = trasforma_in_stagionale(df, anni, 'ACCESSI_TOTALI_ER')
salva_csv(df_s, "LOMBARDIA", "access_tot_stagionale.csv")
grafico_stagionale(df_s, 'ACCESSI_TOTALI_ER',
                   "Total ER Visits — Regione Lombardia",
                   "access_tot_lombardia")

# --- ACCESS IN ER (ILI) ---
print("\n[2/6] ACCESS IN ER (ILI)")
df = tutti_fogli_lom["ACCESS IN ER (ILI)"].copy()
anni = [c for c in df.columns if c != 'WEEK']
df = applica_correzione(df, anni)
df_s = trasforma_in_stagionale(df, anni, 'ACCESSI_ILI_ER')
salva_csv(df_s, "LOMBARDIA", "access_er_ili_stagionale.csv")
grafico_stagionale(df_s, 'ACCESSI_ILI_ER',
                   "ILI Emergency Room Visits — Regione Lombardia",
                   "access_er_ili_lombardia")

# --- ADMISSION AFTER ER (ILI) ---
print("\n[3/6] ADMISSION AFTER ER (ILI)")
df = tutti_fogli_lom["ADMISSION AFTER ER (ILI)"].copy()
anni = [c for c in df.columns if c != 'WEEK']
df = applica_correzione(df, anni)
df_s = trasforma_in_stagionale(df, anni, 'RICOVERI_DOPO_ER')
salva_csv(df_s, "LOMBARDIA", "admission_after_er_stagionale.csv")
grafico_stagionale(df_s, 'RICOVERI_DOPO_ER',
                   "Hospital Admissions after ER (ILI) — Regione Lombardia",
                   "admission_after_er_lombardia")

# --- ACCESS IN ER PER AGE (ILI) ---
print("\n[4/6] ACCESS IN ER PER AGE (ILI)")
df = tutti_fogli_lom["ACCESS IN ER PER AGE (ILI)"].copy()
anni = [c for c in df.columns if c not in ['AGE GROUP', 'WEEK']]
df = applica_correzione(df, anni)
df_s = trasforma_in_stagionale(df, anni, 'ACCESSI_ILI_ER', col_gruppo='AGE GROUP')
salva_csv(df_s, "LOMBARDIA", "ili_er_per_age_stagionale.csv")
grafico_stagionale(df_s, 'ACCESSI_ILI_ER',
                   "ILI ER Visits by Age Group — Lombardia",
                   "ili_er_per_age_lombardia",
                   col_gruppo='AGE GROUP')

# --- TOTAL ACCESS IN ER (ATS MILAN) ---
print("\n[5/6] TOTAL ACCESS IN ER (ATS MILAN)")
df = tutti_fogli_lom["TOTAL ACCESS IN ER (ATS MILAN)"].copy()
anni = [c for c in df.columns if c != 'WEEK']
df = applica_correzione(df, anni)
df_s = trasforma_in_stagionale(df, anni, 'ACCESSI_TOTALI_ER_MILANO')
salva_csv(df_s, "ATS_MILANO", "access_tot_milano_stagionale.csv")
grafico_stagionale(df_s, 'ACCESSI_TOTALI_ER_MILANO',
                   "Total ER Visits — ATS Milano",
                   "access_tot_milano")

# --- ACCESS IN ATS MILANO (ILI) ---
print("\n[6/6] ACCESS IN ATS MILANO (ILI)")
df = tutti_fogli_lom["ACCESS IN ATS MILANO (ILI)"].copy()
anni = [c for c in df.columns if c != 'WEEK']
df = applica_correzione(df, anni)
df_s = trasforma_in_stagionale(df, anni, 'ACCESSI_ILI_ATS_MILANO')
salva_csv(df_s, "ATS_MILANO", "ili_ats_milano_stagionale.csv")
grafico_stagionale(df_s, 'ACCESSI_ILI_ATS_MILANO',
                   "ILI Visits — ATS Milano",
                   "ili_ats_milano")

# -------------------------------------------------------
# STEP 2: ATS BERGAMO FILE (Data_ILI_ATS_Bergamo.xlsx)
# -------------------------------------------------------
print("\n" + "=" * 65)
print("FILE: Data_ILI_ATS_Bergamo.xlsx  →  ATS BERGAMO")
print("=" * 65)

tutti_fogli_bg = pd.read_excel(FILE_BERGAMO, sheet_name=None)
print(f"Sheets found: {list(tutti_fogli_bg.keys())}")

# --- TOTAL ACCESS IN ER (BERGAMO) ---
print("\n[1/3] TOTAL ACCESS IN ER — ATS Bergamo")
df = tutti_fogli_bg["TOTAL ACCESS IN ER (BERGAMO)"].copy()
anni = [c for c in df.columns if c != 'WEEK']
df = applica_correzione(df, anni)
df_tot_bergamo = trasforma_in_stagionale(df, anni, 'ACCESSI_TOTALI_ER_BERGAMO')
salva_csv(df_tot_bergamo, "ATS_BERGAMO", "access_tot_bergamo_stagionale.csv")
grafico_stagionale(df_tot_bergamo, 'ACCESSI_TOTALI_ER_BERGAMO',
                   "Total ER Visits — ATS Bergamo",
                   "access_tot_bergamo")

# --- ACCESS IN ATS BERGAMO (ILI) ---
print("\n[2/3] ACCESS ILI — ATS Bergamo")
df = tutti_fogli_bg["ACCESS IN ATS BERGAMO (ILI)"].copy()
anni = [c for c in df.columns if c != 'WEEK']
df = applica_correzione(df, anni)
df_ili_bergamo = trasforma_in_stagionale(df, anni, 'ACCESSI_ILI_ATS_BERGAMO')
if not df_ili_bergamo.empty and 'ACCESSI_ILI_ATS_BERGAMO' in df_ili_bergamo.columns:
    df_ili_bergamo = df_ili_bergamo.dropna(subset=['ACCESSI_ILI_ATS_BERGAMO'])
if df_ili_bergamo.empty:
    print("  ⚠ ILI Bergamo sheet is empty — no CSV produced.")
else:
    salva_csv(df_ili_bergamo, "ATS_BERGAMO", "ili_ats_bergamo_stagionale.csv")
    grafico_stagionale(df_ili_bergamo, 'ACCESSI_ILI_ATS_BERGAMO',
                       "ILI Visits — ATS Bergamo",
                       "ili_ats_bergamo")

# --- PERCENTAGE ILI / TOTAL — ATS Bergamo ---
# This plot answers the question: what proportion of total ER visits at
# Bergamo are ILI-related, week by week?
# Useful for comparing seasons even when absolute visit volumes differ.
print("\n[3/3] % ILI / Total ER Visits — ATS Bergamo")
if not df_tot_bergamo.empty and not df_ili_bergamo.empty:
    grafico_percentuale_ili(
        df_totale  = df_tot_bergamo,
        col_totale = 'ACCESSI_TOTALI_ER_BERGAMO',
        df_ili     = df_ili_bergamo,
        col_ili    = 'ACCESSI_ILI_ATS_BERGAMO',
        titolo     = "% ILI Visits out of Total ER Visits — ATS Bergamo",
        nome_img   = "pct_ili_bergamo"
    )
else:
    print("  ⚠ Total or ILI Bergamo data missing — percentage plot not produced.")


# -------------------------------------------------------
# STEP 3: ATS MONTAGNA FILE (Data_ILI_ATS_Montagna.xlsx)
# -------------------------------------------------------
print("\n" + "=" * 65)
print("FILE: Data_ILI_ATS_Montagna.xlsx  →  ATS MONTAGNA")
print("=" * 65)

tutti_fogli_mt = pd.read_excel(FILE_MONTAGNA, sheet_name=None)
print(f"Sheets found: {list(tutti_fogli_mt.keys())}")

# --- TOTAL ACCESS IN ER (MONTAGNA) ---
print("\n[1/3] TOTAL ACCESS IN ER — ATS Montagna")
df = tutti_fogli_mt["TOTAL ACCESS IN ER (MONTAGNA)"].copy()
anni = [c for c in df.columns if c != 'WEEK']
df = applica_correzione(df, anni)
df_tot_montagna = trasforma_in_stagionale(df, anni, 'ACCESSI_TOTALI_ER_MONTAGNA')
salva_csv(df_tot_montagna, "ATS_MONTAGNA", "access_tot_montagna_stagionale.csv")
grafico_stagionale(df_tot_montagna, 'ACCESSI_TOTALI_ER_MONTAGNA',
                   "Total ER Visits — ATS Montagna",
                   "access_tot_montagna")

# --- ACCESS IN ATS MONTAGNA (ILI) ---
print("\n[2/3] ACCESS ILI — ATS Montagna")
df = tutti_fogli_mt["ACCESS IN ATS MONTAGNA (ILI)"].copy()
anni = [c for c in df.columns if c != 'WEEK']
df = applica_correzione(df, anni)
df_ili_montagna = trasforma_in_stagionale(df, anni, 'ACCESSI_ILI_ATS_MONTAGNA')
if not df_ili_montagna.empty and 'ACCESSI_ILI_ATS_MONTAGNA' in df_ili_montagna.columns:
    df_ili_montagna = df_ili_montagna.dropna(subset=['ACCESSI_ILI_ATS_MONTAGNA'])
if df_ili_montagna.empty:
    print("  ⚠ ILI Montagna sheet is empty — no CSV produced.")
else:
    salva_csv(df_ili_montagna, "ATS_MONTAGNA", "ili_ats_montagna_stagionale.csv")
    grafico_stagionale(df_ili_montagna, 'ACCESSI_ILI_ATS_MONTAGNA',
                       "ILI Visits — ATS Montagna",
                       "ili_ats_montagna")

# --- PERCENTAGE ILI / TOTAL — ATS Montagna ---
# Same reasoning as Bergamo: normalising by total ER volume is particularly
# important for ATS Montagna, where the catchment population is smaller
# and absolute variations may be noisier.
print("\n[3/3] % ILI / Total ER Visits — ATS Montagna")
if not df_tot_montagna.empty and not df_ili_montagna.empty:
    grafico_percentuale_ili(
        df_totale  = df_tot_montagna,
        col_totale = 'ACCESSI_TOTALI_ER_MONTAGNA',
        df_ili     = df_ili_montagna,
        col_ili    = 'ACCESSI_ILI_ATS_MONTAGNA',
        titolo     = "% ILI Visits out of Total ER Visits — ATS Montagna",
        nome_img   = "pct_ili_montagna"
    )
else:
    print("  ⚠ Total or ILI Montagna data missing — percentage plot not produced.")

# -------------------------------------------------------
# FINAL SUMMARY
# -------------------------------------------------------
print("\n" + "=" * 65)
print("✅ SCRIPT 1 COMPLETE!")
print("\n CSV files produced per subfolder:")
for key, path in CARTELLE.items():
    files = os.listdir(path)
    print(f"  {path}/")
    for f in sorted(files):
        print(f"    └─ {f}")

print("\n Plots saved to: output/grafici/")
print("\n ⚠ NOTE ON ECOLOGICAL BIAS:")
print("\n  Data are aggregated at ATS level (not individual level). Observed correlations cannot be interpreted as causal relationships at the individual patient level.")
print("\n Next step: Script 2 for environmental data.")
print("=" * 65)