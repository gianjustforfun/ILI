"""
=============================================================
SCRIPT 1 — ESPLORAZIONE E PULIZIA DEI DATI ILI
=============================================================

COSA FA QUESTO SCRIPT:
    Carica i tre file Excel con i dati ILI (Lombardia/ATS Milano,
    ATS Bergamo, ATS Montagna), corregge i valori mal interpretati
    da Python (separatore migliaia "." letto come separatore decimale),
    riorganizza i dati per STAGIONE INFLUENZALE e salva i CSV in
    sottocartelle separate per area geografica.

    È il PRIMO passo obbligatorio prima di qualsiasi analisi.

FILE DI INPUT:
    - Data_ILI.xlsx          → dati Regione Lombardia + ATS Milano
    - Data_ILI_ATS_Bergamo.xlsx  → dati ATS Bergamo
    - Data_ILI_ATS_Montagna.xlsx → dati ATS Montagna (+ Bergamo duplicato)

STRUTTURA DELLE STAGIONI INFLUENZALI:
    Le stagioni coprono da settimana 48 (novembre) a settimana 15
    (aprile dell'anno successivo). Convenzione usata:

        Stagione 21-22: solo sett. 1-15 del 2022
                        (mancano le ultime 4 sett. del 2021)
        Stagione 22-23: sett. 48-52 del 2022 + sett. 1-15 del 2023
        Stagione 23-24: sett. 48-52 del 2023 + sett. 1-15 del 2024
        Stagione 24-25: sett. 48-52 del 2024 + sett. 1-15 del 2025
        Stagione 25-26: sett. 48-52 del 2025 + sett. 1-15 del 2026
                        (dati ancora in arrivo)

PROBLEMA NEL FILE (e come lo risolviamo):
    Il punto "." nel file Excel è usato come SEPARATORE DELLE MIGLIAIA
    (convenzione italiana: 1.692 = milleseicentonovantadue).
    Python ha letto "1.692" come il numero decimale 1,692 invece
    dell'intero 1692.

    REGOLA UNICA per tutti i fogli con CONTEGGI ASSOLUTI:
        Se il valore letto da Python è < 100 → moltiplica per 1000
        Se il valore è >= 100              → è già corretto

    Questo funziona perché:
        - Valori con il punto (es. 1.692, 2.459) vengono letti come
          1.692 e 2.459 → sono < 100 → x 1000 → 1692 e 2459  ✓
        - Valori sotto il migliaia senza punto (es. 766, 958) vengono
          letti già correttamente come 766 e 958 → >= 100 → invariati ✓

    ATTENZIONE — VALORI GIÀ CORRETTI (non richiedono moltiplicazione):
        Alcuni fogli (es. ADMISSION AFTER ER) hanno già valori interi
        nel range corretto (centinaia/migliaia), quindi la soglia di 100
        li lascia invariati correttamente.

STRUTTURA OUTPUT (sottocartelle CSV):
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

BIAS E LIMITAZIONI DA TENERE PRESENTI:
    - Bias ecologico: i dati sono aggregati a livello ATS, non individuale.
      Le correlazioni aggregate non implicano relazioni a livello di paziente.
    - Finestra temporale breve (4-5 stagioni): aumenta il rischio di
      spurious correlations nelle analisi successive. Interpretare con cautela.
    - La stagione 21-22 è incompleta (mancano sett. 48-52 del 2021).
    - I dati 25-26 sono parziali (stagione in corso).

REQUISITI:
    pip install pandas openpyxl matplotlib
=============================================================
"""

import pandas as pd
import os
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# -------------------------------------------------------
# CONFIGURAZIONE — modifica solo questi percorsi se necessario
# -------------------------------------------------------
FILE_LOMBARDIA  = "Data_ILI.xlsx"
FILE_BERGAMO    = "Data_ILI_ATS_Bergamo.xlsx"
FILE_MONTAGNA   = "Data_ILI_ATS_Montagna.xlsx"

# Sottocartelle di output
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
# FUNZIONI
# -------------------------------------------------------

def correggi_separatore_migliaia(val):
    """
    Corregge i valori in cui il punto separatore delle migliaia è stato
    letto da Python come separatore decimale.

    Regola: se il valore letto è < 100, era originalmente un numero con
    il punto delle migliaia → moltiplica per 1000.

    Esempi:
        1.692 (letto) → 1692 (reale)   perché 1.692 < 100
        2.459 (letto) → 2459 (reale)   perché 2.459 < 100
        766   (letto) → 766  (reale)   perché 766 >= 100, già corretto
        576   (letto) → 576  (reale)   perché 576 >= 100, già corretto
    """
    if pd.isna(val):
        return val
    if isinstance(val, (int, float)):
        num = float(val)
    else:
        pulito = str(val).replace('.', '').replace(' ', '')
        try:
            num = float(pulito)
        except ValueError:
            return float('nan')
    return num * 1000 if num < 100 else num


def applica_correzione(df, colonne_anni):
    """Applica correggi_separatore_migliaia su tutte le colonne anno."""
    for col in colonne_anni:
        df[col] = df[col].apply(correggi_separatore_migliaia)
        df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')
    return df


def assegna_stagione(week, anno):
    """
    Assegna la stagione influenzale (es. '22-23') dato week e anno.

    Logica:
        - Settimane 48-52: appartengono alla stagione ANNO — (ANNO+1)
        - Settimane  1-15: appartengono alla stagione (ANNO-1) — ANNO

    Stagioni attese:
        21-22: solo sett. 1-15 del 2022 (dati 2021 mancanti)
        22-23: sett. 48-52/2022 + sett. 1-15/2023
        23-24: sett. 48-52/2023 + sett. 1-15/2024
        24-25: sett. 48-52/2024 + sett. 1-15/2025
        25-26: sett. 48-52/2025 + sett. 1-15/2026
    """
    if week >= 48:
        y1, y2 = anno, anno + 1
    else:  # week 1-15
        y1, y2 = anno - 1, anno
    return f"{str(y1)[-2:]}-{str(y2)[-2:]}"


def ordine_stagionale(week):
    """
    Posizione progressiva nella stagione influenzale.
    Sett. 48 → 1, sett. 52 → 5, sett. 1 → 6, sett. 15 → 20.
    """
    return week - 47 if week >= 48 else week + 5


def trasforma_in_stagionale(df, colonne_anni, nome_valore, col_gruppo=None):
    """
    Trasforma un dataframe da formato LARGO (colonne = anni) a formato
    LUNGO con una colonna STAGIONE.

    Parametri:
        df:            dataframe con colonna WEEK (e opzionalmente AGE GROUP)
        colonne_anni:  lista delle colonne anno (es. [2022, 2023, ...])
        nome_valore:   nome da assegnare alla colonna dei valori
        col_gruppo:    nome colonna aggiuntiva (es. 'AGE GROUP'), se presente

    Ritorna un dataframe con colonne:
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
        return result  # foglio vuoto: restituisce DataFrame vuoto senza errori
    if col_gruppo:
        result = result[[col_gruppo, 'STAGIONE', 'WEEK', 'ORDINE', nome_valore]]
        return result.sort_values([col_gruppo, 'STAGIONE', 'ORDINE']).reset_index(drop=True)
    else:
        result = result[['STAGIONE', 'WEEK', 'ORDINE', nome_valore]]
        return result.sort_values(['STAGIONE', 'ORDINE']).reset_index(drop=True)


def salva_csv(df, cartella_key, nome_file):
    """Salva il dataframe nella sottocartella corretta."""
    path = os.path.join(CARTELLE[cartella_key], nome_file)
    df.to_csv(path, index=False)
    print(f"  ✓ Salvato: {path}  ({len(df)} righe)")
    return path


def grafico_stagionale(df, nome_valore, titolo, nome_img, col_gruppo=None):
    """
    Crea un grafico per stagione influenzale.
    Se col_gruppo è specificato, produce un grafico per ogni gruppo.
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
    """Disegna e salva un singolo grafico stagionale."""
    fig, ax = plt.subplots(figsize=(12, 6))
    stagioni = sorted(df['STAGIONE'].unique())

    for stagione in stagioni:
        subset = df[df['STAGIONE'] == stagione].sort_values('ORDINE')
        valid  = subset.dropna(subset=[nome_valore])
        if not valid.empty:
            ax.plot(valid['ORDINE'], valid[nome_valore],
                    marker='o', label=stagione)

    # Asse X: etichette = settimane reali in ordine stagionale
    tick_map = df[['ORDINE', 'WEEK']].drop_duplicates().sort_values('ORDINE')
    ax.set_xticks(tick_map['ORDINE'])
    ax.set_xticklabels(tick_map['WEEK'], rotation=45)

    ax.set_title(titolo)
    ax.set_xlabel("Settimana")
    ax.set_ylabel(nome_valore)
    ax.legend(title="Stagione")
    ax.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join("output/grafici", nome_file))
    plt.close()
    print(f"  ✓ Grafico: output/grafici/{nome_file}")


# -------------------------------------------------------
# PASSO 1: FILE LOMBARDIA (Data_ILI.xlsx)
# -------------------------------------------------------
print("\n" + "=" * 65)
print("FILE: Data_ILI.xlsx  →  LOMBARDIA + ATS MILANO")
print("=" * 65)

tutti_fogli_lom = pd.read_excel(FILE_LOMBARDIA, sheet_name=None)
print(f"Fogli trovati: {list(tutti_fogli_lom.keys())}")

# --- TOTAL ACCESS IN ER (REGION) ---
print("\n[1/6] TOTAL ACCESS IN ER (REGION)")
df = tutti_fogli_lom["TOTAL ACCESS IN ER (REGION)"].copy()
anni = [c for c in df.columns if c != 'WEEK']
df = applica_correzione(df, anni)
df_s = trasforma_in_stagionale(df, anni, 'ACCESSI_TOTALI_ER')
salva_csv(df_s, "LOMBARDIA", "access_tot_stagionale.csv")
grafico_stagionale(df_s, 'ACCESSI_TOTALI_ER',
                   "Accessi Totali ER — Regione Lombardia",
                   "access_tot_lombardia")

# --- ACCESS IN ER (ILI) ---
print("\n[2/6] ACCESS IN ER (ILI)")
df = tutti_fogli_lom["ACCESS IN ER (ILI)"].copy()
anni = [c for c in df.columns if c != 'WEEK']
df = applica_correzione(df, anni)
df_s = trasforma_in_stagionale(df, anni, 'ACCESSI_ILI_ER')
salva_csv(df_s, "LOMBARDIA", "access_er_ili_stagionale.csv")
grafico_stagionale(df_s, 'ACCESSI_ILI_ER',
                   "Accessi ILI al Pronto Soccorso — Regione Lombardia",
                   "access_er_ili_lombardia")

# --- ADMISSION AFTER ER (ILI) ---
print("\n[3/6] ADMISSION AFTER ER (ILI)")
df = tutti_fogli_lom["ADMISSION AFTER ER (ILI)"].copy()
anni = [c for c in df.columns if c != 'WEEK']
df = applica_correzione(df, anni)
df_s = trasforma_in_stagionale(df, anni, 'RICOVERI_DOPO_ER')
salva_csv(df_s, "LOMBARDIA", "admission_after_er_stagionale.csv")
grafico_stagionale(df_s, 'RICOVERI_DOPO_ER',
                   "Ricoveri dopo PS (ILI) — Regione Lombardia",
                   "admission_after_er_lombardia")

# --- ACCESS IN ER PER AGE (ILI) ---
print("\n[4/6] ACCESS IN ER PER AGE (ILI)")
df = tutti_fogli_lom["ACCESS IN ER PER AGE (ILI)"].copy()
anni = [c for c in df.columns if c not in ['AGE GROUP', 'WEEK']]
df = applica_correzione(df, anni)
df_s = trasforma_in_stagionale(df, anni, 'ACCESSI_ILI_ER', col_gruppo='AGE GROUP')
salva_csv(df_s, "LOMBARDIA", "ili_er_per_age_stagionale.csv")
grafico_stagionale(df_s, 'ACCESSI_ILI_ER',
                   "Accessi ILI per Fascia d'Età — Lombardia",
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
                   "Accessi Totali ER — ATS Milano",
                   "access_tot_milano")

# --- ACCESS IN ATS MILANO (ILI) ---
print("\n[6/6] ACCESS IN ATS MILANO (ILI)")
df = tutti_fogli_lom["ACCESS IN ATS MILANO (ILI)"].copy()
anni = [c for c in df.columns if c != 'WEEK']
df = applica_correzione(df, anni)
df_s = trasforma_in_stagionale(df, anni, 'ACCESSI_ILI_ATS_MILANO')
salva_csv(df_s, "ATS_MILANO", "ili_ats_milano_stagionale.csv")
grafico_stagionale(df_s, 'ACCESSI_ILI_ATS_MILANO',
                   "Accessi ILI — ATS Milano",
                   "ili_ats_milano")

# -------------------------------------------------------
# PASSO 2: FILE ATS BERGAMO (Data_ILI_ATS_Bergamo.xlsx)
# -------------------------------------------------------
print("\n" + "=" * 65)
print("FILE: Data_ILI_ATS_Bergamo.xlsx  →  ATS BERGAMO")
print("=" * 65)

tutti_fogli_bg = pd.read_excel(FILE_BERGAMO, sheet_name=None)
print(f"Fogli trovati: {list(tutti_fogli_bg.keys())}")

# --- TOTAL ACCESS IN ER (BERGAMO) ---
print("\n[1/2] TOTAL ACCESS IN ER — ATS Bergamo")
df = tutti_fogli_bg["TOTAL ACCESS IN ER (BERGAMO)"].copy()
anni = [c for c in df.columns if c != 'WEEK']
df = applica_correzione(df, anni)
df_s = trasforma_in_stagionale(df, anni, 'ACCESSI_TOTALI_ER_BERGAMO')
salva_csv(df_s, "ATS_BERGAMO", "access_tot_bergamo_stagionale.csv")
grafico_stagionale(df_s, 'ACCESSI_TOTALI_ER_BERGAMO',
                   "Accessi Totali ER — ATS Bergamo",
                   "access_tot_bergamo")

# --- ACCESS IN ATS BERGAMO (ILI) ---
print("\n[2/2] ACCESS ILI — ATS Bergamo")
df = tutti_fogli_bg["ACCESS IN ATS BERGAMO (ILI)"].copy()
anni = [c for c in df.columns if c != 'WEEK']
df = applica_correzione(df, anni)
df_s = trasforma_in_stagionale(df, anni, 'ACCESSI_ILI_ATS_BERGAMO')
if not df_s.empty and 'ACCESSI_ILI_ATS_BERGAMO' in df_s.columns:
    df_s = df_s.dropna(subset=['ACCESSI_ILI_ATS_BERGAMO'])
if df_s.empty:
    print("  ⚠ Foglio ILI Bergamo vuoto — nessun CSV prodotto.")
else:
    salva_csv(df_s, "ATS_BERGAMO", "ili_ats_bergamo_stagionale.csv")
    grafico_stagionale(df_s, 'ACCESSI_ILI_ATS_BERGAMO',
                       "Accessi ILI — ATS Bergamo",
                       "ili_ats_bergamo")


# -------------------------------------------------------
# PASSO 3: FILE ATS MONTAGNA (Data_ILI_ATS_Montagna.xlsx)
# -------------------------------------------------------
print("\n" + "=" * 65)
print("FILE: Data_ILI_ATS_Montagna.xlsx  →  ATS MONTAGNA")
print("=" * 65)

tutti_fogli_mt = pd.read_excel(FILE_MONTAGNA, sheet_name=None)
print(f"Fogli trovati: {list(tutti_fogli_mt.keys())}")

# --- TOTAL ACCESS IN ER (MONTAGNA) ---
print("\n[1/2] TOTAL ACCESS IN ER — ATS Montagna")
df = tutti_fogli_mt["TOTAL ACCESS IN ER (MONTAGNA)"].copy()
anni = [c for c in df.columns if c != 'WEEK']
df = applica_correzione(df, anni)
df_s = trasforma_in_stagionale(df, anni, 'ACCESSI_TOTALI_ER_MONTAGNA')
salva_csv(df_s, "ATS_MONTAGNA", "access_tot_montagna_stagionale.csv")
grafico_stagionale(df_s, 'ACCESSI_TOTALI_ER_MONTAGNA',
                   "Accessi Totali ER — ATS Montagna",
                   "access_tot_montagna")

# --- ACCESS IN ATS MONTAGNA (ILI) ---
print("\n[2/2] ACCESS ILI — ATS Montagna")
df = tutti_fogli_mt["ACCESS IN ATS MONTAGNA (ILI)"].copy()
anni = [c for c in df.columns if c != 'WEEK']
df = applica_correzione(df, anni)
df_s = trasforma_in_stagionale(df, anni, 'ACCESSI_ILI_ATS_MONTAGNA')
if not df_s.empty and 'ACCESSI_ILI_ATS_MONTAGNA' in df_s.columns:
    df_s = df_s.dropna(subset=['ACCESSI_ILI_ATS_MONTAGNA'])
if df_s.empty:
    print("  ⚠ Foglio ILI Montagna vuoto — nessun CSV prodotto.")
else:
    salva_csv(df_s, "ATS_MONTAGNA", "ili_ats_montagna_stagionale.csv")
    grafico_stagionale(df_s, 'ACCESSI_ILI_ATS_MONTAGNA',
                       "Accessi ILI — ATS Montagna",
                       "ili_ats_montagna")

# -------------------------------------------------------
# RIEPILOGO FINALE
# -------------------------------------------------------
print("\n" + "=" * 65)
print("✅ SCRIPT 1 COMPLETATO!")
print()
print("CSV prodotti per sottocartella:")
for key, path in CARTELLE.items():
    files = os.listdir(path)
    print(f"  {path}/")
    for f in sorted(files):
        print(f"    └─ {f}")
print()
print("Grafici salvati in: output/grafici/")
print()
print("⚠ NOTA SUL BIAS ECOLOGICO:")
print("  I dati sono aggregati a livello ATS (non individuale).")
print("  Le correlazioni osservate non possono essere interpretate")
print("  come relazioni causali a livello di singolo paziente.")
print()
print("Prossimo passo: Script 2 per i dati ambientali.")
print("=" * 65)