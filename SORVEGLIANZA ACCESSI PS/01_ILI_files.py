"""
=============================================================
SCRIPT 1 — ILI DATA EXPLORATION AND CLEANING
=============================================================

COSA FA QUESTO SCRIPT:
    Carica i quattro file Excel con i dati ILI (Regione Lombardia,
    ATS Bergamo, ATS Brianza, ATS Montagna), riorganizza i dati per
    STAGIONE INFLUENZALE e salva file CSV in sottocartelle separate
    per area geografica.

    Questo è il PRIMO PASSO OBBLIGATORIO prima di qualsiasi analisi.

FILE DI INPUT:
    - Data_ILI.xlsx              → Regione Lombardia (5 fogli)
    - Data_ILI_ATS_Bergamo.xlsx  → ATS Bergamo (2 fogli)
    - Data_ILI_ATS_Brianza.xlsx  → ATS Brianza (2 fogli)  [NUOVO]
    - Data_ILI_ATS_Montagna.xlsx → ATS Montagna (2 fogli)

STRUTTURA DELLA STAGIONE INFLUENZALE:
    Le stagioni vanno dalla settimana 42 (ottobre) alla settimana 16
    (aprile dell'anno successivo). Convenzione usata:

        Stagione 21-22: settimane 42-52 del 2021 + settimane 1-16 del 2022
        Stagione 22-23: settimane 42-52 del 2022 + settimane 1-16 del 2023
        Stagione 23-24: settimane 42-52 del 2023 + settimane 1-16 del 2024
        Stagione 24-25: settimane 42-52 del 2024 + settimane 1-16 del 2025
        Stagione 25-26: settimane 42-52 del 2025 + settimane 1-16 del 2026

    Tutte le stagioni sono COMPLETE (nessuna settimana mancante).

STRUTTURA OUTPUT (sottocartelle CSV):
    output/
    ├── LOMBARDIA/
    │   ├── access_tot_stagionale.csv
    │   ├── admission_tot_stagionale.csv          [NUOVO]
    │   ├── access_er_ili_stagionale.csv
    │   ├── admission_after_er_stagionale.csv
    │   └── ili_er_per_age_stagionale.csv
    ├── ATS_BERGAMO/
    │   ├── access_tot_bergamo_stagionale.csv
    │   └── ili_ats_bergamo_stagionale.csv
    ├── ATS_BRIANZA/                               [NUOVO]
    │   ├── access_tot_brianza_stagionale.csv
    │   └── ili_ats_brianza_stagionale.csv
    └── ATS_MONTAGNA/
        ├── access_tot_montagna_stagionale.csv
        └── ili_ats_montagna_stagionale.csv

GRAFICI PRODOTTI (output/grafici/):
    Per ogni variabile: serie temporale per stagione influenzale.
    Per ogni ATS (Bergamo, Brianza, Montagna):
        - %ILI_accessi  = accessi ILI ATS / accessi tot ATS * 100
        - %ILI_ricoveri = ricoveri ILI regione / ricoveri tot regione * 100
          (rapporto aggiunto rispetto alla versione precedente)

    Il rapporto %ILI normalizza i casi ILI rispetto al volume
    complessivo di attività, rendendo comparabili stagioni con
    volumi diversi.

BIAS E LIMITAZIONI DA TENERE A MENTE:
    - Bias ecologico: i dati sono aggregati a livello ATS, non
      individuale. Le correlazioni aggregate NON possono essere
      interpretate come relazioni causali a livello di paziente.
    - Finestra temporale di 5 stagioni: aumenta il rischio di
      correlazioni spurie nelle analisi successive. Interpretare
      con cautela.

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
FILE_BRIANZA    = "Data_ILI_ATS_Brianza.xlsx"
FILE_MONTAGNA   = "Data_ILI_ATS_Montagna.xlsx"

CARTELLE = {
    "LOMBARDIA":    "output/LOMBARDIA",
    "ATS_BERGAMO":  "output/ATS_BERGAMO",
    "ATS_BRIANZA":  "output/ATS_BRIANZA",
    "ATS_MONTAGNA": "output/ATS_MONTAGNA",
}
for path in CARTELLE.values():
    os.makedirs(path, exist_ok=True)
os.makedirs("output/grafici", exist_ok=True)


# -------------------------------------------------------
# FUNZIONI
# -------------------------------------------------------

def assegna_stagione(week, anno):
    """
    Assegna la stagione influenzale (es. '22-23') dato settimana e anno.

    Logica:
        - Settimane 42-52: appartengono alla stagione ANNO — (ANNO+1)
        - Settimane  1-16: appartengono alla stagione (ANNO-1) — ANNO

    Perché questa logica?
        L'influenza stagionale parte tipicamente a ottobre (sett. 42)
        e termina ad aprile (sett. 16). Poiché attraversa il cambio
        d'anno, usiamo la convenzione europee di denominare la stagione
        con i due anni che attraversa.
    """
    if week >= 42:
        y1, y2 = anno, anno + 1
    else:  # settimane 1-16
        y1, y2 = anno - 1, anno
    return f"{str(y1)[-2:]}-{str(y2)[-2:]}"


def ordine_stagionale(week):
    """
    Posizione sequenziale all'interno della stagione influenzale.
    Settimana 42 → posizione 1
    Settimana 52 → posizione 11
    Settimana  1 → posizione 12
    Settimana 16 → posizione 27

    Perché serve?
        Per tracciare i grafici con l'asse X in ordine cronologico
        stagionale, invece che in ordine numerico di settimana
        (che spezzerebbe la curva tra dic e gen).
    """
    return week - 41 if week >= 42 else week + 11


def trasforma_in_stagionale(df, colonne_anni, nome_valore, col_gruppo=None):
    """
    Trasforma un dataframe da formato WIDE (colonne = anni) a formato
    LONG con colonna STAGIONE.

    Parametri:
        df:            dataframe con colonna WEEK (e opzionalmente GRUPPO ETÀ)
        colonne_anni:  lista delle colonne anno (es. [2021, 2022, ...])
        nome_valore:   nome da assegnare alla colonna dei valori
        col_gruppo:    nome di una colonna di raggruppamento aggiuntiva
                       (es. 'AGE GROUP'), se presente

    Restituisce un dataframe con colonne:
        [col_gruppo?,] STAGIONE, WEEK, ORDINE, <nome_valore>
    """
    righe = []
    for _, row in df.iterrows():
        week = row['WEEK']
        gruppo = row[col_gruppo] if col_gruppo else None
        for anno in colonne_anni:
            val = row[anno]
            if pd.isna(val):
                continue
            entry = {
                'STAGIONE': assegna_stagione(week, anno),
                'WEEK':     week,
                'ORDINE':   ordine_stagionale(week),
                nome_valore: int(val)
            }
            if col_gruppo:
                entry[col_gruppo] = gruppo
            righe.append(entry)

    result = pd.DataFrame(righe)
    if result.empty:
        return result

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
    print(f"  ✓ CSV salvato: {path}  ({len(df)} righe)")
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


def grafico_percentuale_ili(df_num, col_num, df_den, col_den,
                             titolo, nome_img, etichetta_y=None):
    """
    Calcola e traccia il rapporto percentuale tra due serie temporali,
    per stagione influenzale.

    USO 1 — %ILI accessi/ricoveri su totale ATS:
        df_num = accessi/ricoveri ILI dell'ATS
        df_den = accessi/ricoveri totali dell'ATS

    USO 2 — quota ATS su regione:  [NUOVO]
        df_num = accessi/ricoveri ILI dell'ATS
        df_den = accessi/ricoveri ILI della regione

    Perché questi rapporti sono utili?
        - %ILI / totale: normalizza per il volume complessivo di PS,
          rendendo comparabili stagioni con afflusso diverso.
        - quota ATS / regione: permette di capire se una ATS ha
          contribuito più o meno alla media regionale in una stagione.

    ⚠ Limitazione: il rapporto può variare per ragioni non legate
      all'influenza (es. campagne che spostano accessi non-ILI,
      variazioni soglie di triage). Interpretare con cautela.

    Parametri:
        df_num:     dataframe del numeratore
        col_num:    colonna del numeratore
        df_den:     dataframe del denominatore
        col_den:    colonna del denominatore
        titolo:     titolo del grafico
        nome_img:   nome file immagine (senza estensione)
        etichetta_y: etichetta asse Y (default: '% num / den')
    """
    df_merge = pd.merge(
        df_num[['STAGIONE', 'WEEK', 'ORDINE', col_num]],
        df_den[['STAGIONE', 'WEEK', 'ORDINE', col_den]],
        on=['STAGIONE', 'WEEK', 'ORDINE'],
        how='inner'
    )

    n_num   = len(df_num)
    n_den   = len(df_den)
    n_merge = len(df_merge)
    if n_merge < max(n_num, n_den):
        print(f"  ⚠ Merge percentuale: {n_merge} righe usate su "
              f"{n_num} (num) / {n_den} (den). Settimane non abbinate scartate.")

    df_merge['PCT'] = (
        df_merge[col_num].astype(float) /
        df_merge[col_den].astype(float) * 100
    ).where(df_merge[col_den] > 0)

    if df_merge['PCT'].isna().all():
        print(f"  ⚠ Nessun valore calcolabile per {nome_img} — grafico non prodotto.")
        return

    fig, ax = plt.subplots(figsize=(12, 6))
    for stagione in sorted(df_merge['STAGIONE'].unique()):
        subset = df_merge[df_merge['STAGIONE'] == stagione].sort_values('ORDINE')
        valid  = subset.dropna(subset=['PCT'])
        if not valid.empty:
            ax.plot(valid['ORDINE'], valid['PCT'], marker='o', label=stagione)

    tick_map = df_merge[['ORDINE', 'WEEK']].drop_duplicates().sort_values('ORDINE')
    ax.set_xticks(tick_map['ORDINE'])
    ax.set_xticklabels(tick_map['WEEK'], rotation=45)

    ax.set_title(titolo)
    ax.set_xlabel("Settimana")
    ax.set_ylabel(etichetta_y or f"% {col_num} / {col_den}")
    ax.legend(title="Stagione")
    ax.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join("output/grafici", f"{nome_img}.png"))
    plt.close()
    print(f"  ✓ Grafico: output/grafici/{nome_img}.png")


def get_anni(df, escludi=('WEEK', 'Settimana', 'AGE GROUP')):
    """Restituisce le colonne anno (intero) di un dataframe."""
    return [c for c in df.columns if c not in escludi]


def normalizza_col_week(df):
    """
    Rinomina la colonna settimana in 'WEEK' indipendentemente
    dal nome originale ('WEEK' o 'Settimana').
    """
    if 'Settimana' in df.columns:
        df = df.rename(columns={'Settimana': 'WEEK'})
    return df


# -------------------------------------------------------
# STEP 1: FILE LOMBARDIA (Data_ILI.xlsx)
# -------------------------------------------------------
print("\n" + "=" * 65)
print("FILE: Data_ILI.xlsx  →  REGIONE LOMBARDIA")
print("=" * 65)

fogli_lom = pd.read_excel(FILE_LOMBARDIA, sheet_name=None)
print(f"Fogli trovati: {list(fogli_lom.keys())}")

# --- 1. ACCESSI TOTALI IN PS (REGIONE) ---
print("\n[1/5] ACCESSI TOTALI IN PS — Regione Lombardia")
df = normalizza_col_week(fogli_lom["TOTAL ACCESS IN ER (REGION)"].copy())
df_tot_reg = trasforma_in_stagionale(df, get_anni(df), 'ACCESSI_TOTALI_ER')
salva_csv(df_tot_reg, "LOMBARDIA", "access_tot_stagionale.csv")
grafico_stagionale(df_tot_reg, 'ACCESSI_TOTALI_ER',
                   "Accessi Totali PS — Regione Lombardia",
                   "access_tot_lombardia")

# --- 2. RICOVERI TOTALI DOPO PS (REGIONE) [NUOVO] ---
print("\n[2/5] RICOVERI TOTALI DOPO PS — Regione Lombardia")
df = normalizza_col_week(fogli_lom["TOTAL ADMISSION AFTER ER (REG)"].copy())
df_ric_tot_reg = trasforma_in_stagionale(df, get_anni(df), 'RICOVERI_TOTALI_REG')
salva_csv(df_ric_tot_reg, "LOMBARDIA", "admission_tot_stagionale.csv")
grafico_stagionale(df_ric_tot_reg, 'RICOVERI_TOTALI_REG',
                   "Ricoveri Totali dopo PS — Regione Lombardia",
                   "admission_tot_lombardia")

# --- 3. ACCESSI ILI IN PS (REGIONE) ---
print("\n[3/5] ACCESSI ILI IN PS — Regione Lombardia")
df = normalizza_col_week(fogli_lom["ACCESS IN ER (ILI)"].copy())
df_ili_er_reg = trasforma_in_stagionale(df, get_anni(df), 'ACCESSI_ILI_ER')
salva_csv(df_ili_er_reg, "LOMBARDIA", "access_er_ili_stagionale.csv")
grafico_stagionale(df_ili_er_reg, 'ACCESSI_ILI_ER',
                   "Accessi ILI in PS — Regione Lombardia",
                   "access_er_ili_lombardia")

# --- 4. RICOVERI ILI DOPO PS (REGIONE) ---
print("\n[4/5] RICOVERI ILI DOPO PS — Regione Lombardia")
df = normalizza_col_week(fogli_lom["ADMISSION AFTER ER (ILI)"].copy())
df_ric_ili_reg = trasforma_in_stagionale(df, get_anni(df), 'RICOVERI_DOPO_ER')
salva_csv(df_ric_ili_reg, "LOMBARDIA", "admission_after_er_stagionale.csv")
grafico_stagionale(df_ric_ili_reg, 'RICOVERI_DOPO_ER',
                   "Ricoveri ILI dopo PS — Regione Lombardia",
                   "admission_after_er_lombardia")

# --- 4b. %RICOVERI ILI su TOTALI — Regione (indicatore normalizzato) ---
print("\n[4b] % Ricoveri ILI / Ricoveri Totali — Regione Lombardia")
grafico_percentuale_ili(
    df_num=df_ric_ili_reg, col_num='RICOVERI_DOPO_ER',
    df_den=df_ric_tot_reg, col_den='RICOVERI_TOTALI_REG',
    titolo="% Ricoveri ILI su Ricoveri Totali — Regione Lombardia",
    nome_img="pct_ricoveri_ili_lombardia",
    etichetta_y="% Ricoveri ILI / Ricoveri Totali"
)

# --- 4c. %ACCESSI ILI su TOTALI — Regione (indicatore normalizzato) ---
print("\n[4c] % Accessi ILI / Accessi Totali — Regione Lombardia")
grafico_percentuale_ili(
    df_num=df_ili_er_reg, col_num='ACCESSI_ILI_ER',
    df_den=df_tot_reg,    col_den='ACCESSI_TOTALI_ER',
    titolo="% Accessi ILI su Accessi Totali PS — Regione Lombardia",
    nome_img="pct_accessi_ili_lombardia",
    etichetta_y="% Accessi ILI / Accessi Totali"
)

# --- 5. ACCESSI ILI PER FASCIA ETÀ (REGIONE) ---
print("\n[5/5] ACCESSI ILI PER FASCIA ETÀ — Regione Lombardia")
df = fogli_lom["ACCESS IN ER PER AGE (ILI)"].copy()
# Questa tabella ha già la colonna WEEK (non Settimana)
anni_age = [c for c in df.columns if c not in ('AGE GROUP', 'WEEK')]
df_age = trasforma_in_stagionale(df, anni_age, 'ACCESSI_ILI_ER', col_gruppo='AGE GROUP')
salva_csv(df_age, "LOMBARDIA", "ili_er_per_age_stagionale.csv")
grafico_stagionale(df_age, 'ACCESSI_ILI_ER',
                   "Accessi ILI per Fascia Età — Lombardia",
                   "ili_er_per_age_lombardia",
                   col_gruppo='AGE GROUP')


# -------------------------------------------------------
# STEP 2: FILE ATS BERGAMO (Data_ILI_ATS_Bergamo.xlsx)
# -------------------------------------------------------
print("\n" + "=" * 65)
print("FILE: Data_ILI_ATS_Bergamo.xlsx  →  ATS BERGAMO")
print("=" * 65)

fogli_bg = pd.read_excel(FILE_BERGAMO, sheet_name=None)
print(f"Fogli trovati: {list(fogli_bg.keys())}")

# --- Accessi totali ---
print("\n[1/2] ACCESSI TOTALI PS — ATS Bergamo")
df = fogli_bg["TOTAL ACCESS IN ER (BERGAMO)"].copy()
df_tot_bg = trasforma_in_stagionale(df, get_anni(df), 'ACCESSI_TOTALI_ER_BERGAMO')
salva_csv(df_tot_bg, "ATS_BERGAMO", "access_tot_bergamo_stagionale.csv")
grafico_stagionale(df_tot_bg, 'ACCESSI_TOTALI_ER_BERGAMO',
                   "Accessi Totali PS — ATS Bergamo",
                   "access_tot_bergamo")

# --- Accessi ILI ---
print("\n[2/2] ACCESSI ILI PS — ATS Bergamo")
df = fogli_bg["ACCESS IN ATS BERGAMO (ILI)"].copy()
df_ili_bg = trasforma_in_stagionale(df, get_anni(df), 'ACCESSI_ILI_ATS_BERGAMO')
df_ili_bg = df_ili_bg.dropna(subset=['ACCESSI_ILI_ATS_BERGAMO'])
if df_ili_bg.empty:
    print("  ⚠ Foglio ILI Bergamo vuoto — nessun CSV prodotto.")
else:
    salva_csv(df_ili_bg, "ATS_BERGAMO", "ili_ats_bergamo_stagionale.csv")
    grafico_stagionale(df_ili_bg, 'ACCESSI_ILI_ATS_BERGAMO',
                       "Accessi ILI — ATS Bergamo",
                       "ili_ats_bergamo")

    # --- % ILI / Totale PS — ATS Bergamo ---
    print("\n[2b] % Accessi ILI / Totale PS — ATS Bergamo")
    grafico_percentuale_ili(
        df_num=df_ili_bg, col_num='ACCESSI_ILI_ATS_BERGAMO',
        df_den=df_tot_bg, col_den='ACCESSI_TOTALI_ER_BERGAMO',
        titolo="% Accessi ILI su Totale PS — ATS Bergamo",
        nome_img="pct_ili_bergamo",
        etichetta_y="% Accessi ILI / Accessi Totali PS"
    )

    # --- % ILI Bergamo / ILI Regione [NUOVO] ---
    # Risponde a: quanto pesa Bergamo sull'ILI regionale settimana per settimana?
    print("\n[2c] % Accessi ILI Bergamo / Accessi ILI Regione")
    grafico_percentuale_ili(
        df_num=df_ili_bg,      col_num='ACCESSI_ILI_ATS_BERGAMO',
        df_den=df_ili_er_reg,  col_den='ACCESSI_ILI_ER',
        titolo="Quota Accessi ILI Bergamo su Regione Lombardia (%)",
        nome_img="quota_ili_bergamo_su_reg",
        etichetta_y="% Bergamo / Regione"
    )

    # --- % Ricoveri ILI Regione / Ricoveri Totali Regione —
    # (confronto con ATS Bergamo: la serie regionale è il riferimento) ---
    # Nota: ATS Bergamo non ha dati propri di ricovero, ma possiamo mostrare
    # il grafico regionale come benchmark nella stessa sezione.


# -------------------------------------------------------
# STEP 3: FILE ATS BRIANZA (Data_ILI_ATS_Brianza.xlsx) [NUOVO]
# -------------------------------------------------------
print("\n" + "=" * 65)
print("FILE: Data_ILI_ATS_Brianza.xlsx  →  ATS BRIANZA  [NUOVO]")
print("=" * 65)

fogli_bz = pd.read_excel(FILE_BRIANZA, sheet_name=None)
print(f"Fogli trovati: {list(fogli_bz.keys())}")

# --- Accessi totali ---
print("\n[1/2] ACCESSI TOTALI PS — ATS Brianza")
df = fogli_bz["TOTAL ACCESS IN ER (BRIANZA)"].copy()
df_tot_bz = trasforma_in_stagionale(df, get_anni(df), 'ACCESSI_TOTALI_ER_BRIANZA')
salva_csv(df_tot_bz, "ATS_BRIANZA", "access_tot_brianza_stagionale.csv")
grafico_stagionale(df_tot_bz, 'ACCESSI_TOTALI_ER_BRIANZA',
                   "Accessi Totali PS — ATS Brianza",
                   "access_tot_brianza")

# --- Accessi ILI ---
print("\n[2/2] ACCESSI ILI PS — ATS Brianza")
df = fogli_bz["ACCESS IN ATS BRIANZA (ILI)"].copy()
df_ili_bz = trasforma_in_stagionale(df, get_anni(df), 'ACCESSI_ILI_ATS_BRIANZA')
df_ili_bz = df_ili_bz.dropna(subset=['ACCESSI_ILI_ATS_BRIANZA'])
if df_ili_bz.empty:
    print("  ⚠ Foglio ILI Brianza vuoto — nessun CSV prodotto.")
else:
    salva_csv(df_ili_bz, "ATS_BRIANZA", "ili_ats_brianza_stagionale.csv")
    grafico_stagionale(df_ili_bz, 'ACCESSI_ILI_ATS_BRIANZA',
                       "Accessi ILI — ATS Brianza",
                       "ili_ats_brianza")

    # --- % ILI / Totale PS — ATS Brianza ---
    print("\n[2b] % Accessi ILI / Totale PS — ATS Brianza")
    grafico_percentuale_ili(
        df_num=df_ili_bz, col_num='ACCESSI_ILI_ATS_BRIANZA',
        df_den=df_tot_bz, col_den='ACCESSI_TOTALI_ER_BRIANZA',
        titolo="% Accessi ILI su Totale PS — ATS Brianza",
        nome_img="pct_ili_brianza",
        etichetta_y="% Accessi ILI / Accessi Totali PS"
    )

    # --- % ILI Brianza / ILI Regione [NUOVO] ---
    print("\n[2c] % Accessi ILI Brianza / Accessi ILI Regione")
    grafico_percentuale_ili(
        df_num=df_ili_bz,      col_num='ACCESSI_ILI_ATS_BRIANZA',
        df_den=df_ili_er_reg,  col_den='ACCESSI_ILI_ER',
        titolo="Quota Accessi ILI Brianza su Regione Lombardia (%)",
        nome_img="quota_ili_brianza_su_reg",
        etichetta_y="% Brianza / Regione"
    )


# -------------------------------------------------------
# STEP 4: FILE ATS MONTAGNA (Data_ILI_ATS_Montagna.xlsx)
# -------------------------------------------------------
print("\n" + "=" * 65)
print("FILE: Data_ILI_ATS_Montagna.xlsx  →  ATS MONTAGNA")
print("=" * 65)

fogli_mt = pd.read_excel(FILE_MONTAGNA, sheet_name=None)
print(f"Fogli trovati: {list(fogli_mt.keys())}")

# --- Accessi totali ---
print("\n[1/2] ACCESSI TOTALI PS — ATS Montagna")
df = fogli_mt["TOTAL ACCESS IN ER (MONTAGNA)"].copy()
df_tot_mt = trasforma_in_stagionale(df, get_anni(df), 'ACCESSI_TOTALI_ER_MONTAGNA')
salva_csv(df_tot_mt, "ATS_MONTAGNA", "access_tot_montagna_stagionale.csv")
grafico_stagionale(df_tot_mt, 'ACCESSI_TOTALI_ER_MONTAGNA',
                   "Accessi Totali PS — ATS Montagna",
                   "access_tot_montagna")

# --- Accessi ILI ---
print("\n[2/2] ACCESSI ILI PS — ATS Montagna")
df = fogli_mt["ACCESS IN ATS MONTAGNA (ILI)"].copy()
df_ili_mt = trasforma_in_stagionale(df, get_anni(df), 'ACCESSI_ILI_ATS_MONTAGNA')
df_ili_mt = df_ili_mt.dropna(subset=['ACCESSI_ILI_ATS_MONTAGNA'])
if df_ili_mt.empty:
    print("  ⚠ Foglio ILI Montagna vuoto — nessun CSV prodotto.")
else:
    salva_csv(df_ili_mt, "ATS_MONTAGNA", "ili_ats_montagna_stagionale.csv")
    grafico_stagionale(df_ili_mt, 'ACCESSI_ILI_ATS_MONTAGNA',
                       "Accessi ILI — ATS Montagna",
                       "ili_ats_montagna")

    # --- % ILI / Totale PS — ATS Montagna ---
    print("\n[2b] % Accessi ILI / Totale PS — ATS Montagna")
    grafico_percentuale_ili(
        df_num=df_ili_mt, col_num='ACCESSI_ILI_ATS_MONTAGNA',
        df_den=df_tot_mt, col_den='ACCESSI_TOTALI_ER_MONTAGNA',
        titolo="% Accessi ILI su Totale PS — ATS Montagna",
        nome_img="pct_ili_montagna",
        etichetta_y="% Accessi ILI / Accessi Totali PS"
    )

    # --- % ILI Montagna / ILI Regione [NUOVO] ---
    # Particolarmente rilevante per Montagna: la popolazione è più piccola
    # e i valori assoluti più rumorosi; il peso relativo su regione
    # è un indicatore più robusto.
    print("\n[2c] % Accessi ILI Montagna / Accessi ILI Regione")
    grafico_percentuale_ili(
        df_num=df_ili_mt,      col_num='ACCESSI_ILI_ATS_MONTAGNA',
        df_den=df_ili_er_reg,  col_den='ACCESSI_ILI_ER',
        titolo="Quota Accessi ILI Montagna su Regione Lombardia (%)",
        nome_img="quota_ili_montagna_su_reg",
        etichetta_y="% Montagna / Regione"
    )


# -------------------------------------------------------
# RIEPILOGO FINALE
# -------------------------------------------------------
print("\n" + "=" * 65)
print("✅ SCRIPT 1 COMPLETATO!")
print("\n CSV prodotti per sottocartella:")
for key, path in CARTELLE.items():
    if os.path.exists(path):
        files = os.listdir(path)
        print(f"  {path}/")
        for f in sorted(files):
            print(f"    └─ {f}")

print("\n Grafici salvati in: output/grafici/")
print("""
⚠ NOTE SUI BIAS:
  - Bias ecologico: i dati sono aggregati a livello ATS, non individuale.
    Le correlazioni osservate NON possono essere interpretate come
    relazioni causali a livello di singolo paziente.
  - 5 stagioni complete disponibili: la finestra temporale è sufficiente
    per analisi descrittive ma limitata per modelli multivariati.
    Interpreta con cautela eventuali correlazioni numeriche.

Prossimo passo: Script 2 per i dati ambientali.
""")
print("=" * 65)