"""
=============================================================
SCRIPT 1 — ESPLORAZIONE E PULIZIA DEI DATI ILI
=============================================================

COSA FA QUESTO SCRIPT:
    Carica il file Excel con i dati ILI, corregge i valori mal
    interpretati da Python (separatore migliaia "." letto come separatore
    decimale), e prepara i dataset per analisi successive.

    È il PRIMO passo obbligatorio prima di qualsiasi analisi.

PROBLEMA NEL FILE (e come lo risolviamo):
    Il punto "." nel file Excel è usato come SEPARATORE DELLE MIGLIAIA
    (convenzione italiana: 1.692 = milleseicento novantadue).
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

COSA PRODUCE:
    - Stampa con valori corretti e statistiche
    - Grafico andamento delle varie categorie
    - 6 file CSV aggiuntivi:
        access_tot.csv
        access_milano.csv
        ili_ats_milano.csv
        ili_er_per_age.csv
        access_er_ili.csv
        admission_after_er.csv

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
# CONFIGURAZIONE
# -------------------------------------------------------
FILE_EXCEL = "Data_ILI.xlsx"

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
            3.48  (letto) → 3480 (reale)   perché 3.48  < 100
            10.02 (letto) → 10020 (reale)  perché 10.02 < 100
            766   (letto) → 766  (reale)   perché 766 >= 100, già corretto
            958   (letto) → 958  (reale)   perché 958 >= 100, già corretto
        """
    if pd.isna(val):
        return val

        # Se è già un numero, restituiscilo
    if isinstance(val, (int, float)):
        num = val
    else:
        # Pulisci: rimuovi sia il punto che lo spazio
        pulito = str(val).replace('.', '').replace(' ', '')
        try:
            num = float(pulito)
        except ValueError:
            # Se la cella contiene testo non convertibile, restituisci NaN
            return float('nan')

        # Applica la tua logica di correzione
    if num < 100:
        return num * 1000
    return num


def a_formato_lungo(df, nome_valore):
    """
    Trasforma un dataframe da formato LARGO a formato LUNGO.

    Formato largo (come nel file Excel):
        SETTIMANA | 2022 | 2023 | 2024
             1    | 2698 | 3877 | 4967

    Formato lungo (più utile per grafici e analisi):
        SETTIMANA | ANNO | VALORE
             1    | 2022 | 2698
             1    | 2023 | 3877
             1    | 2024 | 4967
    """
    df.columns = ['settimana'] + [str(c) for c in df.columns[1:]]
    df_lungo = df.melt(id_vars='settimana', var_name='anno', value_name=nome_valore)
    df_lungo['anno'] = pd.to_numeric(df_lungo['anno'], errors='coerce')
    df_lungo = df_lungo.dropna(subset=[nome_valore])
    return df_lungo


def ordine_settimana(s):
    """
    Assegna un ordine progressivo per visualizzare correttamente la stagione
    influenzale che va da settimana 48 (novembre) a settimana 15 (aprile).

    sett. 48 → posizione  1  (inizio stagione)
    sett. 52 → posizione  5
    sett.  1 → posizione  6  (anno nuovo, stagione continua)
    sett. 15 → posizione 20  (fine stagione)
    """
    return s - 47 if s >= 48 else s + 5

# -------------------------------------------------------
# PASSO 1: STRUTTURA DEL FILE
# -------------------------------------------------------
print("=" * 60)
print("CARICAMENTO DEI FOGLI EXCEL...")
print("=" * 60)

tutti_fogli = pd.read_excel(FILE_EXCEL, sheet_name=None)
print(f"\nFogli trovati nel file ({len(tutti_fogli)}):")
for nome, df in tutti_fogli.items():
    print(f"  - '{nome}': {df.shape[0]} righe x {df.shape[1]} colonne")

# -------------------------------------------------------
# PASSO 2: ACCESSI TOTALI
# -------------------------------------------------------
print("\n" + "=" * 60)
print("FOGLIO: 'TOTAL ACCESS IN ER (REGION)'")
print("=" * 60)

df_access_tot = pd.read_excel(FILE_EXCEL, sheet_name="TOTAL ACCESS IN ER (REGION)")
anni_access_tot = [c for c in df_access_tot.columns if c != 'WEEK']

for col in anni_access_tot:
    df_access_tot[col] = df_access_tot[col].apply(correggi_separatore_migliaia)
    df_access_tot[col] = pd.to_numeric(df_access_tot[col], errors='coerce').round().astype('Int64')

# Salvataggio raw ILI (prima trasformazione lunga)
df_access_tot.to_csv("access_tot.csv", index=False)
print(f"\nSalvato CSV: access_tot.csv  ({len(df_access_tot)} righe)")

# -------------------------------------------------------
# PASSO 3: ACCESSI TOTALI ATS MILANO
# -------------------------------------------------------
print("\n" + "=" * 60)
print("FOGLIO: 'TOTAL ACCESS IN ER (ATS MILAN)'")
print("=" * 60)

df_access_mil = pd.read_excel(FILE_EXCEL, sheet_name="TOTAL ACCESS IN ER (ATS MILAN)")
anni_access_mil = [c for c in df_access_mil.columns if c != 'WEEK']

for col in anni_access_mil:
    df_access_mil[col] = df_access_mil[col].apply(correggi_separatore_migliaia)
    df_access_mil[col] = pd.to_numeric(df_access_mil[col], errors='coerce').round().astype('Int64')

# Salvataggio raw ILI (prima trasformazione lunga)
df_access_mil.to_csv("access_milano.csv", index=False)
print(f"\nSalvato CSV: access_milano.csv  ({len(df_access_mil)} righe)")

# -------------------------------------------------------
# PASSO 4: ATS Milano (ILI)
# -------------------------------------------------------
print("\n" + "=" * 60)
print("FOGLIO: 'ACCESS IN ATS MILANO (ILI)'")
print("=" * 60)

df_milan = pd.read_excel(FILE_EXCEL, sheet_name="ACCESS IN ATS MILANO (ILI)")
anni_milan = [c for c in df_milan.columns if c != 'WEEK']

for col in anni_milan:
    df_milan[col] = df_milan[col].apply(correggi_separatore_migliaia)
    df_milan[col] = pd.to_numeric(df_milan[col], errors='coerce').round().astype('Int64')

# Salvataggio ATS Milano corretto
df_milan.to_csv("ili_ats_milano.csv", index=False)
print(f"\nSalvato CSV: ili_ats_milano.csv  ({len(df_milan)} righe)")

# -------------------------------------------------------
# PASSO 5: ACCESSI PER FASCIA D'ETA' (ILI)
# -------------------------------------------------------
print("\n" + "=" * 60)
print("FOGLIO: 'ACCESS IN ER PER AGE (ILI)'")
print("=" * 60)

df_age = pd.read_excel(FILE_EXCEL, sheet_name="ACCESS IN ER PER AGE (ILI)")
anni_age = [c for c in df_age.columns if c not in ['AGE GROUP', 'WEEK']]
fasce = df_age.iloc[:, 0].unique()

for col in anni_age:
    df_age[col] = df_age[col].apply(correggi_separatore_migliaia)
    df_age[col] = pd.to_numeric(df_age[col], errors='coerce').round().astype('Int64')

# Salvataggio ER per age corretto
df_age.to_csv("ili_er_per_age.csv", index=False)
print(f"\nSalvato CSV: ili_er_per_age.csv  ({len(df_age)} righe)")

# -------------------------------------------------------
# PASSO 6: ACCESSI IN PRONTO SOCCORSO (ILI)
# -------------------------------------------------------
print("\n" + "=" * 60)
print("FOGLIO: 'ACCESS IN ER (ILI)'")
print("=" * 60)

df_access_ili = pd.read_excel(FILE_EXCEL, sheet_name="ACCESS IN ER (ILI)")
anni_access_ili = [c for c in df_access_ili.columns if c != 'WEEK']

for col in anni_access_ili:
    df_access_ili[col] = df_access_ili[col].apply(correggi_separatore_migliaia)
    df_access_ili[col] = pd.to_numeric(df_access_ili[col], errors='coerce').round().astype('Int64')

# Salvataggio ricoveri raw
df_access_ili.to_csv("access_er_ili.csv", index=False)
print(f"\nSalvato CSV: access_er_ili.csv  ({len(df_access_ili)} righe)")

# -------------------------------------------------------
# PASSO 7: RICOVERI DOPO PRONTO SOCCORSO (ILI)
# -------------------------------------------------------
print("\n" + "=" * 60)
print("FOGLIO: 'ADMISSION AFTER ER (ILI)'")
print("=" * 60)

df_ric = pd.read_excel(FILE_EXCEL, sheet_name="ADMISSION AFTER ER (ILI)")
anni_ric = [c for c in df_ric.columns if c != 'WEEK']

for col in anni_ric:
    df_ric[col] = df_ric[col].apply(correggi_separatore_migliaia)
    df_ric[col] = pd.to_numeric(df_ric[col], errors='coerce').round().astype('Int64')

# Salvataggio ricoveri raw
df_ric.to_csv("admission_after_er.csv", index=False)
print(f"\nSalvato CSV: admission_after_er.csv  ({len(df_ric)} righe)")

# -------------------------------------------------------
# PASSO 8: PRODUZIONE GRAFICI
# -------------------------------------------------------

print("\n" + "=" * 60)
print("PRODUZIONE GRAFICI...")
print("=" * 60)

# Creazione cartella dedicata
os.makedirs("grafici", exist_ok=True)

# Lista dei file CSV generati nei passi precedenti
file_da_analizzare = [
    ("access_tot.csv", "Andamento Totale Accessi (Regione)", "access_tot_trend.png"),
    ("access_milano.csv", "Andamento Totale Accessi (ATS Milano)", "access_milano_trend.png"),
    ("ili_ats_milano.csv", "Andamento Accessi ILI (ATS Milano)", "ili_ats_milano_trend.png"),
    ("access_er_ili.csv", "Andamento Accessi ILI (ER)", "access_er_ili_trend.png"),
    ("admission_after_er.csv", "Andamento Ricoveri dopo ER (ILI)", "admission_after_er_trend.png")
]

for nome_file, titolo, nome_img in file_da_analizzare:
    try:
        df = pd.read_csv(nome_file)

        # 1. Crea una colonna di ordinamento basata sulla logica stagionale
        df['ordine_stagione'] = df['WEEK'].apply(ordine_settimana)

        # 2. Ordina per questa nuova colonna
        df = df.sort_values('ordine_stagione')

        plt.figure(figsize=(10, 6))
        colonne_anni = [c for c in df.columns if c not in ['WEEK', 'ordine_stagione']]

        for anno in colonne_anni:
            # Filtriamo solo le righe dove il dato per quell'anno esiste
            subset = df[['ordine_stagione', 'WEEK', anno]].dropna(subset=[anno])

            # Se il subset non è vuoto, disegniamo la linea
            if not subset.empty:
                plt.plot(subset['ordine_stagione'], subset[anno], marker='o', label=anno)

        # 3. Personalizza l'asse X per mostrare le settimane reali
        plt.xticks(df['ordine_stagione'], df['WEEK'])

        plt.title(titolo)
        plt.xlabel("Settimana")
        plt.ylabel("Numero Accessi")
        plt.legend(title="Anno")
        plt.grid(True, linestyle='--', alpha=0.7)

        plt.savefig(os.path.join("grafici", nome_img))
        plt.close()
    except Exception as e:
        print(f"Errore nella creazione di {nome_img}: {e}")

# Gestione speciale per le fasce d'età (raggruppate)
try:
    df_age = pd.read_csv("ili_er_per_age.csv")

    # 1. Applichiamo l'ordinamento stagionale anche qui
    df_age['ordine_stagione'] = df_age['WEEK'].apply(ordine_settimana)

    for fascia in df_age['AGE GROUP'].unique():
        # Filtriamo per fascia d'età e prendiamo solo le colonne utili
        subset_fascia = df_age[df_age['AGE GROUP'] == fascia].copy()
        subset_fascia = subset_fascia.sort_values('ordine_stagione')

        plt.figure(figsize=(10, 6))
        colonne_anni = [c for c in subset_fascia.columns if c not in ['AGE GROUP', 'WEEK', 'ordine_stagione']]

        for anno in colonne_anni:
            # 2. Rimuoviamo gli zeri o NaN per non avere linee che scendono a zero
            # Se hai convertito a Int64 nel passo precedente, questi saranno <NA>
            plot_data = subset_fascia[['ordine_stagione', 'WEEK', anno]].copy()
            plot_data[anno] = plot_data[anno].replace(0, pd.NA)  # Assicura che anche gli zeri non vengano plottati
            plot_data = plot_data.dropna(subset=[anno])

            if not plot_data.empty:
                plt.plot(plot_data['ordine_stagione'], plot_data[anno], marker='o', label=anno)

        # 3. Personalizziamo le etichette con le settimane reali
        plt.xticks(subset_fascia['ordine_stagione'], subset_fascia['WEEK'])

        plt.title(f"Andamento Fascia Età: {fascia}")
        plt.xlabel("Settimana")
        plt.ylabel("Numero Accessi")
        plt.legend(title="Anno")
        plt.grid(True, linestyle='--', alpha=0.7)

        nome_file_age = f"age_{fascia.replace(' ', '_').replace('-', '_')}.png"
        plt.savefig(os.path.join("grafici", nome_file_age))
        plt.close()
        print(f"Grafico fascia età salvato: grafici/{nome_file_age}")
except Exception as e:
    print(f"Errore nella creazione grafici età: {e}")

print("\n" + "=" * 60)
print("✅ TUTTI I GRAFICI SONO STATI SALVATI NELLA CARTELLA 'grafici'")
print("=" * 60)

# -------------------------------------------------------
# FINE
# -------------------------------------------------------

print("\n" + "=" * 60)
print("✅ SCRIPT 1 COMPLETATO!")
print("Prossimo passo: esegui Script 2 per i dati ambientali.")
print("=" * 60)