"""
SCRIPT 4 — Merge finale dei dataset
=====================================
Unisce meteo (ERA5) e inquinanti (ARPA) in un unico dataset
pronto per l'analisi statistica (GAM, regressione, ecc.)

INPUT:
  meteo_per_ats_giornaliero.csv      (dallo script 02)
  inquinanti_per_ats_giornaliero.csv (dallo script 03)

OUTPUT:
  dataset_finale.csv  — dataset panel giornaliero per ATS, pronto per l'analisi

STRUTTURA OUTPUT:
  data | ATS | temperatura_C | umidita_assoluta_gm3 | umidita_relativa_pct
       | NO2 | PM2.5 | PM10 | stagione | settimana_anno
"""

import pandas as pd
import numpy as np
from pathlib import Path


def assegna_stagione(data: pd.Timestamp) -> str:
    """
    Assegna la stagione influenzale nel formato 'AAAA/AA'.
    La stagione va da ottobre a aprile dell'anno successivo.
    Es: ottobre 2022 → aprile 2023 = stagione '2022/23'
    """
    anno = data.year
    mese = data.month
    if mese >= 10:
        return f"{anno}/{str(anno+1)[-2:]}"
    elif mese <= 4:
        return f"{anno-1}/{str(anno)[-2:]}"
    else:
        return None  # maggio-settembre: fuori stagione influenzale


def main():
    # --- 1. Carica i due dataset ---
    file_meteo      = 'meteo_per_ats_giornaliero.csv'
    file_inquinanti = 'inquinanti_per_ats_giornaliero.csv'

    for f in [file_meteo, file_inquinanti]:
        if not Path(f).exists():
            raise FileNotFoundError(f"File non trovato: {f}. Eseguire prima gli script 02 e 03.")

    df_meteo = pd.read_csv(file_meteo)
    df_inq   = pd.read_csv(file_inquinanti)

    print(f"Meteo:      {len(df_meteo)} righe")
    print(f"Inquinanti: {len(df_inq)} righe")

    # Uniforma formato data
    df_meteo['data'] = pd.to_datetime(df_meteo['data'])
    df_inq['data']   = pd.to_datetime(df_inq['data'])

    # --- 2. Merge ---
    df = pd.merge(df_meteo, df_inq, on=['data', 'ATS'], how='outer')
    df = df.sort_values(['ATS', 'data']).reset_index(drop=True)

    # --- 3. Aggiungi variabili temporali utili per l'analisi ---
    df['stagione']      = df['data'].apply(assegna_stagione)
    df['settimana_iso'] = df['data'].dt.isocalendar().week.astype(int)
    df['anno']          = df['data'].dt.year
    df['mese']          = df['data'].dt.month
    df['giorno_anno']   = df['data'].dt.dayofyear

    # Rimuovi giorni fuori stagione influenzale (maggio-settembre)
    df_stagionale = df[df['stagione'].notna()].copy()
    print(f"\nRighe dopo filtro stagionale (ott-apr): {len(df_stagionale)}")

    # --- 4. Report missing values (CRITICO per la metodologia) ---
    print("\n--- VALORI MANCANTI (%) ---")
    print("Questo è importante per la sezione 'limitazioni' del vostro report.\n")
    missingness = df_stagionale.groupby('ATS').apply(
        lambda g: g[['temperatura_C', 'umidita_assoluta_gm3',
                     'NO2', 'PM2.5', 'PM10']].isna().mean() * 100
    ).round(1)
    print(missingness.to_string())

    print("\n⚠️  INTERPRETAZIONE:")
    print("  • I valori mancanti per inquinanti in ATS_Montagna sono attesi")
    print("    (poche stazioni ARPA in zona montana)")
    print("  • NON imputate i valori mancanti con la media: introduce bias")
    print("  • Nelle analisi statistiche usate modelli che gestiscono i NA")
    print("    (i modelli GAM/glm in R gestiscono NA rimuovendo le righe)")

    # --- 5. Salva ---
    output_file = 'dataset_finale.csv'
    df_stagionale.to_csv(output_file, index=False)

    print(f"\nSalvato: {output_file}  ({len(df_stagionale)} righe, {len(df_stagionale.columns)} colonne)")
    print(f"Colonne: {df_stagionale.columns.tolist()}")
    print("\nPrime righe:")
    print(df_stagionale.head(10).to_string(index=False))

    # --- 6. Statistiche descrittive finali ---
    print("\n--- STATISTICHE DESCRITTIVE PER ATS ---")
    cols = ['temperatura_C', 'umidita_assoluta_gm3', 'NO2', 'PM2.5', 'PM10']
    cols_presenti = [c for c in cols if c in df_stagionale.columns]
    print(df_stagionale.groupby('ATS')[cols_presenti].describe().round(2).to_string())


if __name__ == '__main__':
    main()
