"""
SCRIPT 4 — Merge finale dei dataset
=====================================
Unisce meteo (ERA5, Script 2) e inquinanti (ARPA, Script 3) in un unico
dataset panel giornaliero per ATS, pronto per l'analisi statistica.

INPUT:
  meteo_per_ats_giornaliero.csv   (Script 02)
    colonne: data | ATS | temperatura_C | umidita_assoluta_gm3 | umidita_relativa_pct

  inquinanti_per_ats_pivot.csv    (Script 03 — formato PIVOT)
    colonne: giorno | ATS | NO2_mean_ugm3 | PM10_mean_ugm3 | PM25_mean_ugm3

OUTPUT:
  dataset_finale.csv — dataset panel giornaliero, pronto per analisi GAM/regressione
    colonne: data | ATS | temperatura_C | umidita_assoluta_gm3 | umidita_relativa_pct
           | NO2_ugm3 | PM10_ugm3 | PM25_ugm3
           | stagione | settimana_iso | anno | mese | giorno_anno

PREREQUISITI:
  pip install pandas numpy
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIGURAZIONE
# ---------------------------------------------------------------------------

FILE_METEO      = "meteo_per_ats_giornaliero.csv"
FILE_INQUINANTI = "inquinanti_per_ats_pivot.csv"    # pivot con colonne separate NO2/PM10/PM25
OUTPUT_FILE     = "dataset_finale.csv"

# ---------------------------------------------------------------------------
# FUNZIONI
# ---------------------------------------------------------------------------

def assegna_stagione(data: pd.Timestamp) -> str | None:
    """
    Assegna la stagione influenzale nel formato 'AAAA/AA'.
    La stagione va da ottobre a aprile dell'anno successivo.
      Es: ottobre 2022 → aprile 2023  →  '2022/23'
    Restituisce None per i mesi fuori stagione (maggio-settembre).
    """
    mese = data.month
    anno = data.year
    if mese >= 10:
        return f"{anno}/{str(anno + 1)[-2:]}"
    elif mese <= 4:
        return f"{anno - 1}/{str(anno)[-2:]}"
    return None  # maggio-settembre: fuori stagione influenzale


def main():
    print("=" * 60)
    print("SCRIPT 4 — Merge meteo + inquinanti")
    print("=" * 60)

    # ------------------------------------------------------------------
    # 1. Carica i dataset
    # ------------------------------------------------------------------
    for f in [FILE_METEO, FILE_INQUINANTI]:
        if not Path(f).exists():
            raise FileNotFoundError(
                f"\nFile non trovato: {f}\n"
                "Eseguire prima gli script 02 (meteo) e 03 (inquinanti)."
            )

    print(f"\n[1/5] Carico dataset...")
    df_meteo = pd.read_csv(FILE_METEO)
    df_inq   = pd.read_csv(FILE_INQUINANTI)

    print(f"  Meteo      : {len(df_meteo):,} righe | colonne: {df_meteo.columns.tolist()}")
    print(f"  Inquinanti : {len(df_inq):,} righe  | colonne: {df_inq.columns.tolist()}")

    # ------------------------------------------------------------------
    # 2. Uniforma colonna data
    #    - Script 02 → colonna 'data'
    #    - Script 03 → colonna 'giorno'   ← rinomina prima del merge
    # ------------------------------------------------------------------
    print("\n[2/5] Uniformo formato date...")

    df_meteo["data"] = pd.to_datetime(df_meteo["data"]).dt.normalize()

    # Script 03 chiama la colonna 'giorno' — la rinominiamo in 'data'
    if "giorno" in df_inq.columns:
        df_inq = df_inq.rename(columns={"giorno": "data"})
    df_inq["data"] = pd.to_datetime(df_inq["data"]).dt.normalize()

    # Rinomina colonne inquinanti: togli il suffisso _mean_ugm3
    # per nomi più leggibili nel dataset finale
    df_inq = df_inq.rename(columns={
        "NO2_mean_ugm3":  "NO2_ugm3",
        "PM10_mean_ugm3": "PM10_ugm3",
        "PM25_mean_ugm3": "PM25_ugm3",
    })

    print(f"  Meteo  : {df_meteo['data'].min().date()} → {df_meteo['data'].max().date()}")
    print(f"  Inquin.: {df_inq['data'].min().date()} → {df_inq['data'].max().date()}")

    # ------------------------------------------------------------------
    # 3. Merge su (data, ATS)
    #    - how='outer' per non perdere giorni con solo meteo o solo inquinanti
    # ------------------------------------------------------------------
    print("\n[3/5] Merge su (data, ATS)...")

    df = pd.merge(df_meteo, df_inq, on=["data", "ATS"], how="outer")
    df = df.sort_values(["ATS", "data"]).reset_index(drop=True)

    print(f"  Righe dopo merge: {len(df):,}")
    print(f"  ATS presenti: {sorted(df['ATS'].unique().tolist())}")

    # ------------------------------------------------------------------
    # 4. Aggiungi variabili temporali
    # ------------------------------------------------------------------
    print("\n[4/5] Aggiungo variabili temporali...")

    df["stagione"]     = df["data"].apply(assegna_stagione)
    df["settimana_iso"] = df["data"].dt.isocalendar().week.astype(int)
    df["anno"]         = df["data"].dt.year
    df["mese"]         = df["data"].dt.month
    df["giorno_anno"]  = df["data"].dt.dayofyear

    # Filtra solo mesi stagione influenzale (ottobre-aprile)
    df_stagionale = df[df["stagione"].notna()].copy().reset_index(drop=True)

    print(f"  Righe totali dopo merge:          {len(df):,}")
    print(f"  Righe stagione influenzale (ott-apr): {len(df_stagionale):,}")
    print(f"  Stagioni coperte: {sorted(df_stagionale['stagione'].unique().tolist())}")

    # ------------------------------------------------------------------
    # 5. Report valori mancanti
    # ------------------------------------------------------------------
    print("\n[5/5] Report valori mancanti...")

    cols_analisi = [
        "temperatura_C", "umidita_assoluta_gm3", "umidita_relativa_pct",
        "NO2_ugm3", "PM10_ugm3", "PM25_ugm3",
    ]
    cols_presenti = [c for c in cols_analisi if c in df_stagionale.columns]

    print("\n  Valori mancanti (%) per ATS — importante per la sezione 'Limitazioni':")
    missingness = (
        df_stagionale
        .groupby("ATS")[cols_presenti]
        .apply(lambda g: g.isna().mean() * 100)
        .round(1)
    )
    print(missingness.to_string())

    print("\n  ⚠️  NOTE METODOLOGICHE:")
    print("  • I valori mancanti per inquinanti in ATS_Montagna sono attesi")
    print("    (poche stazioni ARPA in zona montana)")
    print("  • NON imputare i NA con la media: introduce bias nelle stime")
    print("  • I modelli GAM/GLM in R gestiscono i NA rimuovendo le righe")

    # ------------------------------------------------------------------
    # Ordine colonne nel file finale
    # ------------------------------------------------------------------
    col_order = [
        "data", "ATS",
        "temperatura_C", "umidita_assoluta_gm3", "umidita_relativa_pct",
        "NO2_ugm3", "PM10_ugm3", "PM25_ugm3",
        "stagione", "settimana_iso", "anno", "mese", "giorno_anno",
    ]
    # Mantieni solo le colonne che esistono effettivamente
    col_order = [c for c in col_order if c in df_stagionale.columns]
    df_stagionale = df_stagionale[col_order]

    # ------------------------------------------------------------------
    # Salva output
    # ------------------------------------------------------------------
    df_stagionale.to_csv(OUTPUT_FILE, index=False)

    print(f"\n{'=' * 60}")
    print(f"✓ Salvato: {OUTPUT_FILE}")
    print(f"  Righe  : {len(df_stagionale):,}")
    print(f"  Colonne: {df_stagionale.columns.tolist()}")

    # ------------------------------------------------------------------
    # Anteprima
    # ------------------------------------------------------------------
    print("\nPrime 10 righe:")
    print(df_stagionale.head(10).to_string(index=False))

    # ------------------------------------------------------------------
    # Statistiche descrittive per ATS
    # ------------------------------------------------------------------
    print("\n--- STATISTICHE DESCRITTIVE PER ATS ---")
    stats = (
        df_stagionale
        .groupby("ATS")[cols_presenti]
        .describe()
        .round(2)
    )
    print(stats.to_string())

    # ------------------------------------------------------------------
    # Copertura per stagione e ATS
    # ------------------------------------------------------------------
    print("\n--- COPERTURA GIORNI PER STAGIONE E ATS ---")
    copertura = (
        df_stagionale
        .groupby(["stagione", "ATS"])
        .agg(
            giorni_totali=("data", "count"),
            NO2_disponibile=("NO2_ugm3",   lambda x: x.notna().sum()),
            PM10_disponibile=("PM10_ugm3", lambda x: x.notna().sum()),
            PM25_disponibile=("PM25_ugm3", lambda x: x.notna().sum()),
            meteo_disponibile=("temperatura_C", lambda x: x.notna().sum()),
        )
    )
    print(copertura.to_string())

    print("\n✓ Script completato con successo.")


if __name__ == "__main__":
    main()

