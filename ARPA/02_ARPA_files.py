"""
=============================================================
SCRIPT 2 — UNIONE DEI DATI AMBIENTALI ARPA
=============================================================

COSA FA QUESTO SCRIPT:
    Questo script legge automaticamente tutti i file CSV presenti
    nelle cartelle ambientali ARPA del progetto e costruisce,
    per ciascuna tipologia, un unico file finale in formato WIDE
    (cioè in parallelo per data/ora).

    Produce in totale 5 file finali:
        - TEMPERATURE_unificato.csv
        - HUMIDITY_unificato.csv
        - NO2_unificato.csv
        - PM10_unificato.csv
        - PM25_unificato.csv

OBIETTIVO:
    Per ogni data/ora, avere sulla stessa riga i valori delle varie
    stazioni o dei vari sensori, invece di avere i dati messi uno
    sotto l'altro in formato lungo.

LOGICA USATA PER OGNI TIPOLOGIA:

    1. TEMPERATURE
       I file contengono tipicamente colonne del tipo:
           Id Sensore | Data-Ora | Valore ...
       In questo caso, ogni sensore viene trasformato in una colonna
       distinta. Il file finale avrà quindi:
           Data/Ora | 2001 | 5897 | 5909 | ...

    2. HUMIDITY
       Anche i file di umidità sono organizzati per Id Sensore e Data-Ora,
       ma contengono TRE misure diverse per ogni giorno:
           - Valore Medio Giornaliero
           - Minimo Valore Medio Orario
           - Massimo Valore Medio Orario

       Quindi, per HUMIDITY, NON si salva un solo valore.
       Si salvano tutte e tre le misure, creando per ogni sensore
       tre colonne distinte, ad esempio:
           2002_media | 2002_min | 2002_max

       Il file finale avrà quindi una struttura del tipo:
           Data/Ora | 2002_media | 2002_min | 2002_max | 6174_media | ...

    3. NO2, PM10, PM25
       Per gli inquinanti, ogni file CSV rappresenta generalmente una
       stazione diversa. In questo caso:
           - si legge il nome della stazione dal file
           - si estraggono Data/Ora e valore misurato
           - il valore viene rinominato con il nome della stazione
           - tutti i file vengono uniti in parallelo sulla Data/Ora

       ATTENZIONE:
       I nomi delle stazioni non sono uguali tra NO2, PM10 e PM25.
       Quindi ciascuno dei tre file finali avrà un proprio insieme
       specifico di colonne.

PERCHÉ QUESTO SCRIPT È NECESSARIO:
    I dati ARPA sono distribuiti in molti file separati per anno,
    sensore o stazione. Per fare analisi temporali, correlazioni con
    i dati ILI, confronti tra sensori o calcoli aggregati, è molto più
    utile avere un solo file pulito per ogni tipologia, con i dati già
    disposti in parallelo.

COSA RISOLVE:
    - evita l'unione verticale "uno sotto l'altro"
    - crea file finali pronti per analisi e merge successivi
    - mantiene tutti i sensori/stazioni sulla stessa riga temporale
    - per HUMIDITY conserva tutte e tre le misure giornaliere
    - per gli inquinanti usa il nome reale della stazione
    - per TEMPERATURE e HUMIDITY usa il numero del sensore

OUTPUT FINALI:
    - ARPA/TEMPERATURE/TEMPERATURE_unificato.csv
    - ARPA/HUMIDITY/HUMIDITY_unificato.csv
    - ARPA/NO2/NO2_unificato.csv
    - ARPA/PM10/PM10_unificato.csv
    - ARPA/PM25/PM25_unificato.csv

NOTE IMPORTANTI:
    - vengono letti solo i file .csv
    - i file Legenda.txt vengono ignorati automaticamente
    - i valori -999 vengono convertiti in NaN
    - il merge è di tipo outer, quindi nessuna data viene persa
    - se una stazione inquinante compare in più file, i valori vengono
      fusi nella stessa colonna

REQUISITI:
    pip install pandas
=============================================================
"""

from pathlib import Path
import pandas as pd

# -------------------------------------------------------
# 1. CONFIGURAZIONE BASE
# -------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
CARTELLA_ARPA = BASE_DIR / "ARPA"

TIPOLOGIE = {
    "TEMPERATURE": CARTELLA_ARPA / "TEMPERATURE",
    "HUMIDITY"   : CARTELLA_ARPA / "HUMIDITY",
    "NO2"        : CARTELLA_ARPA / "NO2",
    "PM10"       : CARTELLA_ARPA / "PM10",
    "PM25"       : CARTELLA_ARPA / "PM25",
}

# -------------------------------------------------------
# 2. FUNZIONI DI UTILITÀ
# -------------------------------------------------------
def trova_csv(cartella):
    """Ritorna lista di tutti i file CSV dentro cartella (e sottocartelle)."""
    return sorted(f for f in cartella.rglob("*.csv") if f.is_file())

def pulisci_col(df):
    """Pulisce i nomi delle colonne e converte la data."""
    df.columns = [str(c).strip() for c in df.columns]
    for c in df.columns:
        if "Data" in c or "Ora" in c:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

def valori_validi(df, col):
    """Converte -999 a NaN e converte a numerico."""
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df.loc[df[col] == -999, col] = pd.NA
    return df

# -------------------------------------------------------
# 3. HUMIDITY: 3 misure per sensore
# -------------------------------------------------------
def unisci_temp_hum(cartella):
    dfs = []
    for file in trova_csv(cartella):
        df = pd.read_csv(file)
        df = pulisci_col(df)

        col_sens = next((c for c in df.columns if "Sensore" in c), None)
        col_data = next((c for c in df.columns if "Data" in c), None)
        col_med  = next((c for c in df.columns if "Valore Medio Giornaliero" in c), None)
        col_min  = next((c for c in df.columns if "Minimo Valore Medio Orario" in c), None)
        col_max  = next((c for c in df.columns if "Massimo Valore Medio Orario" in c), None)

        if not all([col_sens, col_data, col_med, col_min, col_max]):
            continue

        df = df[[col_sens, col_data, col_med, col_min, col_max]].copy()
        df.columns = ["Id Sensore", "Data/Ora", "media", "min", "max"]

        df["Id Sensore"] = df["Id Sensore"].astype(str).str.strip()
        df = valori_validi(df, "media")
        df = valori_validi(df, "min")
        df = valori_validi(df, "max")
        df = df.dropna(subset=["Data/Ora"])

        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    df_tot = pd.concat(dfs, ignore_index=True).drop_duplicates(
        subset=["Id Sensore", "Data/Ora"], keep="first"
    )

    df_wide = (
        df_tot
        .pivot(index="Data/Ora", columns="Id Sensore", values=["media", "min", "max"])
        .pipe(lambda x: x.set_axis([f"{s}_{m}" for m, s in x.columns], axis=1))
        .reset_index()
        .sort_values("Data/Ora")
    )

    cols = df_wide.columns.tolist()
    data_col = "Data/Ora"
    other_cols = [c for c in cols if c != data_col]

    # Ordina: sensore (numero), poi media/min/max
    def key_ord(c):
        s, m = c.rsplit("_", 1)
        s_num = int(s) if s.isdigit() else s
        ord_m = {"media": 1, "min": 2, "max": 3}.get(m, 99)
        return (s_num, ord_m)

    other_cols.sort(key=key_ord)

    return df_wide[[data_col] + other_cols]

# -------------------------------------------------------
# 4. INQUINANTI (NO2/PM10/PM25): unione per stazione
# -------------------------------------------------------
def leggi_inquinante(file):
    """Legge un CSV inquinante e restituisce: Data/Ora | nome_stazione."""
    df = pd.read_csv(file, header=None)

    if df.shape[0] < 3 or df.shape[1] < 2:
        return None, None

    stazione = str(df.iloc[0, 1]).strip()

    idx = None
    for i in range(min(10, len(df))):
        v = str(df.iloc[i, 0]).strip()
        if "Data/" in v or "Data-Ora" in v:
            idx = i
            break

    if idx is None:
        return None, None

    cols = df.iloc[idx].tolist()
    df = df.iloc[idx+1:].copy()
    df.columns = cols

    df = df.iloc[:, :2].copy()
    df.columns = ["Data/Ora", stazione]

    df["Data/Ora"] = pd.to_datetime(df["Data/Ora"], errors="coerce")
    df = valori_validi(df, stazione)
    df = df.dropna(subset=["Data/Ora"]).drop_duplicates(subset=["Data/Ora"]).sort_values("Data/Ora")

    return df, stazione

def unisci_inquinanti(cartella, nome_tipologia):
    stazioni = {}

    for file in trova_csv(cartella):
        df_staz, staz = leggi_inquinante(file)

        if df_staz is None or staz is None:
            continue

        if staz not in stazioni:
            stazioni[staz] = df_staz
        else:
            # Unisci con outer join e combine_first
            df_merge = pd.merge(
                stazioni[staz], df_staz, on="Data/Ora", how="outer",
                suffixes=("_old", "_new")
            )
            col_old = f"{staz}_old"
            col_new = f"{staz}_new"
            df_merge[staz] = df_merge[col_old].combine_first(df_merge[col_new])
            stazioni[staz] = df_merge[["Data/Ora", staz]]

    if not stazioni:
        return pd.DataFrame()

    df_finale = None
    for df in stazioni.values():
        if df_finale is None:
            df_finale = df
        else:
            df_finale = pd.merge(df_finale, df, on="Data/Ora", how="outer")

    df_finale = df_finale.sort_values("Data/Ora").reset_index(drop=True)

    cols = df_finale.columns.tolist()
    return df_finale[["Data/Ora"] + sorted(cols[1:])]

# -------------------------------------------------------
# 5. LANCIO DELLO SCRIPT
# -------------------------------------------------------
print("=" * 60)
print("UNIONE DEI DATI AMBIENTALI ARPA")
print("=" * 60)

for nome, cartella in TIPOLOGIE.items():

    if nome == "TEMPERATURE":
        df = unisci_temp_hum(cartella)
    elif nome == "HUMIDITY":
        df = unisci_temp_hum(cartella)
    else:
        df = unisci_inquinanti(cartella, nome)

    if df is not None and not df.empty:
        out_file = cartella / f"{nome}_unificato.csv"
        df.to_csv(out_file, index=False)
        print(f"Salvato: {out_file}")
    else:
        print(f"NESSUN DATO PRODOTTO PER {nome}")

print("\n✅ CREAZIONE DEI FILE UNIFICATI COMPLETATA")