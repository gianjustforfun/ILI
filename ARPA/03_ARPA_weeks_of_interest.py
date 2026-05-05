"""
=============================================================
SCRIPT 3 — MEDIA SETTIMANALE INQUINANTI (SETT 48-15)
=============================================================

COSA FA QUESTO SCRIPT:
    Questo script legge i file unificati ARPA già prodotti
    (TEMPERATURE_unificato.csv, HUMIDITY_unificato.csv, NO2_unificato.csv,
    PM10_unificato.csv, PM25_unificato.csv) e crea, dentro ARPA,
    una nuova struttura di cartelle contenente SOLO i periodi di interesse
    stagionale (da settimana 48 di un anno a settimana 15 dell'anno dopo).

STRUTTURA OUTPUT:
    ARPA/
        SETTIMANE_DI_INTERESSE/
            TEMPERATURE/
                TEMPERATURE_2022_2023.csv
                TEMPERATURE_2023_2024.csv
                TEMPERATURE_2024_2025.csv
                TEMPERATURE_2025_2026.csv
            HUMIDITY/
                ...
            NO2/
                ...
            PM10/
                ...
            PM25/
                ...
            GRAFICI
                TEMPERATURE_grafico.png
                HUMIDITY_grafico.png
                NO2_grafico.png
                PM10_grafico.png
                PM25_grafico.png

COSA CONTIENE OGNI FILE:
    Ogni file contiene le medie settimanali nel periodo:
        - settimana 48 dell'anno iniziale
        - settimana 15 dell'anno successivo
    Ordine righe: 48, 49, 50, 51, 52, 1, 2, ..., 15

PERIODI DI INTERESSE:
    2022-2023: 28/11/2022 - 16/04/2023
    2023-2024: 27/11/2023 - 14/04/2024
    2024-2025: 25/11/2024 - 13/04/2025
    2025-2026: 24/11/2025 - 12/04/2026

REQUISITI:
    pip install pandas
=============================================================
"""

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

# -------------------------------------------------------
# 1. CONFIGURAZIONE BASE
# -------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
CARTELLA_ARPA = BASE_DIR / "ARPA"
CARTELLA_OUTPUT = CARTELLA_ARPA / "SETTIMANE_DI_INTERESSE"

TIPOLOGIE = {
    "TEMPERATURE": CARTELLA_ARPA / "TEMPERATURE" / "TEMPERATURE_unificato.csv",
    "HUMIDITY"   : CARTELLA_ARPA / "HUMIDITY"    / "HUMIDITY_unificato.csv",
    "NO2"        : CARTELLA_ARPA / "NO2"         / "NO2_unificato.csv",
    "PM10"       : CARTELLA_ARPA / "PM10"        / "PM10_unificato.csv",
    "PM25"       : CARTELLA_ARPA / "PM25"        / "PM25_unificato.csv",
}

PERIODI = [
    {"label": "2022_2023", "inizio": "28/11/2022", "fine": "16/04/2023"},
    {"label": "2023_2024", "inizio": "27/11/2023", "fine": "14/04/2024"},
    {"label": "2024_2025", "inizio": "25/11/2024", "fine": "13/04/2025"},
    {"label": "2025_2026", "inizio": "24/11/2025", "fine": "12/04/2026"},
]

# Settimane della stagione influenzale in ordine cronologico
SETTIMANE_STAGIONE = [48, 49, 50, 51, 52] + list(range(1, 16))

N_DECIMALI = 2

CARTELLA_OUTPUT.mkdir(exist_ok=True)


# -------------------------------------------------------
# 2. FUNZIONI DI UTILITÀ
# -------------------------------------------------------

def pulisci_colonne(df):
    """Rimuove spazi bianchi dai nomi delle colonne."""
    df.columns = [str(c).strip() for c in df.columns]
    return df


def converti_data(df):
    """
    Converte la colonna 'Data/Ora' in datetime.
    Le righe con data non interpretabile vengono scartate.

    NOTA: NON usare dayfirst=True — i file ARPA usano il formato ISO
    YYYY-MM-DD (oppure YYYY-MM-DD HH:MM:SS per i dati orari come NO2).
    Con dayfirst=True, pandas interpreta erroneamente le date con giorno
    > 12 (es. 2022-01-13) come ambigue e le converte in NaT, eliminando
    circa il 60% dei dati. Lasciando dayfirst al default (False), il
    parser riconosce correttamente il formato ISO senza ambiguità.
    """
    if "Data/Ora" not in df.columns:
        print("      ⚠️  Colonna 'Data/Ora' non trovata nel file.")
        return pd.DataFrame()

    df["Data/Ora"] = pd.to_datetime(df["Data/Ora"], errors="coerce")
    n_nan = df["Data/Ora"].isna().sum()
    if n_nan > 0:
        print(f"      ⚠️  {n_nan} righe con data non valida rimosse.")
    return df.dropna(subset=["Data/Ora"]).copy()


def converti_valori(df):
    """
    Converte tutte le colonne dati (non 'Data/Ora') in numerico.
    I valori -999 (codice ARPA per dato mancante) diventano NaN.
    """
    cols_dati = [c for c in df.columns if c != "Data/Ora"]
    for c in cols_dati:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        df.loc[df[c] == -999, c] = pd.NA
    return df


def filtra_periodo(df, data_inizio, data_fine):
    """
    Filtra il dataframe tenendo solo le righe comprese
    nell'intervallo [data_inizio, data_fine] inclusi.
    """
    start = pd.to_datetime(data_inizio, dayfirst=True)
    end   = pd.to_datetime(data_fine,   dayfirst=True)
    return df[(df["Data/Ora"] >= start) & (df["Data/Ora"] <= end)].copy()


def ordine_settimana(s):
    """
    Mappa il numero di settimana ISO a una posizione progressiva
    per ordinare correttamente la stagione influenzale:

        Sett. 48 → pos.  1
        Sett. 52 → pos.  5
        Sett.  1 → pos.  6
        Sett. 15 → pos. 20
    """
    return s - 47 if s >= 48 else s + 5


def aggiungi_info_settimana(df):
    """
    Aggiunge le colonne Anno_ISO, Settimana e Ordine_settimana
    usando il calendario ISO.

    FIX: le righe con settimana 53 vengono rimosse esplicitamente.
    La settimana 53 esiste in alcuni anni ISO (es. 2020, 2026) e
    non fa parte della stagione di interesse (48-52, 1-15).
    Se non venisse rimossa, il successivo groupby la includerebbe,
    ma il reindex la scarterebbe, causando NaN nelle settimane
    adiacenti (52 o 1) perché parte dei loro dati sarebbe finita
    nella settimana 53.

    Nota: usiamo .values quando assegniamo le colonne da isocalendar()
    per evitare warning di pandas su index misalignment.
    """
    df = df.copy()
    iso = df["Data/Ora"].dt.isocalendar()

    df["Anno_ISO"]         = iso["year"].values
    df["Settimana"]        = iso["week"].astype("Int64").values
    df["Ordine_settimana"] = df["Settimana"].apply(ordine_settimana)

    # Rimuovi settimana 53: non appartiene alla stagione e rompe il reindex
    n_sett53 = (df["Settimana"] == 53).sum()
    if n_sett53 > 0:
        print(f"      ℹ️  Rimossi {n_sett53} giorni con settimana ISO 53.")
    df = df[df["Settimana"] != 53].copy()

    return df


def calcola_media_settimanale(df):
    """
    Calcola la media settimanale di tutte le colonne numeriche,
    raggruppando per numero di settimana ISO.

    Nota: a questo punto ogni settimana appartiene a un solo anno
    (grazie al filtro per data + rimozione sett. 53), quindi il
    groupby su solo 'Settimana' è sicuro.
    """
    if df.empty:
        return pd.DataFrame()

    cols_dati = [
        c for c in df.columns
        if c not in ["Data/Ora", "Anno_ISO", "Settimana", "Ordine_settimana"]
    ]

    if not cols_dati:
        return pd.DataFrame()

    df_week = (
        df.groupby("Settimana", as_index=False)[cols_dati]
        .mean()
    )
    return df_week


def ordina_settimane(df):
    """
    Ordina le righe nell'ordine cronologico della stagione influenzale:
        48, 49, 50, 51, 52, 1, 2, ..., 15
    """
    df = df.copy()
    df["Ordine_settimana"] = df["Settimana"].apply(ordine_settimana)
    df = df.sort_values("Ordine_settimana").reset_index(drop=True)
    df = df.drop(columns=["Ordine_settimana"])
    return df


def arrotonda_valori(df, n_decimali=2):
    """Arrotonda tutte le colonne numeriche tranne 'Settimana'."""
    cols_num = [c for c in df.columns if c != "Settimana"]
    if cols_num:
        df[cols_num] = df[cols_num].round(n_decimali)
    return df


def ordina_colonne_finali(df):
    """
    Riordina le colonne: Settimana prima, poi le altre in ordine alfabetico.
    """
    cols_altre = sorted([c for c in df.columns if c != "Settimana"])
    return df[["Settimana"] + cols_altre]


def stampa_riepilogo(df_week, label):
    """
    Stampa un breve riepilogo per diagnosticare eventuali NaN residui.
    Utile per verificare che il fix abbia risolto il problema.
    """
    n_nan = df_week.drop(columns=["Settimana"]).isna().any(axis=1).sum()
    if n_nan > 0:
        sett_nan = df_week[
            df_week.drop(columns=["Settimana"]).isna().any(axis=1)
        ]["Settimana"].tolist()
        print(f"      ⚠️  {n_nan} settimane con NaN: {sett_nan}")
    else:
        print(f"      ✅ Nessun NaN — tutte le 20 settimane complete.")


# -------------------------------------------------------
# 3. ELABORAZIONE PRINCIPALE
# -------------------------------------------------------
print("=" * 60)
print("SETTIMANE DI INTERESSE ARPA (48 -> 15)")
print("=" * 60)

for nome_tipologia, file_input in TIPOLOGIE.items():
    print(f"\n📁 Tipologia: {nome_tipologia}")

    if not file_input.exists():
        print(f"   ❌ File non trovato: {file_input}")
        continue

    # Crea cartella output per questa tipologia
    cartella_tipologia_out = CARTELLA_OUTPUT / nome_tipologia
    cartella_tipologia_out.mkdir(exist_ok=True)

    # Leggi e prepara il file unificato
    df = pd.read_csv(file_input)
    df = pulisci_colonne(df)
    df = converti_data(df)

    if df.empty:
        print(f"   ❌ Nessun dato valido in {file_input}")
        continue

    df = converti_valori(df)

    # Processa ogni stagione
    for periodo in PERIODI:
        label       = periodo["label"]
        data_inizio = periodo["inizio"]
        data_fine   = periodo["fine"]

        print(f"\n   📅 Periodo {label}: {data_inizio} → {data_fine}")

        # Filtra per date
        df_periodo = filtra_periodo(df, data_inizio, data_fine)

        if df_periodo.empty:
            print("      ⚠️  Nessun dato nel periodo: creo file con tutte le settimane vuote.")
            df_week = pd.DataFrame({"Settimana": SETTIMANE_STAGIONE})
        else:
            # Aggiungi info settimana (con rimozione sett. 53)
            df_periodo = aggiungi_info_settimana(df_periodo)

            # Calcola medie settimanali
            df_week = calcola_media_settimanale(df_periodo)

        # Ordina, arrotonda, riordina colonne
        df_week = ordina_settimane(df_week)
        df_week = arrotonda_valori(df_week, N_DECIMALI)
        df_week = ordina_colonne_finali(df_week)

        # Diagnostica NaN residui
        stampa_riepilogo(df_week, label)

        # Salva
        file_output = cartella_tipologia_out / f"{nome_tipologia}_{label}.csv"
        df_week.to_csv(file_output, index=False)
        print(f"      💾 Salvato: {file_output}")

print("\n" + "=" * 60)
print("✅ CREAZIONE FILE SETTIMANALI COMPLETATA")
print(f"📂 Risultati in: {CARTELLA_OUTPUT}")
print("=" * 60)

# =============================================================================
# 4. CREAZIONE GRAFICI
# =============================================================================
# Scopo:
#   Leggere i file CSV prodotti dalla pipeline di preprocessing ambientale
#   (una cartella per tipologia, un CSV per stagione) e produrre un grafico
#   per ciascuna tipologia con le stagioni sovrapposte sullo stesso asse X.
#
# Struttura attesa della cartella di output del preprocessing:
#   CARTELLA_OUTPUT/
#       TEMPERATURE/
#           TEMPERATURE_2022_2023.csv
#           TEMPERATURE_2023_2024.csv
#           ...
#       HUMIDITY/
#           HUMIDITY_2022_2023.csv
#           ...
#       PM25/
#           PM25_2022_2023.csv
#           ...
#       PM10/  NO2/  ...
#
# Struttura attesa dei CSV:
#   - TEMPERATURE e HUMIDITY: colonne "Settimana", "<cod_ATS>_max",
#     "<cod_ATS>_media", "<cod_ATS>_min"  (più colonne per ATS)
#   - PM2.5, PM10, NO2: colonne "Settimana", "<cod_stazione>", ...
#     (un solo valore numerico per stazione, nessun suffisso _max/_min)
#
# Nota metodologica (bias da tenere a mente nel report):
#   I dati sono aggregati a livello ATS, non individuale. Qualunque
#   correlazione tra variabili ambientali e ILI è un'associazione
#   ecologica: non è possibile inferire relazioni causali a livello
#   di singolo individuo. Con ~20 settimane per stagione il rischio
#   di spurious correlation è elevato — interpretare con cautela.
# =============================================================================

# Cartella radice che contiene le sottocartelle per tipologia
CARTELLA_OUTPUT = Path("SETTIMANE_DI_INTERESSE")

# Dizionario delle tipologie da elaborare.
# Chiave  = nome della sottocartella (case-sensitive, deve corrispondere
#            al nome effettivo della cartella sul disco)
# Valore  = etichetta leggibile usata nel titolo del grafico
TIPOLOGIE = {
    "TEMPERATURE": "Temperatura (°C)",
    "HUMIDITY":    "Umidità relativa (%)",
    "PM25":        "PM2.5 (µg/m³)",
    "PM10":        "PM10 (µg/m³)",
    "NO2":         "NO₂ (µg/m³)",
}

# Tipologie che hanno struttura max/media/min per sensore.
# Per queste si considera SOLO la colonna "_media" di ogni ATS,
# evitando di mescolare grandezze diverse (massimo, media, minimo)
# nello stesso calcolo.
# Per tutte le altre tipologie (PM, NO2) si usano tutte le colonne
# numeriche, perché la struttura è già piatta (un valore per stazione).
TIPOLOGIE_CON_SOTTOSTAT = {"TEMPERATURE", "HUMIDITY"}

# =============================================================================
# FUNZIONI DI SUPPORTO
# =============================================================================

def ordine_settimana(s: int) -> int:
    """
    Mappa il numero di settimana ISO a una posizione progressiva (1-based)
    che rispetta l'ordine della stagione influenzale:

        Settimana 48 → posizione  1
        Settimana 49 → posizione  2
        ...
        Settimana 52 → posizione  5
        Settimana  1 → posizione  6  ← inizio anno solare nuovo
        Settimana  2 → posizione  7
        ...
        Settimana 15 → posizione 20

    Perché serve?
        Matplotlib ordina l'asse X numericamente: senza questo mapping
        le settimane 1-15 verrebbero disegnate PRIMA delle 48-52,
        spezzando visivamente la curva stagionale.

    Parameters
    ----------
    s : int
        Numero di settimana ISO (1-52).

    Returns
    -------
    int
        Posizione progressiva nella stagione influenzale.
    """
    return s - 47 if s >= 48 else s + 5


def carica_e_unisci_file_tipologia(cartella_tipologia: Path) -> pd.DataFrame:
    """
    Legge tutti i file CSV presenti in una cartella di tipologia e
    restituisce un unico DataFrame con una colonna aggiuntiva "Periodo".

    Il "Periodo" viene estratto dal nome del file: il codice assume che
    il nome segua il pattern  <TIPOLOGIA>_<anno_inizio>_<anno_fine>.csv
    (es. TEMPERATURE_2022_2023.csv → Periodo = "2022_2023").
    Se il nome non ha almeno 3 parti separate da "_", viene usato il
    nome file completo come Periodo.

    Parameters
    ----------
    cartella_tipologia : Path
        Percorso della sottocartella della tipologia (es. output/TEMPERATURE).

    Returns
    -------
    pd.DataFrame
        DataFrame unificato con colonna "Periodo", oppure DataFrame vuoto
        se nessun file valido è stato trovato.
    """
    files = sorted(cartella_tipologia.glob("*.csv"))
    dfs = []

    for file in files:
        try:
            df = pd.read_csv(file)

            # Salta file vuoti o senza la colonna obbligatoria "Settimana"
            if df.empty or "Settimana" not in df.columns:
                print(f"   ⚠️ File ignorato (vuoto o senza colonna 'Settimana'): {file.name}")
                continue

            # Estrai il periodo dal nome file
            # Esempio: "TEMPERATURE_2022_2023" → parts = ["TEMPERATURE","2022","2023"]
            parts = file.stem.split("_")
            periodo = "_".join(parts[-2:]) if len(parts) >= 3 else file.stem

            df["Periodo"] = periodo
            dfs.append(df)

        except Exception as e:
            print(f"   ⚠️ Errore leggendo {file.name}: {e}")

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def settimane_stagione_ordinate() -> list:
    """
    Restituisce la lista completa delle settimane della stagione influenzale
    nell'ordine cronologico corretto (48–52, poi 1–15).

    Returns
    -------
    list of int
        [48, 49, 50, 51, 52, 1, 2, ..., 15]
    """
    settimane = [48, 49, 50, 51, 52] + list(range(1, 16))
    return sorted(settimane, key=ordine_settimana)


def prepara_media_plot(df: pd.DataFrame, nome_tipologia: str = "") -> pd.DataFrame:
    """
    A partire dal DataFrame grezzo (una tipologia, tutte le stagioni),
    calcola la "Media_complessiva" per ogni coppia (Periodo, Settimana)
    e restituisce un DataFrame pronto per il plot.

    Logica di selezione delle colonne
    ----------------------------------
    TEMPERATURE / HUMIDITY (in TIPOLOGIE_CON_SOTTOSTAT):
        I CSV hanno colonne del tipo "<cod_ATS>_max", "<cod_ATS>_media",
        "<cod_ATS>_min". Usare tutte le colonne significherebbe mediare
        grandezze diverse (un massimo giornaliero con una media
        giornaliera), producendo un numero privo di interpretazione
        statistica chiara.
        → Si selezionano SOLO le colonne che terminano in "_media",
          ottenendo la temperatura/umidità media regionale come media
          delle medie delle singole ATS.

    PM2.5 / PM10 / NO2 (tutte le altre):
        I CSV hanno già una sola colonna per stazione (struttura piatta).
        → Si usano TUTTE le colonne numeriche (comportamento originale).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame grezzo con colonne "Settimana", "Periodo" e colonne
        numeriche dei sensori/stazioni.
    nome_tipologia : str, optional
        Nome della tipologia (es. "TEMPERATURE"). Usato per decidere
        quale sottoinsieme di colonne considerare.
        Default "" → comportamento "tutte le colonne" (compatibilità).

    Returns
    -------
    pd.DataFrame
        Colonne: Settimana | Periodo | Media_complessiva | Ordine_settimana
        Ordinato per (Periodo, Ordine_settimana).
        DataFrame vuoto se i dati di input sono insufficienti.
    """
    if df.empty:
        return pd.DataFrame()

    df = df.copy()

    # Converti "Settimana" in intero, scarta righe non parsabili
    df["Settimana"] = pd.to_numeric(df["Settimana"], errors="coerce")
    df = df.dropna(subset=["Settimana"])
    df["Settimana"] = df["Settimana"].astype(int)

    # -------------------------------------------------------------------------
    # Selezione colonne numeriche in base alla tipologia
    # -------------------------------------------------------------------------
    if nome_tipologia.upper() in TIPOLOGIE_CON_SOTTOSTAT:
        # Solo le colonne che rappresentano la media del sensore/ATS
        cols_num = [
            c for c in df.columns
            if c not in ("Settimana", "Periodo") and c.endswith("_media")
        ]
        if not cols_num:
            print(
                f"   ⚠️ Nessuna colonna '_media' trovata per {nome_tipologia}. "
                f"Colonne disponibili: {list(df.columns)}"
            )
            return pd.DataFrame()
    else:
        # Struttura piatta: tutte le colonne non-metadato sono dati
        cols_num = [c for c in df.columns if c not in ("Settimana", "Periodo")]

    if not cols_num:
        return pd.DataFrame()

    # Media tra tutti i sensori/stazioni sulla stessa riga (asse=1 → per riga)
    df["Media_complessiva"] = df[cols_num].mean(axis=1)

    df_plot = df[["Settimana", "Periodo", "Media_complessiva"]].copy()

    # Aggrega eventuali duplicati (stessa settimana, stesso periodo)
    # che potrebbero emergere dalla concatenazione di file
    df_plot = (
        df_plot
        .groupby(["Periodo", "Settimana"], as_index=False)["Media_complessiva"]
        .mean()
    )

    # Aggiunge la posizione stagionale per l'ordinamento corretto sull'asse X
    df_plot["Ordine_settimana"] = df_plot["Settimana"].apply(ordine_settimana)
    df_plot = df_plot.sort_values(["Periodo", "Ordine_settimana"]).reset_index(drop=True)

    return df_plot


# =============================================================================
# CREAZIONE DEI GRAFICI
# =============================================================================

def crea_grafico_tipologia_sovrapposto(nome_tipologia: str, df: pd.DataFrame) -> None:
    """
    Crea e salva un grafico PNG con le stagioni sovrapposte per una tipologia.

    Asse X: settimane della stagione influenzale (48→52, 1→15) nell'ordine
            cronologico corretto, usando ordine_settimana() come proxy.
    Asse Y: valore medio della variabile ambientale (unità dipende dalla tipologia).
    Una linea per stagione (Periodo), con legenda.

    Il file viene salvato in CARTELLA_OUTPUT/grafici/<nome_tipologia>_grafico.png

    Parameters
    ----------
    nome_tipologia : str
        Nome della tipologia (corrisponde alla chiave in TIPOLOGIE).
    df : pd.DataFrame
        DataFrame grezzo restituito da carica_e_unisci_file_tipologia().
    """
    if df.empty:
        print(f"   ⚠️ Nessun dato disponibile per {nome_tipologia}, grafico saltato.")
        return

    # Calcola la media da plottare, con la logica corretta per tipologia
    df_plot = prepara_media_plot(df, nome_tipologia)

    if df_plot.empty:
        print(f"   ⚠️ Nessun dato plottabile per {nome_tipologia}, grafico saltato.")
        return

    # Asse X: lista ordinata di tutte le settimane della stagione
    settimane_asse_x = settimane_stagione_ordinate()

    # Lista dei periodi presenti nei dati, in ordine alfabetico
    periodi = sorted(df_plot["Periodo"].dropna().unique())

    # Etichetta asse Y: usa il nome leggibile da TIPOLOGIE, o il nome grezzo
    etichetta_y = TIPOLOGIE.get(nome_tipologia, nome_tipologia)

    fig, ax = plt.subplots(figsize=(12, 6))

    for periodo in periodi:
        df_p = df_plot[df_plot["Periodo"] == periodo].copy()

        # Reindex sulle settimane attese: se mancano dati per alcune settimane
        # si inserisce NaN → la linea avrà un "buco" invece di collegare
        # punti non contigui, rendendo visibili i dati mancanti
        df_p = (
            df_p
            .set_index("Settimana")
            .reindex(settimane_asse_x)
            .reset_index()
            .rename(columns={"index": "Settimana"})
        )

        # Ricalcola ordine dopo il reindex (i NaN non influenzano l'asse X)
        df_p["Ordine_settimana"] = df_p["Settimana"].apply(ordine_settimana)
        df_p = df_p.sort_values("Ordine_settimana")

        ax.plot(
            df_p["Ordine_settimana"],
            df_p["Media_complessiva"],
            marker="o",
            linewidth=2,
            markersize=4,
            label=periodo,
        )

    # Titolo e assi
    ax.set_title(
        f"{etichetta_y} — stagioni sovrapposte",
        fontsize=14,
        weight="bold",
    )
    ax.set_xlabel("Settimana (numero ISO)", fontsize=11)
    ax.set_ylabel(etichetta_y, fontsize=11)

    # Tick sull'asse X: posizioni ordine_settimana(s), etichette numero ISO
    ax.set_xticks([ordine_settimana(s) for s in settimane_asse_x])
    ax.set_xticklabels([str(s) for s in settimane_asse_x])

    ax.grid(True, alpha=0.3)
    ax.legend(title="Stagione")

    plt.tight_layout()

    # Salvataggio
    cartella_grafici = CARTELLA_OUTPUT / "grafici"
    cartella_grafici.mkdir(parents=True, exist_ok=True)

    file_grafico = cartella_grafici / f"{nome_tipologia}_grafico.png"
    fig.savefig(file_grafico, dpi=300, bbox_inches="tight")
    plt.close(fig)

    print(f"   📈 Grafico salvato: {file_grafico}")


# =============================================================================
# PUNTO DI INGRESSO PRINCIPALE
# =============================================================================

if __name__ == "__main__":

    print("\n" + "=" * 60)
    print("CREAZIONE GRAFICI FINALI (STAGIONI SOVRAPPOSTE)")
    print("=" * 60)

    for nome_tipologia in TIPOLOGIE.keys():

        cartella_tipologia_out = CARTELLA_OUTPUT / nome_tipologia

        # Verifica che la cartella esista prima di procedere
        if not cartella_tipologia_out.exists():
            print(f"\n⚠️  Cartella non trovata: {cartella_tipologia_out} — tipologia saltata.")
            continue

        print(f"\n📊 Elaborazione {nome_tipologia}...")

        # 1. Carica e unisce tutti i CSV della tipologia in un unico DataFrame
        df_tipologia = carica_e_unisci_file_tipologia(cartella_tipologia_out)

        # 2. Crea e salva il grafico con le stagioni sovrapposte
        crea_grafico_tipologia_sovrapposto(nome_tipologia, df_tipologia)

    print("\n✅ ELABORAZIONE COMPLETATA")
    print(f"📂 Grafici salvati in: {CARTELLA_OUTPUT / 'grafici'}")