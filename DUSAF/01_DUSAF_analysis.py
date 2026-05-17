"""
=============================================================
analisi_dusaf_ats.py  —  Analisi Uso del Suolo DUSAF per ATS
=============================================================

COLLOCAZIONE CONSIGLIATA: ILI/DUSAF/analisi_dusaf_ats.py
OUTPUT:                   ILI/DUSAF/dusaf_output/

=============================================================
DATI DI INPUT — COME SCARICARE IL DUSAF 7
=============================================================

Fonte: Geoportale di Regione Lombardia
Link diretto al pacchetto di download:
    https://www.geoportale.regione.lombardia.it/download-pacchetti?
    p_p_id=dwnpackageportlet_WAR_gptdownloadportlet&p_p_lifecycle=0
    &p_p_state=normal&p_p_mode=view&
    _dwnpackageportlet_WAR_gptdownloadportlet_metadataid=
    r_lombar%3A7cd05e9f-b693-4d7e-a8de-71b40b45f54e
    &_jsfBridgeRedirect=true

Procedura:
    1. Aprire il link nel browser (richiede registrazione gratuita
       al Geoportale di Regione Lombardia se non gia effettuata)
    2. Scaricare lo zip del DUSAF 7 (aggiornamento 2021)
    3. Estrarre il contenuto dello zip
    4. Copiare nella cartella ILI/DUSAF/data/ SOLO i file
       del layer principale, cioe quelli che iniziano con
       "DUSAF7." (es. DUSAF7.shp, DUSAF7.dbf, DUSAF7.shx,
       DUSAF7.prj, DUSAF7.cpg, DUSAF7.sbn, DUSAF7.sbx, ...)

    NON copiare i file "DUSAF7_FILARI.*"
    Il layer FILARI contiene elementi lineari (siepi,
    filari di alberi lungo i campi agricoli) che descrivono
    strutture del paesaggio agrario tradizionale.
    Hanno geometria di tipo LINEA, non poligono: non hanno
    superficie misurabile e non possono essere aggregati
    in macro-categorie di uso del suolo per area.
    Includerli causerebbe errori nel calcolo delle aree
    e non aggiunge informazione rilevante per l'analisi ILI.

Struttura attesa della cartella dopo la copia:
    ILI/DUSAF/
    ├── analisi_dusaf_ats.py        <- questo script
    ├── dusaf_output/               <- creata automaticamente
    └── data/
        ├── DUSAF7.shp              <- geometrie poligonali
        ├── DUSAF7.dbf              <- attributi (COD_TOT, ecc.)
        ├── DUSAF7.shx              <- indice geometrie
        ├── DUSAF7.prj              <- sistema di riferimento
        ├── DUSAF7.cpg              <- codifica caratteri
        ├── DUSAF7.sbn              <- indice spaziale (opzionale)
        └── DUSAF7.sbx              <- indice spaziale (opzionale)

=============================================================
DESCRIZIONE GENERALE
=============================================================

Questo script analizza il dataset DUSAF 7 (uso del suolo
Regione Lombardia, aggiornato al 2021) per quantificare e
confrontare la composizione territoriale di TUTTE LE 8 ATS
della Lombardia.

Analizzare tutte le ATS e non solo le tre selezionate
e metodologicamente piu robusto: permette di dimostrare che
ATS Brianza, ATS Bergamo e ATS Montagna rappresentano punti
significativi dello spettro lombardo in termini di presenza
industriale tra le ATS non metropolitane, e che la loro scelta
non e arbitraria ma giustificata da un confronto sistematico
su dati reali.

Risultati osservati (DUSAF 7, 2021):
    ATS Brianza:  7.36% di superficie industriale/commerciale
                  -> seconda piu industrializzata dopo Milano
    ATS Bergamo:  posizione intermedio-alta nel ranking
    ATS Montagna: 0.52% di superficie industriale/commerciale
                  -> meno industrializzata di tutte le ATS
    Rapporto Brianza/Montagna: ~14x

ATS e territori di competenza (fonte: Regione Lombardia):

    ATS Città Metropolitana di Milano → MI, LO
    ATS dell'Insubria                 → VA, CO (escluso Medio Alto Lario)
    ATS della Brianza                 → MB, LC
    ATS di Bergamo                    → BG
    ATS di Brescia                    → BS (esclusa Valle Camonica)
    ATS di Pavia                      → PV
    ATS della Val Padana              → CR, MN
    ATS della Montagna                → SO + Valle Camonica (BS) + Medio Alto Lario (CO)

Per le ATS che corrispondono ESATTAMENTE a una o più province
il filtro avviene per COD_PROV nello shapefile ISTAT.

Per le due ATS con territorio SUB-PROVINCIALE (Brescia e Montagna)
è necessario escludere/includere comuni specifici:
    - ATS Brescia:   prov BS esclusi i comuni della Valle Camonica
    - ATS Montagna:  prov SO + comuni Valle Camonica (BS) + comuni MAL (CO)

=============================================================
ATS EVIDENZIATE NEI GRAFICI
=============================================================

Tre ATS vengono evidenziate con colori distinti in tutti i
grafici, per motivare visivamente la loro selezione nel progetto:

    ATS Brianza  (label "Brianza") → BLU   (#2980b9)
        Seconda ATS più industrializzata di Lombardia (esclusa Milano).
        Rappresenta il polo ad alta pressione industriale tra le
        ATS non metropolitane selezionate.

    ATS Bergamo  (label "Bergamo") → ROSSO (#e74c3c)
        Posizione intermedio-alta nel ranking. Inclusa come
        confronto per distinguere effetti ILI tra aree a diversa
        vocazione manifatturiera.

    ATS Montagna (label "Montagna") → VERDE (#27ae60)
        ATS meno industrializzata di tutta la Lombardia.
        Funge da "controllo negativo" per l'esposizione a
        inquinanti di origine industriale.

=============================================================
COLONNA CODICE DUSAF
=============================================================

Si usa esclusivamente la colonna COD_TOT del DUSAF 7.
I codici sono interi senza punto (es. 1211, 3111, 2111).
La classificazione usa i primi N caratteri come prefisso.

Struttura COD_TOT:
    Prima cifra   → macro-categoria (1=artificiale, 2=agricola, ...)
    Prime 3 cifre → sottocategoria (es. 121 = industriale/comm.)

=============================================================
BIAS E LIMITAZIONI DA DICHIARARE NEL REPORT
=============================================================

1. DISALLINEAMENTO TEMPORALE:
   DUSAF 7 è del 2021. Possibili cambiamenti marginali
   nell'uso del suolo nelle stagioni ILI successive.

2. USO DEL SUOLO ≠ EMISSIONI EFFETTIVE:
   La presenza di aree industriali è una PROXY dell'esposizione
   a inquinanti, non una misura diretta di PM2.5/NO2.
   Integrare sempre con i dati ARPA di qualità dell'aria.

3. BIAS ECOLOGICO:
   Il confronto è a scala di ATS (aggregazione territoriale).
   Non permette inferenze sull'esposizione individuale.

4. LISTE COMUNI SUB-PROVINCIALI:
   Le liste COMUNI_VALCAMONICA e COMUNI_MEDIO_ALTO_LARIO
   sono derivate dalla definizione istituzionale delle ATS
   (DGR Regione Lombardia). Verificare corrispondenza con
   i comuni nel dataset ARPA.

=============================================================
REQUISITI
=============================================================

    pip install geopandas pandas matplotlib

=============================================================
"""

import os
import sys
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import geopandas as gpd

warnings.filterwarnings("ignore")


# ═══════════════════════════════════════════════════════════
# SEZIONE 1 — CONFIGURAZIONE
# ═══════════════════════════════════════════════════════════

DUSAF_PATH            = "data/DUSAF7.shp"
SHAPEFILE_COMUNI_PATH = "../COPERNICUS/data/raw/Com01012026_g_WGS84.shp"
OUTPUT_DIR            = "dusaf_output"

# Codici ISTAT di provincia
COD_MI = 15
COD_LO = 98
COD_VA = 12
COD_CO = 13
COD_MB = 108
COD_LC = 97
COD_BG = 16
COD_BS = 17
COD_PV = 18
COD_CR = 19
COD_MN = 20
COD_SO = 14

# ── Colori e label per le tre ATS selezionate ───────────────
# Centralizzati qui per garantire coerenza in tutti i grafici.
# Modificare solo questo dizionario per cambiare colori o
# aggiungere/rimuovere ATS evidenziate.
ATS_EVIDENZIATE = {
    "Brianza":  {"colore": "#2980b9", "nome_display": "ATS Brianza"},
    "Bergamo":  {"colore": "#e74c3c", "nome_display": "ATS Bergamo"},
    "Montagna": {"colore": "#27ae60", "nome_display": "ATS Montagna"},
}

# Colori di sfondo (versione semi-trasparente) per i grafici a barre.
# Derivati dai colori principali con alpha applicato a livello matplotlib.
ATS_SFONDO = {
    "Brianza":  "#ddeef8",   # blu chiaro
    "Bergamo":  "#ffe0e0",   # rosso chiaro
    "Montagna": "#e8f8e8",   # verde chiaro
}


# ═══════════════════════════════════════════════════════════
# SEZIONE 2 — DEFINIZIONE ATS E LISTE COMUNI SUB-PROVINCIALI
# ═══════════════════════════════════════════════════════════

# Comuni Valle Camonica (prov BS) → appartengono ad ATS Montagna
COMUNI_VALCAMONICA = [
    "Angolo Terme", "Artogne", "Berzo Demo", "Berzo Inferiore",
    "Bienno", "Borno", "Braone", "Breno", "Capo Di Ponte",
    "Cedegolo", "Cerveno", "Ceto", "Cevo", "Cimbergo",
    "Corteno Golgi", "Darfo Boario Terme", "Edolo", "Esine",
    "Gianico", "Incudine", "Lozio", "Malegno", "Malonno",
    "Monno", "Niardo", "Ono San Pietro", "Ossimo",
    "Paisco Loveno", "Paspardo", "Piancogno", "Pisogne",
    "Ponte Di Legno", "Prestine", "Saviore Dell'Adamello",
    "Sellero", "Sonico", "Temù", "Vezza D'Oglio",
    "Villa D'Allegno", "Vione",
]

# Comuni Medio Alto Lario (prov CO) → appartengono ad ATS Montagna
COMUNI_MEDIO_ALTO_LARIO = [
    "Gravedona Ed Uniti", "Gera Lario", "Dongo", "Sorico",
    "San Siro", "Peglio", "Stazzona", "Livo",
    "Pianello Del Lario", "Cremia", "Musso", "Vercana",
    "Domaso", "Colico", "Bellano", "Varenna", "Perledo",
    "Esino Lario", "Premana", "Casargo", "Taceno", "Margno",
    "Introzzo", "Tremenico", "Valvarrone",
]

# ── Definizione delle 8 ATS ──────────────────────────────────
#
# Ogni ATS è descritta da un dict con:
#   label:        nome breve usato nei file di output
#   nome_lungo:   nome istituzionale completo
#   prov_intere:  lista COD_PROV che appartengono interamente all'ATS
#   prov_includi: COD_PROV da cui includere SOLO i comuni in lista_includi
#   lista_includi: lista nomi comuni da includere (subset sub-provinciale)
#   prov_escludi: COD_PROV da cui escludere i comuni in lista_escludi
#   lista_escludi: lista nomi comuni da escludere (subset sub-provinciale)
#
# LOGICA in ottieni_confine_ats():
#   confine = tutti i comuni di prov_intere
#           + comuni di prov_includi che sono in lista_includi
#           + tutti i comuni di prov_escludi TRANNE quelli in lista_escludi

ATS_DEFINIZIONI = [
    {
        "label":        "Milano",
        "nome_lungo":   "ATS Città Metropolitana di Milano",
        "prov_intere":  [COD_MI, COD_LO],
        "prov_includi": None,
        "lista_includi": None,
        "prov_escludi": None,
        "lista_escludi": None,
    },
    {
        "label":        "Insubria",
        "nome_lungo":   "ATS dell'Insubria",
        # VA intera + CO esclusi i comuni del Medio Alto Lario
        "prov_intere":  [COD_VA],
        "prov_includi": None,
        "lista_includi": None,
        "prov_escludi": COD_CO,
        "lista_escludi": COMUNI_MEDIO_ALTO_LARIO,
    },
    {
        "label":        "Brianza",
        "nome_lungo":   "ATS della Brianza",
        "prov_intere":  [COD_MB, COD_LC],
        "prov_includi": None,
        "lista_includi": None,
        "prov_escludi": None,
        "lista_escludi": None,
    },
    {
        "label":        "Bergamo",
        "nome_lungo":   "ATS di Bergamo",
        "prov_intere":  [COD_BG],
        "prov_includi": None,
        "lista_includi": None,
        "prov_escludi": None,
        "lista_escludi": None,
    },
    {
        "label":        "Brescia",
        "nome_lungo":   "ATS di Brescia",
        # BS intera esclusi i comuni della Valle Camonica
        "prov_intere":  [],
        "prov_includi": None,
        "lista_includi": None,
        "prov_escludi": COD_BS,
        "lista_escludi": COMUNI_VALCAMONICA,
    },
    {
        "label":        "Pavia",
        "nome_lungo":   "ATS di Pavia",
        "prov_intere":  [COD_PV],
        "prov_includi": None,
        "lista_includi": None,
        "prov_escludi": None,
        "lista_escludi": None,
    },
    {
        "label":        "ValPadana",
        "nome_lungo":   "ATS della Val Padana",
        "prov_intere":  [COD_CR, COD_MN],
        "prov_includi": None,
        "lista_includi": None,
        "prov_escludi": None,
        "lista_escludi": None,
    },
    {
        "label":        "Montagna",
        "nome_lungo":   "ATS della Montagna",
        # SO intera + comuni Valle Camonica (BS) + comuni MAL (CO)
        "prov_intere":  [COD_SO],
        "prov_includi": [COD_BS, COD_CO],
        "lista_includi": COMUNI_VALCAMONICA + COMUNI_MEDIO_ALTO_LARIO,
        "prov_escludi": None,
        "lista_escludi": None,
    },
]


# ═══════════════════════════════════════════════════════════
# SEZIONE 3 — CLASSIFICAZIONE DUSAF
# ═══════════════════════════════════════════════════════════

# Classificazione basata su COD_TOT (intero, senza punto).
# Prefisso = primi N caratteri della stringa del codice.
# Ordine: dal più specifico al più generico (primo match vince).

CLASSIFICAZIONE_DUSAF = [
    ("121", "Industriale / Commerciale"),
    ("122", "Zone estrattive e cantieri"),
    ("111", "Residenziale"),
    ("112", "Residenziale"),
    ("114", "Verde urbano"),
    ("1",   "Altre superfici artificiali"),
    ("2",   "Agricola"),
    ("3",   "Boschi e zone naturali"),
    ("4",   "Zone umide"),
    ("5",   "Corpi idrici"),
]

COLORI_CATEGORIE = {
    "Industriale / Commerciale":    "#e74c3c",
    "Zone estrattive e cantieri":   "#c0392b",
    "Residenziale":                 "#3498db",
    "Verde urbano":                 "#1abc9c",
    "Altre superfici artificiali":  "#95a5a6",
    "Agricola":                     "#f39c12",
    "Boschi e zone naturali":       "#27ae60",
    "Zone umide":                   "#2980b9",
    "Corpi idrici":                 "#5dade2",
    "Altro / Non classificato":     "#ecf0f1",
}

ORDINE_CATEGORIE = list(COLORI_CATEGORIE.keys())


# ═══════════════════════════════════════════════════════════
# SEZIONE 4 — FUNZIONI DI SUPPORTO
# ═══════════════════════════════════════════════════════════

def _normalizza_nome_comune(nome: str) -> str:
    """
    Normalizza un nome comune per il join tra dataset.
    Coerente con 02_Seasons.py — non modificare in isolamento.
    """
    return (
        str(nome).strip().lower()
        .replace("'", " ").replace("`", " ")
        .replace("-", " ").replace("  ", " ").strip()
    )


def classifica_uso_suolo(codice: str) -> str:
    """Mappa COD_TOT nella macro-categoria (primo prefisso che matcha)."""
    codice_str = str(codice).strip()
    for prefisso, categoria in CLASSIFICAZIONE_DUSAF:
        if codice_str.startswith(prefisso):
            return categoria
    return "Altro / Non classificato"


def carica_dusaf(dusaf_path: str) -> gpd.GeoDataFrame:
    """
    Carica DUSAF 7 usando la colonna COD_TOT come codice.
    Aggiunge la colonna 'codice_dusaf' (stringa normalizzata).
    Stampa un campione dei valori COD_TOT per verifica.
    """
    if not os.path.exists(dusaf_path):
        raise FileNotFoundError(
            f"File DUSAF non trovato: {dusaf_path}\n"
            f"Aggiornare DUSAF_PATH nella SEZIONE 1."
        )

    print(f"  Caricamento DUSAF: {dusaf_path}")
    gdf = gpd.read_file(dusaf_path)
    print(f"  ✓ {len(gdf):,} poligoni | CRS: {gdf.crs}")

    if "COD_TOT" not in gdf.columns:
        raise ValueError(
            f"Colonna 'COD_TOT' non trovata nel DUSAF.\n"
            f"Colonne disponibili: {gdf.columns.tolist()}"
        )

    gdf["codice_dusaf"] = gdf["COD_TOT"].astype(str).str.strip()

    campione = sorted(gdf["codice_dusaf"].unique())[:15]
    print(f"  Valori COD_TOT (primi 15 per verifica): {campione}")

    return gdf


def carica_comuni(shapefile_path: str) -> gpd.GeoDataFrame:
    """
    Carica shapefile comuni ISTAT.
    Aggiunge colonne normalizzate '_nome_norm' e '_cod_prov'
    usate dai join nei metodi di costruzione dei confini ATS.
    """
    if not os.path.exists(shapefile_path):
        raise FileNotFoundError(
            f"Shapefile comuni non trovato: {shapefile_path}\n"
            f"Aggiornare SHAPEFILE_COMUNI_PATH nella SEZIONE 1."
        )

    gdf = gpd.read_file(shapefile_path)
    print(f"  ✓ {len(gdf):,} comuni | CRS: {gdf.crs}")

    # Identifica colonna nome comune
    col_comune = None
    for candidato in ["COMUNE", "DENOMINAZIONE", "DEN_COM", "NOME"]:
        if candidato in gdf.columns:
            col_comune = candidato
            break
    if col_comune is None:
        raise ValueError(
            f"Colonna nome comune non trovata.\n"
            f"Colonne: {gdf.columns.tolist()}"
        )

    # Identifica colonna codice provincia
    col_prov = None
    for candidato in ["COD_PROV", "COD_PRO", "PRO_COM"]:
        if candidato in gdf.columns:
            col_prov = candidato
            break
    if col_prov is None:
        raise ValueError(
            f"Colonna codice provincia non trovata.\n"
            f"Colonne: {gdf.columns.tolist()}"
        )

    gdf["_nome_norm"] = gdf[col_comune].apply(_normalizza_nome_comune)
    gdf["_cod_prov"]  = gdf[col_prov].astype(str).str.strip().astype(int)

    return gdf


def _trova_indici_per_nome(gdf_comuni: gpd.GeoDataFrame,
                           cod_prov: int,
                           lista_nomi: list,
                           modalita: str) -> list:
    """
    Trova gli indici dei comuni di una data provincia
    i cui nomi corrispondono alla lista fornita.

    Usa prima match esatto su nome normalizzato, poi
    match parziale come fallback (stesso approccio di 02_Seasons.py).

    Args:
        gdf_comuni: shapefile comuni con '_nome_norm' e '_cod_prov'
        cod_prov:   codice provincia su cui cercare
        lista_nomi: lista nomi comuni da trovare
        modalita:   'includi' o 'escludi' (solo per log)

    Returns:
        lista di indici del GeoDataFrame trovati
    """
    gdf_prov = gdf_comuni[gdf_comuni["_cod_prov"] == cod_prov].copy()
    lookup   = dict(zip(gdf_prov["_nome_norm"], gdf_prov.index))

    indici_trovati = []
    non_trovati    = []

    for nome in lista_nomi:
        norm = _normalizza_nome_comune(nome)
        idx  = lookup.get(norm)
        if idx is not None:
            indici_trovati.append(idx)
        else:
            # Match parziale come fallback
            candidati = [k for k in lookup if norm in k or k in norm]
            if len(candidati) == 1:
                indici_trovati.append(lookup[candidati[0]])
            else:
                non_trovati.append(nome)

    if non_trovati:
        print(f"    ⚠ [{modalita}] {len(non_trovati)} comuni non trovati "
              f"in prov {cod_prov}:")
        for c in non_trovati[:5]:
            print(f"        '{c}'")
        if len(non_trovati) > 5:
            print(f"        ... e altri {len(non_trovati)-5}")

    return indici_trovati


def ottieni_confine_ats(gdf_comuni: gpd.GeoDataFrame,
                        defn: dict) -> gpd.GeoDataFrame:
    """
    Costruisce il confine geografico di un'ATS.

    Logica:
      1. Tutti i comuni delle province intere (prov_intere)
      2. Subset da includere da altre province (prov_includi + lista_includi)
      3. Tutti i comuni di una provincia tranne quelli in lista_escludi
         (prov_escludi + lista_escludi)

    Dissolve il tutto in un unico poligono.
    """
    indici = []

    # ── Province intere ───────────────────────────────────────
    for cod in (defn["prov_intere"] or []):
        mask     = gdf_comuni["_cod_prov"] == cod
        trovati  = gdf_comuni[mask].index.tolist()
        print(f"    Prov {cod}: {len(trovati)} comuni (intera)")
        indici.extend(trovati)

    # ── Subset da includere ───────────────────────────────────
    if defn["prov_includi"] and defn["lista_includi"]:
        for cod in defn["prov_includi"]:
            idx = _trova_indici_per_nome(
                gdf_comuni, cod, defn["lista_includi"], "includi"
            )
            print(f"    Prov {cod}: {len(idx)} comuni inclusi (subset)")
            indici.extend(idx)

    # ── Province con esclusioni ───────────────────────────────
    if defn["prov_escludi"] is not None:
        cod          = defn["prov_escludi"]
        gdf_prov     = gdf_comuni[gdf_comuni["_cod_prov"] == cod]
        idx_escludi  = set(_trova_indici_per_nome(
            gdf_comuni, cod, defn["lista_escludi"], "escludi"
        ))
        idx_tenuti   = [i for i in gdf_prov.index if i not in idx_escludi]
        print(f"    Prov {cod}: {len(gdf_prov)} totali — "
              f"{len(idx_escludi)} esclusi — {len(idx_tenuti)} tenuti")
        indici.extend(idx_tenuti)

    # Rimuovi duplicati mantenendo l'ordine
    indici = list(dict.fromkeys(indici))

    gdf_ats  = gdf_comuni.loc[indici].copy()
    confine  = gdf_ats.dissolve().reset_index(drop=True)
    print(f"  → {defn['label']}: {len(gdf_ats)} comuni totali")
    return confine


def clip_e_classifica(gdf_dusaf: gpd.GeoDataFrame,
                      confine_ats: gpd.GeoDataFrame,
                      label: str) -> gpd.GeoDataFrame:
    """
    Ritaglia il DUSAF sul confine dell'ATS, classifica e
    calcola le aree in km².
    Allinea il CRS del confine a quello del DUSAF prima del clip.
    """
    if confine_ats.crs != gdf_dusaf.crs:
        confine_ats = confine_ats.to_crs(gdf_dusaf.crs)

    gdf_clip              = gpd.clip(gdf_dusaf, confine_ats).copy()
    gdf_clip["macro_cat"] = gdf_clip["codice_dusaf"].apply(classifica_uso_suolo)
    gdf_clip["area_km2"]  = gdf_clip.geometry.area / 1_000_000

    print(f"  ✓ {label}: {len(gdf_clip):,} poligoni | "
          f"area totale: {gdf_clip['area_km2'].sum():.1f} km²")
    return gdf_clip


def calcola_statistiche(gdf_clip: gpd.GeoDataFrame,
                        label: str,
                        nome_lungo: str) -> pd.DataFrame:
    """
    Aggrega area per macro-categoria e calcola percentuali.
    Aggiunge righe con area=0 per le categorie assenti
    (necessario per grafici uniformi tra ATS).

    Returns:
        DataFrame con colonne: ats, nome_lungo, macro_cat, area_km2, percentuale
    """
    area_tot = gdf_clip["area_km2"].sum()

    df = (
        gdf_clip.groupby("macro_cat")["area_km2"]
        .sum().reset_index()
    )
    df["percentuale"] = df["area_km2"] / area_tot * 100
    df["ats"]         = label
    df["nome_lungo"]  = nome_lungo

    # Aggiungi categorie mancanti con 0 per uniformità tra ATS
    for cat in ORDINE_CATEGORIE:
        if cat not in df["macro_cat"].values:
            df = pd.concat([df, pd.DataFrame([{
                "macro_cat": cat, "area_km2": 0.0,
                "percentuale": 0.0, "ats": label, "nome_lungo": nome_lungo
            }])], ignore_index=True)

    df = df.sort_values("area_km2", ascending=False).reset_index(drop=True)
    return df[["ats", "nome_lungo", "macro_cat", "area_km2", "percentuale"]]


def salva_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False, float_format="%.4f")
    print(f"  ✓ {os.path.basename(path)} ({os.path.getsize(path)/1024:.1f} KB)")


def _costruisci_titolo_legenda_grafici() -> str:
    """
    Genera la stringa del sottotitolo per i grafici,
    elencando le ATS evidenziate e il loro colore.
    Costruita dinamicamente da ATS_EVIDENZIATE per evitare
    disallineamenti tra codice e testo se la lista cambia.
    """
    parti = [
        f"Sfondo {info['colore']} = {info['nome_display']}"
        for label, info in ATS_EVIDENZIATE.items()
    ]
    return "  |  ".join(parti)


# ═══════════════════════════════════════════════════════════
# SEZIONE 5 — VISUALIZZAZIONI
# ═══════════════════════════════════════════════════════════

def grafico_barre_tutte_ats(df_all: pd.DataFrame, output_dir: str):
    """
    Grafico a barre raggruppate: asse X = ATS, barre colorate
    per macro-categoria. ATS ordinate per quota industriale
    decrescente.

    Le tre ATS selezionate (Brianza, Bergamo, Montagna) vengono
    evidenziate con uno sfondo colorato dietro le barre,
    usando i colori definiti in ATS_SFONDO.
    """
    df_pivot = df_all.pivot_table(
        index="ats", columns="macro_cat",
        values="percentuale", aggfunc="sum"
    ).fillna(0)

    cols_presenti = [c for c in ORDINE_CATEGORIE if c in df_pivot.columns]
    df_pivot      = df_pivot[cols_presenti]

    if "Industriale / Commerciale" in df_pivot.columns:
        df_pivot = df_pivot.sort_values(
            "Industriale / Commerciale", ascending=False
        )

    ats_labels = df_pivot.index.tolist()
    n_cat      = len(cols_presenti)
    x          = np.arange(len(ats_labels))
    width      = 0.8 / n_cat

    fig, ax = plt.subplots(figsize=(15, 7))

    for i, cat in enumerate(cols_presenti):
        colore = COLORI_CATEGORIE.get(cat, "#bdc3c7")
        offset = (i - n_cat / 2 + 0.5) * width
        ax.bar(
            x + offset, df_pivot[cat].values, width,
            label=cat, color=colore,
            edgecolor="white", linewidth=0.4,
        )

    # Sfondo colorato per le tre ATS evidenziate.
    # Usa ATS_SFONDO per mantenere i colori centralizzati.
    for idx, ats in enumerate(ats_labels):
        if ats in ATS_SFONDO:
            ax.axvspan(idx - 0.45, idx + 0.45,
                       color=ATS_SFONDO[ats], zorder=0, alpha=0.5)

    ax.set_xticks(x)
    ax.set_xticklabels(ats_labels, rotation=20, ha="right", fontsize=9)
    ax.set_ylabel("% sul territorio ATS", fontsize=10)
    ax.set_title(
        "Uso del Suolo DUSAF 7 (2021) — Tutte le ATS della Lombardia\n"
        "Sfondo blu = ATS Brianza  |  Sfondo rosso = ATS Bergamo  |  Sfondo verde = ATS Montagna",
        fontsize=11, fontweight="bold"
    )
    ax.legend(loc="upper right", fontsize=7.5, ncol=2,
              framealpha=0.9, title="Macro-categoria")
    ax.yaxis.grid(True, linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)

    plt.tight_layout()
    path = os.path.join(output_dir, "dusaf_tutte_ats.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  ✓ {os.path.basename(path)}")


def grafico_industriale_ranking(df_all: pd.DataFrame, output_dir: str):
    """
    Grafico a barre orizzontali ordinato per quota industriale.
    È il grafico più diretto per comunicare il ranking delle ATS
    e giustificare la scelta delle tre ATS nel progetto.

    Le barre vengono colorate usando i colori di ATS_EVIDENZIATE:
      - Brianza  → blu   (alta industrializzazione non-metropolitana)
      - Bergamo  → rosso (posizione intermedio-alta)
      - Montagna → verde (minima industrializzazione)
    Le altre ATS usano il grigio neutro (#95a5a6).
    """
    df_ind = (
        df_all[df_all["macro_cat"] == "Industriale / Commerciale"]
        [["ats", "percentuale"]]
        .sort_values("percentuale", ascending=True)
        .reset_index(drop=True)
    )

    if df_ind.empty:
        print("  ⚠ Nessun dato per Industriale/Commerciale — "
              "verificare che COD_TOT contenga codici che iniziano con '121'.")
        return

    # Colore della barra: usa ATS_EVIDENZIATE se l'ATS è selezionata,
    # altrimenti grigio neutro per le ATS non evidenziate.
    colori_bar = [
        ATS_EVIDENZIATE[ats]["colore"] if ats in ATS_EVIDENZIATE else "#95a5a6"
        for ats in df_ind["ats"]
    ]

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(
        df_ind["ats"], df_ind["percentuale"],
        color=colori_bar, edgecolor="white", height=0.6
    )

    for bar, pct in zip(bars, df_ind["percentuale"]):
        ax.text(
            bar.get_width() + 0.15,
            bar.get_y() + bar.get_height() / 2,
            f"{pct:.1f}%",
            va="center", ha="left", fontsize=9
        )

    ax.set_xlabel("% territorio classificato come Industriale / Commerciale",
                  fontsize=10)
    ax.set_title(
        "Ranking ATS Lombarde per Quota Industriale — DUSAF 7 (2021)\n"
        "Blu = ATS Brianza  |  Rosso = ATS Bergamo  |  Verde = ATS Montagna",
        fontsize=10, fontweight="bold"
    )
    ax.xaxis.grid(True, linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)
    ax.set_xlim(0, df_ind["percentuale"].max() * 1.25)

    plt.tight_layout()
    path = os.path.join(output_dir, "dusaf_ranking_industriale.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  ✓ {os.path.basename(path)}")


def grafico_mappa_lombardia(risultati: dict, output_dir: str):
    """
    Mappa 2×4: una cella per ATS, poligoni DUSAF colorati
    per macro-categoria. Utile come figura geospaziale nel report.

    Le tre ATS evidenziate (Brianza, Bergamo, Montagna) ricevono
    un bordo colorato intorno alla propria cella, usando i colori
    definiti in ATS_EVIDENZIATE.
    """
    labels_ordine = [d["label"] for d in ATS_DEFINIZIONI]
    fig, axes     = plt.subplots(2, 4, figsize=(20, 10))
    fig.suptitle(
        "Uso del Suolo DUSAF 7 (2021) — ATS della Lombardia",
        fontsize=13, fontweight="bold"
    )

    for ax, label in zip(axes.flatten(), labels_ordine):
        if label not in risultati:
            ax.set_visible(False)
            continue

        gdf_clip   = risultati[label]["gdf"]
        nome_lungo = risultati[label]["nome_lungo"]

        for cat, colore in COLORI_CATEGORIE.items():
            subset = gdf_clip[gdf_clip["macro_cat"] == cat]
            if not subset.empty:
                subset.plot(ax=ax, color=colore, linewidth=0)

        titolo = nome_lungo.replace("ATS ", "").replace("dell'", "").replace("della ", "")
        ax.set_title(titolo, fontsize=7.5, fontweight="bold")
        ax.set_axis_off()

        # Bordo colorato per le tre ATS selezionate.
        # Le altre ATS non hanno bordo aggiuntivo.
        if label in ATS_EVIDENZIATE:
            colore_bordo = ATS_EVIDENZIATE[label]["colore"]
            for spine in ax.spines.values():
                spine.set_edgecolor(colore_bordo)
                spine.set_linewidth(2)
                spine.set_visible(True)

    # Legenda globale delle macro-categorie DUSAF
    patches = [
        mpatches.Patch(color=colore, label=cat)
        for cat, colore in COLORI_CATEGORIE.items()
        if cat != "Altro / Non classificato"
    ]
    fig.legend(
        handles=patches, loc="lower center", ncol=5,
        fontsize=8, bbox_to_anchor=(0.5, -0.03), framealpha=0.9
    )

    plt.tight_layout()
    path = os.path.join(output_dir, "dusaf_mappa_lombardia.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  ✓ {os.path.basename(path)}")


# ═══════════════════════════════════════════════════════════
# SEZIONE 6 — MAIN
# ═══════════════════════════════════════════════════════════

def main():
    print("\n" + "=" * 65)
    print("analisi_dusaf_ats.py — DUSAF per tutte le ATS Lombardia")
    print("=" * 65)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── STEP 1: Caricamento dati ─────────────────────────────
    print("\n" + "─" * 65)
    print("STEP 1: Caricamento DUSAF 7 e shapefile comuni")
    print("─" * 65)

    gdf_dusaf  = carica_dusaf(DUSAF_PATH)
    gdf_comuni = carica_comuni(SHAPEFILE_COMUNI_PATH)

    # ── STEP 2-4: Loop su tutte le ATS ───────────────────────
    print("\n" + "─" * 65)
    print("STEP 2-4: Confini → Clip → Statistiche per ogni ATS")
    print("─" * 65)

    risultati = {}
    all_stats = []

    for defn in ATS_DEFINIZIONI:
        label      = defn["label"]
        nome_lungo = defn["nome_lungo"]
        print(f"\n  ── {nome_lungo} ──")

        confine  = ottieni_confine_ats(gdf_comuni, defn)
        gdf_clip = clip_e_classifica(gdf_dusaf, confine, label)
        df_stats = calcola_statistiche(gdf_clip, label, nome_lungo)

        all_stats.append(df_stats)
        risultati[label] = {"gdf": gdf_clip, "stats": df_stats,
                             "nome_lungo": nome_lungo}

    # ── STEP 5: Riepilogo ────────────────────────────────────
    print("\n" + "─" * 65)
    print("STEP 5: Ranking per quota industriale")
    print("─" * 65)

    df_all     = pd.concat(all_stats, ignore_index=True)
    df_ranking = (
        df_all[df_all["macro_cat"] == "Industriale / Commerciale"]
        [["ats", "nome_lungo", "area_km2", "percentuale"]]
        .sort_values("percentuale", ascending=False)
        .reset_index(drop=True)
    )
    df_ranking.index += 1

    print(df_ranking.to_string(float_format=lambda x: f"{x:.2f}"))

    # Stampa il dato chiave per tutte e tre le ATS evidenziate
    print(f"\n  ★ DATI CHIAVE PER IL REPORT (ATS evidenziate):")
    for label in ATS_EVIDENZIATE:
        try:
            riga = df_ranking[df_ranking["ats"] == label].iloc[0]
            rank = df_ranking[df_ranking["ats"] == label].index[0]
            print(f"    {ATS_EVIDENZIATE[label]['nome_display']:25s}  "
                  f"{riga['percentuale']:.1f}% industriale → #{rank} su 8 ATS")
        except (IndexError, KeyError):
            print(f"    ⚠ '{label}' non trovata — verificare i label in ATS_DEFINIZIONI.")

    # Rapporto Brianza / Montagna (distanza estremi)
    try:
        pct_brianza  = df_ranking[df_ranking["ats"] == "Brianza"].iloc[0]["percentuale"]
        pct_montagna = df_ranking[df_ranking["ats"] == "Montagna"].iloc[0]["percentuale"]
        print(f"\n    Rapporto Brianza/Montagna: "
              f"{pct_brianza / max(pct_montagna, 0.01):.1f}x "
              f"più industriale Brianza vs Montagna")
    except (IndexError, KeyError):
        pass

    # ── STEP 6: CSV ──────────────────────────────────────────
    print("\n" + "─" * 65)
    print("STEP 6: Salvataggio CSV")
    print("─" * 65)

    salva_csv(df_all,     os.path.join(OUTPUT_DIR, "dusaf_tutte_ats.csv"))
    salva_csv(df_ranking, os.path.join(OUTPUT_DIR, "dusaf_ranking_industriale.csv"))

    for defn in ATS_DEFINIZIONI:
        label = defn["label"]
        if label in risultati:
            salva_csv(
                risultati[label]["stats"],
                os.path.join(OUTPUT_DIR, f"dusaf_stats_{label.lower()}.csv")
            )

    # ── STEP 7: Grafici ──────────────────────────────────────
    print("\n" + "─" * 65)
    print("STEP 7: Grafici")
    print("─" * 65)

    grafico_barre_tutte_ats(df_all, OUTPUT_DIR)
    grafico_industriale_ranking(df_all, OUTPUT_DIR)
    grafico_mappa_lombardia(risultati, OUTPUT_DIR)

    # ── Riepilogo finale ─────────────────────────────────────
    print("\n" + "=" * 65)
    print("✅ COMPLETATO")
    print("=" * 65)
    print(f"\nOutput in: {os.path.abspath(OUTPUT_DIR)}/")
    for f in sorted(os.listdir(OUTPUT_DIR)):
        fp = os.path.join(OUTPUT_DIR, f)
        print(f"  └─ {f}  ({os.path.getsize(fp)/1024:.1f} KB)")

    print("""
─────────────────────────────────────────────────────────────
GRAFICI E LORO USO NEL REPORT
─────────────────────────────────────────────────────────────

dusaf_ranking_industriale.png  ← FIGURA PRINCIPALE
    Ranking delle 8 ATS per quota industriale. Le tre ATS
    selezionate sono colorate:
      Blu   = ATS Brianza  (alta industrializzazione non-metro)
      Rosso = ATS Bergamo  (posizione intermedio-alta)
      Verde = ATS Montagna (minima industrializzazione)
    Dimostra che la scelta delle ATS è motivata da dati reali.

dusaf_tutte_ats.png
    Confronto completo di tutte le macro-categorie per ATS.
    Sfondo colorato sulle tre ATS selezionate.

dusaf_mappa_lombardia.png
    Figura geospaziale: uso del suolo su mappa per ogni ATS.
    Bordo colorato sulle tre ATS selezionate.

CSV chiave: dusaf_ranking_industriale.csv

BIAS DA DICHIARARE:
  1. DUSAF 2021 — possibile disallineamento con stagioni >2021
  2. Uso suolo ≠ emissioni effettive (proxy, non misura diretta)
  3. Bias ecologico — scala ATS, non individuale
  4. Liste comuni sub-provinciali da verificare con DGR RL
─────────────────────────────────────────────────────────────
""")


if __name__ == "__main__":
    main()