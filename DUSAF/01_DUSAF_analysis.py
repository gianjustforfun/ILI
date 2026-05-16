"""
=============================================================
analisi_dusaf_ats.py  —  Analisi Uso del Suolo DUSAF per ATS
=============================================================

COLLOCAZIONE CONSIGLIATA: ILI/DUSAF/analisi_dusaf_ats.py
OUTPUT:                   ILI/DUSAF/dusaf_output/

=============================================================
DESCRIZIONE GENERALE
=============================================================

Questo script analizza il dataset DUSAF 7 (uso del suolo
Regione Lombardia, aggiornato al 2021) per quantificare e
confrontare la composizione territoriale delle due ATS
selezionate per il progetto ILI:

    - ATS Bergamo  (provincia di Bergamo, COD_PROV = 16)
    - ATS Montagna (subset di SO + porzioni BS + porzioni CO)

Lo scopo è GIUSTIFICARE LA SCELTA DI QUESTE DUE ATS come
contesti territoriali contrastanti:

    ATS Bergamo  → territorio con alta presenza industriale
                   (pianura padana, val Seriana, val Brembana),
                   maggiore esposizione antropica a PM2.5/NO2.

    ATS Montagna → territorio prevalentemente montano/boschivo
                   (Valtellina, Valchiavenna, Alpi lombarde),
                   minore pressione industriale antropica.

Questo contrasto è rilevante per la Research Question del
progetto: l'esposizione differenziale agli inquinanti
atmosferici può spiegare differenze nell'incidenza di ILI
tra le due ATS (cfr. Feng et al. 2016; Merl et al. 2021).

=============================================================
STRUTTURA DELL'ANALISI
=============================================================

STEP 1: Caricamento DUSAF 7 e shapefile ATS
STEP 2: Ritaglio (clip) del DUSAF per ciascuna ATS
        - ATS Bergamo  → clip sul confine provincia BG
        - ATS Montagna → clip sull'unione dei comuni etichettati
                         come ATS_Montagna (lista hardcoded da
                         dataset ARPA, coerente con 02_Seasons.py)
STEP 3: Classificazione macro-categorie DUSAF
STEP 4: Calcolo aree (km²) e percentuali
STEP 5: Output CSV + grafici (torta + barre comparativo)

=============================================================
DUSAF — NOTE SULLA STRUTTURA DEL DATASET
=============================================================

Il DUSAF 7 (2021) usa una classificazione gerarchica
ispirata a CORINE Land Cover (CLC) ma più dettagliata.

Le colonne rilevanti sono:
    - CLASSE_1:  macro-categoria (1 cifra)
    - CLASSE_2:  categoria (3 caratteri, es. '1.2')
    - USO_SUOLO: codice esteso (es. '1.2.1')
    - DESCRIZION (o simile): descrizione testuale

Macro-categorie CLASSE_1:
    1 → Superfici artificiali
    2 → Superfici agricole
    3 → Zone boscate e ambienti semi-naturali
    4 → Zone umide
    5 → Corpi idrici

Sottocategorie rilevanti per il progetto (CLASSE_2):
    1.1 → Tessuto urbano (residenziale)
    1.2 → Zone industriali, commerciali, infrastrutturali
    1.3 → Zone estrattive, cantieri, discariche
    1.4 → Zone verdi artificiali (parchi urbani)
    2.x → Agricoltura (seminativi, vigneti, frutteti, prati)
    3.x → Boschi, arbusteti, zone aperte con veg. rada
    4.x → Zone umide (paludi, torbiere)
    5.x → Corpi idrici (fiumi, laghi, bacini)

=============================================================
NOTA SULLA DEFINIZIONE DI ATS MONTAGNA
=============================================================

ATS Montagna non corrisponde esattamente a nessuna provincia.
Comprende:
    - TUTTA la provincia di Sondrio (SO)
    - SUBSET della provincia di Brescia (BS): comuni montani
      della Valcamonica e valli limitrofe
    - SUBSET della provincia di Como (CO): comuni della
      val Chiavenna e alta Valsassina

In questo script usiamo due strategie alternative (configurabili):

    STRATEGIA A (default, CONSIGLIATA):
        Clip sul confine della provincia di Sondrio (COD_PROV=14)
        come PROXY dell'ATS Montagna. È un'approssimazione
        conservativa (esclude i comuni BS e CO), ma è robusta
        e non richiede la lista esplicita dei comuni.
        BIAS: leggermente sotto-stima l'area di ATS Montagna.

    STRATEGIA B (opzionale, più precisa):
        Clip sull'unione dei poligoni comunali della lista
        COMUNI_ATS_MONTAGNA hardcoded (derivata dal dataset ARPA).
        Richiede che lo shapefile dei comuni sia disponibile
        al percorso SHAPEFILE_COMUNI_PATH (identico a 02_Seasons.py).

    Configurare con: MONTAGNA_STRATEGIA = "A" oppure "B"

=============================================================
BIAS E LIMITAZIONI DA DICHIARARE NEL REPORT
=============================================================

1. DISALLINEAMENTO TEMPORALE:
   DUSAF 7 è del 2021. Se la vostra analisi ILI copre
   stagioni 2021-2026, l'uso del suolo può essere cambiato
   marginalmente. Menzionatelo come limitazione.

2. USO DEL SUOLO ≠ EMISSIONI EFFETTIVE:
   La presenza di aree industriali è una PROXY dell'esposizione
   a inquinanti, non una misura diretta. La concentrazione
   effettiva di PM2.5/NO2 dipende anche da meteorologia,
   tecnologia industriale, traffico. Integrate con dati ARPA.

3. BIAS ECOLOGICO:
   Il confronto è a scala di ATS (aggregazione territoriale).
   Non permette inferenze sull'esposizione individuale.

4. CLIP APPROSSIMATO PER ATS MONTAGNA (solo strategia A):
   Usare la provincia di Sondrio come proxy esclude i comuni
   BS e CO dell'ATS Montagna. Il bias tende a AUMENTARE
   la quota di territorio naturale (i comuni BS/CO aggiuntivi
   sono comunque prevalentemente montani/boschivi).

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
from pathlib import Path

warnings.filterwarnings("ignore")


# ═══════════════════════════════════════════════════════════
# SEZIONE 1 — CONFIGURAZIONE
# ═══════════════════════════════════════════════════════════

# ── Percorsi ─────────────────────────────────────────────────

# Shapefile DUSAF 7 (tutta la Lombardia) — MODIFICATE con il vostro path
DUSAF_PATH = "data/DUSAF7.shp"

# Shapefile comuni ISTAT — stesso usato in 02_Seasons.py
SHAPEFILE_COMUNI_PATH = "../COPERNICUS/data/raw/Com01012026_g_WGS84.shp"

# Cartella output
OUTPUT_DIR = "dusaf_output"

# ── Strategia per ATS Montagna ────────────────────────────────
# "A" → usa provincia Sondrio come proxy (robusta, consigliata)
# "B" → usa lista comuni da dataset ARPA (più precisa)
MONTAGNA_STRATEGIA = "B"

# ── Codici ISTAT di provincia ─────────────────────────────────
# Coerenti con ATS_PROV_CODES in 02_Seasons.py
COD_PROV_BERGAMO  = 16   # BG
COD_PROV_SONDRIO  = 14   # SO (proxy ATS Montagna - strategia A)
COD_PROV_BRESCIA  = 17   # BS (per strategia B, subset)
COD_PROV_COMO     = 13   # CO (per strategia B, subset)

# ── Lista comuni ATS Montagna (strategia B) ───────────────────
# Include: tutta SO + comuni montani BS (Valcamonica) + comuni CO (Valchiavenna).
# ATTENZIONE: aggiornare se il dataset ARPA cambia.
# Fonte: colonna 'comune' del dataset ARPA filtrata per ats == 'ATS_Montagna'
COMUNI_ATS_MONTAGNA = [
    # Provincia di Sondrio (SO) — tutti
    "Albosaggia", "Albaredo Per San Marco", "Andalo Valtellino",
    "Aprica", "Ardenno", "Bema", "Berbenno Di Valtellina",
    "Bianzone", "Bormio", "Buglio In Monte", "Campodolcino",
    "Caspoggio", "Castello Dell'Acqua", "Castione Andevenno",
    "Cedrasco", "Cercino", "Chiavenna", "Chiesa In Valmalenco",
    "Chiuro", "Cino", "Civo", "Colorina", "Cosio Valtellino",
    "Dazio", "Delebio", "Dubino", "Faedo Valtellino",
    "Forcola", "Fusine", "Gordona", "Grosio", "Grosotto",
    "Lanzada", "Livigno", "Lovero", "Madesimo", "Mantello",
    "Mazzo Di Valtellina", "Mello", "Menarola", "Mese",
    "Montagna In Valtellina", "Morbegno", "Mornico Losana",
    "Novate Mezzola", "Piantedo", "Piuro", "Poggiridenti",
    "Ponte In Valtellina", "Postalesio", "Prata Camportaccio",
    "Rasura", "Rogolo", "Samolaco", "San Giacomo Filippo",
    "Sernio", "Sondalo", "Sondrio", "Spriana", "Talamona",
    "Tartano", "Teglio", "Tirano", "Torre Di Santa Maria",
    "Tovo Di Sant'Agata", "Traona", "Tresivio", "Val Masino",
    "Valdidentro", "Valdisotto", "Valfurva", "Verceia",
    "Vervio", "Villa Di Chiavenna", "Villa Di Tirano",
    # Provincia di Brescia (BS) — comuni Valcamonica/ATS Montagna
    "Angolo Terme", "Artogne", "Berzo Demo", "Berzo Inferiore",
    "Bienno", "Borno", "Braone", "Breno", "Capo Di Ponte",
    "Cedegolo", "Cerveno", "Ceto", "Cevo", "Cimbergo",
    "Cividade Di Malegno", "Coccaglio", "Corteno Golgi",
    "Darfo Boario Terme", "Edolo", "Esine", "Forno Allione",
    "Gianico", "Incudine", "Lozio", "Malegno", "Malonno",
    "Monno", "Niardo", "Ono San Pietro", "Ossimo", "Paisco Loveno",
    "Paspardo", "Piancogno", "Pisogne", "Ponte Di Legno",
    "Prestine", "Saviore Dell'Adamello", "Sellero", "Sonico",
    "Temù", "Vezza D'Oglio", "Villa D'Allegno", "Vione",
    # Provincia di Como (CO) — comuni val Chiavenna / alta Valsassina
    "Gravedona Ed Uniti", "Gera Lario", "Dongo", "Sorico",
    "San Siro", "Peglio", "Stazzona", "Livo", "Pianello Del Lario",
    "Cremia", "Musso", "Vercana", "Domaso", "Colico",
    "Bellano", "Varenna", "Perledo", "Esino Lario",
    "Premana", "Casargo", "Taceno", "Margno", "Introzzo",
    "Tremenico", "Valvarrone",
]


# ═══════════════════════════════════════════════════════════
# SEZIONE 2 — CLASSIFICAZIONE DUSAF
# ═══════════════════════════════════════════════════════════

# Mappa codice DUSAF → macro-categoria interpretabile.
# Le chiavi sono prefissi della colonna USO_SUOLO (o CLASSE_2).
# L'ordine CONTA: il primo match vince (dal più specifico al più generico).
CLASSIFICAZIONE_DUSAF = [
    # ── Superfici artificiali ──────────────────────────────
    ("121", "Industriale / Commerciale / Infrastrutture"),  # era "1.2" → ora "12x"
    ("122", "Zone estrattive e cantieri"),
    ("111", "Residenziale"),
    ("112", "Residenziale"),                                # tessuto urbano discontinuo
    ("114", "Verde urbano"),
    ("12",  "Altre superfici artificiali"),
    ("1",   "Altre superfici artificiali"),
    # ── Agricoltura ────────────────────────────────────────
    ("2",   "Agricola"),
    # ── Naturale ───────────────────────────────────────────
    ("3",   "Boschi e zone naturali"),
    # ── Zone umide e corpi idrici ──────────────────────────
    ("4",   "Zone umide"),
    ("5",   "Corpi idrici"),
]

# Colori per i grafici (coerenti con l'ordine CLASSIFICAZIONE_DUSAF)
COLORI_CATEGORIE = {
    "Industriale / Commerciale / Infrastrutture": "#e74c3c",  # rosso — la più importante
    "Zone estrattive e cantieri":                 "#c0392b",  # rosso scuro
    "Residenziale":                               "#3498db",  # blu
    "Verde urbano":                               "#1abc9c",  # verde acqua
    "Altre superfici artificiali":                "#95a5a6",  # grigio
    "Agricola":                                   "#f39c12",  # arancione
    "Boschi e zone naturali":                     "#27ae60",  # verde
    "Zone umide":                                 "#2980b9",  # blu scuro
    "Corpi idrici":                               "#5dade2",  # azzurro
}


# ═══════════════════════════════════════════════════════════
# SEZIONE 3 — FUNZIONI DI SUPPORTO
# ═══════════════════════════════════════════════════════════

def _normalizza_nome_comune(nome: str) -> str:
    """
    Normalizza un nome comune per il join tra dataset.
    Stessa funzione usata in 02_Seasons.py — NON modificare
    senza aggiornare anche quella.
    """
    return (
        str(nome)
        .strip()
        .lower()
        .replace("'", " ")
        .replace("`", " ")
        .replace("-", " ")
        .replace("  ", " ")
        .strip()
    )


def classifica_uso_suolo(codice: str) -> str:
    """
    Mappa un codice DUSAF nella macro-categoria corrispondente.

    Usa il prefisso del codice e scorre CLASSIFICAZIONE_DUSAF
    in ordine (primo match vince → dal più specifico al più generico).

    Args:
        codice: stringa codice DUSAF (es. '1.2.1', '3.1.1.1')

    Returns:
        stringa macro-categoria
    """
    codice_str = str(codice).strip()
    for prefisso, categoria in CLASSIFICAZIONE_DUSAF:
        if codice_str.startswith(prefisso):
            return categoria
    return "Altro / Non classificato"


def carica_dusaf(dusaf_path: str) -> gpd.GeoDataFrame:
    """
    Carica il DUSAF 7 e identifica la colonna codice.

    Il DUSAF 7 può avere nomi di colonna leggermente diversi
    tra release. Questa funzione tenta i nomi più comuni e
    standardizza in una colonna 'codice_dusaf'.

    Returns:
        GeoDataFrame con colonna 'codice_dusaf' aggiunta.
    """
    print(f"  Caricamento DUSAF da: {dusaf_path}")

    if not os.path.exists(dusaf_path):
        raise FileNotFoundError(
            f"File DUSAF non trovato: {dusaf_path}\n"
            f"Aggiornare DUSAF_PATH nella SEZIONE 1 — CONFIGURAZIONE."
        )

    gdf = gpd.read_file(dusaf_path)
    print(f"  ✓ DUSAF caricato: {len(gdf):,} poligoni")
    print(f"  CRS: {gdf.crs}")
    print(f"  Colonne disponibili: {gdf.columns.tolist()}")

    # ── Identifica colonna codice ────────────────────────────
    # Candidati in ordine di preferenza (dal più dettagliato al meno)
    candidati_codice = ["COD_TOT", "USO_SUOLO", "CLASSE_2", "CLASSE", "CODICE", "COD_USO", "CLC_CODE"]
    colonna_trovata = None
    for candidato in candidati_codice:
        if candidato in gdf.columns:
            colonna_trovata = candidato
            break

    if colonna_trovata is None:
        print("\n  ⚠ ATTENZIONE: nessuna colonna codice trovata tra i candidati noti.")
        print(f"  Colonne disponibili: {gdf.columns.tolist()}")
        print("  Aggiornare la lista 'candidati_codice' in carica_dusaf().")
        raise ValueError("Colonna codice DUSAF non trovata. Vedere messaggio sopra.")

    print(f"  ✓ Colonna codice identificata: '{colonna_trovata}'")
    gdf["codice_dusaf"] = gdf[colonna_trovata].astype(str).str.strip()

    # Mostra i valori unici per verifica
    valori_unici = sorted(gdf["codice_dusaf"].unique())
    print(f"  Valori unici codice ({len(valori_unici)} totali, primi 15):")
    print(f"    {valori_unici[:15]}")
    if len(valori_unici) > 15:
        print(f"    ... e altri {len(valori_unici) - 15}")

    return gdf


def ottieni_confine_bergamo(gdf_comuni: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Estrae il confine dell'ATS Bergamo dallo shapefile comuni.

    ATS Bergamo = intera provincia di Bergamo (COD_PROV = 16).
    Dissolve tutti i comuni BG in un unico poligono.

    Returns:
        GeoDataFrame con un solo poligono (confine ATS Bergamo).
    """
    col_prov = None
    for candidato in ["COD_PROV", "COD_PRO", "PRO_COM", "COD_PROV_NUM"]:
        if candidato in gdf_comuni.columns:
            col_prov = candidato
            break

    if col_prov is None:
        raise ValueError(
            f"Colonna provincia non trovata nel shapefile comuni.\n"
            f"Colonne: {gdf_comuni.columns.tolist()}"
        )

    # Normalizza a intero per confronto robusto
    gdf_bg = gdf_comuni[
        gdf_comuni[col_prov].astype(str).str.strip()
        == str(COD_PROV_BERGAMO)
    ].copy()

    if len(gdf_bg) == 0:
        raise ValueError(
            f"Nessun comune trovato con {col_prov} = {COD_PROV_BERGAMO}.\n"
            f"Valori unici nella colonna: {gdf_comuni[col_prov].unique()[:10]}"
        )

    print(f"  ATS Bergamo: {len(gdf_bg)} comuni (provincia BG)")
    confine = gdf_bg.dissolve().reset_index(drop=True)
    return confine


def ottieni_confine_montagna_A(gdf_comuni: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Strategia A: usa provincia Sondrio come proxy ATS Montagna.

    Dissolve tutti i comuni SO (COD_PROV = 14) in un unico poligono.
    È una proxy conservativa: esclude i comuni BS e CO dell'ATS Montagna,
    ma questi sono anch'essi prevalentemente montani/boschivi,
    quindi il bias nella composizione di uso del suolo è minimo.
    """
    col_prov = None
    for candidato in ["COD_PROV", "COD_PRO", "PRO_COM"]:
        if candidato in gdf_comuni.columns:
            col_prov = candidato
            break

    gdf_so = gdf_comuni[
        gdf_comuni[col_prov].astype(str).str.strip()
        == str(COD_PROV_SONDRIO)
    ].copy()

    print(f"  ATS Montagna (strategia A - proxy SO): {len(gdf_so)} comuni")
    confine = gdf_so.dissolve().reset_index(drop=True)
    return confine


def ottieni_confine_montagna_B(gdf_comuni: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Strategia B: usa lista esplicita COMUNI_ATS_MONTAGNA.

    Per ogni comune nella lista, cerca il match nello shapefile
    ISTAT con la stessa logica di normalizzazione di 02_Seasons.py.
    Dissolve i comuni trovati in un unico poligono.

    ATTENZIONE: i comuni non trovati nello shapefile vengono
    segnalati ma non bloccano l'esecuzione.
    """
    col_comune = None
    for candidato in ["COMUNE", "DENOMINAZIONE", "DEN_COM", "NOME"]:
        if candidato in gdf_comuni.columns:
            col_comune = candidato
            break

    if col_comune is None:
        raise ValueError(
            f"Colonna nome comune non trovata. Colonne: {gdf_comuni.columns.tolist()}"
        )

    # Dizionario nome_normalizzato → indice per join rapido
    gdf_comuni["_nome_norm"] = gdf_comuni[col_comune].apply(_normalizza_nome_comune)
    lookup = dict(zip(gdf_comuni["_nome_norm"], gdf_comuni.index))

    indici_trovati = []
    non_trovati    = []

    for comune in COMUNI_ATS_MONTAGNA:
        norm = _normalizza_nome_comune(comune)
        idx  = lookup.get(norm)
        if idx is not None:
            indici_trovati.append(idx)
        else:
            # Tenta match parziale
            candidati = [k for k in lookup if norm in k or k in norm]
            if len(candidati) == 1:
                indici_trovati.append(lookup[candidati[0]])
            else:
                non_trovati.append(comune)

    if non_trovati:
        print(f"  ⚠ {len(non_trovati)} comuni non trovati nello shapefile ISTAT:")
        for c in non_trovati[:10]:
            print(f"      '{c}'")
        if len(non_trovati) > 10:
            print(f"      ... e altri {len(non_trovati)-10}")

    gdf_mt = gdf_comuni.loc[indici_trovati].copy()
    print(f"  ATS Montagna (strategia B): {len(gdf_mt)}/{len(COMUNI_ATS_MONTAGNA)} comuni trovati")

    confine = gdf_mt.dissolve().reset_index(drop=True)
    return confine


def clip_e_classifica(
    gdf_dusaf:  gpd.GeoDataFrame,
    confine_ats: gpd.GeoDataFrame,
    ats_label:  str
) -> gpd.GeoDataFrame:
    """
    Ritaglia il DUSAF sul confine dell'ATS e aggiunge macro-categoria.

    Il CRS del confine viene reproiettato su quello del DUSAF
    prima del clip, come raccomandato da geopandas per evitare
    errori di allineamento spaziale.

    Args:
        gdf_dusaf:   DUSAF 7 completo
        confine_ats: GeoDataFrame con il poligono confine ATS
        ats_label:   stringa per i messaggi di log

    Returns:
        GeoDataFrame ritagliato con colonna 'macro_cat' e 'area_km2'.
    """
    print(f"  Clip DUSAF per {ats_label}...")

    # Allinea CRS
    if confine_ats.crs != gdf_dusaf.crs:
        confine_ats = confine_ats.to_crs(gdf_dusaf.crs)

    # Clip spaziale
    gdf_clip = gpd.clip(gdf_dusaf, confine_ats)
    print(f"  ✓ {ats_label}: {len(gdf_clip):,} poligoni dopo clip")

    # Classificazione macro-categoria
    gdf_clip = gdf_clip.copy()
    gdf_clip["macro_cat"] = gdf_clip["codice_dusaf"].apply(classifica_uso_suolo)

    # Area in km² (il CRS DUSAF è in metri, tipicamente EPSG:32632)
    # Verifica: se l'area media fosse in gradi (CRS geografico),
    # i valori sarebbero molto piccoli (< 0.001). In quel caso,
    # reproiettare prima del calcolo.
    gdf_clip["area_km2"] = gdf_clip.geometry.area / 1_000_000

    area_totale = gdf_clip["area_km2"].sum()
    print(f"  Area totale {ats_label}: {area_totale:.1f} km²")

    return gdf_clip


def calcola_statistiche_uso_suolo(
    gdf_clip: gpd.GeoDataFrame,
    ats_label: str
) -> pd.DataFrame:
    """
    Aggrega l'area per macro-categoria e calcola le percentuali.

    Returns:
        pd.DataFrame con colonne:
            ats, macro_cat, area_km2, percentuale
        ordinato per area_km2 decrescente.
    """
    area_totale = gdf_clip["area_km2"].sum()

    df_stats = (
        gdf_clip
        .groupby("macro_cat")["area_km2"]
        .sum()
        .reset_index()
    )
    df_stats["percentuale"] = df_stats["area_km2"] / area_totale * 100
    df_stats["ats"] = ats_label
    df_stats = df_stats.sort_values("area_km2", ascending=False).reset_index(drop=True)

    return df_stats[["ats", "macro_cat", "area_km2", "percentuale"]]


def salva_csv(df: pd.DataFrame, path: str):
    """Salva DataFrame in CSV con messaggio di conferma."""
    df.to_csv(path, index=False, float_format="%.4f")
    size_kb = os.path.getsize(path) / 1024
    print(f"  ✓ Salvato: {os.path.basename(path)} ({size_kb:.1f} KB)")


# ═══════════════════════════════════════════════════════════
# SEZIONE 4 — FUNZIONI DI VISUALIZZAZIONE
# ═══════════════════════════════════════════════════════════

def _get_colori(categorie: list) -> list:
    """Restituisce la lista colori per le categorie date."""
    fallback_colors = ["#bdc3c7", "#ecf0f1", "#7f8c8d"]
    colori = []
    fb_idx = 0
    for cat in categorie:
        if cat in COLORI_CATEGORIE:
            colori.append(COLORI_CATEGORIE[cat])
        else:
            colori.append(fallback_colors[fb_idx % len(fallback_colors)])
            fb_idx += 1
    return colori


def grafico_torte(df_berg: pd.DataFrame, df_mont: pd.DataFrame, output_dir: str):
    """
    Produce i grafici a torta dell'uso del suolo per le due ATS.
    Evidenzia la categoria industriale con un 'explode'.
    """
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    fig.suptitle(
        "Uso del Suolo DUSAF 7 (2021)\nATS Bergamo vs ATS Montagna",
        fontsize=14, fontweight="bold", y=1.02
    )

    for ax, df, titolo in zip(
        axes,
        [df_berg, df_mont],
        ["ATS Bergamo", "ATS Montagna"]
    ):
        categorie = df["macro_cat"].tolist()
        valori    = df["percentuale"].tolist()
        colori    = _get_colori(categorie)

        # "Esplodi" la categoria industriale per evidenziarla
        explode = [
            0.08 if "Industriale" in cat else 0.0
            for cat in categorie
        ]

        wedges, texts, autotexts = ax.pie(
            valori,
            labels=None,           # legenda separata
            colors=colori,
            explode=explode,
            autopct=lambda p: f"{p:.1f}%" if p > 2.5 else "",
            startangle=90,
            pctdistance=0.75,
        )
        for autotext in autotexts:
            autotext.set_fontsize(8)

        ax.set_title(titolo, fontsize=12, fontweight="bold", pad=15)

        # Legenda con percentuali
        legend_labels = [
            f"{cat}\n({pct:.1f}%, {area:.0f} km²)"
            for cat, pct, area in zip(
                df["macro_cat"], df["percentuale"], df["area_km2"]
            )
        ]
        ax.legend(
            wedges, legend_labels,
            loc="lower center",
            bbox_to_anchor=(0.5, -0.35),
            fontsize=7.5,
            ncol=2,
            framealpha=0.8,
        )

    plt.tight_layout()
    out_path = os.path.join(output_dir, "dusaf_torte_ats.png")
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Grafico torte salvato: {os.path.basename(out_path)}")


def grafico_barre_comparativo(
    df_confronto: pd.DataFrame,
    output_dir: str
):
    """
    Produce un grafico a barre raggruppate (clustered bar chart)
    per confrontare direttamente le percentuali tra le due ATS.

    Questo è il grafico più utile per il report: mostra
    immediatamente la differenza nella quota industriale.
    """
    categorie = df_confronto["macro_cat"].tolist()
    pct_bg   = df_confronto["pct_bergamo"].tolist()
    pct_mt   = df_confronto["pct_montagna"].tolist()
    colori   = _get_colori(categorie)

    x     = np.arange(len(categorie))
    width = 0.35

    fig, ax = plt.subplots(figsize=(13, 6))

    bars_bg = ax.bar(
        x - width / 2, pct_bg, width,
        label="ATS Bergamo",
        color=[c for c in colori],
        edgecolor="white", linewidth=0.5,
        alpha=0.85,
    )
    bars_mt = ax.bar(
        x + width / 2, pct_mt, width,
        label="ATS Montagna",
        color=[c for c in colori],
        edgecolor="white", linewidth=0.5,
        alpha=0.5,           # più trasparente per distinguerle
        hatch="///",
    )

    # Etichette valori sulle barre
    for bar in bars_bg:
        h = bar.get_height()
        if h > 0.5:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                h + 0.3, f"{h:.1f}%",
                ha="center", va="bottom", fontsize=7.5, fontweight="bold"
            )
    for bar in bars_mt:
        h = bar.get_height()
        if h > 0.5:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                h + 0.3, f"{h:.1f}%",
                ha="center", va="bottom", fontsize=7.5, color="#555"
            )

    ax.set_xticks(x)
    ax.set_xticklabels(categorie, rotation=30, ha="right", fontsize=9)
    ax.set_ylabel("Percentuale sul territorio ATS (%)", fontsize=10)
    ax.set_title(
        "Confronto Uso del Suolo DUSAF 7 (2021)\nATS Bergamo vs ATS Montagna",
        fontsize=12, fontweight="bold"
    )
    ax.legend(fontsize=10)
    ax.set_ylim(0, max(max(pct_bg), max(pct_mt)) * 1.2)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5)
    ax.set_axisbelow(True)

    # Evidenzia la colonna "Industriale"
    for i, cat in enumerate(categorie):
        if "Industriale" in cat:
            ax.axvspan(i - 0.5, i + 0.5, color="#ffe0e0", zorder=0, alpha=0.5)
            ax.text(
                i, max(pct_bg[i], pct_mt[i]) * 1.05 + 2,
                "★ categoria\nchiave",
                ha="center", fontsize=7, color="#c0392b", style="italic"
            )

    plt.tight_layout()
    out_path = os.path.join(output_dir, "dusaf_barre_comparativo.png")
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Grafico barre comparativo salvato: {os.path.basename(out_path)}")


def grafico_mappa_confronto(
    gdf_berg: gpd.GeoDataFrame,
    gdf_mont: gpd.GeoDataFrame,
    output_dir: str
):
    """
    Mappa dell'uso del suolo per le due ATS colorata per macro-categoria.
    Utile come figura geospaziale nel report.
    """
    fig, axes = plt.subplots(1, 2, figsize=(16, 8))
    fig.suptitle(
        "Mappa Uso del Suolo DUSAF 7 — ATS Bergamo e ATS Montagna",
        fontsize=13, fontweight="bold"
    )

    for ax, gdf, titolo in zip(
        axes,
        [gdf_berg, gdf_mont],
        ["ATS Bergamo", "ATS Montagna"]
    ):
        # Colorazione per macro-categoria
        categorie_presenti = gdf["macro_cat"].unique()

        for cat in COLORI_CATEGORIE:
            if cat in categorie_presenti:
                subset = gdf[gdf["macro_cat"] == cat]
                subset.plot(ax=ax, color=COLORI_CATEGORIE[cat], linewidth=0)

        ax.set_title(titolo, fontsize=11, fontweight="bold")
        ax.set_axis_off()

    # Legenda globale
    patches = [
        mpatches.Patch(color=colore, label=cat)
        for cat, colore in COLORI_CATEGORIE.items()
    ]
    fig.legend(
        handles=patches,
        loc="lower center",
        ncol=3,
        fontsize=8,
        bbox_to_anchor=(0.5, -0.05),
        framealpha=0.9,
    )

    plt.tight_layout()
    out_path = os.path.join(output_dir, "dusaf_mappa_ats.png")
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Mappa salvata: {os.path.basename(out_path)}")


# ═══════════════════════════════════════════════════════════
# SEZIONE 5 — MAIN
# ═══════════════════════════════════════════════════════════

def main():
    print("\n" + "=" * 65)
    print("analisi_dusaf_ats.py — Uso del Suolo DUSAF per ATS")
    print("=" * 65)

    # ── Creazione cartella output ────────────────────────────
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── STEP 1: Caricamento DUSAF ────────────────────────────
    print("\n" + "─" * 65)
    print("STEP 1: Caricamento DUSAF 7")
    print("─" * 65)

    gdf_dusaf = carica_dusaf(DUSAF_PATH)

    # ── STEP 2: Confini ATS ──────────────────────────────────
    print("\n" + "─" * 65)
    print("STEP 2: Estrazione confini ATS da shapefile comuni")
    print("─" * 65)

    if not os.path.exists(SHAPEFILE_COMUNI_PATH):
        print(f"\n  ERRORE: shapefile comuni non trovato: {SHAPEFILE_COMUNI_PATH}")
        print(f"  Aggiornare SHAPEFILE_COMUNI_PATH nella SEZIONE 1 — CONFIGURAZIONE.")
        sys.exit(1)

    print(f"  Caricamento shapefile comuni: {SHAPEFILE_COMUNI_PATH}")
    gdf_comuni = gpd.read_file(SHAPEFILE_COMUNI_PATH)
    print(f"  ✓ {len(gdf_comuni):,} comuni caricati | CRS: {gdf_comuni.crs}")

    # ATS Bergamo
    print("\n  → ATS Bergamo:")
    confine_bg = ottieni_confine_bergamo(gdf_comuni)

    # ATS Montagna
    print(f"\n  → ATS Montagna (strategia {MONTAGNA_STRATEGIA}):")
    if MONTAGNA_STRATEGIA == "A":
        confine_mt = ottieni_confine_montagna_A(gdf_comuni)
    else:
        confine_mt = ottieni_confine_montagna_B(gdf_comuni)

    # ── STEP 3: Clip DUSAF per ATS ──────────────────────────
    print("\n" + "─" * 65)
    print("STEP 3: Clip DUSAF per ATS (può richiedere qualche minuto)")
    print("─" * 65)

    print("\n  → ATS Bergamo:")
    gdf_berg_clip = clip_e_classifica(gdf_dusaf, confine_bg, "ATS Bergamo")

    print("\n  → ATS Montagna:")
    gdf_mont_clip = clip_e_classifica(gdf_dusaf, confine_mt, "ATS Montagna")

    # ── STEP 4: Statistiche ──────────────────────────────────
    print("\n" + "─" * 65)
    print("STEP 4: Calcolo statistiche uso del suolo")
    print("─" * 65)

    df_berg_stats = calcola_statistiche_uso_suolo(gdf_berg_clip, "ATS Bergamo")
    df_mont_stats = calcola_statistiche_uso_suolo(gdf_mont_clip, "ATS Montagna")

    # Tabella confronto
    df_confronto = pd.merge(
        df_berg_stats[["macro_cat", "area_km2", "percentuale"]].rename(
            columns={"area_km2": "km2_bergamo", "percentuale": "pct_bergamo"}
        ),
        df_mont_stats[["macro_cat", "area_km2", "percentuale"]].rename(
            columns={"area_km2": "km2_montagna", "percentuale": "pct_montagna"}
        ),
        on="macro_cat", how="outer"
    ).fillna(0)

    # Ordina per percentuale Bergamo (dal più al meno rilevante)
    df_confronto = df_confronto.sort_values("pct_bergamo", ascending=False).reset_index(drop=True)

    # ── STEP 5: Stampa risultati ─────────────────────────────
    print("\n" + "─" * 65)
    print("RISULTATI — ATS BERGAMO")
    print("─" * 65)
    print(df_berg_stats.to_string(index=False, float_format=lambda x: f"{x:.2f}"))

    print("\n" + "─" * 65)
    print("RISULTATI — ATS MONTAGNA")
    print("─" * 65)
    print(df_mont_stats.to_string(index=False, float_format=lambda x: f"{x:.2f}"))

    print("\n" + "─" * 65)
    print("CONFRONTO DIRETTO (ordinato per quota Bergamo)")
    print("─" * 65)
    print(df_confronto.to_string(index=False, float_format=lambda x: f"{x:.2f}"))

    # Evidenzia il dato chiave per la giustificazione
    try:
        row_ind = df_confronto[
            df_confronto["macro_cat"].str.contains("Industriale", na=False)
        ].iloc[0]
        print(f"\n  ★ DATO CHIAVE PER IL REPORT:")
        print(f"    Industriale/Commerciale/Infrastrutture:")
        print(f"      ATS Bergamo:  {row_ind['pct_bergamo']:.1f}% "
              f"({row_ind['km2_bergamo']:.0f} km²)")
        print(f"      ATS Montagna: {row_ind['pct_montagna']:.1f}% "
              f"({row_ind['km2_montagna']:.0f} km²)")
        print(f"    → Rapporto: {row_ind['pct_bergamo']/row_ind['pct_montagna']:.1f}x "
              f"più industriale in ATS Bergamo")
    except IndexError:
        print("  ⚠ Categoria industriale non trovata — verificare la classificazione.")

    # ── STEP 6: Salvataggio CSV ──────────────────────────────
    print("\n" + "─" * 65)
    print("STEP 6: Salvataggio CSV")
    print("─" * 65)

    salva_csv(df_berg_stats, os.path.join(OUTPUT_DIR, "dusaf_stats_bergamo.csv"))
    salva_csv(df_mont_stats, os.path.join(OUTPUT_DIR, "dusaf_stats_montagna.csv"))
    salva_csv(df_confronto,  os.path.join(OUTPUT_DIR, "dusaf_confronto_ats.csv"))

    # CSV dettagliato con tutti i poligoni classificati (per mappe in QGIS)
    gdf_berg_clip[["codice_dusaf", "macro_cat", "area_km2"]].to_csv(
        os.path.join(OUTPUT_DIR, "dusaf_poligoni_bergamo.csv"), index=False
    )
    gdf_mont_clip[["codice_dusaf", "macro_cat", "area_km2"]].to_csv(
        os.path.join(OUTPUT_DIR, "dusaf_poligoni_montagna.csv"), index=False
    )
    print(f"  ✓ CSV poligoni dettagliati salvati (utili per QGIS/verifica)")

    # ── STEP 7: Grafici ──────────────────────────────────────
    print("\n" + "─" * 65)
    print("STEP 7: Generazione grafici")
    print("─" * 65)

    grafico_torte(df_berg_stats, df_mont_stats, OUTPUT_DIR)
    grafico_barre_comparativo(df_confronto, OUTPUT_DIR)
    grafico_mappa_confronto(gdf_berg_clip, gdf_mont_clip, OUTPUT_DIR)

    # ── Riepilogo finale ─────────────────────────────────────
    print("\n" + "=" * 65)
    print("✅ analisi_dusaf_ats.py COMPLETATO")
    print("=" * 65)
    print(f"\nOutput in: {os.path.abspath(OUTPUT_DIR)}/")
    files = sorted(os.listdir(OUTPUT_DIR))
    for f in files:
        fpath = os.path.join(OUTPUT_DIR, f)
        size_kb = os.path.getsize(fpath) / 1024
        print(f"  └─ {f}  ({size_kb:.1f} KB)")

    print("""
─────────────────────────────────────────────────────────────
NOTE FINALI PER IL REPORT
─────────────────────────────────────────────────────────────

Usa dusaf_barre_comparativo.png come figura principale
nel report: mostra immediatamente il contrasto.

Usa dusaf_mappa_ats.png per la sezione geospaziale.

Per il testo del report, cita:
  - DUSAF 7 (2021), Regione Lombardia — Geoportale Regionale
  - Feng et al. (2016): PM2.5 aumenta il rischio ILI
  - Merl et al. (2021): relazione esponenziale PM2.5-ILI

BIAS DA DICHIARARE:
  1. DUSAF 2021: possibile disallineamento con stagioni >2021
  2. Uso suolo ≠ emissioni effettive: è una proxy
  3. Bias ecologico: scala ATS, non individuale
  4. Se strategia A: ATS Montagna = solo provincia SO (proxy)
─────────────────────────────────────────────────────────────
""")


# ═══════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()