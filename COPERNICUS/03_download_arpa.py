"""
SCRIPT 3 — Download e processamento dati ARPA Lombardia
=========================================================
Scarica i dati di qualità dell'aria (NO2, PM2.5, PM10) dal portale
open data di ARPA Lombardia e li aggrega per ATS, con colonne separate
per ciascun inquinante.

LOGICA:
  1. Scarica l'anagrafica dei sensori (con coordinate + nometiposensore)
  2. Filtra i sensori che misurano NO2, PM10, PM2.5
  3. Spatial join con lo shapefile comuni → assegna ATS
  4. Scarica le misure da DUE dataset (necessario per coprire 2021-oggi):
       - g2hp-ar79  →  2018-01-01 : 2025-01-01
       - nicp-bhqi  →  2024-01-01 : oggi
     I due dataset si sovrappongono nel 2024: deduplicazione su (idsensore, data)
  5. Aggrega a livello giornaliero per (giorno, ATS, parametro)
  6. Pivot → colonne separate: NO2_mean_ugm3, PM10_mean_ugm3, PM25_mean_ugm3

NOTE API ARPA (dati.lombardia.it / Socrata):
  - idsensore è TEXT → va quotato: idsensore in ('123','456')
  - -9999 è il codice ARPA per dato mancante → viene rimosso
  - stato: VA=validato  PR=provvisorio  NA=non valido

PREREQUISITI:
  pip install requests pandas geopandas shapely
"""

import requests
import pandas as pd
import geopandas as gpd
from io import StringIO
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIGURAZIONE
# ---------------------------------------------------------------------------

# Due dataset necessari per coprire 2021-oggi
DATASET_STORICI = {
    "g2hp-ar79": {
        "url":   "https://www.dati.lombardia.it/resource/g2hp-ar79.csv",
        "range": ("2018-01-01", "2025-01-01"),   # range reale del dataset
    },
    "nicp-bhqi": {
        "url":   "https://www.dati.lombardia.it/resource/nicp-bhqi.csv",
        "range": ("2024-01-01", None),           # None = fino ad oggi
    },
}

URL_STAZIONI  = "https://www.dati.lombardia.it/resource/ib47-atvt.json?$limit=10000"

# Periodo di interesse
DATA_INIZIO = "2021-01-01"   # incluso
DATA_FINE   = None           # None = fino ad oggi

# Shapefile comuni ISTAT
COMUNI_SHP = "Com01012024_g_WGS84.shp"

# Definizione ATS
ATS_DEFINIZIONE = {
    "ATS_Bergamo": {
        "province_intere": ["016"],
        "comuni_specifici": [],
    },
    "ATS_Montagna": {
        "province_intere": ["098"],
        "comuni_specifici": [
            "Angolo Terme", "Artogne", "Berzo Demo", "Berzo Inferiore",
            "Bienno", "Borno", "Braone", "Breno", "Capo di Ponte",
            "Cedegolo", "Cerveno", "Ceto", "Cevo", "Cimbergo",
            "Cividate Camuno", "Corteno Golgi", "Darfo Boario Terme",
            "Edolo", "Esine", "Gianico", "Incudine", "Losine", "Lozio",
            "Malegno", "Malonno", "Monno", "Niardo", "Ono San Pietro",
            "Ossimo", "Paisco Loveno", "Paspardo", "Pian Camuno",
            "Piancogno", "Pisogne", "Ponte di Legno",
            "Saviore dell'Adamello", "Sellero", "Sonico", "Temù",
            "Vezza d'Oglio", "Vione",
            "Cremia", "Domaso", "Dongo", "Dosso del Liro", "Garzeno",
            "Gera Lario", "Gravedona ed Uniti", "Livo", "Montemezzo",
            "Musso", "Peglio", "Pianello del Lario", "Sorico", "Stazzona",
            "Trezzone", "Vercana",
        ],
    },
}

# ---------------------------------------------------------------------------
# FUNZIONI — anagrafica e geografia
# ---------------------------------------------------------------------------

def normalizza_parametro(nome: str) -> str | None:
    if not isinstance(nome, str):
        return None
    n = nome.lower().strip()
    if "biossido di azoto" in n or ("no" in n and "2" in n):
        return "NO2"
    if "pm10" in n:
        return "PM10"
    if "pm2" in n and "5" in n:
        return "PM25"
    return None


def scarica_anagrafica_stazioni() -> gpd.GeoDataFrame:
    cache = Path("stazioni_arpa.geojson")
    if cache.exists():
        print("✓ Carico anagrafica stazioni da cache...")
        return gpd.read_file(cache)

    print("→ Scarico anagrafica stazioni da ARPA...")
    resp = requests.get(URL_STAZIONI, timeout=60)
    resp.raise_for_status()
    stazioni = pd.DataFrame(resp.json())
    print(f"  Totale sensori: {len(stazioni)}")

    if "storico" in stazioni.columns:
        stazioni = stazioni[stazioni["storico"] == "N"]
        print(f"  Sensori attivi: {len(stazioni)}")

    stazioni["lat"] = pd.to_numeric(stazioni["lat"], errors="coerce")
    stazioni["lng"] = pd.to_numeric(stazioni["lng"], errors="coerce")
    stazioni = stazioni.dropna(subset=["lat", "lng"])

    gdf = gpd.GeoDataFrame(
        stazioni,
        geometry=gpd.points_from_xy(stazioni["lng"], stazioni["lat"]),
        crs="EPSG:4326",
    )
    gdf.to_file(cache, driver="GeoJSON")
    return gdf


def costruisci_confini_ats() -> gpd.GeoDataFrame:
    cache = Path("confini_ats.geojson")
    if cache.exists():
        return gpd.read_file(cache)

    print(f"→ Costruisco confini ATS da {COMUNI_SHP}...")
    comuni = gpd.read_file(COMUNI_SHP)
    comuni.columns = [c.upper() for c in comuni.columns]

    rename_map = {}
    for col in comuni.columns:
        if col in ("COMUNE", "DENOMINAZI", "DENOMVD", "NOME", "DEN_COM"):
            rename_map[col] = "COMUNE"
        if col in ("COD_PROV", "COD_PRO", "CODPRO"):
            rename_map[col] = "COD_PROV"
    comuni = comuni.rename(columns=rename_map)

    if "COD_PROV" not in comuni.columns:
        raise ValueError(f"Colonna COD_PROV non trovata. Disponibili: {comuni.columns.tolist()}")

    comuni["COD_PROV"] = comuni["COD_PROV"].astype(str).str.zfill(3)

    risultati = []
    for nome_ats, cfg in ATS_DEFINIZIONE.items():
        mask_prov   = comuni["COD_PROV"].isin(cfg["province_intere"])
        mask_comuni = (
            comuni["COMUNE"].isin(cfg["comuni_specifici"])
            if cfg["comuni_specifici"]
            else pd.Series(False, index=comuni.index)
        )
        sel = comuni[mask_prov | mask_comuni].copy()
        sel["ATS"] = nome_ats
        risultati.append(sel)
        print(f"  {nome_ats}: {len(sel)} comuni")

    comuni_ats = pd.concat(risultati, ignore_index=True)
    poligoni = comuni_ats.dissolve(by="ATS").reset_index()[["ATS", "geometry"]]
    poligoni.to_file(cache, driver="GeoJSON")
    return poligoni


def assegna_ats_a_stazioni(stazioni_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    confini = costruisci_confini_ats()
    joined = gpd.sjoin(
        stazioni_gdf,
        confini[["ATS", "geometry"]],
        how="left",
        predicate="within",
    )
    return joined.dropna(subset=["ATS"]).copy()


# ---------------------------------------------------------------------------
# FUNZIONI — download misure
# ---------------------------------------------------------------------------

def _scarica_da_dataset(
    dataset_id:  str,
    url_base:    str,
    ids_sensori: list[str],
    data_inizio: str,
    data_fine:   str | None,
    page_size:   int = 50_000,
) -> pd.DataFrame:
    """
    Scarica le misure da un singolo dataset Socrata per i sensori e il
    periodo indicati. Gestisce la paginazione automaticamente.
    """
    filtro_data = f"AND data >= '{data_inizio}T00:00:00'"
    if data_fine:
        filtro_data += f" AND data <= '{data_fine}T23:59:59'"

    id_chunks = [ids_sensori[i:i+200] for i in range(0, len(ids_sensori), 200)]
    frames    = []
    totale    = 0

    for ci, id_chunk in enumerate(id_chunks):
        ids_quotati  = ",".join(f"'{i}'" for i in id_chunk)
        where_clause = f"idsensore in ({ids_quotati}) {filtro_data}"
        offset = 0

        while True:
            params = {
                "$where":  where_clause,
                "$limit":  page_size,
                "$offset": offset,
                "$order":  "idsensore,data",
            }
            resp = requests.get(url_base, params=params, timeout=180)
            resp.raise_for_status()

            batch = pd.read_csv(StringIO(resp.text))
            if batch.empty:
                break

            frames.append(batch)
            offset += page_size
            totale += len(batch)
            print(
                f"  [{dataset_id}] chunk {ci+1}/{len(id_chunks)}, "
                f"offset {offset:,}, righe: {totale:,}",
                end="\r",
            )

            if len(batch) < page_size:
                break

    print()
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def scarica_misure_per_sensori(
    ids_sensori: list[str],
    data_inizio: str = DATA_INIZIO,
    data_fine:   str | None = DATA_FINE,
    page_size:   int = 50_000,
) -> pd.DataFrame:
    """
    Scarica e unisce le misure dai dataset ARPA necessari a coprire
    il periodo richiesto. Gestisce automaticamente la selezione dei
    dataset e la deduplicazione dell'overlap.

    Struttura dataset ARPA:
        g2hp-ar79 → 2018-01-01 : 2025-01-01
        nicp-bhqi → 2024-01-01 : oggi
    """
    inizio_tag = data_inizio.replace("-", "")
    fine_tag   = data_fine.replace("-", "") if data_fine else "oggi"
    cache      = Path(f"misure_arpa_ats_{inizio_tag}_{fine_tag}.csv")

    if cache.exists():
        print(f"✓ Carico misure da cache: {cache}")
        df = pd.read_csv(cache)
        print(f"  Righe: {len(df):,} | Colonne: {df.columns.tolist()}")
        return df

    d_inizio = pd.Timestamp(data_inizio)
    d_fine   = pd.Timestamp(data_fine) if data_fine else pd.Timestamp.now()

    frames = []

    for ds_id, ds_info in DATASET_STORICI.items():
        ds_start = pd.Timestamp(ds_info["range"][0])
        ds_end   = pd.Timestamp(ds_info["range"][1]) if ds_info["range"][1] else pd.Timestamp.now()

        # Controlla overlap tra periodo richiesto e copertura del dataset
        overlap_start = max(d_inizio, ds_start)
        overlap_end   = min(d_fine,   ds_end)

        if overlap_start > overlap_end:
            print(f"  [{ds_id}] Nessun overlap con il periodo richiesto → skip")
            continue

        print(f"\n  [{ds_id}] Scarico {overlap_start.date()} → {overlap_end.date()}...")

        df_ds = _scarica_da_dataset(
            dataset_id  = ds_id,
            url_base    = ds_info["url"],
            ids_sensori = ids_sensori,
            data_inizio = str(overlap_start.date()),
            data_fine   = str(overlap_end.date()),
            page_size   = page_size,
        )

        if not df_ds.empty:
            df_ds["_source"] = ds_id
            frames.append(df_ds)
            print(f"  [{ds_id}] ✓ {len(df_ds):,} righe scaricate")

    if not frames:
        raise RuntimeError("Nessuna misura scaricata. Controlla ID sensori e periodo.")

    df = pd.concat(frames, ignore_index=True)

    # Deduplica righe uguali nell'overlap 2024 (stesso idsensore + data)
    n_pre = len(df)
    df["data_str"] = df["data"].astype(str)
    df = df.drop_duplicates(subset=["idsensore", "data_str"], keep="last")
    df = df.drop(columns=["data_str", "_source"])
    n_post = len(df)
    if n_pre != n_post:
        print(f"  Deduplicazione overlap: {n_pre:,} → {n_post:,} righe")

    df.to_csv(cache, index=False)
    print(f"\n  ✓ Cache salvato: {cache} ({len(df):,} righe totali)")
    return df


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    periodo = f"{DATA_INIZIO} → {DATA_FINE if DATA_FINE else 'oggi'}"
    print("=" * 60)
    print("SCRIPT 3 — ARPA Lombardia: qualità aria per ATS")
    print(f"  Periodo: {periodo}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # 1. Anagrafica sensori
    # ------------------------------------------------------------------
    print("\n[1/5] Anagrafica sensori ARPA...")
    stazioni = scarica_anagrafica_stazioni()

    # ------------------------------------------------------------------
    # 2. Filtro per parametro
    # ------------------------------------------------------------------
    print("\n[2/5] Filtro sensori per parametro (NO2, PM10, PM2.5)...")
    stazioni["parametro"] = stazioni["nometiposensore"].apply(normalizza_parametro)
    stazioni_interesse    = stazioni[stazioni["parametro"].notna()].copy()
    print(f"  Sensori NO2/PM10/PM25: {len(stazioni_interesse)}")
    print(stazioni_interesse.groupby("parametro").size().to_string())

    # ------------------------------------------------------------------
    # 3. Spatial join → ATS
    # ------------------------------------------------------------------
    print("\n[3/5] Spatial join: assegno ATS ai sensori...")
    stazioni_ats = assegna_ats_a_stazioni(stazioni_interesse)

    if stazioni_ats.empty:
        raise RuntimeError("Nessun sensore trovato nelle ATS. Verifica shapefile.")

    print(f"\n  Sensori nelle ATS: {len(stazioni_ats)}")
    print(stazioni_ats.groupby(["ATS", "parametro"]).size().to_string())

    stazioni_ats["idsensore_str"] = stazioni_ats["idsensore"].astype(str)
    mappa_sensore    = stazioni_ats.set_index("idsensore_str")[["ATS", "parametro"]]
    ids_da_scaricare = mappa_sensore.index.tolist()

    # ------------------------------------------------------------------
    # 4. Scarica misure (multi-dataset, 2021-oggi)
    # ------------------------------------------------------------------
    print(f"\n[4/5] Scarico misure ({periodo})...")
    df_misure = scarica_misure_per_sensori(
        ids_da_scaricare,
        data_inizio=DATA_INIZIO,
        data_fine=DATA_FINE,
    )

    # ------------------------------------------------------------------
    # 5. Elaborazione e aggregazione
    # ------------------------------------------------------------------
    print("\n[5/5] Elaborazione dati...")

    df_misure.columns  = [c.lower() for c in df_misure.columns]
    df_misure["idsensore"] = df_misure["idsensore"].astype(str)
    df_misure["valore"]    = pd.to_numeric(df_misure["valore"], errors="coerce")
    df_misure["data"]      = pd.to_datetime(df_misure["data"], errors="coerce")

    # Filtra stato valido
    if "stato" in df_misure.columns:
        n_pre = len(df_misure)
        df_misure = df_misure[df_misure["stato"].isin(["VA", "PR"])]
        print(f"  Filtro stato VA/PR: {n_pre:,} → {len(df_misure):,} righe")

    # Rimuovi -9999 (codice dato mancante ARPA)
    df_misure = df_misure[df_misure["valore"] > -9000]

    # Aggiungi ATS e parametro
    df_misure = df_misure.join(mappa_sensore, on="idsensore", how="inner")
    df_misure["giorno"] = df_misure["data"].dt.date
    df_misure = df_misure.dropna(subset=["giorno", "valore", "ATS", "parametro"])

    print(f"  Righe valide finali: {len(df_misure):,}")

    # Media giornaliera per (giorno, ATS, parametro)
    df_daily = (
        df_misure
        .groupby(["giorno", "ATS", "parametro"])
        .agg(valore_medio=("valore", "mean"), n_misure=("valore", "count"))
        .reset_index()
    )
    df_daily["valore_medio"] = df_daily["valore_medio"].round(2)
    print(f"  Righe aggregate (giorno×ATS×parametro): {len(df_daily):,}")

    # Pivot: una colonna per parametro
    df_pivot = df_daily.pivot_table(
        index=["giorno", "ATS"],
        columns="parametro",
        values="valore_medio",
        aggfunc="mean",
    ).reset_index()

    df_pivot.columns.name = None
    rename_cols = {"NO2": "NO2_mean_ugm3", "PM10": "PM10_mean_ugm3", "PM25": "PM25_mean_ugm3"}
    df_pivot = df_pivot.rename(columns={k: v for k, v in rename_cols.items()
                                         if k in df_pivot.columns})
    df_pivot = df_pivot.sort_values(["giorno", "ATS"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # Salvataggio
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)

    out_pivot = "inquinanti_per_ats_pivot.csv"
    df_pivot.to_csv(out_pivot, index=False)
    print(f"✓ {out_pivot}  — {len(df_pivot):,} righe | colonne: {df_pivot.columns.tolist()}")

    out_long = "inquinanti_per_ats_long.csv"
    df_daily.to_csv(out_long, index=False)
    print(f"✓ {out_long}  — {len(df_daily):,} righe (formato long)")

    # Anteprima
    print("\nPrime 10 righe (pivot):")
    print(df_pivot.head(10).to_string(index=False))

    print("\nStatistiche per ATS e parametro [µg/m³]:")
    stats = (
        df_daily
        .groupby(["ATS", "parametro"])["valore_medio"]
        .agg(n="count", media="mean", minimo="min", massimo="max")
        .round(2)
    )
    print(stats.to_string())

    print("\n✓ Script completato con successo.")


if __name__ == "__main__":
    main()
