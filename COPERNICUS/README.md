# Pipeline Dati Ambientali — ATS Bergamo & ATS della Montagna

## Struttura degli script

```
01_download_era5.py      → Scarica dati meteorologici da Copernicus (ERA5)
02_processa_era5.py      → Calcola T, umidità assoluta, aggrega per ATS
03_download_arpa.py      → Scarica NO2, PM2.5, PM10 da ARPA Lombardia
04_merge_finale.py       → Unisce tutto in dataset_finale.csv
```

Eseguire in ordine: 01 → 02 → 03 → 04.

---

## Installazione dipendenze

```bash
pip install cdsapi xarray netCDF4 numpy pandas geopandas shapely requests tqdm
```

---

## Setup Copernicus (necessario per script 01)

1. Registrarsi su https://cds.climate.copernicus.eu (gratuito)
2. Andare su "Your Profile" e copiare UID e API Key
3. Creare il file `~/.cdsapirc` con questo contenuto:

```
url: https://cds.climate.copernicus.eu/api/v2
key: TUO-UID:TUA-API-KEY
```

---

## Download shapefile comuni ISTAT (necessario per script 02 e 03)

1. Andare su: https://www.istat.it/it/archivio/222527
2. Scaricare "Limiti delle unità amministrative - 1 gennaio 2024"
3. Scegliere il file dei Comuni in formato WGS84
4. Estrarre nella stessa cartella degli script
5. Il file si chiama tipicamente `Com01012024_g_WGS84.shp`
   (con i file accessori .dbf, .prj, .shx)

---

## File prodotti

| File | Contenuto | Peso stimato |
|------|-----------|-------------|
| `era5_ats.nc` | Dati meteo grezzi ERA5 | 2–8 MB |
| `stazioni_arpa.geojson` | Anagrafica stazioni (cache) | < 1 MB |
| `confini_ats.geojson` | Confini ATS (da comuni ISTAT) | < 1 MB |
| `meteo_per_ats_giornaliero.csv` | T, AH, RH per ATS e giorno | < 0.5 MB |
| `inquinanti_per_ats_giornaliero.csv` | NO2, PM2.5, PM10 per ATS e giorno | < 0.5 MB |
| `dataset_finale.csv` | Merge completo, pronto per analisi | < 1 MB |

---

## Bias da dichiarare nel report

### Bias da aggregazione ecologica
I dati sono aggregati a livello di ATS (unità geografica ampia), non a
livello individuale. Non è possibile inferire l'esposizione individuale
di ogni paziente ILI.

### Copertura spaziale disomogenea (ARPA)
ATS della Montagna ha poche stazioni ARPA, concentrate nei fondovalle.
I comuni montani isolati non hanno misure dirette. Le medie per ATS
sono quindi meno rappresentative per ATS_Montagna che per ATS_Bergamo.

### ERA5 vs misure dirette
ERA5 è una rianalisi modellata, non una misura. In aree con forti
gradienti di quota (come Sondrio), la griglia ~11 km può non catturare
le variazioni locali di temperatura e umidità.

### Finestra temporale
Più anni avete, più solido è il modello. Con 2-3 stagioni i risultati
sono indicativi ma con elevata incertezza. Dichiararlo esplicitamente.

---

## Domande frequenti

**Quanto pesa il download ERA5?**
Con le ottimizzazioni applicate (1 timestep/giorno, solo T+Td, bbox stretto,
3 anni di stagioni influenzali) stimiamo 2-8 MB. Molto meno dei 100+ MB
di un download non ottimizzato.

**I dati ARPA sono gratuiti?**
Sì, sono open data. Non serve autenticazione per l'API di base.
Con troppe richieste rapide potreste incappare in rate limiting:
gli script usano già richieste per-mese per minimizzare il problema.

**Cosa faccio se uno script si interrompe?**
Gli script 01 e 03 salvano cache intermedie (era5_ats.nc,
stazioni_arpa.geojson). Se li rilanciate partono dalla cache.
