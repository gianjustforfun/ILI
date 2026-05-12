"""
SCRIPT 1 — Download dati meteorologici ERA5 (Copernicus)
=========================================================
Scarica temperatura e dew point per calcolare l'umidità assoluta.
Territorio: ATS Bergamo (prov. Bergamo) + ATS della Montagna
            (prov. Sondrio + comuni montagna di Brescia e Como).

OTTIMIZZAZIONI per ridurre il peso del file:
  1. Bounding box stretto sui soli territori di interesse
  2. Solo le 2 variabili minime necessarie (T2m + dew point)
  3. Un solo timestep al giorno (ore 12:00 UTC, ora più rappresentativa)
     → risparmio ~75% rispetto al download orario
  4. Solo i mesi della stagione influenzale (ott-apr)
  5. Formato NetCDF con compressione

PREREQUISITI:
  pip install cdsapi xarray netCDF4 numpy pandas
  Credenziali in ~/.cdsapirc (vedi README)

OUTPUT:
  era5_ats.nc  (~2-5 MB per stagione, stima)
"""

import cdsapi
import os

# ---------------------------------------------------------------------------
# CONFIGURAZIONE — MODIFICATE QUESTI PARAMETRI
# ---------------------------------------------------------------------------

# Anni delle stagioni influenzali che volete analizzare.
# Una "stagione" va da ottobre ANNO a aprile ANNO+1.
# Esempio: stagione 2022/23 → anni ['2022', '2023'], mesi ott-apr
ANNI = ['2022', '2023', '2024']   # <-- adattate ai vostri dati ILI

# Mesi stagione influenzale (ottobre=10 ... aprile=4)
# Nota: gennaio 2025 appartiene alla stagione 2024/25, quindi includete
# tutti i mesi che vi servono come range continuo.
MESI = ['10', '11', '12', '01', '02', '03', '04']

# Ora del giorno da scaricare (UTC). Usiamo solo le 12:00.
# Le 12:00 UTC = 13:00-14:00 ora italiana → buona rappresentatività della T diurna.
# Se volete la media giornaliera reale, cambiate in ['00:00','06:00','12:00','18:00']
# ma il file peserà 4 volte tanto.
ORA = ['12:00']

# Bounding box [Nord, Ovest, Sud, Est] in gradi decimali.
# Calcolato per contenere strettamente:
#   - Provincia di Bergamo
#   - Provincia di Sondrio
#   - Comuni montagna di Brescia (Val Camonica, fino a Edolo/Ponte di Legno)
#   - Comuni montagna di Como (Alto Lago di Como, Gravedona area)
# Margine di 0.1° aggiunto per non troncare celle di bordo.
BBOX = [46.7,   # Nord  (punta nord di Sondrio / confine CH)
        9.3,    # Ovest (Lago di Como occidentale)
        45.4,   # Sud   (confine sud prov. Bergamo)
        10.6]   # Est   (Val Camonica est)

OUTPUT_FILE = 'era5_ats.nc'

# ---------------------------------------------------------------------------
# DOWNLOAD
# ---------------------------------------------------------------------------

def main():
    if os.path.exists(OUTPUT_FILE):
        print(f"File {OUTPUT_FILE} già presente. Cancellarlo per ri-scaricare.")
        return

    print("Connessione al CDS Copernicus...")
    c = cdsapi.Client()

    print(f"Richiesta dati ERA5 per anni {ANNI}, mesi {MESI}...")
    print(f"Bounding box: {BBOX}")
    print("Variabili: 2m_temperature + 2m_dewpoint_temperature")
    print(f"Ora giornaliera: {ORA} UTC  (solo 1 timestep/giorno → file più leggero)")

    c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'variable': [
                '2m_temperature',           # T in Kelvin
                '2m_dewpoint_temperature',  # Td in Kelvin, serve per calcolare AH e RH
            ],
            'year': ANNI,
            'month': MESI,
            # Scarica tutti i giorni (i mesi corti semplicemente non avranno il giorno 31)
            'day': [str(d).zfill(2) for d in range(1, 32)],
            'time': ORA,
            'format': 'netcdf',
            'area': BBOX,   # ritaglia solo la nostra regione
        },
        OUTPUT_FILE
    )

    size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"\nDownload completato: {OUTPUT_FILE}  ({size_mb:.1f} MB)")


if __name__ == '__main__':
    main()
