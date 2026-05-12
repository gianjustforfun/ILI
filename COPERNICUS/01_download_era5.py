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
  era5_ats.nc  — copre ottobre 2021 → aprile 2026

NOTA SUL PERIODO:
  Le stagioni influenzali di interesse vanno da ottobre 2021 ad aprile 2026.
  Questo significa scaricare anni 2021-2026 per i mesi ott-apr.
  Es: stagione 2021/22 → ott 2021 (anno 2021) + nov 2021-apr 2022 (anno 2022)
  Il file copre tutto il range in un'unica richiesta.
"""

import cdsapi
import os

# ---------------------------------------------------------------------------
# CONFIGURAZIONE
# ---------------------------------------------------------------------------

# Anni necessari per coprire le stagioni influenzali 2021/22 → 2025/26:
#   stagione 2021/22 → ottobre 2021 ... aprile 2022
#   stagione 2022/23 → ottobre 2022 ... aprile 2023
#   stagione 2023/24 → ottobre 2023 ... aprile 2024
#   stagione 2024/25 → ottobre 2024 ... aprile 2025
#   stagione 2025/26 → ottobre 2025 ... aprile 2026
# → servono gli anni 2021, 2022, 2023, 2024, 2025, 2026
ANNI = ['2021', '2022', '2023', '2024', '2025', '2026']

# Mesi della stagione influenzale (ottobre → aprile)
MESI = ['01', '02', '03', '04', '10', '11', '12']

# Ora del giorno (UTC). Solo 12:00 → risparmio ~75% vs download orario.
# 12:00 UTC = 13:00-14:00 ora italiana → buona rappresentatività della T diurna.
ORA = ['12:00']

# Bounding box [Nord, Ovest, Sud, Est] in gradi decimali.
# Copre strettamente:
#   - Provincia di Bergamo
#   - Provincia di Sondrio
#   - Comuni montagna di Brescia (Val Camonica, fino a Ponte di Legno)
#   - Comuni montagna di Como (Alto Lago, area Gravedona)
# Margine 0.1° per non troncare celle di bordo ERA5.
BBOX = [46.7,   # Nord  (punta nord Sondrio / confine CH)
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

    print(f"Richiesta dati ERA5...")
    print(f"  Anni  : {ANNI}")
    print(f"  Mesi  : {MESI}  (solo stagione influenzale ott-apr)")
    print(f"  Bbox  : {BBOX}")
    print(f"  Ora   : {ORA} UTC  (1 timestep/giorno)")
    print(f"  Output: {OUTPUT_FILE}")
    print()
    print("⚠️  NOTA: il download copre 6 anni × 7 mesi.")
    print("   Il CDS potrebbe mettere la richiesta in coda — attesa normale.")

    c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'variable': [
                '2m_temperature',           # T in Kelvin
                '2m_dewpoint_temperature',  # Td in Kelvin → calcolo AH e RH
            ],
            'year':  ANNI,
            'month': MESI,
            # Tutti i giorni: i mesi corti ignorano semplicemente il giorno 31
            'day':   [str(d).zfill(2) for d in range(1, 32)],
            'time':  ORA,
            'format': 'netcdf',
            'area':  BBOX,
        },
        OUTPUT_FILE
    )

    size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"\n✓ Download completato: {OUTPUT_FILE}  ({size_mb:.1f} MB)")

if __name__ == '__main__':
    main()
