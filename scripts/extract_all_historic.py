import os
import pandas as pd
from datetime import datetime, timedelta

from weather_examen_pipeline.scripts.extract_historic import extract_historic

def extract_all_historic(date: str):
    """
    Lit les fichiers geo_*.csv à la date donnée,
    et appelle extract_historic(...) (basée sur Meteostat) pour chaque ville.
    """
    geo_dir = f"data/raw/{date}"
    if not os.path.exists(geo_dir):
        print(f"Dossier introuvable : {geo_dir}")
        return

    start_date = "2019-01-01"
    # Calcul de la date de fin (veille du jour actuel)
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    for file in os.listdir(geo_dir):
        if file.startswith("geo_") and file.endswith(".csv"):
            df = pd.read_csv(os.path.join(geo_dir, file))
            if df.empty:
                continue

            city = df.at[0, 'ville']
            lat = df.at[0, 'latitude']
            lon = df.at[0, 'longitude']

            print(f"Extraction historique pour {city}")
            extract_historic(city, lat, lon, start_date, end_date)