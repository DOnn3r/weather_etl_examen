import os
import pandas as pd
from datetime import datetime
from meteostat import Daily, Stations

def extract_historic(city, lat, lon, start_date, end_date):
    """
    Récupère les données météo historiques pour une ville avec la librairie Meteostat.
    Stocke les données journalières dans un CSV.
    """

    output_dir = f"data/raw/historical/{city}"
    os.makedirs(output_dir, exist_ok=True)

    # Dates
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    try:
        # Station météo la plus proche
        stations = Stations().nearby(lat, lon)
        station = stations.fetch(1)

        if station.empty:
            print(f"Aucune station trouvée pour {city}")
            return

        station_id = station.index[0]

        # Données journalières
        df = Daily(station_id, start, end).fetch()

        if df.empty:
            print(f"Aucune donnée météo pour {city} ({station_id})")
            return

        df = df.reset_index()
        df['ville'] = city

        # Renommer si les colonnes existent
        renames = {
            'tavg': 'temp_moyenne',
            'tmin': 'temp_min',
            'tmax': 'temp_max',
            'prcp': 'precipitation_totale',
            'wspd': 'vent_moyen',
            'rhum': 'humidite_moyenne'
        }
        df.rename(columns={k: v for k, v in renames.items() if k in df.columns}, inplace=True)

        # Colonne jours pluvieux
        if 'precipitation_totale' in df.columns:
            df['jours_pluvieux'] = df['precipitation_totale'].apply(lambda x: 1 if x > 0 else 0)
        else:
            df['jours_pluvieux'] = 0

        # Colonnes finales
        columns = ['ville', 'time', 'temp_moyenne', 'temp_min', 'temp_max',
                   'humidite_moyenne', 'precipitation_totale', 'vent_moyen', 'jours_pluvieux']
        columns_existantes = [col for col in columns if col in df.columns]
        df = df[columns_existantes]
        df.rename(columns={'time': 'date'}, inplace=True)

        # Export CSV
        output_file = f"{output_dir}/historical_weather_{start_date}_to_{end_date}.csv"
        df.to_csv(output_file, index=False)
        print(f"Données sauvegardées pour {city} dans {output_file}")

    except Exception as e:
        print(f"Erreur pour {city}: {e}")
