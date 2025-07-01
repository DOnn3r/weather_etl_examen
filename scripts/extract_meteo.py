import os
import logging
import requests
import pandas as pd
from datetime import datetime

def extract_meteo(city: str, api_key: str, date: str) -> bool:
    """
    Extrait la météo actuelle en utilisant les coordonnées lues depuis geo_{ville}.csv
    """
    try:
        geo_path = f"data/raw/{date}/geo_{city}.csv"
        if not os.path.exists(geo_path):
            logging.error(f"Fichier de coordonnées manquant pour {city}")
            return False

        geo_df = pd.read_csv(geo_path)
        lat = geo_df.at[0, 'latitude']
        lon = geo_df.at[0, 'longitude']

        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': api_key,
            'units': 'metric',
            'lang': 'fr'
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        weather_data = {
            'ville': city,
            'date_extraction': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'temperature': data['main']['temp'],
            'humidite': data['main']['humidity'],
            'pression': data['main']['pressure'],
            'description': data['weather'][0]['description']
        }

        os.makedirs(f"data/raw/{date}", exist_ok=True)
        pd.DataFrame([weather_data]).to_csv(
            f"data/raw/{date}/meteo_{city}.csv", index=False
        )
        return True

    except Exception as e:
        logging.error(f"Erreur pour {city}: {e}")
        return False
