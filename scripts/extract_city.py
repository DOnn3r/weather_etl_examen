import os
import requests
import pandas as pd
from datetime import datetime
import logging

def extract_city(city: str, country: str, api_key: str, date: str) -> bool:
    """
    Extrait les coordonnées géographiques (lat, lon) d'une ville via l'API OpenWeather
    
    Args:
        city (str): Nom de la ville
        country (str): Code pays ISO (ex: "FR")
        api_key (str): Clé API OpenWeather
        date (str): Date d'extraction (format YYYY-MM-DD)
        
    Returns:
        bool: True si l'extraction réussit, False sinon
    """
    try:
        url = "http://api.openweathermap.org/geo/1.0/direct"
        params = {
            "q": f"{city},{country}",
            "limit": 1,
            "appid": api_key
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data:
            lat = data[0]["lat"]
            lon = data[0]["lon"]
            city_data = {
                "ville": city,
                "pays": country,
                "latitude": lat,
                "longitude": lon,
                "date_extraction": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            # Créer le dossier de sauvegarde
            output_dir = f"data/raw/{date}"
            os.makedirs(output_dir, exist_ok=True)

            # Sauvegarder en CSV
            output_file = os.path.join(output_dir, f"geo_{city}.csv")
            pd.DataFrame([city_data]).to_csv(output_file, index=False)

            logging.info(f"Coordonnées extraites pour {city}, sauvegardées dans {output_file}")
            return True
        else:
            logging.warning(f"Aucun résultat pour {city}, {country}")
            return False

    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur réseau/API pour {city}: {str(e)}")
    except KeyError as e:
        logging.error(f"Champ manquant dans la réponse pour {city}: {str(e)}")
    except Exception as e:
        logging.error(f"Erreur inattendue pour {city}: {str(e)}")

    return False
