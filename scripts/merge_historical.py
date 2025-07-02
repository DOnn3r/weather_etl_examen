import os
import pandas as pd

def merge_all_historic_to_csv() -> str:
    """
    Fusionne tous les fichiers historiques Meteostat dans un seul fichier CSV global.
    """
    input_dir = "data/raw/historical"
    output_file = "data/processed/historical_all.csv"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    all_data = []

    for city in os.listdir(input_dir):
        city_path = os.path.join(input_dir, city)
        if os.path.isdir(city_path):
            for file in os.listdir(city_path):
                if file.startswith("historical_weather_") and file.endswith(".csv"):
                    full_path = os.path.join(city_path, file)
                    df = pd.read_csv(full_path)
                    all_data.append(df)

    if not all_data:
        raise ValueError("Aucune donnée historique trouvée.")

    df_all = pd.concat(all_data, ignore_index=True)
    df_all.to_csv(output_file, index=False)
    print(f"[OK] Fichier global sauvegardé : {output_file}")
    return output_file
