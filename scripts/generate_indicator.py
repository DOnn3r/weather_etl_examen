import os
import pandas as pd

def generate_indicateurs():
    """
    Fusionne tous les fichiers historiques pour produire un résumé par ville
    """
    input_dir = "data/raw/historical"
    output_file = "data/indicateurs/indicateurs_ville.csv"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    all_data = []

    for city in os.listdir(input_dir):
        city_path = os.path.join(input_dir, city)
        if not os.path.isdir(city_path):
            continue

        for file in os.listdir(city_path):
            if file.startswith("historical_weather_") and file.endswith(".csv"):
                path = os.path.join(city_path, file)
                try:
                    df = pd.read_csv(path)
                    if not df.empty:
                        all_data.append(df)
                    else:
                        print(f"[!] Fichier vide ignoré : {file}")
                except Exception as e:
                    print(f"[!] Erreur lors de la lecture de {file} : {e}")

    if not all_data:
        raise ValueError("Aucune donnée valide trouvée dans les fichiers historiques. Vérifiez les exports Meteostat.")

    df_all = pd.concat(all_data)

    # Grouper par ville et résumer
    resume = df_all.groupby("ville").agg({
        "temp_moyenne": "mean",
        "jours_pluvieux": "sum",
        "precipitation_totale": "sum"
    }).reset_index()

    resume.columns = ["ville", "temp_moyenne_moy", "nb_jours_pluvieux", "precip_total"]
    resume.to_csv(output_file, index=False)
    print(f"Indicateurs exportés vers {output_file}")
