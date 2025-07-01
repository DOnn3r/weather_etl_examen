import pandas as pd
import os

def merge_files(date: str) -> str:
    """Ajoute les nouvelles données au fichier global existant"""
    input_dir = f"data/raw/{date}"
    output_file = "data/processed/meteo_examen.csv"
    
    # Créer le dossier processed si inexistant
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Charger les données existantes (si fichier existe)
    if os.path.exists(output_file):
        global_df = pd.read_csv(output_file)
    else:
        global_df = pd.DataFrame()
    
    # Lire les nouveaux fichiers
    new_data = []
    for file in os.listdir(input_dir):
        if file.startswith('meteo_') and file.endswith('.csv'):
            new_data.append(pd.read_csv(f"{input_dir}/{file}"))
    
    if not new_data:
        raise ValueError(f"Aucune nouvelle donnée à fusionner pour {date}")
    
    # Concaténation et déduplication
    updated_df = pd.concat([global_df] + new_data, ignore_index=True)
    updated_df = updated_df.drop_duplicates(
        subset=['ville', 'date_extraction'],  # Clé unique
        keep='last'                          # Garde la dernière version
    )
    
    # Sauvegarde
    updated_df.to_csv(output_file, index=False)
    return output_file