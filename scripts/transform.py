import pandas as pd
import os

def transform_to_star() -> str:
    """
    Transforme les données météo en schéma en étoile simplifié avec :
    - Une table de faits (mesures météo)
    - Une dimension ville (référence commune)
    
    Returns:
        str: Chemin vers le fichier des faits généré
    """
    
    # 1. Configuration des chemins
    input_file = "data/processed/meteo_examen.csv"  # Fichier source
    output_dir = "data/star_schema"                # Dossier de sortie
    os.makedirs(output_dir, exist_ok=True)         # Crée le dossier si besoin
    
    # 2. Chargement des données brutes
    meteo_data = pd.read_csv(input_file)
    
    # 3. Gestion de la dimension Ville
    dim_ville_path = f"{output_dir}/dim_ville.csv"
    
    # Charger ou initialiser la dimension
    if os.path.exists(dim_ville_path):
        dim_ville = pd.read_csv(dim_ville_path)
    else:
        dim_ville = pd.DataFrame(columns=['ville_id', 'ville'])
    
    # Identifier les nouvelles villes
    villes_existantes = set(dim_ville['ville'])
    nouvelles_villes = set(meteo_data['ville']) - villes_existantes
    
    # Ajouter les nouvelles villes avec des IDs incrémentaux
    if nouvelles_villes:
        nouveau_id = dim_ville['ville_id'].max() + 1 if not dim_ville.empty else 1
        nouvelles_lignes = pd.DataFrame({
            'ville_id': range(nouveau_id, nouveau_id + len(nouvelles_villes)),
            'ville': list(nouvelles_villes)
        })
        dim_ville = pd.concat([dim_ville, nouvelles_lignes], ignore_index=True)
        dim_ville.to_csv(dim_ville_path, index=False)  # Sauvegarde
    
    # 4. Création de la table de faits
    faits_meteo = meteo_data.merge(
        dim_ville,
        on='ville',
        how='left'
    ).drop(columns=['ville'])
    
    # 5. Sauvegarde des faits
    faits_path = f"{output_dir}/fact_weather.csv"
    faits_meteo.to_csv(faits_path, index=False)
    
    return faits_path