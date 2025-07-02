from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable

# ==== Imports de tes fonctions ====
from weather_examen_pipeline.scripts.extract_city import extract_city
from weather_examen_pipeline.scripts.extract_meteo import extract_meteo
from weather_examen_pipeline.scripts.extract_all_historic import extract_all_historic
from weather_examen_pipeline.scripts.merge import merge_files
from weather_examen_pipeline.scripts.transform import transform_to_star
from weather_examen_pipeline.scripts.generate_indicator import generate_indicateurs
from weather_examen_pipeline.scripts.export_to_gsheet import export_csv_to_gsheet
from weather_examen_pipeline.scripts.merge_historical import merge_all_historic_to_csv
from weather_examen_pipeline.scripts.export_to_gsheet import export_csv_to_gsheet


# ==== Configuration du DAG ====
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 30),
}

# Liste des villes à traiter (ville, code pays)
CITIES = [
    ("Paris", "FR"),
    ("London", "GB"),
    ("Tokyo", "JP"),
    ("New York", "US"),
    ("Sydney", "AU")
]

with DAG(
    'weather_etl_examen_pipeline',
    default_args=default_args,
    schedule='0 8 * * *',
    catchup=False,
    max_active_runs=1,
    tags=["weather", "etl", "openweather", "meteostat"]
) as dag:

    # ==== Tâches d'extraction des coordonnées (geo_*.csv) ====
    extract_city_tasks = [
        PythonOperator(
            task_id=f"extract_city_{city.lower().replace(' ', '_')}",
            python_callable=extract_city,
            op_args=[city, country, "{{ var.value.API_KEY }}", "{{ ds }}"]
        )
        for city, country in CITIES
    ]

    # ==== Tâches d'extraction météo actuelle ====
    extract_meteo_tasks = [
        PythonOperator(
            task_id=f"extract_meteo_{city.lower().replace(' ', '_')}",
            python_callable=extract_meteo,
            op_args=[city, "{{ var.value.API_KEY }}", "{{ ds }}"]
        )
        for city, _ in CITIES
    ]

    # ==== Tâche d'extraction des historiques via Meteostat ====
    extract_historic_task = PythonOperator(
    task_id="extract_all_historic",
    python_callable=extract_all_historic,
    op_args=["{{ ds }}"]
    )

    # ==== Tâche de fusion des fichiers ====
    merge_task = PythonOperator(
        task_id='merge_all_data',
        python_callable=merge_files,
        op_args=["{{ ds }}"]
    )

    # ==== Tâche de transformation en schéma en étoile ====
    transform_task = PythonOperator(
        task_id='transform_to_star',
        python_callable=transform_to_star
    )

    # ==== Orchestration ====
    # Lier chaque ville : extract_city >> extract_meteo
    for city_task, meteo_task in zip(extract_city_tasks, extract_meteo_tasks):
        city_task >> meteo_task

    # Une fois toutes les meteo finies, on lance le reste
    extract_meteo_tasks >> extract_historic_task
    extract_historic_task >> merge_task >> transform_task 

    generate_indicateurs_task = PythonOperator(
    task_id='generate_indicateurs',
    python_callable=generate_indicateurs
    )

    export_fact_weather_task = PythonOperator(
    task_id='export_fact_weather',
    python_callable=export_csv_to_gsheet,
    op_args=["data/star_schema/fact_weather.csv", "Weather_examen", "Faits_meteo"]
    )

    export_indicateurs_task = PythonOperator(
    task_id='export_indicateurs',
    python_callable=export_csv_to_gsheet,
    op_args=["data/indicateurs/indicateurs_ville.csv", "Weather_examen", "Indicateurs"]
    )

    merge_historic_task = PythonOperator(
    task_id="merge_historic_data",
    python_callable=merge_all_historic_to_csv
    )

    export_historic_task = PythonOperator(      
    task_id="export_historic_to_gsheet",
    python_callable=export_csv_to_gsheet,
    op_args=["data/processed/historical_all.csv", "Weather_examen", "Historique"]
    )

    transform_task >> generate_indicateurs_task >> [export_fact_weather_task, export_indicateurs_task]
    generate_indicateurs_task >> merge_historic_task >> export_historic_task