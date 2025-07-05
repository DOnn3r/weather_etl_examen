import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np

def export_csv_to_gsheet(csv_path: str, sheet_name: str, tab_name: str):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "/home/donner/airflow/credentials/alien-segment-464610-h0-fc7c19d19925.json", scope)
    client = gspread.authorize(creds)

    # Ouvre ou crée le Google Sheet
    try:
        sheet = client.open(sheet_name)
    except gspread.SpreadsheetNotFound:
        sheet = client.create(sheet_name)

    # Crée ou réutilise l'onglet
    try:
        worksheet = sheet.worksheet(tab_name)
        worksheet.clear()  # On vide l'onglet au lieu de le supprimer
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=tab_name, rows="1000", cols="20")

    # Charger les données CSV
    df = pd.read_csv(csv_path)

    # Nettoyage des données pour compatibilité JSON
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df = df.fillna("")

    # Exporter dans la feuille
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())
    print(f"Données exportées vers Google Sheets > {sheet_name} > {tab_name}")