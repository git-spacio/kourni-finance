import os
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timedelta
import pandas as pd

def read_data_from_sheet(sheet_id, sheet_range, creds):
    try:
        # Crea un servicio de Google Sheets con las credenciales proporcionadas
        service = build("sheets", "v4", credentials=creds)

        # Lee los datos de la hoja de cálculo
        result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=sheet_range).execute()
        values = result.get("values", [])

        # Convierte los datos leídos en un DataFrame de Pandas
        if values:
            df = pd.DataFrame(values[1:], columns=values[0])
            return df
        else:
            print("No data found.")
            return None
    except HttpError as error:
        print(f"An error occurred: {error}")
        return None

def authorize(credentials_file):
    creds = None
    if os.path.exists(credentials_file):
        creds = service_account.Credentials.from_service_account_file(
            credentials_file, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
    return creds

def extract_new_data(df_old, df_new):
    # Identificar los registros en df_new que no están en df_old utilizando la columna "Id"
    nuevos_registros = df_new[~df_new['Id'].isin(df_old['Id'])]

    # Reiniciar el índice del DataFrame resultante
    nuevos_registros = nuevos_registros.reset_index(drop=True)

    return nuevos_registros

def append_data_to_sheet(sheet_id, sheet_range, data, creds):
    try:
        # Crea un servicio de Google Sheets con las credenciales proporcionadas
        service = build("sheets", "v4", credentials=creds)

        # Convierte los datos de la API en una lista de listas
        values = [data.columns.tolist()] + data.values.tolist()

        # Crea el cuerpo del mensaje en formato JSON
        body = {"values": values[1:]}

        # Encuentra la primera fila vacía en la columna C
        result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range="Consolidado!A:A").execute()
        first_empty_row = len(result.get("values", [])) + 1
        sheet_range = f"Consolidado!A{first_empty_row}"

        # Agrega los datos a la hoja de cálculo
        result = (
            service.spreadsheets()
            .values()
            .append(
                spreadsheetId=sheet_id,
                range=sheet_range,
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body=body,
            )
            .execute()
        )
    # Captura y maneja cualquier excepción HttpError
    except HttpError as error:
        print(f"An error occurred: {error}")
        return None
    
def convert_date_columns_to_string(df):
    for column in df.columns:
        if df[column].dtype == 'datetime64[ns]':
            df[column] = df[column].dt.strftime('%Y-%m-%d')
    return df

def format_date_columns(df, columns, date_format):
    for col in columns:
        if df[col].dtype == 'datetime64[ns]':
            df[col] = df[col].apply(lambda x: x.strftime(date_format) if pd.notnull(x) else '-')
        elif df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: pd.to_datetime(x, errors='coerce').strftime(date_format) if pd.notnull(pd.to_datetime(x, errors='coerce')) else '-')
    return df



