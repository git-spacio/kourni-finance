from bs4 import BeautifulSoup
from selenium import webdriver
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date
from selenium.webdriver.chrome.options import Options
import csv
import os

chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--headless")

driver = webdriver.Chrome(options=chrome_options)

url = "https://www.google.com/finance/quote/CLF-CLP"


try:
    driver.get(url)
    time.sleep(5)  # Espera a que se cargue la página
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    div = soup.find('div', {'class': 'YMlKec fxKbKc'})
    
    if div:
        value = div.text
        
        # Preparar las credenciales
        scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("/home/snparada/Spacionatural/Libraries/sheets_lib/creds/gs_credentials.json", scope)
        gs_client = gspread.authorize(creds)

        # Abrir el Google Sheets y seleccionar la primera hoja
        sheet = gs_client.open_by_key('1pt8-AhET3xT3y-38OD90YZw7HpaRR7DwVqCzj0JIDx8').sheet1

        # Obtener la fecha de hoy
        today = date.today().strftime("%d/%B/%y")  # Formato: "01/January/23"
        
        # Obtener todas las fechas de la primera columna
        existing_dates = sheet.col_values(1)
        
        # Obtener todos los valores de Google Sheets
        all_values = sheet.get_all_values()
        
        # Guardar todos los valores en CSV de forma limpia
        csv_path = "/home/snparada/Spacionatural/Data/Historical/Finance/uf_value_by_day.csv"
        
        # Crear el directorio si no existe
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        
        # Escribir todos los valores en el CSV
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Fecha', 'Valor UF'])  # Headers
            
            for row in all_values[1:]:  # Saltamos la primera fila (headers)
                date = row[0]
                # Limpiar el valor (quitar $, comas, puntos y comillas simples)
                clean_value = row[1].replace("$", "").replace(",", "").replace(".", "").replace("'", "").strip()
                writer.writerow([date, clean_value])

        # Continuar con la lógica de agregar nuevo valor si no existe
        if today not in existing_dates:
            formatted_value = "$" + value.split(".")[0]
            sheet_row = [today, formatted_value]
            sheet.append_row(sheet_row)
        else:
            print(f"Ya existe un registro para {today}, no se agregará un duplicado")
    else:
        print('No se encontró el valor de la UF')

except Exception as e:
    # Enviar mensaje a Slack
    print(f"Error: {str(e)}")
finally:
    # Cerrar el navegador
    driver.quit()