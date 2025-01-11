import sys
sys.path.append('/home/snparada/Spacionatural/Finance/Purchase_Management/')
from dataFunctions import read_data_from_sheet, authorize, extract_new_data, append_data_to_sheet, convert_date_columns_to_string, format_date_columns
from apiLaudusFunctions import get_invoice_data
import json
import traceback


token_file='Libraries/laudus_lib/creds/laudusToken.json'
CREDS_FILE = "/home/snparada/Spacionatural/Libraries/sheets_lib/creds/gs_credentials.json"

with open(token_file, 'r') as f:
        token = json.load(f)['token']

headers = {
    "Authorization": "Bearer "+ token,
    "Content-type": "application/json",
    "Accept": "application/json"
}

body = {
  "options": {
    "offset": 0,
    "limit": 0
  },
  "fields": [
    "monthlyPosition", "fiscalPeriod", "supplier.name", "supplier.supplierId", "docType.name", "docNumber","supplier.VATId", 'dueDate',
    "issuedDate", "totalsOriginalCurrency.net","totalsOriginalCurrency.total", "createdBy.name","purchaseInvoiceId", "nullDoc",'costCenterAllocation.costCenter.name',
  ],
  "filterBy": [
    {
      "field": "issuedDate",
      "operator": ">=",
      "value": "2024-03-03T00:27:05.467Z"
    }
  ]
}

# Get the new data trough the Laudus API 
df_new = get_invoice_data(headers, body)

# Look for the actual data
creds = authorize(CREDS_FILE)
sheet_id = '1Euj1NSL0hz8NUYWd0UYw25RV2k_lubIkjeAFMijKSXY'
sheet_range= 'Consolidado!A:Y'
df_old = read_data_from_sheet(sheet_id,sheet_range, creds)

#Merge actual data
# Convierte la columna 'Id' en df_old y df_new a int
df_old['Id'] = df_old['Id'].astype(int)

if not df_new.empty:

  df_new['Id'] = df_new['Id'].astype(int)

  # Llama a extract_new_data() después de convertir la columna 'Id'
  new_data = extract_new_data(df_old, df_new)


  # Append the new data into the sheets
  # Convert the date columns to the desired format
  date_columns = ['Fecha Emisión', 'Vence', 'Semana de Pago']
  date_format = '%d/%B/%y'
  formatted_new_data = format_date_columns(new_data, date_columns, date_format)
  new_data = convert_date_columns_to_string(new_data)

  append_data_to_sheet(sheet_id,sheet_range, new_data, creds)

