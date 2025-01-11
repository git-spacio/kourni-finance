import pandas as pd
import sys
sys.path.append('/home/snparada/Spacionatural/Libraries/sheets_lib')
from main_sheets import GoogleSheets

# Leer el archivo CSV de ventas
df = pd.read_csv('/home/snparada/Spacionatural/Data/Historical/Finance/historic_sales_with_items.csv', low_memory=False)

# Filtrar los SKUs de envío
shipping_skus = ['6911', '6889', '6912']
df = df[~df['items_product_sku'].isin(shipping_skus)]

# Calcular el total neto real por ítem
df['real_net_total'] = (df['items_quantity'] * df['items_unitPrice']) / 1.19

# Agrupar por fecha y sumar real_net_total
sales_by_day = df.groupby('issuedDate')['real_net_total'].sum().reset_index()

# Ordenar por fecha
sales_by_day = sales_by_day.sort_values('issuedDate')

# Leer datos de UF desde Google Sheets
gs = GoogleSheets('1pt8-AhET3xT3y-38OD90YZw7HpaRR7DwVqCzj0JIDx8')
uf_data = gs.read_dataframe('Consolidado')

# Limpiar la columna Precio_uf
uf_data['Precio_uf'] = uf_data['Precio_uf'].astype(str).str.replace('$', '').str.replace(',', '').str.replace("'", '').astype(float)

# Convertir la columna de fecha en uf_data al formato correcto
uf_data['Fecha'] = pd.to_datetime(uf_data['Fecha'], format='%d/%B/%y').dt.strftime('%Y-%m-%d')

# Hacer el merge de los dataframes
result = pd.merge(sales_by_day, 
                 uf_data,
                 left_on='issuedDate',
                 right_on='Fecha',
                 how='left')

# Calcular totals_net_uf
result['totals_net_uf'] = result['real_net_total'] / result['Precio_uf']

# Seleccionar solo las columnas necesarias y renombrar para mantener consistencia
final_df = result[['issuedDate', 'real_net_total', 'totals_net_uf']]
final_df = final_df.rename(columns={'real_net_total': 'totals_net'})

# Guardar el DataFrame en CSV
final_df.to_csv('/home/snparada/Spacionatural/Data/Historical/Finance/historic_sales_by_day_UF.csv', index=False)

