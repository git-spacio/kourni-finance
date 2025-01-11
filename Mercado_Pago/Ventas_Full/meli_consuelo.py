# ... existing code ...

# 1. Crear una copia del DataFrame original
df_copy = df.copy()

# 2. Realizar la agrupación
df_agrupado = df_copy.groupby(['fecha', 'codigo']).agg({
    'quantity': 'sum',
    'full_unit_price': 'first',
    'shipping_cost_buyer': 'sum',
    'shipping_cost_seller': 'sum',
    'sale_fee': 'sum',
    'shipping_logistic.type': 'first',
    'shipping_external_reference': 'first'
}).reset_index()

# 3. Ordenar por fecha
df_agrupado = df_agrupado.sort_values('fecha')

# 4. Verificar los resultados antes de actualizar
print("Forma del DataFrame original:", df.shape)
print("Forma del DataFrame agrupado:", df_agrupado.shape)

# 5. Guardar en una nueva hoja para no sobrescribir los datos originales
try:
    worksheet = spreadsheet.add_worksheet(title="Datos_Agrupados", rows=df_agrupado.shape[0], cols=df_agrupado.shape[1])
    worksheet.update([df_agrupado.columns.values.tolist()] + df_agrupado.values.tolist())
    print("Datos guardados exitosamente en nueva hoja 'Datos_Agrupados'")
except Exception as e:
    print(f"Error al guardar: {e}") 