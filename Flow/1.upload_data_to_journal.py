from datetime import datetime, timedelta
import sys
sys.path.append('/home/snparada/Spacionatural/Libraries')
from shopify_lib.orders import ShopifyOrders
from flow_lib.payments import FlowPayment
import pandas as pd
from odoo_lib.journal import OdooJournal

def A_process_flow_payments(start_date: datetime, end_date: datetime) -> list:
    """
    Procesa los pagos de Flow entre las fechas especificadas y filtra los que tienen status 2
    """
    flow = FlowPayment(sandbox=False)
    pagos_filtrados = []
    
    # Iterar por cada día en el rango
    current_date = start_date
    while current_date <= end_date:
        # Obtener pagos para el día actual
        flow_payments = flow.read_payments_by_date(current_date)
        
        # Procesar los pagos del día
        for pago in flow_payments['data']:
            if pago['status'] == 2:
                pago_filtrado = {
                    'amount': pago['amount'],
                    'commerceOrder': pago['commerceOrder'],
                    'email': pago['payer'].lower(),
                    'balance': pago['paymentData']['balance'],
                    'fee': pago['paymentData']['fee'],
                    'tax_fee': pago['paymentData']['taxes'],
                    'media': pago['paymentData']['media'],
                    'date': pago['paymentData']['date'],
                    'transferDate': pago['paymentData']['transferDate']
                }
                pagos_filtrados.append(pago_filtrado)
        
        # Avanzar al siguiente día
        current_date += timedelta(days=1)
    
    return pagos_filtrados

def B_extract_order_details(start_date, end_date):
    # Inicializar el cliente de Shopify
    shopify = ShopifyOrders()
    new_orders = shopify.read_all_orders_by_date(start_date, end_date)
    
    # Extraer número de pedido y commerceOrder
    order_details = []
    for order in new_orders:
        payment_gateways = order.get('payment_gateway_names', [])
        financial_status = order.get('financial_status', '')
        
        if 'flow' in [gateway.lower() for gateway in payment_gateways] and financial_status == 'paid':
            emails = list(set([order['contact_email'].lower(), order['email'].lower()]))
            
            # Calcular el total si current_total_price no está disponible
            total = order.get('current_total_price')
            if not total:
                subtotal = float(order.get('current_subtotal_price', 0))
                discounts = float(order.get('current_total_discounts', 0))
                tax = float(order.get('current_total_tax', 0))
                shipping = float(order.get('total_shipping_price_set', {}).get('presentment_money', {}).get('amount', 0))
                total = subtotal - discounts + tax + shipping
            
            order_details.append({
                'order_number': order['order_number'],
                'commerce_order': order.get('source_identifier', None),
                'email': emails[0] if len(emails) == 1 else emails,
                'total': float(total),
            })
            
    return order_details

def C_merge_flow_shopify(pagos_flow: list, ordenes_shopify: list) -> pd.DataFrame:
    """
    Combina los pagos de Flow con las órdenes de Shopify usando email y monto como llaves
    
    Args:
        pagos_flow: Lista de diccionarios con los pagos de Flow
        ordenes_shopify: Lista de diccionarios con las órdenes de Shopify
    
    Returns:
        DataFrame con los pagos y órdenes combinados
    """
    # Convertir listas a DataFrames
    df_flow = pd.DataFrame(pagos_flow)
    df_shopify = pd.DataFrame(ordenes_shopify)
    
    # Asegurar que los montos estén en el mismo formato
    df_flow['amount'] = df_flow['amount'].astype(float)
    df_shopify['total'] = df_shopify['total'].astype(float)
    
    # Hacer el merge usando email y monto
    df_merged = pd.merge(
        df_flow,
        df_shopify,
        left_on=['email', 'amount'],
        right_on=['email', 'total'],
        how='outer',
        indicator=True
    )
    
    # Agregar columna que indique si hubo match
    df_merged['matched'] = df_merged['_merge'].map({
        'both': 'OK',
        'left_only': 'Solo en Flow',
        'right_only': 'Solo en Shopify'
    })
    
    # Limpiar y ordenar columnas
    df_merged = df_merged.drop(['_merge'], axis=1)
    df_merged = df_merged[['date', 'transferDate', 'email', 'amount', 'total', 'matched', 
                          'order_number', 'commerceOrder', 'fee', 'tax_fee']]

    
    return df_merged

def D_filtering_data(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra y formatea los datos del DataFrame para obtener una tabla con:
    - Fecha (date en formato YYYY-MM-DD, fecha de aprobación del pago)
    - Etiqueta (order_number + commerceOrder)
    - Importe (amount o total)
    """
    # Crear una copia para no modificar el DataFrame original
    df_filtered = df_merged.copy()
    df_filtered = df_filtered[df_filtered['matched']=='OK']
    
    # Asegurarnos de que las columnas existan y manejar valores nulos
    if 'order_number' not in df_filtered.columns:
        df_filtered['order_number'] = ''
    if 'commerceOrder' not in df_filtered.columns:
        df_filtered['commerceOrder'] = ''
    
    # Convertir order_number a entero si es posible y luego a string
    df_filtered['order_number'] = pd.to_numeric(df_filtered['order_number'], errors='coerce').fillna(0).astype(int)
        
    # Convertir order_number a string después del filtrado
    df_filtered['order_number'] = df_filtered['order_number'].astype(str)
    
    # Crear la columna Etiqueta combinando order_number y commerceOrder
    df_filtered['Etiqueta'] = df_filtered['order_number'].fillna('').astype(str) + ' - ' + \
                             df_filtered['commerceOrder'].fillna('').astype(str)
    
    # Usar 'amount' cuando 'total' es nulo y viceversa
    df_filtered['Importe'] = df_filtered['total'].fillna(df_filtered['amount'])
    
    # Convertir el importe a entero, redondeando primero
    df_filtered['Importe'] = df_filtered['Importe'].round().fillna(0).astype(int)
    
    # Convertir date (fecha de aprobación) a datetime y formatear como YYYY-MM-DD
    df_filtered['date'] = pd.to_datetime(df_filtered['date']).dt.strftime('%Y-%m-%d')
    
    # Seleccionar y renombrar las columnas necesarias
    df_result = df_filtered[['date', 'Etiqueta', 'Importe']].rename(
        columns={
            'date': 'Fecha'
        }
    )
    
    return df_result

def main():
    start_date = datetime(2024, 12, 2)
    end_date = datetime(2024, 12, 11)
    
    print("\n1. Procesando pagos de Flow...")
    pagos_filtrados = A_process_flow_payments(start_date, end_date)
    print(f"Pagos encontrados en Flow: {len(pagos_filtrados)}")
    if pagos_filtrados:
        print("Ejemplo primer pago:", pagos_filtrados[0])
    
    print("\n2. Extrayendo órdenes de Shopify...")
    ordenes_shopify = B_extract_order_details(
        start_date - timedelta(days=1), 
        end_date + timedelta(days=1)
    )
    print(f"Órdenes encontradas en Shopify: {len(ordenes_shopify)}")
    if ordenes_shopify:
        print("Ejemplo primera orden:", ordenes_shopify[0])
    
    print("\n3. Combinando datos...")
    df_resultado = C_merge_flow_shopify(pagos_filtrados, ordenes_shopify)
    print("Shape del DataFrame combinado:", df_resultado.shape)
    print("\nDistribución de matches:")
    print(df_resultado['matched'].value_counts())
    
    print("\n4. Aplicando filtrado final...")
    df_final = D_filtering_data(df_resultado)
    print("Shape del DataFrame final:", df_final.shape)
    if not df_final.empty:
        print("\nPrimeras 3 filas del DataFrame final:")
        print(df_final.head(3))
    
    print("\n5. Creando entradas en el diario...")
    odoo = OdooJournal()
    resultado = odoo.create_journal_entries('FLOW', df_final)
    print("Resultado de la creación:", resultado)

if __name__ == "__main__":
    main()