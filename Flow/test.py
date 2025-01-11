import sys
sys.path.append('/home/snparada/Spacionatural/Libraries')

from shopify_lib.orders import ShopifyOrders
from pprint import pprint
from datetime import datetime
import json

def extract_order_details():
    # Inicializar el cliente de Shopify
    shopify = ShopifyOrders()
    start_date=datetime(2024, 12, 23)
    end_date=datetime(2024, 12, 31)
    new_orders = shopify.read_all_orders_by_date(start_date, end_date)
    
    # Extraer número de pedido y commerceOrder
    order_details = []
    for order in new_orders:
        payment_gateways = order.get('payment_gateway_names', [])
        financial_status = order.get('financial_status', '')
        
        if 'flow' in [gateway.lower() for gateway in payment_gateways] and financial_status == 'paid':
            emails = list(set([order['contact_email'].lower(), order['email'].lower()]))
            order_details.append({
                'order_number': order['order_number'],
                'commerce_order': order.get('source_identifier', None),
                'email': emails[0] if len(emails) == 1 else emails,
                'total': order['current_total_price'],
            })
            
    return order_details
pprint(extract_order_details())