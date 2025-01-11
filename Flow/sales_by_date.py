from datetime import datetime, timedelta
import sys
sys.path.append('/home/snparada/Spacionatural/Libraries')
from flow_lib.payments import FlowPayment
from pprint import pprint

def get_extended_payment_details(flow_orders: list) -> None:
    """
    Obtiene y muestra los detalles extendidos de pagos para una lista de órdenes Flow
    """
    flow = FlowPayment(sandbox=False)
    
    print("\n=== Detalles Extendidos de Pagos ===")
    for order in flow_orders:
        try:
            payment_details = flow.read_payment_by_flow_order_extended(order)
            pprint(payment_details)
            exit()
            print(f"\nOrden Flow #{order}:")
            print(f"- Estado: {payment_details.get('status', 'N/A')}")
            print(f"- Monto: {payment_details.get('amount', 'N/A')} {payment_details.get('currency', 'CLP')}")
            print(f"- Medio de Pago: {payment_details.get('paymentData', {}).get('media', 'N/A')}")
            print(f"- Fecha: {payment_details.get('date', 'N/A')}")
            print(f"- Orden Comercio: {payment_details.get('commerceOrder', 'N/A')}")
        except Exception as e:
            print(f"Error al obtener detalles de orden #{order}: {str(e)}")

def get_orders_by_date_range(start_date: datetime, end_date: datetime) -> list:
    """
    Obtiene todos los pagos Flow entre las fechas especificadas
    """
    flow = FlowPayment(sandbox=False)
    flow_orders = []
    current_date = start_date
    
    while current_date <= end_date:
        print(f"\nObteniendo pagos para {current_date.strftime('%Y-%m-%d')}")
        
        # Obtener pagos con paginación
        start = 0
        has_more = True
        
        while has_more:
            payments = flow.read_payments_by_date(current_date, start=start, limit=100)
            for payment in payments.get('data', []):
                flow_orders.append(payment.get('flowOrder'))
            
            has_more = payments.get('hasMore', False)
            start += 100
            
        current_date += timedelta(days=1)
    
    return flow_orders

def main():
    # Definir rango de fechas (agosto a noviembre 2024)
    start_date = datetime(2024, 8, 1)
    end_date = datetime(2024, 11, 30)
    
    # Obtener todas las órdenes en el rango de fechas
    flow_orders = get_orders_by_date_range(start_date, end_date)
    print(f"\nSe encontraron {len(flow_orders)} pagos en total")
    
    # Obtener detalles extendidos de cada orden
    get_extended_payment_details(flow_orders)

if __name__ == "__main__":
    main()
