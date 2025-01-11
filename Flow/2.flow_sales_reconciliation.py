from datetime import datetime
import sys
sys.path.append('/home/snparada/Spacionatural/Libraries')
import pandas as pd
from odoo_lib.journal import OdooJournal

odoo_journal = OdooJournal()

def filter_account_moves_by_order(self, order_number, amount):
    """
    Filtra asientos contables basados en número de orden e importe
    
    Args:
        order_number (str): Número de orden a buscar
        amount (float): Importe a comparar
        
    Returns:
        list: Lista de asientos contables que coinciden con los criterios
    """
    domain = [
        ('name', 'ilike', order_number),
        ('debit', '=', float(abs(amount))) if amount > 0 else ('credit', '=', float(abs(amount))),
        ('journal_id.name', '=', 'FLOW'),
        ('reconciled', '=', False),
        ('parent_state', '=', 'posted')
    ]
    
    return self.models.execute_kw(
        self.db, self.uid, self.password,
        'account.move.line', 'search_read',
        [domain],
        {'fields': ['id', 'name', 'debit', 'credit', 'date']}
    )

def merge_bank_statement_with_moves(self, statement_line_id):
    """
    Concilia una línea de extracto bancario con su asiento contable correspondiente
    y muestra una tabla con los detalles de la conciliación
    
    Args:
        statement_line_id (int): ID de la línea de extracto bancario
        
    Returns:
        str: Mensaje indicando el resultado de la conciliación
    """
    try:
        # Obtener detalles de la línea de extracto
        statement_line = self.models.execute_kw(
            self.db, self.uid, self.password,
            'account.bank.statement.line', 'search_read',
            [[('id', '=', statement_line_id)]],
            {'fields': ['date', 'payment_ref', 'amount']}
        )[0]
        
        # Extraer el número de orden de la etiqueta
        order_number = statement_line['payment_ref'].split(' - ')[0].strip()
        
        # Buscar asientos contables coincidentes
        matching_moves = self.filter_account_moves_by_order(order_number, statement_line['amount'])
        
        if not matching_moves:
            return "No se encontraron asientos contables coincidentes"
            
        # Filtrar por fecha
        matching_moves = [
            move for move in matching_moves 
            if move['date'] == statement_line['date']
        ]
        
        if not matching_moves:
            return "No se encontraron asientos con la misma fecha"
            
        # Crear DataFrame para mostrar la conciliación
        reconciliation_data = {
            'Fecha': [statement_line['date']],
            'Nro Orden': [order_number],
            'Referencia': [statement_line['payment_ref']],
            'Importe': [statement_line['amount']],
            'ID Asiento': [matching_moves[0]['id']],
            'Nombre Asiento': [matching_moves[0]['name']]
        }
        
        df_reconciliation = pd.DataFrame(reconciliation_data)
        print("\nDetalles de la conciliación:")
        print("-" * 100)  # Línea separadora
        print(f"{'Fecha':<12} {'Nro Orden':<10} {'Referencia':<30} {'Importe':>10} {'ID Asiento':>10} {'Nombre Asiento':<20}")
        print("-" * 100)  # Línea separadora
        print(f"{df_reconciliation['Fecha'].iloc[0]:<12} "
              f"{df_reconciliation['Nro Orden'].iloc[0]:<10} "
              f"{df_reconciliation['Referencia'].iloc[0]:<30} "
              f"{df_reconciliation['Importe'].iloc[0]:>10.2f} "
              f"{df_reconciliation['ID Asiento'].iloc[0]:>10} "
              f"{df_reconciliation['Nombre Asiento'].iloc[0]:<20}")
        print("-" * 100)  # Línea separadora
            
        # Realizar la conciliación
        self.models.execute_kw(
            self.db, self.uid, self.password,
            'account.bank.statement.line',
            'process_reconciliation',
            [statement_line_id],
            {'payment_aml_ids': [(6, 0, [matching_moves[0]['id']])]}
        )
        
        return "Conciliación realizada exitosamente"
        
    except Exception as e:
        return f"Error durante la conciliación: {str(e)}"

