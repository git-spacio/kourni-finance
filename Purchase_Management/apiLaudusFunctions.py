import requests
import json
from datetime import timedelta
import pandas as pd


def get_invoice_data(headers, body):
    url = "https://api.laudus.cl/purchases/invoices/list"

    response = requests.post(url, headers=headers, json=body)
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        print(df.T)
        df = df.rename(columns={'account_name':'Concepto de Gasto','costCenterAllocation_costCenter_name':'Centro de Costo','supplier_supplierId':'Id Proveedor','dueDate':'Vence','docType_name':'Tipo Documento','purchaseInvoiceId':'Id','totalsOriginalCurrency_total':'Total','totalsOriginalCurrency_net':'Neto','issuedDate':'Fecha Emisión','supplier_VATId':'Rut','createdBy_name':'Aprobado por','monthlyPosition':'N° Laudus','fiscalPeriod': 'Mes Fiscal','supplier_name':'Proveedor','docType.name':'Tipo Documento','docNumber':'N° Documento'})
        df['Área'] = ''
        df['N° OC'] = ''
        df['Fecha Emisión'] = pd.to_datetime(df['Fecha Emisión'])
        df['Vence'] = pd.to_datetime(df['Vence'])
        df['Semana de Pago'] = df.apply(lambda row: get_payment_weekday(row['Fecha Emisión'], row['Vence']), axis=1)
        df['Fecha de Pago'] = ''
        df['Estatus'] = 'POR PAGAR'
        df['IVA'] = pd.to_numeric(df['Total'])-pd.to_numeric(df['Neto'])
        for index, row in df.iterrows():
            if row['Neto'] == 0:
                df.at[index, 'Neto'] = row['IVA']
                df.at[index, 'IVA'] = 0
        df['Neto'] = convert_currency_column(df,'Neto')
        df['IVA'] = convert_currency_column(df,'IVA')
        df['Total'] = convert_currency_column(df,'Total')
        df['Concepto de Gasto'] = ''
        df['Tipo Pago'] = ''
        df['Reembolso'] = ''
        df['Fecha de Reembolso'] = ''
        df['Comentarios'] = ''
        df['Fecha de Recepción'] = ''
        df = df[['N° Laudus', 'Mes Fiscal','Id Proveedor', 'Proveedor', 'Centro de Costo', 'Concepto de Gasto',
                 'Tipo Documento', 'N° Documento', 
                 'N° OC','Fecha Emisión','Vence', 'Semana de Pago', 
                 'Fecha de Pago', 'Estatus', 'Neto', 'IVA', 'Total',
                 'Tipo Pago','Reembolso','Fecha de Reembolso', 'Comentarios',
                 'Fecha de Recepción', 'Aprobado por', 'Id', 'nullDoc'
                 ]]
        return df
    else:
        print(f"Error en la solicitud: {response.status_code}")
        return None
    
def get_payment_weekday(issued_date, due_date):
    if issued_date == due_date:
        return '-'

    weekday = due_date.weekday()
    if weekday >= 2:  # Si es miércoles, jueves o viernes
        return (due_date - timedelta(days=(weekday - 2))).strftime('%Y-%m-%d')
    else:  # Si es lunes o martes
        return (due_date - timedelta(days=(weekday + 5))).strftime('%Y-%m-%d')

def convert_currency_column(df, column):
    # Función interna para convertir un valor a moneda sin decimales
    def to_currency(value):
        return "${:,.0f}".format(value)
    
    # Asegurar que los datos de la columna sean numéricos
    numeric_column = pd.to_numeric(df[column])
    
    # Aplicar la función to_currency a la columna especificada y devolver la nueva serie
    return numeric_column.apply(to_currency)