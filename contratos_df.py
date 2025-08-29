import pandas as pd
import re
pd.options.display.float_format = '{:.0f}'.format

#Conversión de CSVs de contratos ComprasMX a dfs.
contratos = []

for i in range(2012,2026):
    contrato = pd.read_csv('Contratos_CompraNet_{}.csv'.format(i), encoding='ISO-8859-1',low_memory=False, on_bad_lines='skip')
    contratos.append(contrato)

#Loop para modificación de columnas de dfs (2010-2017)
for i in range(0,6):
     contratos[i] = contratos[i].rename(columns={'SIGLAS':'Siglas de la Institución',
'CLAVEUC': 'Clave de la UC',
'NOMBRE_DE_LA_UC':'Nombre de la UC',
'TIPO_PROCEDIMIENTO':'Tipo Procedimiento',
'CODIGO_CONTRATO':'Código del contrato',
'TITULO_CONTRATO':'Título del contrato',
'IMPORTE_CONTRATO':'Importe DRC',
'MONEDA':'Moneda',
'PROVEEDOR_CONTRATISTA':'Proveedor o contratista',
'SIGLAS_PAIS':'País de la empresa',
'ESTRATIFICACION_MPC':'Estratificación',
'ANUNCIO':'Dirección del anuncio',}
                                       )
#Loop para modificación de columnas de dfs (2018-2022)
for i in range(6,12):
     contratos[i] = contratos[i].rename(columns={'Importe del contrato':'Importe DRC',
'Moneda del contrato':'Moneda',
'Clave del país de la empresa':'País de la empresa',
'Estratificación de la empresa':'Estratificación',
'Tipo de procedimiento': 'Tipo Procedimiento'})

#Loop para modificación de columnas de dfs en contratos
for i in range(0,len(contratos)):
    contratos[i] = contratos[i][['Siglas de la Institución', 'Clave de la UC', 'Nombre de la UC',
'Tipo Procedimiento', 'Código del contrato', 
'Título del contrato', 'Importe DRC', 
'Moneda', 'Proveedor o contratista', 'País de la empresa', 
'Estratificación', 'Dirección del anuncio']]

#Añadir columna "Años" a dfs en lista contratos
años = list(range(2012, 2026))

for df, año in zip(contratos, años):
    df['Año'] = año

#Stack de todos los dfs en contratos
contratos_stack = pd.concat(contratos, axis=0)

#Funcion útil para aggs y cleanups
def group_agg(df, col_group, col_agg, aggr, renaming):
    group_df = (df.groupby(col_group).agg({col_agg:aggr})
                .reset_index()
                .rename(columns={col_agg:renaming})
                .sort_values(by=renaming, ascending=False)
               )
    return group_df
