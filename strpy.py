import pyodbc
import Credentials as C
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

import logging
logging.basicConfig(filename='etl.log', level=logging.INFO,
 format='%(asctime)s:%(levelname)s:%(message)s')

try:
    server = C.server
    database = C.database
    driver = 'ODBC Driver 17 for SQL Server'

    connection_string = f'mssql+pyodbc://@{server}/{database}?driver={driver}&trusted_connection=yes'
    engine = create_engine(connection_string)

    Crime_dataQ = 'pyquery.sql'

    with open(Crime_dataQ, 'r') as file:
        query = file.read()

    DF = pd.read_sql(query, engine)

    #LIMPIEZA Y TRANSFORMACIÓN DE DATOS
    DF['Date_Rptd'] = pd.to_datetime(DF['Date_Rptd']).dt.date
    DF['DATE_OCC'] = pd.to_datetime(DF['Date_Rptd']).dt.date
    DF['TIME_OCC'] = DF['TIME_OCC'].astype(int)
    DF['Weapon_Used_Cd'] = DF['Weapon_Used_Cd'].fillna(0).astype(int)

    DF['Mocodes'] = DF['Mocodes'].fillna('No aplica')

    DF['Vict_Age'] = DF['Vict_Age'].fillna(0).astype(int)
    DF.loc[(DF['Vict_Age'] < 0) | (DF['Vict_Age'] > 99), 'Vict_Age'] = 0
    
    logging.info('Filtro de edades correcto')

    DF["Mocodes"].fillna("0", inplace = True)
    DF["Vict_Sex"] = DF['Vict_Sex'].fillna("No aplica", inplace = True).str.upper()
    DF["Vict_Descent"] = DF['Vict_Descent'].fillna("No aplica", inplace = True).str.upper()
    
    DF["Weapon_Used_Cd"].fillna("No aplica", inplace = True)
    DF["Weapon_Used_Cd"] = DF['Weapon_Used_Cd'].fillna("No aplica", inplace = True)
    DF['LOCATION'] = DF['LOCATION'].apply(lambda x: re.sub(r'\s+', ' ', str(x).strip()))


    DF.drop(columns=['Crm_Cd_1', 'Crm_Cd_2', 'Crm_Cd_3', 'Crm_Cd_4'], inplace=True)

    DF["Weapon_Desc"].fillna("No aplica", inplace = True)
    DF["Cross_Street"] = DF['Cross_Street'].apply(lambda x: re.sub(r'\s+', ' ', str(x).strip()))
    DF["Status"] = DF['Status'].fillna("CC").str.upper()
    DF["Status_Desc"] = DF['Status_Desc'].fillna("No aplica")

    DF['LAT'] = pd.to_numeric(DF['LAT'], errors='coerce').fillna(0).astype(float)
    DF['LON'] = pd.to_numeric(DF['LON'], errors='coerce').fillna(0).astype(float)
    

    logging.info('Datos limpiados correctamente')

    logging.info('Datos extraídos exitosamente de SQL Server')

    logging.info('Limpieza de espacios satisfactoria')

except Exception as e:
    logging .error('Error al extraer datos: {e}')
    raise


print(DF.head(40))



