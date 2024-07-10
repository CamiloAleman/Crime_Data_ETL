#import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import numpy as np
import re

import logging
logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

try:
    server = 'localhost'
    database = 'Crime_data_Staging'
    driver = 'ODBC Driver 17 for SQL Server'

    connection_string = f'mssql+pyodbc://@{server}/{database}?driver={driver}&trusted_connection=yes'
    engine = create_engine(connection_string, connect_args={'autocommit': True})

    main_Crime_data = 'pyquery.sql'

    with open(main_Crime_data, 'r') as file:
        query = file.read()

    DF = pd.read_sql(query, engine)
    logging.info('Datos extraídos exitosamente de SQL Server')

    print(list(DF.columns))

    #LIMPIEZA Y TRANSFORMACIÓN DE DATOS
    DF['Date_Rptd'] = pd.to_datetime(DF['Date_Rptd']).dt.date
    DF['DATE_OCC'] = pd.to_datetime(DF['Date_Rptd']).dt.date
    DF['Weapon_Used_Cd'] = DF['Weapon_Used_Cd'].fillna(0).astype(int)

    DF['Mocodes'] = DF['Mocodes'].fillna('No aplica')

    DF['Vict_Age'] = DF['Vict_Age'].fillna(0).astype(int)
    DF.loc[(DF['Vict_Age'] < 0) | (DF['Vict_Age'] > 99), 'Vict_Age'] = 0
    
    logging.info('Filtro de edades correcto')

    DF["Mocodes"].fillna("0", inplace = True)
    DF["Vict_Sex"] = DF['Vict_Sex'].fillna('-')
    DF["Vict_Descent"] = DF['Vict_Descent'].fillna("No aplica", inplace = True)
    
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

except Exception as e:
    logging.error('Error al extraer y limpiar datos: {e}')
    raise


#CREACION DE MODELO DIMENSIONAL
with engine.connect() as conn:
    with open("Dim_Crimes.sql") as file:
        query = text(file.read())
        conn.execute(query)
    
    with open("Dim_CheckNCreateTables.sql") as file:
        query = text(file.read())
        conn.execute(query)

logging.info('Tablas dimensionales creadas exitosamente')


#INSERCIÓN DE DATOS EN MODELO DIMENSIONAL
try:
    
    DF_Facts = DF[['DR_NO', 'Date_Rptd', 'DATE_OCC', 'TIME_OCC', 'AREA', 'Crm_Cd', 'Mocodes', 'Vict_Age', 'Vict_Descent', 
        'Weapon_Used_Cd', 'LAT', 'LON', 'LOCATION', 'Cross_Street', 'Premis_Cd', 'Status', 'Part_1_2', 'Rpt_Dist_No']]
    DF_Facts.to_sql('FactCrime', con=engine, if_exists='replace')

    DF_Crime = DF[['Crm_Cd', 'Crm_Cd_Desc']]
    DF_Crime.to_sql('DimCrime', con=engine, if_exists='replace')
    logging.info('Datos insertados correctamente en tabla DimCrime')

    DF['Date_Rptd'].to_sql('DimDateRptd', con=engine, if_exists='replace')
    logging.info('Datos insertados correctamente en tabla DimDateRptd')

    DF_Premis = DF[['Premis_Cd', 'Premis_Desc']]
    DF_Premis.to_sql('DimPremis', con=engine, if_exists='replace')
    logging.info('Datos insertados correctamente en tabla DimPremis')

    DF_weapon = DF[['Weapon_Used_Cd', 'Weapon_Desc']]
    DF_weapon.to_sql('DimWeapon', con=engine, if_exists='replace')
    logging.info('Datos insertados correctamente en tabla DimWeapon')

    DF_status = DF[['Status', 'Status_Desc']]
    DF_status.to_sql('DimStatus', con=engine, if_exists='replace')
    logging.info('Datos insertados correctamente en tabla DimStatus')

    DF['Vict_Sex'].to_sql('DimVictSex', con=engine, if_exists='replace')
    logging.info('Datos insertados correctamente en tabla DimVictSex')

    DF_Area = DF[['AREA', 'AREA_NAME']]
    DF_Area.to_sql('DimArea', con=engine, if_exists='replace')
    logging.info('Datos insertados correctamente en tabla DimArea')
    
    DF['Vict_Descent'].to_sql('DimVictDescent', con=engine, if_exists='replace')
    logging.info('Datos insertados correctamente en tabla DimVictDescent')

    print("Los datos fueron insertados correctamente en las tablas dimensionales.")

except Exception as e:
    logging.error('Error al insertar datos en tablas dimensionales: {e}')
    raise

