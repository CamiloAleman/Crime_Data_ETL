IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DimModel_Crimes')
CREATE DATABASE DimModel_Crimes;