USE [DimModel_Crimes]

DROP TABLE IF EXISTS [dbo].[FactCrime];
DROP TABLE IF EXISTS [dbo].[DimDateRptd];
DROP TABLE IF EXISTS [dbo].[DimArea];
DROP TABLE IF EXISTS [dbo].[DimVictSex];
DROP TABLE IF EXISTS [dbo].[DimVictDescent];
DROP TABLE IF EXISTS [dbo].[DimWeapon];
DROP TABLE IF EXISTS [dbo].[DimStatus];
DROP TABLE IF EXISTS [dbo].[DimPremis];
DROP TABLE IF EXISTS [dbo].[DimCrime];


CREATE TABLE DimPremis (
    Premis_Cd INT PRIMARY KEY,
    Premis_Desc VARCHAR(255)
);

CREATE TABLE DimWeapon (
    Weapon_Used_Cd INT PRIMARY KEY,
    Weapon_Desc VARCHAR(255)
);

CREATE TABLE DimArea (
    AREA INT PRIMARY KEY,
    AREA_NAME VARCHAR(255)
);

--CREATE TABLE DimVictSex (
  --  Vict_Sex_Id INT PRIMARY KEY,
   -- Vict_Sex VARCHAR(255)
--);

CREATE TABLE DimVictDescent (
    Vict_Descent_Id INT PRIMARY KEY,
    Vict_Descent VARCHAR(255)
);

CREATE TABLE DimStatus (
    Status VARCHAR(255) PRIMARY KEY,
    Status_Desc VARCHAR(255)
);

CREATE TABLE DimCrime (
    Crm_Cd INT PRIMARY KEY,
    Crm_Desc VARCHAR(255)
);

CREATE TABLE DimDateRptd (
    Date_Rptd DATE PRIMARY KEY,
    Day INT,
    Month INT,
    Year INT
);


CREATE TABLE FactCrime (
    DR_NO INT PRIMARY KEY,
    Date_Rptd DATE,
    DATE_OCC DATE,
    TIME_OCC INT,
    AREA INT,
    Rpt_Dist_No INT,
    Part_1_2 INT,
    Crm_Cd INT,
    Mocodes VARCHAR(255),
    Vict_Age INT,
    Premis_Cd INT,
    Weapon_Used_Cd INT,
    --Vict_Sex_Id INT,
    Vict_Descent_Id INT,
    LOCATION VARCHAR(255),
    Status VARCHAR(255),
    Croos_Stret VARCHAR(255),
    LAT DECIMAL (10, 5),
    LON DECIMAL (10, 5),

    CONSTRAINT FK_FactCrime_DimPremis FOREIGN KEY (Premis_Cd) REFERENCES DimPremis(Premis_Cd),
    CONSTRAINT FK_FactCrime_DimWeapon  FOREIGN KEY (Weapon_Used_Cd) REFERENCES DimWeapon(Weapon_Used_Cd),
    CONSTRAINT FK_FactCrime_DimArea FOREIGN KEY (AREA) REFERENCES DimArea(AREA),
    --CONSTRAINT FK_FactCrime_DimVictSex FOREIGN KEY (Vict_Sex_Id) REFERENCES DimVictSex(Vict_Sex_Id),
    CONSTRAINT FK_FactCrime_DimVictDescent FOREIGN KEY (Vict_Descent_Id) REFERENCES DimVictDescent(Vict_Descent_Id),    
    CONSTRAINT FK_FactCrime_DimDateRptd FOREIGN KEY (Date_Rptd) REFERENCES DimDateRptd(Date_Rptd),
    CONSTRAINT FK_FactCrime_DimStatus FOREIGN KEY (Status) REFERENCES DimStatus(Status),
    CONSTRAINT FK_FactCrime_DimCrime FOREIGN KEY (Crm_Cd) REFERENCES DimCrime(Crm_Cd)
);