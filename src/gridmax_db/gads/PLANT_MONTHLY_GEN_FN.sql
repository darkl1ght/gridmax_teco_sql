USE [HIS_USER]
GO
  /****** Object:  UserDefinedFunction [GADS].[PLANT_MONTHLY_GEN_FN]    Script Date: 3/27/2025 8:38:09 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO;
CREATE
OR ALTER FUNCTION [GADS].[PLANT_MONTHLY_GEN_FN] (
  @Plant VARCHAR(256),
  @Year INT,
  @Month INT
) RETURNS @RESULTS TABLE(
  YEAR INT,
  MONTH INT,
  DATEPART INT,
  STATION VARCHAR(256),
  PLANT VARCHAR(256),
  UNIT VARCHAR(256),
  UNIT_GROSS FLOAT,
  UNIT_NET FLOAT
) AS BEGIN
DECLARE @SearchUID VARCHAR(256),
  @StartDate DATETIME,
  @EndDate DATETIME;
DECLARE @PLANT_MONTHLY_GEN TABLE (
    DATEPART INT,
    STATION VARCHAR(256),
    PLANT VARCHAR(256),
    UNIT VARCHAR(256),
    UNIT_GROSS FLOAT,
    UNIT_NET FLOAT
  );
-- Set the appropriate SearchUID based on the plant parameter
IF @Plant = 'BAYSIDE'
SET @SearchUID = 'GADS Plant Net Bayside'
  ELSE IF @Plant = 'BIGBEND'
SET @SearchUID = 'GADS Plant Net Big Bend'
  ELSE IF @Plant = 'Polk'
SET @SearchUID = 'GADS Plant Net Polk'
  ELSE IF @Plant = 'MacDill'
SET @SearchUID = 'GADS Plant Net MacDill'
  ELSE IF @Plant = 'Solar'
SET @SearchUID = 'GADS Plant Net Solar'
  ELSE IF @Plant = '*'
  OR @Plant = 'ALL'
SET @SearchUID = 'GADS Plant Net Fleet'
  ELSE -- Default case
SET @SearchUID = 'GADS Plant Net Bayside';
-- Calculate start and end dates
SELECT @StartDate = DATEADD(
    HOUR,
    1,
    CONVERT(DATETIME, DATEFROMPARTS(@Year, @Month, 1), 120) AT TIME ZONE 'Eastern Standard Time'
  );
SELECT @EndDate = DATEADD(MONTH, 1, @StartDate);
-- Insert data from Bayside plant if it's specifically requested or if Fleet data is requested
IF @SearchUID = 'GADS Plant Net Bayside'
OR @SearchUID = 'GADS Plant Net Fleet' BEGIN
INSERT INTO @PLANT_MONTHLY_GEN
SELECT *
FROM [his_user].[GADS].[BAYSIDE_MONTHLY_GEN_FN]('GADS Plant Net Bayside', @StartDate, @EndDate);
END IF @SearchUID = 'GADS Plant Net Big Bend'
OR @SearchUID = 'GADS Plant Net Fleet' BEGIN
INSERT INTO @PLANT_MONTHLY_GEN
SELECT *
FROM [his_user].[GADS].BIGBEND_MONTHLY_GEN_FN('GADS Plant Net Big Bend', @StartDate, @EndDate);
END -- Insert data from Polk plant if it's specifically requested or if Fleet data is requested
IF @SearchUID = 'GADS Plant Net Polk'
OR @SearchUID = 'GADS Plant Net Fleet' BEGIN
INSERT INTO @PLANT_MONTHLY_GEN
SELECT *
FROM [his_user].[GADS].POLK_MONTHLY_GEN_FN('GADS Plant Net Polk', @StartDate, @EndDate);
END -- Insert data from MacDill plant if it's specifically requested or if Fleet data is requested
IF @SearchUID = 'GADS Plant Net MacDill'
OR @SearchUID = 'GADS Plant Net Fleet' BEGIN
INSERT INTO @PLANT_MONTHLY_GEN
SELECT *
FROM [his_user].[GADS].MACDILL_MONTHLY_GEN_FN('GADS Plant Net MacDill', @StartDate, @EndDate);
END -- Insert data from Solar plant if it's specifically requested or if Fleet data is requested
IF @SearchUID = 'GADS Plant Net Solar'
OR @SearchUID = 'GADS Plant Net Fleet' BEGIN
INSERT INTO @PLANT_MONTHLY_GEN
SELECT *
FROM [his_user].[GADS].SOLAR_MONTHLY_GEN_FN('GADS Plant Net Solar', @StartDate, @EndDate);
END
INSERT INTO @RESULTS (
    YEAR,
    MONTH,
    DATEPART,
    STATION,
    PLANT,
    UNIT,
    UNIT_GROSS,
    UNIT_NET
  )
SELECT @Year as YEAR,
  @Month AS MONTH,
  DATEPART,
  STATION,
  PLANT,
  UNIT,
  UNIT_GROSS,
  UNIT_NET
FROM @PLANT_MONTHLY_GEN RETURN
END;
GO