USE [HIS_USER]
GO
SET
  ANSI_NULLS ON
GO
SET
  QUOTED_IDENTIFIER ON
GO
;

CREATE FUNCTION [GADS].[PLANT_MONTHLY_GEN_FN] (
  @Plant VARCHAR(256),
  @Year INT,
  @Month INT
) RETURNS @RESULTS TABLE(
  DATEPART INT,
  STATION VARCHAR(256),
  PLANT VARCHAR(256),
  UNIT VARCHAR(256),
  UNIT_GROSS FLOAT,
  UNIT_NET FLOAT,
  YEAR INT,
  MONTH INT
) AS 
BEGIN 

  DECLARE @SearchUID VARCHAR(256),
  @StartDate DATETIME,
  @EndDate DATETIME;

  DECLARE @PLANT_METER_READINGS TABLE (
    DATEPART INT,
    STATION VARCHAR(256),
    PLANT VARCHAR(256),
    UNIT VARCHAR(256),
    VALUE INT,
    VALUETYPE VARCHAR(256)
  );

  -- Set the appropriate SearchUID based on the plant parameter
  IF @Plant = 'BAYSIDE'
  SET
    @SearchUID = 'Plant Net Bayside'
    ELSE IF @Plant = 'BIGBEND'
  SET
    @SearchUID = 'Plant Net Big Bend'
    ELSE IF @Plant = 'Polk'
  SET
    @SearchUID = 'Plant Net Polk'
    ELSE IF @Plant = 'MacDill'
  SET
    @SearchUID = 'Plant Net MacDill'
    ELSE IF @Plant = 'Solar'
  SET
    @SearchUID = 'Plant Net Solar'
    ELSE IF @Plant = '*'
    OR @Plant = 'ALL'
  SET
    @SearchUID = 'Plant Net Fleet'
    ELSE -- Default case
  SET
    @SearchUID = 'Plant Net Bayside';

  -- Calculate start and end dates
  SELECT
    @StartDate = his_user.fn_DateTime_Offset(
      DATEADD(
        HOUR,
        1,
        CAST(DATEFROMPARTS(@Year, @Month, 1) as DATETIME)
      ),
      's'
    );

  SELECT
    @EndDate = his_user.fn_DateTime_Offset(DATEADD(MONTH, 1, @StartDate), 's');

  -- Insert data from Bayside plant if it's specifically requested or if Fleet data is requested
  IF @SearchUID = 'Plant Net Bayside'
  OR @SearchUID = 'Plant Net Fleet' BEGIN
  INSERT INTO
    @PLANT_METER_READINGS
  SELECT
    *
  FROM
    his_user.BAYSIDE_MONTHLY_GEN_FN('Plant Net Bayside', @StartDate, @EndDate);

  END -- Insert data from Big Bend plant if it's specifically requested or if Fleet data is requested
  IF @SearchUID = 'Plant Net Big Bend'
  OR @SearchUID = 'Plant Net Fleet' BEGIN
  INSERT INTO
    @PLANT_METER_READINGS
  SELECT
    *
  FROM
    his_user.BIG_BEND_MONTHLY_GEN_FN('Plant Net Big Bend', @StartDate, @EndDate);

  END -- Insert data from Polk plant if it's specifically requested or if Fleet data is requested
  IF @SearchUID = 'Plant Net Polk'
  OR @SearchUID = 'Plant Net Fleet' BEGIN
  INSERT INTO
    @PLANT_METER_READINGS
  SELECT
    *
  FROM
    his_user.POLK_MONTHLY_GEN_FN('Plant Net Polk', @StartDate, @EndDate);

  END -- Insert data from MacDill plant if it's specifically requested or if Fleet data is requested
  IF @SearchUID = 'Plant Net MacDill'
  OR @SearchUID = 'Plant Net Fleet' BEGIN
  INSERT INTO
    @PLANT_METER_READINGS
  SELECT
    *
  FROM
    his_user.MACDILL_MONTHLY_GEN_FN('Plant Net MacDill', @StartDate, @EndDate);

  END -- Insert data from Solar plant if it's specifically requested or if Fleet data is requested
  IF @SearchUID = 'Plant Net Solar'
  OR @SearchUID = 'Plant Net Fleet' BEGIN
  INSERT INTO
    @PLANT_METER_READINGS
  SELECT
    *
  FROM
    his_user.SOLAR_MONTHLY_GEN_FN('Plant Net Solar', @StartDate, @EndDate);

  INSERT INTO
    @RESULTS (
      DATEPART,
      STATION,
      PLANT,
      UNIT,
      UNIT_GROSS,
      UNIT_NET,
      YEAR,
      MONTH
    )
  SELECT
    DATEPART,
    STATION,
    PLANT,
    UNIT,
    UNIT_GROSS,
    UNIT_NET,
    @Year,
    @Month
  FROM
    @PLANT_METER_READINGS 

  RETURN;

END;