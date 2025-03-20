USE [HIS_USER]
GO
/****** Object:  UserDefinedFunction [GADS].[PLANT_MONTHLY_GEN_FN]    Script Date: 3/20/2025 1:17:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
;

ALTER FUNCTION [GADS].[PLANT_MONTHLY_GEN_FN] (
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

  DECLARE @PLANT_MONTHLY_GEN TABLE (
    DATEPART INT,
    STATION VARCHAR(256),
    PLANT VARCHAR(256),
    UNIT VARCHAR(256),
    UNIT_GROSS VARCHAR(256),
    UNIT_NET VARCHAR(256)
  );

  -- Set the appropriate SearchUID based on the plant parameter
  IF @Plant = 'BAYSIDE'
  SET
    @SearchUID = 'GADS Plant Net Bayside'
    ELSE IF @Plant = 'BIGBEND'
  SET
    @SearchUID = 'GADS Plant Net Big Bend'
    ELSE IF @Plant = 'Polk'
  SET
    @SearchUID = 'GADS Plant Net Polk'
    ELSE IF @Plant = 'MacDill'
  SET
    @SearchUID = 'GADS Plant Net MacDill'
    ELSE IF @Plant = 'Solar'
  SET
    @SearchUID = 'GADS Plant Net Solar'
    ELSE IF @Plant = '*'
    OR @Plant = 'ALL'
  SET
    @SearchUID = 'GADS Plant Net Fleet'
    ELSE -- Default case
  SET
    @SearchUID = 'GADS Plant Net Bayside';

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
  IF @SearchUID = 'GADS Plant Net Bayside'
  OR @SearchUID = 'GADS Plant Net Fleet' BEGIN
  INSERT INTO
    @PLANT_MONTHLY_GEN
  SELECT
    *
  FROM
    [his_user].[GADS].[BAYSIDE_MONTHLY_GEN_FN]('GADS Plant Net Bayside', @StartDate, @EndDate);

  END
  IF @SearchUID = 'GADS Plant Net Big Bend'
  OR @SearchUID = 'GADS Plant Net Fleet' BEGIN
  INSERT INTO
    @PLANT_MONTHLY_GEN
  SELECT
    *
  FROM
    [his_user].[GADS].BIGBEND_MONTHLY_GEN_FN('GADS Plant Net Big Bend', @StartDate, @EndDate);

  END
  -- Insert data from Polk plant if it's specifically requested or if Fleet data is requested
  IF @SearchUID = 'GADS Plant Net Polk'
  OR @SearchUID = 'GADS Plant Net Fleet' BEGIN
  INSERT INTO
    @PLANT_MONTHLY_GEN
  SELECT
    *
  FROM
    [his_user].[GADS].POLK_MONTHLY_GEN_FN('GADS Plant Net Polk', @StartDate, @EndDate);

  END
  -- Insert data from MacDill plant if it's specifically requested or if Fleet data is requested
  IF @SearchUID = 'GADS Plant Net MacDill'
  OR @SearchUID = 'GADS Plant Net Fleet' BEGIN
  INSERT INTO
    @PLANT_MONTHLY_GEN
  SELECT
    *
  FROM
    [his_user].[GADS].MACDILL_MONTHLY_GEN_FN('GADS Plant Net MacDill', @StartDate, @EndDate);

  END
  -- Insert data from Solar plant if it's specifically requested or if Fleet data is requested
  IF @SearchUID = 'GADS Plant Net Solar'
  OR @SearchUID = 'GADS Plant Net Fleet' BEGIN
  INSERT INTO
    @PLANT_MONTHLY_GEN
  SELECT
    *
  FROM
    [his_user].[GADS].SOLAR_MONTHLY_GEN_FN('GADS Plant Net Solar', @StartDate, @EndDate);
  END

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
    @Year as YEAR,
    @Month AS MONTH
  FROM
    @PLANT_MONTHLY_GEN

	RETURN

END;

