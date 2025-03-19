USE [HIS_USER]
GO
SET
  ANSI_NULLS ON
GO
SET
  QUOTED_IDENTIFIER ON
GO
;

CREATE FUNCTION [GADS].[PLANT_MONTHLY_GEN_FN] (@Plant VARCHAR(256), @Year INT, @Month INT) RETURNS @RESULTS TABLE(
  DATEPART INT,
  STATION VARCHAR(256),
  PLANT VARCHAR(256),
  UNIT VARCHAR(256),
  UNIT_GROSS FLOAT,
  UNIT_NET FLOAT
) AS BEGIN DECLARE @SearchUID VARCHAR(256),
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
  OR @Plant IS 'ALL'
SET
  @SearchUID = 'Plant Net Fleet'
  ELSE
SET
  -- Default case
  @SearchUID = 'Plant Net Bayside';

SELECT
  @StartDate = his_user.fn_DateTime_Offset(
    DATEADD(
      HOUR,
      1,
      CAST(DATEFROMPARTS(@Year, @Month, 1) as DATETIME)
    ),
    's'
  );

-- 2025-01-01 01:00:00.000
SELECT
  @EndDate = his_user.fn_DateTime_Offset(DATEADD(MONTH, 1, @StartDate), 's');

-- 2025-02-01 01:00:00.000
WITH PLANT_NET_GEN AS (
  IF @SearchUID = 'Plant Net Bayside'
  SELECT
    *
  FROM
    his_user.BAYSIDE_MONTHLY_GEN_FN(@SearchUID, @StartDate, @EndDate)
    ELSE IF @SearchUID = 'Plant Net Big Bend'
  SELECT
    *
  FROM
    his_user.BIG_BEND_MONTHLY_GEN_FN(@SearchUID, @StartDate, @EndDate)
    ELSE IF @SearchUID = 'Plant Net Polk'
  SELECT
    *
  FROM
    his_user.POLK_MONTHLY_GEN_FN(@SearchUID, @StartDate, @EndDate)
    ELSE IF @SearchUID = 'Plant Net MacDill'
  SELECT
    *
  FROM
    his_user.MACDILL_MONTHLY_GEN_FN(@SearchUID, @StartDate, @EndDate)
    ELSE IF @SearchUID = 'Plant Net Solar'
  SELECT
    *
  FROM
    his_user.SOLAR_MONTHLY_GEN_FN(@SearchUID, @StartDate, @EndDate)
    ELSE IF @SearchUID = 'Plant Net Fleet'
  SELECT
    *
  FROM
    his_user.BAYSIDE_MONTHLY_GEN_FN(@SearchUID, @StartDate, @EndDate)
  UNION
  ALL
  SELECT
    *
  FROM
    his_user.BIG_BEND_MONTHLY_GEN_FN(@SearchUID, @StartDate, @EndDate)
  UNION
  ALL
  SELECT
    *
  FROM
    his_user.POLK_MONTHLY_GEN_FN(@SearchUID, @StartDate, @EndDate)
  UNION
  ALL
  SELECT
    *
  FROM
    his_user.MACDILL_MONTHLY_GEN_FN(@SearchUID, @StartDate, @EndDate)
  UNION
  ALL
  SELECT
    *
  FROM
    his_user.SOLAR_MONTHLY_GEN_FN(@SearchUID, @StartDate, @EndDate)
)
INSERT INTO
  @RESULTS (
    DATEPART,
    STATION,
    PLANT,
    UNIT,
    UNIT_GROSS,
    UNIT_NET
  )
SELECT
  DATEPART,
  STATION,
  PLANT,
  UNIT,
  UNIT_GROSS,
  UNIT_NET
FROM
  PLANT_NET_GEN;

END;

GO