USE [HIS_USER]
GO

/****** Object:  UserDefinedFunction [GADS].[MACDILL_MONTHLY_GEN_FN]    Script Date: 3/17/2025 8:07:20 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION [GADS].[MACDILL_MONTHLY_GEN_FN] (@Year  INT,
                                                 @Month INT)
returns @RESULTS TABLE(
    datepart INT,
    station VARCHAR(256),
    plant VARCHAR(256),
    unit VARCHAR(256),
    unit_gross FLOAT,
    unit_net FLOAT)
AS
  BEGIN
    DECLARE @SearchUID VARCHAR(256),
              @StartDate DATETIME,
              @EndDate   DATETIME

    SET @SearchUID = 'Plant Net MacDill';
    SET @StartDate = Dateadd(hour, 1, Cast(Datefromparts(@Year, @Month, 1) AS DATETIME));
    SET @EndDate = Dateadd(month, 1, @StartDate);

    -- Common Table Expression to capture all solar meter readings
    WITH
        plant_meter_readings
        AS
        (
            SELECT Day(Dateadd(hour, -1, hist_timestamp_tz)) AS DATEPART,
                [objectgrandparent]                    AS [Plant],
                [objectparent]                         AS [CombinedUnit],
                [label]                                AS [Unit],
                Sum(Round(CASE
                                  WHEN [measunit] IN( 'KWH', 'KW' ) THEN
                                  Isnull([value] / 1000, 0)
                                  ELSE Isnull([value], 0)
                                END, 3))                     AS 'Value',
                [ValueType] = CASE
                                      WHEN [objecttype] = 'Generator' THEN 'g'
                                      WHEN [objecttype] = 'Station Service' THEN
                                      's'
                                      WHEN [objecttype] = 'Reserve' THEN 'r'
                                    END
            FROM his_user.Goss_search_accumulatorhistory_fn(@SearchUID,
                      @StartDate,
                      @EndDate)
            GROUP  BY [objectname],
                         Year(Dateadd(hour, -1, hist_timestamp_tz)),
                         Month(Dateadd(hour, -1, hist_timestamp_tz)),
                         Day(Dateadd(hour, -1, hist_timestamp_tz)),
                         [hist_timestamp_tz],
                         [objectgrandparent],
                         [objectparent],
                         [objecttype],
                         [measunit],
                         [label]
        )
      --SELECT * FROM PLANT_METER_READINGS ORDER BY DATEPART, Unit; 
      ,
        UNIT_GROSS
        AS
        (
            SELECT DATEPART, PLANT, COMBINEDUNIT, UNIT, SUM(Value) AS UNIT_GROSS
            FROM PLANT_METER_READINGS
            WHERE ValueType = 'g'
            GROUP BY DATEPART,PLANT, COMBINEDUNIT, UNIT
        )
		--SELECT * FROM UNIT_GROSS WHERE DATEPART=1;
		,
        PLANT_GROSS
        AS
        (
            SELECT DATEPART, SUM(Value) AS PLANT_GROSS
            FROM PLANT_METER_READINGS
            WHERE ValueType = 'g'
            GROUP BY DATEPART
        )
		--SELECT * FROM PLANT_GROSS;
		,
        DISTINCT_UNITS
        AS
        (
            SELECT DATEPART, COUNT(DISTINCT UNIT) AS NUM_UNITS
            FROM PLANT_METER_READINGS
            WHERE ValueType = 'g'
            GROUP BY DATEPART
        )
		--SELECT * FROM DISTINCT_UNITS;
		,
        STATION_SERVICE
        AS
        (
            SELECT DATEPART, UNIT, SUM(Value) AS STATION_SVC
            FROM PLANT_METER_READINGS
            WHERE ValueType = 's'
            GROUP BY DATEPART, UNIT
        )
		--SELECT * FROM STATION_SERVICE;
		,
        PRE_FINAL_OUTPUT
        AS
        (
            SELECT a.DATEPART, a.PLANT, a.COMBINEDUNIT, a.UNIT, a.UNIT_GROSS,
                b.PLANT_GROSS, c.NUM_UNITS,
                CASE
					WHEN PLANT_GROSS = 0 THEN (IIF(d.STATION_SVC IS NULL, 0, d.STATION_SVC)/c.NUM_UNITS)
					ELSE ((UNIT_GROSS* IIF(d.STATION_SVC IS NULL, 0, d.STATION_SVC)) /PLANT_GROSS)
				END AS STATION_SERVICE
            FROM UNIT_GROSS a
                LEFT JOIN PLANT_GROSS b ON a.DATEPART=b.DATEPART
                LEFT JOIN DISTINCT_UNITS c ON a.DATEPART=c.DATEPART
                LEFT JOIN STATION_SERVICE d ON a.DATEPART=d.DATEPART
            --ORDER BY DATEPART, UNIT
        )
    --SELECT * FROM PRE_FINAL_OUTPUT;
    INSERT INTO @RESULTS
        (DATEPART,
        STATION,
        PLANT,
        UNIT,
        UNIT_GROSS,
        UNIT_NET)
    SELECT DATEPART, PLANT AS STATION, COMBINEDUNIT AS PLANT, UNIT,
        ROUND(UNIT_GROSS,2) AS UNIT_GROSS,
        (UNIT_GROSS - STATION_SERVICE) AS UNIT_NET
    FROM PRE_FINAL_OUTPUT
    ORDER BY DATEPART, UNIT
    --SELECT * FROM PRE_FINAL_OUTPUT;
    RETURN;
END;

GO


