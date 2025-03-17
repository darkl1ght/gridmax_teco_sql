USE [HIS_USER]
GO

/****** Object:  UserDefinedFunction [GADS].[SOLAR_MONTHLY_GEN_FN]    Script Date: 3/17/2025 8:08:19 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [GADS].[SOLAR_MONTHLY_GEN_FN]
(
	@Year INT,
	@Month INT
)
RETURNS @RESULTS TABLE(DATEPART INT,
    STATION VARCHAR(256),
    PLANT VARCHAR(256),
    UNIT VARCHAR(256),
    UNIT_GROSS FLOAT,
    UNIT_NET FLOAT)
AS
BEGIN

    DECLARE @SearchUID VARCHAR(256), @StartDate DATETIME, @EndDate DATETIME
    SET @SearchUID = 'Plant Net Solar';
    SET @StartDate = DATEADD(HOUR, 1, CAST(DATEFROMPARTS(@Year, @Month, 1) as  DATETIME));
    SET @EndDate = DATEADD(MONTH, 1, @StartDate);

    -- Common Table Expression to capture all solar meter readings
    WITH
        PLANT_METER_READINGS
        as
        (
            SELECT
                DAY(DATEADD(HOUR, -1, HIST_TIMESTAMP_TZ)) AS DATEPART,
                -- [Hist_Timestamp] as [HistTimestamp],  
                [ObjectGrandParent] as [Plant],
                [ObjectParent] as [CombinedUnit],
                -- [MeasUnit] as [UOM],  
                [Label] as [Unit],
                SUM(ROUND(
	  CASE
	   WHEN [MeasUnit] IN('KWH', 'KW') THEN ISNULL([VALUE]/1000,0)
	   ELSE ISNULL([VALUE],0)
	  END
	  ,3)) AS 'Value',
                -- 0 as [Flag],
                [ValueType] = 
	  CASE
	   WHEN [ObjectType] = 'Generator' THEN 'g'
	   WHEN [ObjectType] = 'Station Service' THEN 's'
	   WHEN [ObjectType] = 'Reserve' THEN 'r'
	  END
            FROM his_user.GOSS_Search_AccumulatorHistory_Fn(@SearchUID, @StartDate, @EndDate)
            --WHERE DAY(DATEADD(HOUR, -1, HIST_TIMESTAMP))=2 ----<<<<<<<<<<<TEMPORARY>>>>>>>>>>>--------
            GROUP BY [ObjectName], YEAR(DATEADD(HOUR, -1, HIST_TIMESTAMP_TZ)), MONTH(DATEADD(HOUR, -1, HIST_TIMESTAMP_TZ)), DAY(DATEADD(HOUR, -1, HIST_TIMESTAMP_TZ)), [HIST_TIMESTAMP_TZ], [ObjectGrandParent], [ObjectParent], [ObjectType], [MeasUnit], [Label]
            --ORDER BY
            --DAY(DATEADD(HOUR, -1, HIST_TIMESTAMP)) 
        )
	 --SELECT * FROM PLANT_METER_READINGS; 
	 ,

        UNIT_GROSS
        AS
        (
            SELECT
                DATEPART,
                PLANT,
                CombinedUnit,
                UNIT,
                SUM(Value) as UNIT_GROSS
            FROM PLANT_METER_READINGS
            WHERE ValueType = 'g'
            GROUP BY
	    DATEPART, PLANT, CombinedUnit, UNIT
        )
  --SELECT * FROM UNIT_GROSS ORDER BY DATEPART, Unit;
  ,
        STATION_SERVICE
        AS
        (
            SELECT
                DATEPART,
                UNIT,
                SUM(Value) as STATION_SERVICE
            FROM PLANT_METER_READINGS
            WHERE ValueType = 's'
            GROUP BY
	    DATEPART, UNIT
        )
    --SELECT * FROM STATION_SERVICE ORDER BY DATEPART, Unit;
    INSERT INTO @RESULTS
        (DATEPART,STATION, PLANT, UNIT,UNIT_GROSS, UNIT_NET)
    SELECT
        A.DATEPART as DATEPART,
        A.PLANT AS STATION,
        A.CombinedUnit AS PLANT,
        A.UNIT AS UNIT,
        A.UNIT_GROSS,
        --B.STATION_SERVICE AS STATION_SERVICE,
        ROUND((A.UNIT_GROSS - ISNULL(B.STATION_SERVICE,0)), 3) AS UNIT_NET
    FROM UNIT_GROSS A LEFT JOIN STATION_SERVICE B ON A.Unit = B.Unit AND A.DATEPART = B.DATEPART
    ORDER BY A.DATEPART, A.Unit;
    RETURN;
END;
GO


