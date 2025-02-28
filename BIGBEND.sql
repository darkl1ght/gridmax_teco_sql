USE [HIS_USER]
GO
/****** Object:  UserDefinedFunction [GADS].[BIGBEND_MONTHLY_GEN_FN]    Script Date: 2/27/2025 9:41:05 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER FUNCTION [GADS].[BIGBEND_MONTHLY_GEN_FN]
(
	@Year INT,
	@Month INT
)
RETURNS @RESULTS TABLE(DATEPART INT, STATION VARCHAR(256), PLANT VARCHAR(256), UNIT VARCHAR(256), UNIT_GROSS FLOAT, UNIT_NET FLOAT)
AS
BEGIN

	DECLARE @SearchUID VARCHAR(256), @StartDate DATETIME, @EndDate DATETIME
    SET @SearchUID = 'Plant Net Big Bend';
    SET @StartDate = DATEADD(HOUR, 1, CAST(DATEFROMPARTS(@Year, @Month, 1) as  DATETIME));
    SET @EndDate = DATEADD(MONTH, 1, @StartDate);

    WITH PLANT_METER_READINGS as (
        SELECT   
        DAY(DATEADD(HOUR, -1, HIST_TIMESTAMP)) AS DATEPART,  
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
        GROUP BY [ObjectName], YEAR(DATEADD(HOUR, -1, HIST_TIMESTAMP)), MONTH(DATEADD(HOUR, -1, HIST_TIMESTAMP)), DAY(DATEADD(HOUR, -1, HIST_TIMESTAMP)), [HIST_TIMESTAMP], [ObjectGrandParent], [ObjectParent], [ObjectType], [MeasUnit], [Label]  
        --ORDER BY
        --DAY(DATEADD(HOUR, -1, HIST_TIMESTAMP)) 
    )
    -- SELECT * FROM PLANT_METER_READINGS; 
    ,
    UNIT_GROSS AS (
        SELECT 
        DATEPART, PLANT, COMBINEDUNIT, UNIT, SUM(VALUE) AS UNIT_GROSS
        FROM PLANT_METER_READINGS
        -- UN#1, UN#2, UN#3 has been excluded as they are not included in gross generation calculation
        WHERE VALUETYPE='g' AND UNIT NOT LIKE 'UN#3' AND UNIT NOT LIKE 'UN#2' AND UNIT NOT LIKE 'UN#2'
        GROUP BY DATEPART, PLANT, COMBINEDUNIT, UNIT, VALUETYPE
    )
    --SELECT * FROM UNIT_GROSS;
    ,BIGBEND_GROSS AS (
        SELECT 
        DATEPART, SUM(UNIT_GROSS) AS BIGBEND_GROSS
        FROM UNIT_GROSS
        GROUP BY DATEPART 
    )
    --SELECT * FROM BIGBEND_GROSS;
    ,STATION_SERVICE AS (
        SELECT 
        DATEPART, SUBSTRING(UNIT,1,4) as UNIT, SUM(VALUE) AS STATION_SERVICE
        FROM PLANT_METER_READINGS
        WHERE VALUETYPE='s'
        GROUP BY DATEPART, SUBSTRING(UNIT,1,4), VALUETYPE
    )
    --SELECT * FROM STATION_SERVICE_GROSS;
    ,RESERVE_GROSS AS (
        SELECT 
        DATEPART, SUM(VALUE) AS RESERVE_GROSS
        FROM PLANT_METER_READINGS
        WHERE VALUETYPE='r'
        GROUP BY DATEPART, VALUETYPE
    )
    --SELECT * FROM RESERVE_GROSS;
    ,PRE_FINAL_OUTPUT AS (
        SELECT 
        A.DATEPART,A.PLANT, A.COMBINEDUNIT,A.UNIT,
        A.UNIT_GROSS, 
        B.STATION_SERVICE,  
        C.BIGBEND_GROSS,  
        D.RESERVE_GROSS,
        CASE
        WHEN BIGBEND_GROSS = 0 AND  A.UNIT <> 'UN#4' THEN 0
        ELSE D.RESERVE_GROSS*(A.UNIT_GROSS/C.BIGBEND_GROSS)
        END AS UNIT_RESERVE
        FROM UNIT_GROSS A 
        LEFT JOIN STATION_SERVICE B ON A.DATEPART = B.DATEPART AND A.UNIT=B.UNIT
        LEFT JOIN BIGBEND_GROSS C ON A.DATEPART = C.DATEPART
        LEFT JOIN RESERVE_GROSS D ON A.DATEPART = D.DATEPART 
    )
    --SELECT * from PRE_FINAL_OUTPUT;
    --SELECT DATEPART, PLANT AS STATION, COMBINEDUNIT AS PLANT, 
    --UNIT, UNIT_GROSS, (UNIT_GROSS-STATION_SERVICE-UNIT_RESERVE) AS UNIT_NET 
    --FROM PRE_FINAL_OUTPUT;
    INSERT INTO @RESULTS(DATEPART, STATION, PLANT, UNIT, UNIT_GROSS, UNIT_NET)
    SELECT DATEPART, PLANT as STATION, COMBINEDUNIT AS PLANT, UNIT, UNIT_GROSS,
    ROUND(UNIT_GROSS - STATION_SERVICE - UNIT_RESERVE, 2) AS UNIT_NET
    FROM PRE_FINAL_OUTPUT;
    RETURN;
END;
GO;