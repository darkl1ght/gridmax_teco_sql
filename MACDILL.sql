USE [HIS_USER]
GO
    /****** Object:  UserDefinedFunction [GADS].[MACDILL_MONTHLY_GEN_FN]    Script Date: 3/27/2025 8:38:53 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO CREATE
    OR ALTER FUNCTION [GADS].[MACDILL_MONTHLY_GEN_FN] (
        @SearchUID VARCHAR(256),
        @StartDate DATETIME,
        @EndDate DATETIME
    ) RETURNS @RESULTS TABLE(
        DATEPART INT,
        STATION VARCHAR(256),
        PLANT VARCHAR(256),
        UNIT VARCHAR(256),
        UNIT_GROSS FLOAT,
        UNIT_NET FLOAT
    ) AS BEGIN -- 2025-02-01 01:00:00.000
    WITH PLANT_METER_READINGS as (
        SELECT *
        FROM [GADS].[PLANT_METER_READINGS_FN](@SearchUID, @StartDate, @EndDate)
    ) --SELECT * FROM PLANT_METER_READINGS;
,
    UNIT_GROSS AS (
        SELECT DATEPART,
            STATION,
            PLANT,
            UNIT,
            SUM(Value) AS UNIT_GROSS
        FROM PLANT_METER_READINGS
        WHERE ValueType = 'g'
        GROUP BY DATEPART,
            STATION,
            PLANT,
            UNIT
    ) --SELECT * FROM UNIT_GROSS WHERE DATEPART=1;
,
    PLANT_GROSS AS (
        SELECT DATEPART,
            SUM(Value) AS PLANT_GROSS
        FROM PLANT_METER_READINGS
        WHERE ValueType = 'g'
        GROUP BY DATEPART
    ) --SELECT * FROM PLANT_GROSS;
,
    DISTINCT_UNITS AS (
        SELECT DATEPART,
            COUNT(DISTINCT UNIT) AS NUM_UNITS
        FROM PLANT_METER_READINGS
        WHERE ValueType = 'g'
        GROUP BY DATEPART
    ) --SELECT * FROM DISTINCT_UNITS;
,
    STATION_SERVICE AS (
        SELECT DATEPART,
            UNIT,
            SUM(Value) AS STATION_SVC
        FROM PLANT_METER_READINGS
        WHERE ValueType = 's'
        GROUP BY DATEPART,
            UNIT
    ) --SELECT * FROM STATION_SERVICE;
,
    PRE_FINAL_OUTPUT AS (
        SELECT a.DATEPART,
            a.STATION,
            a.PLANT,
            a.UNIT,
            a.UNIT_GROSS,
            b.PLANT_GROSS,
            c.NUM_UNITS,
            CASE
                WHEN PLANT_GROSS = 0 THEN (
                    IIF(d.STATION_SVC IS NULL, 0, d.STATION_SVC) / c.NUM_UNITS
                )
                ELSE (
                    (
                        UNIT_GROSS * IIF(d.STATION_SVC IS NULL, 0, d.STATION_SVC)
                    ) / PLANT_GROSS
                )
            END AS STATION_SERVICE
        FROM UNIT_GROSS a
            LEFT JOIN PLANT_GROSS b ON a.DATEPART = b.DATEPART
            LEFT JOIN DISTINCT_UNITS c ON a.DATEPART = c.DATEPART
            LEFT JOIN STATION_SERVICE d ON a.DATEPART = d.DATEPART --ORDER BY DATEPART, UNIT
    ) --SELECT * FROM PRE_FINAL_OUTPUT;
INSERT INTO @RESULTS (
        DATEPART,
        STATION,
        PLANT,
        UNIT,
        UNIT_GROSS,
        UNIT_NET
    )
SELECT DATEPART,
    STATION,
    PLANT,
    UNIT,
    ROUND(UNIT_GROSS, 3) AS UNIT_GROSS,
    ROUND((UNIT_GROSS - STATION_SERVICE), 3) AS UNIT_NET
FROM PRE_FINAL_OUTPUT
ORDER BY DATEPART,
    UNIT --SELECT * FROM PRE_FINAL_OUTPUT;
    RETURN
END
GO