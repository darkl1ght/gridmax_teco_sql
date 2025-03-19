USE [HIS_USER]
GO
    /****** Object:  UserDefinedFunction [GADS].[SOLAR_MONTHLY_GEN_FN]    Script Date: 3/17/2025 8:08:19 AM ******/
SET
    ANSI_NULLS ON
GO
SET
    QUOTED_IDENTIFIER ON
GO
    CREATE FUNCTION [GADS].[SOLAR_MONTHLY_GEN_FN] (@SearchUID, @StartDate, @EndDate) RETURNS @RESULTS TABLE(
        DATEPART INT,
        STATION VARCHAR(256),
        PLANT VARCHAR(256),
        UNIT VARCHAR(256),
        UNIT_GROSS FLOAT,
        UNIT_NET FLOAT
    ) AS BEGIN -- 2025-02-01 01:00:00.000
    WITH PLANT_METER_READINGS as (
        SELECT
            *
        FROM
            [GADS].[PLANT_METER_READINGS_FN](@SearchUID, @StartDate, @EndDate)
    ) --SELECT * FROM PLANT_METER_READINGS; 
,
    UNIT_GROSS AS (
        SELECT
            DATEPART,
            STATION,
            PLANT,
            UNIT,
            SUM(Value) as UNIT_GROSS
        FROM
            PLANT_METER_READINGS
        WHERE
            ValueType = 'g'
        GROUP BY
            DATEPART,
            STATION,
            PLANT,
            UNIT
    ) --SELECT * FROM UNIT_GROSS ORDER BY DATEPART, Unit;
,
    STATION_SERVICE AS (
        SELECT
            DATEPART,
            UNIT,
            SUM(Value) as STATION_SERVICE
        FROM
            PLANT_METER_READINGS
        WHERE
            ValueType = 's'
        GROUP BY
            DATEPART,
            UNIT
    ) --SELECT * FROM STATION_SERVICE ORDER BY DATEPART, Unit;
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
    A.DATEPART as DATEPART,
    A.STATION AS STATION,
    A.PLANT AS PLANT,
    A.UNIT AS UNIT,
    A.UNIT_GROSS,
    --B.STATION_SERVICE AS STATION_SERVICE,
    ROUND((A.UNIT_GROSS - ISNULL(B.STATION_SERVICE, 0)), 3) AS UNIT_NET
FROM
    UNIT_GROSS A
    LEFT JOIN STATION_SERVICE B ON A.Unit = B.Unit
    AND A.DATEPART = B.DATEPART
ORDER BY
    A.DATEPART,
    A.Unit;

RETURN;

END;

GO