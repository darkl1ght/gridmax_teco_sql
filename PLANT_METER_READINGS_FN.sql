USE [HIS_USER]
GO
SET
    ANSI_NULLS ON
GO
SET
    QUOTED_IDENTIFIER ON
GO
    CREATE FUNCTION [GADS].[PLANT_METER_READINGS_FN] (
        @SearchUID VARCHAR(256),
        @StartDate DATETIME,
        @EndDate DATETIME
    ) RETURNS @RESULTS TABLE(
        DATEPART INT,
        STATION VARCHAR(256),
        PLANT VARCHAR(256),
        UNIT VARCHAR(256),
        VALUE INT,
        VALUETYPE VARCHAR(256)
    ) AS BEGIN
INSERT INTO
    @RESULTS (
        DATEPART,
        STATION,
        PLANT,
        UNIT,
        VALUE,
        VALUETYPE
    )
SELECT
    DAY(DATEADD(HOUR, -1, HIST_TIMESTAMP_TZ)) AS DATEPART,
    [ObjectGrandParent] as [Station],
    [ObjectParent] as [Plant],
    [Label] as [Unit],
    SUM(
        ROUND(
            CASE
                WHEN [MeasUnit] IN('KWH', 'KW') THEN ISNULL([VALUE] / 1000, 0)
                ELSE ISNULL([VALUE], 0)
            END,
            3
        )
    ) AS 'VALUE',
    CASE
        WHEN [ObjectType] = 'Generator' THEN 'g'
        WHEN [ObjectType] = 'Station Service' THEN 's'
        WHEN [ObjectType] = 'Reserve' THEN 'r'
    END AS [VALUETYPE]
FROM
    his_user.GOSS_Search_AccumulatorHistory_Fn(@SearchUID, @StartDate, @EndDate)
GROUP BY
    DAY(DATEADD(HOUR, -1, HIST_TIMESTAMP_TZ)),
    [ObjectGrandParent],
    [ObjectParent],
    [ObjectType],
    [Label]
END;

GO