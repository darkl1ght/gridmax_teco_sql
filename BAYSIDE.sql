USE [HIS_USER]
GO
SET
  ANSI_NULLS ON
GO
SET
  QUOTED_IDENTIFIER ON
GO
  CREATE FUNCTION [GADS].[BAYSIDE_MONTHLY_GEN_FN] (@SearchUID, @StartDate, @EndDate) RETURNS @RESULTS TABLE(
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
  STGRP AS (
    SELECT
      'ST#1' AS UNIT
    UNION
    SELECT
      'ST#2'
  ),
  BIGCTGRP AS (
    SELECT
      'CT#1A' AS UNIT
    UNION
    SELECT
      'CT#1B'
    UNION
    SELECT
      'CT#1C'
    UNION
    SELECT
      'CT#2A'
    UNION
    SELECT
      'CT#2B'
    UNION
    SELECT
      'CT#2C'
    UNION
    SELECT
      'CT#2D'
  ),
  BAYSIDEGRP1 AS (
    SELECT
      'CT#1A' AS UNIT
    UNION
    SELECT
      'CT#1B'
    UNION
    SELECT
      'CT#1C'
  ),
  BAYSIDEGRP2 AS (
    SELECT
      'CT#2A' AS UNIT
    UNION
    SELECT
      'CT#2B'
    UNION
    SELECT
      'CT#2C'
    UNION
    SELECT
      'CT#2D'
  ),
  RESERVE1 AS (
    SELECT
      'RS#1A' AS UNIT
    UNION
    SELECT
      'RS#1B'
    UNION
    SELECT
      'RS#2A'
    UNION
    SELECT
      'RS#2B'
    UNION
    SELECT
      'RS#3A'
    UNION
    SELECT
      'RS#3B'
  ),
  RESERVE2 AS (
    SELECT
      'RS#4A' AS UNIT
    UNION
    SELECT
      'RS#4B'
  ),
  UNIT_GROSS AS (
    SELECT
      DATEPART,
      STATION,
      PLANT,
      UNIT,
      SUM(VALUE) AS UNIT_GROSS,
      IIF(
        UNIT IN (
          SELECT
            UNIT
          FROM
            STGRP
        ),
        1,
        0
      ) as STGRP,
      IIF(
        UNIT IN (
          SELECT
            UNIT
          FROM
            BIGCTGRP
        ),
        1,
        0
      ) as BIGCTGRP,
      IIF(
        UNIT IN (
          SELECT
            UNIT
          FROM
            BAYSIDEGRP1
        ),
        1,
        0
      ) as BAYSIDEGRP1,
      IIF(
        UNIT IN (
          SELECT
            UNIT
          FROM
            BAYSIDEGRP2
        ),
        1,
        0
      ) as BAYSIDEGRP2
    FROM
      PLANT_METER_READINGS
    WHERE
      VALUETYPE = 'g'
    GROUP BY
      DATEPART,
      STATION,
      PLANT,
      UNIT,
      VALUETYPE
  ),
  BAYSIDEGRP1_GROSS AS (
    SELECT
      DATEPART,
      SUM(UNIT_GROSS) AS BAYSIDEGRP1_GROSS
    FROM
      UNIT_GROSS
    WHERE
      BAYSIDEGRP1 = 1
    GROUP BY
      DATEPART
  ),
  BAYSIDEGRP2_GROSS AS (
    SELECT
      DATEPART,
      SUM(UNIT_GROSS) AS BAYSIDEGRP2_GROSS
    FROM
      UNIT_GROSS
    WHERE
      BAYSIDEGRP2 = 1
    GROUP BY
      DATEPART
  ),
  STGRP_GROSS AS (
    SELECT
      DATEPART,
      SUM(UNIT_GROSS) AS STGRP_GROSS
    FROM
      UNIT_GROSS
    WHERE
      STGRP = 1
    GROUP BY
      DATEPART
  ),
  BIGCTGRP_GROSS AS (
    SELECT
      DATEPART,
      SUM(UNIT_GROSS) AS BIGCTGRP_GROSS
    FROM
      UNIT_GROSS
    WHERE
      BIGCTGRP = 1
    GROUP BY
      DATEPART
  ),
  UNIT_STNSVC AS (
    SELECT
      DATEPART,
      b.UNIT,
      SUM(VALUE) AS STN_SVC
    FROM
      PLANT_METER_READINGS a,
      BAYSIDEGRP1 b
    WHERE
      VALUETYPE = 's'
      AND a.UNIT = 'CT#1'
    GROUP BY
      DATEPART,
      b.UNIT,
      VALUETYPE
    UNION
    ALL
    SELECT
      DATEPART,
      b.UNIT,
      SUM(VALUE) AS STN_SVC
    FROM
      PLANT_METER_READINGS a,
      BAYSIDEGRP2 b
    WHERE
      VALUETYPE = 's'
      AND a.UNIT = 'CT#2'
    GROUP BY
      DATEPART,
      b.UNIT,
      VALUETYPE
    UNION
    ALL
    SELECT
      DATEPART,
      a.UNIT,
      SUM(VALUE) AS STN_SVC
    FROM
      PLANT_METER_READINGS a
    WHERE
      VALUETYPE = 's'
      AND a.UNIT NOT IN ('CT#1', 'CT#2')
    GROUP BY
      DATEPART,
      a.UNIT,
      VALUETYPE
  ),
  RESERVE AS (
    SELECT
      DATEPART,
      UNIT,
      SUM(VALUE) AS RESERVE,
      IIF(
        UNIT IN (
          SELECT
            UNIT
          FROM
            RESERVE1
        ),
        1,
        0
      ) as RESERVE1,
      IIF(
        UNIT IN (
          SELECT
            UNIT
          FROM
            RESERVE2
        ),
        1,
        0
      ) as RESERVE2
    FROM
      PLANT_METER_READINGS
    WHERE
      VALUETYPE = 'r'
    GROUP BY
      DATEPART,
      UNIT,
      VALUETYPE
  ),
  UNIT_RESERVE AS (
    -- RESERVE1SUM to be attached to STGRP
    SELECT
      DATEPART,
      sg.UNIT,
      SUM(RESERVE) AS RESERVE_SUM
    FROM
      RESERVE rv,
      STGRP sg
    WHERE
      RESERVE1 = 1
    GROUP BY
      DATEPART,
      sg.UNIT
    UNION
    ALL -- RESERVE2SUM to be attached to BIGCTGRP
    SELECT
      DATEPART,
      bg.UNIT,
      SUM(RESERVE) AS RESERVE_SUM
    FROM
      RESERVE rv,
      BIGCTGRP bg
    WHERE
      RESERVE2 = 1
    GROUP BY
      DATEPART,
      bg.UNIT
  ),
  PRE_FINAL_OUTPUT AS (
    SELECT
      ug.DATEPART,
      ug.STATION,
      ug.PLANT,
      ug.UNIT,
      ug.UNIT_GROSS,
      b1.BAYSIDEGRP1_GROSS,
      b2.BAYSIDEGRP2_GROSS,
      ss.STN_SVC,
      sg.STGRP_GROSS,
      bg.BIGCTGRP_GROSS,
      ur.RESERVE_SUM,
      CASE
        WHEN BAYSIDEGRP1_GROSS = 0 THEN STN_SVC * 1 / 3
        WHEN BAYSIDEGRP1_GROSS IS NOT NULL
        AND BAYSIDEGRP1_GROSS <> 0 THEN STN_SVC * UNIT_GROSS / BAYSIDEGRP1_GROSS
        WHEN BAYSIDEGRP2_GROSS = 0 THEN STN_SVC * 1 / 4
        WHEN BAYSIDEGRP2_GROSS IS NOT NULL
        AND BAYSIDEGRP2_GROSS <> 0 THEN STN_SVC * UNIT_GROSS / BAYSIDEGRP2_GROSS
        ELSE STN_SVC
      END AS PRORATED_STN_SVC,
      CASE
        WHEN STGRP_GROSS = 0 THEN 0.5 * RESERVE_SUM
        WHEN STGRP_GROSS <> 0
        AND STGRP_GROSS IS NOT NULL THEN RESERVE_SUM * UNIT_GROSS / STGRP_GROSS
        WHEN BIGCTGRP_GROSS = 0 THEN 0.1428571 * RESERVE_SUM
        WHEN BIGCTGRP_GROSS <> 0
        AND BIGCTGRP_GROSS IS NOT NULL THEN RESERVE_SUM * UNIT_GROSS / BIGCTGRP_GROSS
        ELSE 0
      END AS PRORATED_RESERVE
    FROM
      UNIT_GROSS ug
      LEFT JOIN BAYSIDEGRP1_GROSS b1 ON ug.DATEPART = b1.DATEPART
      AND ug.BAYSIDEGRP1 = 1
      LEFT JOIN BAYSIDEGRP2_GROSS b2 ON ug.DATEPART = b2.DATEPART
      AND ug.BAYSIDEGRP2 = 1
      LEFT JOIN STGRP_GROSS sg ON ug.DATEPART = sg.DATEPART
      AND ug.STGRP = 1
      LEFT JOIN BIGCTGRP_GROSS bg ON ug.DATEPART = bg.DATEPART
      AND ug.BIGCTGRP = 1
      LEFT JOIN UNIT_STNSVC ss ON ug.DATEPART = ss.DATEPART
      AND ug.UNIT = ss.UNIT
      LEFT JOIN UNIT_RESERVE ur ON ug.DATEPART = ur.DATEPART
      AND ug.UNIT = ur.UNIT
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
  ROUND(
    UNIT_GROSS - PRORATED_STN_SVC - PRORATED_RESERVE,
    2
  ) AS UNIT_NET
FROM
  PRE_FINAL_OUTPUT;

RETURN;

END;

GO