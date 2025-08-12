----------------------------------------------------
-------------------- Common ------------------------
----------------------------------------------------

delete from ORAP_EVENT_HOURS_SUMMARY;
delete from ORAP_FLEET_RAM_KPIS;
delete from ORAP_GEN_FUEL_PERF;
delete from ORAP_GEN_PERF_INCENTIVES;
delete from ORAP_NET_FACTORS_RATES;
delete from ORAP_OUTAGES;
delete from GRIDMAX_DAILY_GEN;
delete from PI_GADS_DATA;
delete from ALLEGRO_GADS_SLF_CONSUMPTION_QUALITY;
delete from PGSDSS_GADS_DAY;

call SF_DB_AHUB_DEV2.AHUB_PSA.GADS_DATA_CDC('2024-10-01T00:00:00','2024-10-31T00:00:00');

Select 'ORAP_EVENT_HOURS_SUMMARY' as TableName, count(*) from ORAP_EVENT_HOURS_SUMMARY UNION ALL
Select 'ORAP_FLEET_RAM_KPIS' as TableName, count(*) from ORAP_FLEET_RAM_KPIS UNION ALL
Select 'ORAP_GEN_FUEL_PERF' as TableName, count(*) from ORAP_GEN_FUEL_PERF UNION ALL
Select 'ORAP_GEN_PERF_INCENTIVES' as TableName, count(*) from ORAP_GEN_PERF_INCENTIVES UNION ALL
Select 'ORAP_NET_FACTORS_RATES' as TableName, count(*) from ORAP_NET_FACTORS_RATES UNION ALL
Select 'ORAP_OUTAGES' as TableName, count(*) from ORAP_OUTAGES UNION ALL
Select 'GRIDMAX_DAILY_GEN' as TableName, count(*) from GRIDMAX_DAILY_GEN UNION ALL
Select 'PI_GADS_DATA' as TableName, count(*) from PI_GADS_DATA UNION ALL
Select 'ALLEGRO_GADS_SLF_CONSUMPTION_QUALITY' as TableName, count(*) from ALLEGRO_GADS_SLF_CONSUMPTION_QUALITY UNION ALL
Select 'PGSDSS_GADS_DAY' as TableName, count(*) from PGSDSS_GADS_DAY;

----------------------------------------------------
-------------------- Gridmax -----------------------
----------------------------------------------------
call SF_DB_AHUB_DEV2.AHUB_PSA.GRIDMAX_GEN_CDC('ALL','2024-10-01T00:00:00','2024-10-31T00:00:00');
select * from GRIDMAX_DAILY_GEN;
delete from GRIDMAX_DAILY_GEN;

----------------------------------------------------
-------------------- ORAP --------------------------
----------------------------------------------------
call SF_DB_AHUB_DEV2.AHUB_PSA.ORAP_KPI_CDC('GetOutages','2024-10-01T00:00:00','2024-10-31T00:00:00');
select * from ORAP_OUTAGES;
delete from ORAP_OUTAGES;

----------------------------------------------------
-------------------- Allegro -----------------------
----------------------------------------------------

-- Solid & Liquid fuel
-- Consumption Data and Fuel Quality Metrics (Moisture, Sulfur, Ash, Chlorine levels, SO₂ emissions per MMBTU)
CALL SF_DB_AHUB_DEV2.AHUB_PSA.ALLEGRO_GADS_DATA_CDC('SlfConsumptionQuality','2024-10-01T00:00:00','2024-10-31T00:00:00');
SELECT * FROM AHUB_PSA.ALLEGRO_GADS_SLF_CONSUMPTION_QUALITY order by load_ts desc;
DELETE FROM AHUB_PSA.ALLEGRO_GADS_SLF_CONSUMPTION_QUALITY;

----------------------------------------------------
-------------------- PI ----------------------------
----------------------------------------------------
SELECT * FROM AHUB_PSA.PI_GADS_DATA order by load_ts desc;
CALL SF_DB_AHUB_DEV2.AHUB_PSA.PI_GADS_DATA_CDC('PiData','2024-10-01T00:00:00','2024-10-31T00:00:00');
CALL SF_DB_AHUB_DEV2.AHUB_PSA.PI_GADS_DATA_CDC('PiData', null, null);
SELECT * FROM AHUB_PSA.PI_GADS_DATA order by load_ts desc;
SELECT * FROM AHUB_PSA.PI_GADS_DATA where TAG = '1AFGSFI602' order by load_ts desc;
DELETE FROM AHUB_PSA.PI_GADS_DATA;

----------------------------------------------------
-------------------- PGSDSS ------------------------
----------------------------------------------------

CALL SF_DB_AHUB_DEV2.AHUB_PSA.PGSDSS_GADS_DATA_CDC('Day','2024-10-01T00:00:00','2024-10-31T00:00:00');
SELECT * FROM AHUB_PSA.PGSDSS_GADS_DAY order by load_ts desc;
DELETE FROM AHUB_PSA.PGSDSS_GADS_DAY;




