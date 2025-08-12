-- TO run
-- snowsql -c TECO_DBT_DEV -f dev_rollin.sql
--
!spool dev_rollin.log
USE ROLE SF_RL_DBT_DEVELOPER;
USE DATABASE SF_DB_DAP_RAW_DEV;

create schema IF NOT EXISTS GADS;
USE SCHEMA GADS;

create TABLE IF NOT EXISTS EXT_FUNC_CONTROL_TABLE (
	TABLE_NM VARCHAR(200) NOT NULL,
	EXT_FUNC_START_DT TIMESTAMP_NTZ(9) NOT NULL,
	EXT_FUNC_END_DT TIMESTAMP_NTZ(9),
	EXT_FUNC_STS_CD VARCHAR(1) NOT NULL,
	NOTES VARCHAR
);

create TABLE IF NOT EXISTS MANUAL_GADS_METADATA2 (
    MANUAL_GADS_META_KEY_NB NUMBER,
    LABEL VARCHAR,
    METRIC_CD VARCHAR,
    ASSET_TYPE_CD VARCHAR,
    ASSET_CD VARCHAR,
    IS_ACTIVE NUMBER(38,0),
    METRIC_FREQ VARCHAR,
    DISPLAY_ORDER NUMBER(38,0)
);

create TABLE IF NOT EXISTS MANUAL_GADS_DATA2 (
    ASSET_TYPE_CD VARCHAR,
    ASSET_CD VARCHAR,
    MANUAL_GADS_META_KEY_NB_FK NUMBER,
    VALUE NUMBER(38,10),
    BEGIN_DT VARCHAR,
    END_DT VARCHAR,
    LOAD_TS TIMESTAMP_NTZ(9)
);

create TABLE IF NOT EXISTS WORKFLOW_USERS (
	WORKFLOW_USER_KEY_NB NUMBER(38,0),
	EMAIL VARCHAR(16777216),
	NAME VARCHAR(16777216),
	ROLES VARCHAR(16777216),
	ISACTIVE BOOLEAN,
	unique (EMAIL)
);

create TABLE IF NOT EXISTS OVERRIDE_WORKFLOW_DATA (
	ASSET_CD VARCHAR(16777216),
	ASSET_TYPE_CD VARCHAR(16777216),
	BEGIN_DT DATE,
	END_DT DATE,
	SRC_CD VARCHAR(16777216) DEFAULT 'OVERRIDDEN_GADS_DATA',
	LOAD_TS TIMESTAMP_NTZ(9),
	METRIC_CD VARCHAR(16777216),
	VALUE FLOAT,
	PREV_VALUE FLOAT,
	STATUS VARCHAR(16777216),
	SUBMITTER_EMAIL VARCHAR(16777216),
	APPROVER_EMAIL VARCHAR(16777216),
	WORKFLOWCOMMENTS VARCHAR(16777216)
);

CREATE PROCEDURE IF NOT EXISTS GET_STALE_DUPE_RECORDS_QUERY(P_DB VARCHAR, P_SCHEMA VARCHAR, P_TABLE VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS 
$$
DECLARE
    partition_columns_list VARCHAR;
    final_query VARCHAR;
    full_table_name VARCHAR;
    PRUNE_EXCEPTION EXCEPTION (-20002, 'Cannot find columns to partition. Please check params or permissions');
BEGIN
    -- Example: CALL GET_STALE_DUPE_RECORDS_QUERY('SF_DB_DAP_RAW_DEV','GRIDMAX','DAILY_GEN2');
    full_table_name := :P_DB || '.' || :P_SCHEMA || '.' || :P_TABLE;

    SELECT LISTAGG(COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
    INTO partition_columns_list
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_CATALOG = UPPER(:P_DB)
      AND TABLE_SCHEMA = UPPER(:P_SCHEMA)
      AND TABLE_NAME = UPPER(:P_TABLE)
      AND COLUMN_NAME != 'LOAD_TS';

    IF (partition_columns_list IS NULL OR partition_columns_list = '') THEN
            RAISE PRUNE_EXCEPTION;
    END IF;

    final_query := 'SELECT * EXCLUDE RN FROM (SELECT *,' ||
                           ' RANK() OVER (PARTITION BY ' || partition_columns_list ||
                           ' ORDER BY LOAD_TS DESC) AS rn ' ||
                           'FROM ' || full_table_name || ') WHERE RN > 1;';
    
    return final_query; 
END;
$$;

-- CALL GET_STALE_DUPE_RECORDS_QUERY('SF_DB_DAP_RAW_DEV','GRIDMAX','DAILY_GEN2');

CREATE PROCEDURE IF NOT EXISTS GADS_ALL_DATA_CDC2()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER -- Or OWNER, depending on your permission requirements
AS
$$
  var P_BEGIN_DATE;
  var P_END_DATE;
  var DAYS_THRESHOLD = 4;
  var FIRST_DAY_OF_YEAR;
  var LAST_DAY_OF_YEAR;

  var INGEST_ALLEGRO_RESULT; 
  var INGEST_GRIDMAX_RESULT;
  var INGEST_ORAP_GFP_RESULT;
  var INGEST_ORAP_GFP_Y_RESULT;
  var INGEST_ORAP_GPI_RESULT;
  var INGEST_ORAP_OUTAGES_RESULT;
  var INGEST_ORAP_RAMKPI_RESULT;
  var INGEST_ORAP_EVTHRS_RESULT;
  var INGEST_ORAP_NFR_RESULT;
  var INGEST_PGSDSS_HOUR_RESULT;
  var INGEST_PI_DATA_RESULT;

  var STATUS_REPORT;

  try {

    var current_date_rs = snowflake.execute({sqlText: "SELECT CURRENT_DATE()"});
    current_date_rs.next();
    var current_date_val = current_date_rs.getColumnValue(1);

    var current_day_of_month_rs = snowflake.execute({sqlText: "SELECT DATE_PART('DAY', CURRENT_DATE())"});
    current_day_of_month_rs.next();
    var current_day_of_month = current_day_of_month_rs.getColumnValue(1);

    if (current_day_of_month <= DAYS_THRESHOLD) {
      var prev_month_dates_rs = snowflake.execute({
        sqlText: "SELECT TRUNC(DATEADD(MONTH, -1, CURRENT_DATE()), 'MONTH')::VARCHAR, LAST_DAY(DATEADD(MONTH, -1, CURRENT_DATE()))::VARCHAR"
      });
      prev_month_dates_rs.next();
      P_BEGIN_DATE = prev_month_dates_rs.getColumnValue(1);
      P_END_DATE = prev_month_dates_rs.getColumnValue(2);
    } else {
      var curr_month_dates_rs = snowflake.execute({
        sqlText: "SELECT TRUNC(CURRENT_DATE(), 'MONTH')::VARCHAR, LAST_DAY(CURRENT_DATE())::VARCHAR"
      });
      curr_month_dates_rs.next();
      P_BEGIN_DATE = curr_month_dates_rs.getColumnValue(1);
      P_END_DATE = curr_month_dates_rs.getColumnValue(2);
    }

    var year_dates_rs = snowflake.execute({
      sqlText: `SELECT DATE_TRUNC('YEAR', '${P_BEGIN_DATE}'::DATE)::VARCHAR, DATEADD('DAY', -1, DATEADD('YEAR', 1, DATE_TRUNC('YEAR', '${P_BEGIN_DATE}'::DATE)))::VARCHAR`
    });
    year_dates_rs.next();
    FIRST_DAY_OF_YEAR = year_dates_rs.getColumnValue(1);
    LAST_DAY_OF_YEAR = year_dates_rs.getColumnValue(2);
 
    var allegro_rs = snowflake.execute({
        sqlText: `CALL ALLEGRO.GADS_DATA_CDC2('SlfConsumptionQuality', '${P_BEGIN_DATE}', '${P_END_DATE}')`
    });
    allegro_rs.next();
    INGEST_ALLEGRO_RESULT = allegro_rs.getColumnValue(1);

        var gridmax_rs = snowflake.execute({
        sqlText: `CALL GRIDMAX.DAILY_GEN_CDC2('ALL', '${P_BEGIN_DATE}', '${P_END_DATE}')`
    });
    gridmax_rs.next();
    INGEST_GRIDMAX_RESULT = gridmax_rs.getColumnValue(1);

    var orap_gfp_rs = snowflake.execute({
        sqlText: `CALL ORAP.ORAP_KPI_CDC2('GetGenFuelPerf', '${P_BEGIN_DATE}', '${P_END_DATE}')`
    });
    orap_gfp_rs.next();
    INGEST_ORAP_GFP_RESULT = orap_gfp_rs.getColumnValue(1);

    var orap_gfp_y_rs = snowflake.execute({
        sqlText: `CALL ORAP.ORAP_KPI_CDC2('GetGenFuelPerf', '${FIRST_DAY_OF_YEAR}', '${LAST_DAY_OF_YEAR}')`
    });
    orap_gfp_y_rs.next();
    INGEST_ORAP_GFP_Y_RESULT = orap_gfp_y_rs.getColumnValue(1);

    var orap_gpi_rs = snowflake.execute({
        sqlText: `CALL ORAP.ORAP_KPI_CDC2('GetGenPerfIncent', '${P_BEGIN_DATE}', '${P_END_DATE}')`
    });
    orap_gpi_rs.next();
    INGEST_ORAP_GPI_RESULT = orap_gpi_rs.getColumnValue(1);

    var orap_outages_rs = snowflake.execute({
        sqlText: `CALL ORAP.ORAP_KPI_CDC2('GetOutages', '${P_BEGIN_DATE}', '${P_END_DATE}')`
    });
    orap_outages_rs.next();
    INGEST_ORAP_OUTAGES_RESULT = orap_outages_rs.getColumnValue(1);

    var orap_ramkpi_rs = snowflake.execute({
        sqlText: `CALL ORAP.ORAP_KPI_CDC2('GetFleetRAMKPIs', '${P_BEGIN_DATE}', '${P_END_DATE}')`
    });
    orap_ramkpi_rs.next();
    INGEST_ORAP_RAMKPI_RESULT = orap_ramkpi_rs.getColumnValue(1);

    var orap_evthrs_rs = snowflake.execute({
        sqlText: `CALL ORAP.ORAP_KPI_CDC2('GetEvtHrsSummary', '${P_BEGIN_DATE}', '${P_END_DATE}')`
    });
    orap_evthrs_rs.next();
    INGEST_ORAP_EVTHRS_RESULT = orap_evthrs_rs.getColumnValue(1);

    var orap_nfr_rs = snowflake.execute({
        sqlText: `CALL ORAP.ORAP_KPI_CDC2('GetNetFactorsRates', '${P_BEGIN_DATE}', '${P_END_DATE}')`
    });
    orap_nfr_rs.next();
    INGEST_ORAP_NFR_RESULT = orap_nfr_rs.getColumnValue(1);

    var pgsdss_hour_rs = snowflake.execute({
        sqlText: `CALL PGSDSS.GADS_DATA_CDC2('Hour', '${P_BEGIN_DATE}', '${P_END_DATE}')`
    });
    pgsdss_hour_rs.next();
    INGEST_PGSDSS_HOUR_RESULT = pgsdss_hour_rs.getColumnValue(1);

    var pi_data_rs = snowflake.execute({
        sqlText: `CALL PI.GADS_DATA_CDC2('PiData', '${P_BEGIN_DATE}', '${P_END_DATE}')`
    });
    pi_data_rs.next();
    INGEST_PI_DATA_RESULT = pi_data_rs.getColumnValue(1); 

    STATUS_REPORT = `Daily GADS Ingestion Status Report:\n\r` +
                    `-----------------------------\n\r` +
                    `Allegro Result: ${INGEST_ALLEGRO_RESULT || 'N/A'}\n\r` +
                    `Gridmax Result:     ${INGEST_GRIDMAX_RESULT || 'N/A'}\n\r` +
                    `ORAP GetGenFuelPerf Result:     ${INGEST_ORAP_GFP_RESULT || 'N/A'}\n\r` +
                    `ORAP GetGenFuelPerfY Result:     ${INGEST_ORAP_GFP_Y_RESULT || 'N/A'}\n\r` +
                    `ORAP GetGenPerfIncent Result:     ${INGEST_ORAP_GPI_RESULT || 'N/A'}\n\r` +
                    `ORAP GetOutages Result:     ${INGEST_ORAP_OUTAGES_RESULT || 'N/A'}\n\r` +
                    `ORAP GetFleetRAMKPIs Result:     ${INGEST_ORAP_RAMKPI_RESULT || 'N/A'}\n\r` +
                    `ORAP GetEvtHrsSummary Result:     ${INGEST_ORAP_EVTHRS_RESULT || 'N/A'}\n\r` +
                    `ORAP GetNetFactorsRates Result:     ${INGEST_ORAP_NFR_RESULT || 'N/A'}\n\r` +
                    `PGSDSS HOUR Result: ${INGEST_PGSDSS_HOUR_RESULT || 'N/A'}\n\r` +
                    `PI DATA Result: ${INGEST_PI_DATA_RESULT || 'N/A'}\n\r` +
                    `-----------------------------\n\r` +
                    `Processed Period:    ${P_BEGIN_DATE} to ${P_END_DATE}\n\r` +
                    `Run Date:            ${current_date_val}\n\r` +
                    `-----------------------------`;
    return STATUS_REPORT;
  } catch (err) {
    return "Failed to execute GADS_ALL_DATA_CDC()" + err.message;
  }
$$;

create schema IF NOT EXISTS ALLEGRO;
USE SCHEMA ALLEGRO;

CREATE TABLE IF NOT EXISTS GADS_SLF_CONSUMPTION_QUALITY2 (
	AREA VARCHAR,
	DISCHARGE_LOCATION VARCHAR,
	COMMODITY_CLASS VARCHAR,
	QUANTITY VARCHAR,
	BTU_FACTOR VARCHAR,
	ACCOUNTING_DATE TIMESTAMP_NTZ(9),
	OPERATIONAL_DATE TIMESTAMP_NTZ(9),
	BEGIN_DT VARCHAR, 
	END_DT VARCHAR,
	LOAD_TS TIMESTAMP_NTZ(9)
);

CREATE EXTERNAL FUNCTION IF NOT EXISTS ALLEGRO_EXT_FUNC(payload STRING)
RETURNS VARIANT
API_INTEGRATION = INT_API_AZURE_DAP_DEV_GADS
HEADERS = ('Content-Type' = 'application/json')
AS 'https://apim-teco-ahubgads-dev.azure-api.net/azfun-teco-ahubgads-eastus-dev/allegro';

CREATE PROCEDURE IF NOT EXISTS GADS_DATA_CDC2(p_function_name STRING, p_start_date STRING, p_end_date STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var log_messages = [];
    var v_table_name;
    try {
        var p_function_name = arguments[0];
        var v_now = new Date();
        
        // First day of the current month without time
        var v_start_date = new Date(v_now.getFullYear(), v_now.getUTCMonth(), 1);
        v_start_date = P_START_DATE || v_start_date.toISOString().split('T')[0];
        
        // Last day of the current month without time
        var v_end_date = new Date(v_now.getFullYear(), v_now.getUTCMonth() + 1, 0);
        v_end_date = P_END_DATE || v_end_date.toISOString().split('T')[0];
        
        var v_json_request;
        var v_json_response;
        var v_load_timestamp;        
        var insertQuery;
        var columnNames;
        var valuesList = [];        

        log_messages.push("Procedure started");
        
        var rs = snowflake.execute({sqlText: "SELECT TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF')"});
        if (rs.next()) {
            v_load_timestamp = rs.getColumnValue(1);
        }
        
        v_json_request = JSON.stringify({ func: p_function_name, beginDate: v_start_date, endDate: v_end_date });

        switch (p_function_name) {
            case "SlfConsumptionQuality":
                v_table_name = "ALLEGRO.GADS_SLF_CONSUMPTION_QUALITY2";
                columnNames = "(AREA, DISCHARGE_LOCATION, COMMODITY_CLASS, QUANTITY, BTU_FACTOR, ACCOUNTING_DATE, OPERATIONAL_DATE, BEGIN_DT, END_DT, LOAD_TS)";
                break;
        }

        snowflake.execute({ 
            sqlText: 'INSERT INTO GADS.EXT_FUNC_CONTROL_TABLE (TABLE_NM, EXT_FUNC_START_DT, EXT_FUNC_STS_CD) VALUES (?, ?, ?)',                           
            binds: [v_table_name, v_load_timestamp, 'P']
        });
        log_messages.push("Inserted record in control table with status P");
        
        rs = snowflake.execute({
                sqlText: "SELECT ALLEGRO.ALLEGRO_EXT_FUNC(:1)",
                binds: [v_json_request]
            });
        log_messages.push(`Invoked ExtFn with params: ${p_function_name}, ${v_start_date}, ${v_end_date}`);
        
        if (rs.next()) {
            response = rs.getColumnValue(1);
            v_json_response = JSON.parse(response);

            for (var i = 0; i < v_json_response.length; i++) {
                switch (p_function_name) {
                    case "SlfConsumptionQuality":
                        valuesList.push(`('${v_json_response[i].area}', '${v_json_response[i].dischargelocation}', '${v_json_response[i].commodityclass}', '${v_json_response[i].quantity}', '${v_json_response[i].btu_factor}', '${v_json_response[i].accountingdate}', '${v_json_response[i].operationaldate}', '${v_start_date}', '${v_end_date}', '${v_load_timestamp}')`);
                        break;
                }
            }          
        }

        if (valuesList.length > 0) {
            insertQuery = `INSERT INTO ${v_table_name} ${columnNames} VALUES ` + valuesList.join(",");
            snowflake.execute({ sqlText: insertQuery });            
        }
        var summary = "Data inserted into " + v_table_name + ", Rows inserted: " + v_json_response.length;
        log_messages.push(summary);
                
        snowflake.execute({ 
            sqlText: "UPDATE GADS.EXT_FUNC_CONTROL_TABLE SET EXT_FUNC_STS_CD = ?, EXT_FUNC_END_DT = CURRENT_TIMESTAMP, NOTES = ? WHERE TABLE_NM = ? AND EXT_FUNC_STS_CD = 'P';",
            binds: ['C', log_messages.join(';\n'), v_table_name]
        });
        
        return summary;
    } catch (err) {
        log_messages.push("ERROR: " + err.message);
        snowflake.execute({ 
            sqlText: "UPDATE GADS.EXT_FUNC_CONTROL_TABLE SET EXT_FUNC_STS_CD = ?, EXT_FUNC_END_DT = CURRENT_TIMESTAMP, NOTES = ? WHERE TABLE_NM = ? AND EXT_FUNC_STS_CD = 'P';",
            binds: ['E', log_messages.join(';\n'), v_table_name]
        });
        return JSON.stringify({ status: "Error", message: err.message });
    }
$$;

-- call ALLEGRO.GADS_DATA_CDC2('SlfConsumptionQuality','2025-04-01','2025-04-30' );

---------

create schema IF NOT EXISTS GRIDMAX;
USE SCHEMA GRIDMAX;

create TABLE IF NOT EXISTS DAILY_GEN2 (
	YEAR NUMBER(38,0),
	MONTH NUMBER(38,0),
	DATEPART NUMBER(38,0),
	PLANT VARCHAR,
	STATION VARCHAR,
	UNIT VARCHAR,
	UNIT_GROSS NUMBER(38,18),
	UNIT_NET NUMBER(38,3),
    BEGIN_DT VARCHAR,
    END_DT VARCHAR,
	LOAD_TS TIMESTAMP_NTZ(9)
);

CREATE EXTERNAL FUNCTION IF NOT EXISTS GRIDMAX_EXT_FUNC(payload STRING)
RETURNS VARIANT
API_INTEGRATION = INT_API_AZURE_DAP_DEV_GADS
HEADERS = ('Content-Type' = 'application/json')
AS 'https://apim-teco-ahubgads-dev.azure-api.net/azfun-teco-ahubgads-eastus-dev/gridmax';

CREATE PROCEDURE IF NOT EXISTS DAILY_GEN_CDC2(p_function_name STRING, p_start_date STRING, p_end_date STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var log_messages = [];
    var v_table_name;
    try {
        var p_function_name = arguments[0];
        var v_json_request;
        var columnNames;
        var valuesList = [];        
        var v_json_request;
        var v_json_response;
        var v_load_timestamp;
        
        var v_now = new Date();
        
        var v_start_date = new Date(v_now.getFullYear(), v_now.getUTCMonth(), 1);
        v_start_date = P_START_DATE || v_start_date.toISOString().split('T')[0];
        
        var v_end_date = new Date(v_now.getFullYear(), v_now.getUTCMonth() + 1, 0);
        v_end_date = P_END_DATE || v_end_date.toISOString().split('T')[0];

        var dateObj = new Date(v_start_date);
        var v_month = dateObj.getUTCMonth() + 1;
        var v_year = dateObj.getUTCFullYear(); // Note: sf bug: sf js engine give wrong year(1 less) for getFullYear() 

        log_messages.push("Procedure started");
        var rs = snowflake.execute({sqlText: "SELECT TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF')"});
        if (rs.next()) {
            v_load_timestamp = rs.getColumnValue(1);
        }
        
        switch (p_function_name) {
            case "ALL":
                v_table_name = "GRIDMAX.DAILY_GEN2";
                columnNames = "(YEAR, MONTH, DATEPART, PLANT, STATION, UNIT, UNIT_GROSS, UNIT_NET, BEGIN_DT, END_DT, LOAD_TS)";
                break;
        }

        snowflake.execute({ 
            sqlText: 'INSERT INTO GADS.EXT_FUNC_CONTROL_TABLE (TABLE_NM, EXT_FUNC_START_DT, EXT_FUNC_STS_CD) VALUES (?, ?, ?)',                           
            binds: [v_table_name, v_load_timestamp, 'P']
        });
        log_messages.push("Inserted record in control table with status P");

        
        v_json_request = JSON.stringify({ func: p_function_name, year: v_year, month: v_month });
        rs = snowflake.execute({
                sqlText: "SELECT GRIDMAX.GRIDMAX_EXT_FUNC(:1)",
                binds: [v_json_request]
            });
        log_messages.push(`Invoked ExtFn with params: ${p_function_name}, ${v_start_date}, ${v_end_date}`);
        
        if (rs.next()) {
            response = rs.getColumnValue(1);
            v_json_response = JSON.parse(response);

            for (var i = 0; i < v_json_response.length; i++) {
                switch (p_function_name) {
                    case "ALL":
                        valuesList.push(`('${v_year}', '${v_month}', '${v_json_response[i].DATEPART}', '${v_json_response[i].PLANT}', '${v_json_response[i].STATION}', '${v_json_response[i].UNIT}', '${v_json_response[i].UNIT_GROSS}', '${v_json_response[i].UNIT_NET}', '${v_start_date}', '${v_end_date}', '${v_load_timestamp}')`);
                        break;
                }
            }          
        }

        if (valuesList.length > 0) {
            insertQuery = `INSERT INTO ${v_table_name} ${columnNames} VALUES ` + valuesList.join(",");
            snowflake.execute({ sqlText: insertQuery });            
        }
        var summary = "Data inserted into " + v_table_name + ", Rows inserted: " + v_json_response.length;
        log_messages.push(summary);
                
        snowflake.execute({ 
            sqlText: "UPDATE GADS.EXT_FUNC_CONTROL_TABLE SET EXT_FUNC_STS_CD = ?, EXT_FUNC_END_DT = CURRENT_TIMESTAMP, NOTES = ? WHERE TABLE_NM = ? AND EXT_FUNC_STS_CD = 'P';",
            binds: ['C', log_messages.join(';\n'), v_table_name]
        });
        
        return summary;
    } catch (err) {
        log_messages.push("ERROR: " + err.message);
        snowflake.execute({ 
            sqlText: "UPDATE GADS.EXT_FUNC_CONTROL_TABLE SET EXT_FUNC_STS_CD = ?, EXT_FUNC_END_DT = CURRENT_TIMESTAMP, NOTES = ? WHERE TABLE_NM = ? AND EXT_FUNC_STS_CD = 'P';",
            binds: ['E', log_messages.join(';\n'), v_table_name]
        });
        return JSON.stringify({ status: "Error", message: err.message, logs: log_messages });
    }
$$;

-- call GRIDMAX.DAILY_GEN_CDC2('ALL','2025-04-01','2025-04-30' );

---------

create schema IF NOT EXISTS ORAP;
USE SCHEMA ORAP;

create TABLE IF NOT EXISTS EVENT_HOURS_SUMMARY2 (
	LOCATION VARCHAR,
	REPORTING_LEVEL VARCHAR,
	PERIOD VARCHAR,
	NEFDH VARCHAR,
	NEMDH VARCHAR,
	NEPDH VARCHAR,
	NEFDHRS VARCHAR,
	NEMDHRS VARCHAR,
	NEPDHRS VARCHAR,
    BEGIN_DT VARCHAR,
    END_DT VARCHAR,
	LOAD_TS TIMESTAMP_NTZ(9)
);

create TABLE IF NOT EXISTS FLEET_RAM_KPIS2 (
	LOCATION VARCHAR,
	PERIOD VARCHAR,
	WEAF VARCHAR,
	WEMOF VARCHAR,
	WEPOF VARCHAR,
	WEFOF VARCHAR,
	WEFOR VARCHAR,
    BEGIN_DT VARCHAR,
    END_DT VARCHAR,
	LOAD_TS TIMESTAMP_NTZ(9)
);

create TABLE IF NOT EXISTS GEN_FUEL_PERF2 (
	LOCATION VARCHAR,
	REPORTING_LEVEL VARCHAR,
    PERIOD VARCHAR,
	SERVICE_HOURS VARCHAR,
	GROSS_DEPENDABLE_CAPACITY VARCHAR,
	NET_DEPENDABLE_CAPACITY VARCHAR,
	GROSS_GENERATION VARCHAR,
	NET_GENERATION VARCHAR,
	NET_CAPACITY_FACTOR VARCHAR,
	NET_EQUIV_AVAIL_FACTOR VARCHAR,
	NET_OUTPUT_FACTOR VARCHAR,
	NET_EFOR VARCHAR,
    BEGIN_DT VARCHAR,
    END_DT VARCHAR,
	LOAD_TS TIMESTAMP_NTZ(9)
);

create TABLE IF NOT EXISTS GEN_PERF_INCENTIVES2 (
	LOCATION VARCHAR,
	REPORTING_LEVEL VARCHAR,
	PERIOD VARCHAR,
	NET_EQUIV_AVAIL_FACTOR VARCHAR,
	PH VARCHAR,
	SH VARCHAR,
	RH VARCHAR,
	UNAV_HRS VARCHAR,
	POH VARCHAR,
	FOH VARCHAR,
	MOH VARCHAR,
	PARTIAL_POH VARCHAR,
	LOAD_RED_PART_PLANNED VARCHAR,
	PARTIAL_FOH VARCHAR,
	LOAD_RED_PART_FORCED VARCHAR,
	PARTIAL_MOH VARCHAR,
	LOAD_RED_PART_MAINT VARCHAR,
	NET_GENERATION VARCHAR,
	NET_CAPACITY_FACTOR VARCHAR,
	NET_OUTPUT_FACTOR VARCHAR,
	NET_PERIOD_CAPACITY VARCHAR,
	NET_DEPENDABLE_CAPACITY VARCHAR,
	GROSS_DEPENDABLE_CAPACITY VARCHAR,
    BEGIN_DT VARCHAR,
    END_DT VARCHAR,
	LOAD_TS TIMESTAMP_NTZ(9)
);

create TABLE IF NOT EXISTS NET_FACTORS_RATES2 (
	LOCATION VARCHAR,
	REPORTING_LEVEL VARCHAR,
	PERIOD VARCHAR,
	NCF VARCHAR,
	NOF VARCHAR,
	NEUF VARCHAR,
	NEAF VARCHAR,
	NEMOF VARCHAR,
	NEFOF VARCHAR,
	NEUOF VARCHAR,
	NEMOR VARCHAR,
	NEFOR VARCHAR,
	NEUOR VARCHAR,
    BEGIN_DT VARCHAR,
    END_DT VARCHAR,
	LOAD_TS TIMESTAMP_NTZ(9)
);

create TABLE IF NOT EXISTS OUTAGES2 (
	STATION VARCHAR,
	PLANT VARCHAR,
	UNIT VARCHAR,
	GADS_EVT_NUMBER VARCHAR,
	BEGIN_DATE VARCHAR,
	END_DATE VARCHAR,
	DURATION VARCHAR,
	GADS_EVT_TYPE VARCHAR,
	GADS_CAUSE_CODE VARCHAR,
	CAUSE_CODE_DESC VARCHAR,
	NARRATIVE VARCHAR,
	NET_AVAIL_CAPACITY VARCHAR,
	NET_DEPENDABLE_CAPACITY VARCHAR,
    BEGIN_DT VARCHAR,
    END_DT VARCHAR,
	LOAD_TS TIMESTAMP_NTZ(9)
);

CREATE EXTERNAL FUNCTION IF NOT EXISTS ORAP_EXT_FUNC(payload STRING)
RETURNS VARIANT
API_INTEGRATION = INT_API_AZURE_DAP_DEV_GADS
HEADERS = ('Content-Type' = 'application/json')
AS 'https://apim-teco-ahubgads-dev.azure-api.net/azfun-teco-ahubgads-eastus-dev/orap';

CREATE PROCEDURE IF NOT EXISTS ORAP_KPI_CDC2(p_function_name STRING, p_start_date STRING, p_end_date STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    function getPeriod(beginDateStr, endDateStr) {
        var beginDate = new Date(beginDateStr);
        var endDate = new Date(endDateStr);
    
        var year = beginDate.getFullYear();
        var firstDayOfYear = new Date(year, 0, 1); 
        var lastDayOfYear = new Date(year, 11, 31);
    
        if (beginDate.getTime() === firstDayOfYear.getTime() && endDate.getTime() === lastDayOfYear.getTime()) {
            return 'YTD';
        } else {
            // Format beginDate as MM/DD/YYYY
            var month = String(beginDate.getMonth() + 1).padStart(2, '0');
            var day = String(beginDate.getDate()).padStart(2, '0');
            return `${month}/${day}/${year}`;
        }
    }
    
    function escapeSql(value) {      
      if (value === null || value === undefined) return 'NULL';
      return `${value.toString().replace(/'/g, "''")}`;
    }

    var v_table_name;
    var log_messages = [];    
    try {
        var p_function_name = arguments[0];
        var v_now = new Date();
        
        // First day of the current month without time
        var v_start_date = new Date(v_now.getFullYear(), v_now.getUTCMonth(), 1);
        v_start_date = P_START_DATE || v_start_date.toISOString().split('T')[0];
        
        // Last day of the current month without time
        var v_end_date = new Date(v_now.getFullYear(), v_now.getUTCMonth() + 1, 0);
        v_end_date = P_END_DATE || v_end_date.toISOString().split('T')[0];
        
        var v_json_request;
        var v_json_response;
        var v_load_timestamp;        
        var insertQuery;
        var columnNames;
        var valuesList = [];

        log_messages.push("Procedure started");
        
        var rs = snowflake.execute({sqlText: "SELECT TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF')"});
        if (rs.next()) {
            v_load_timestamp = rs.getColumnValue(1);
        }
        
        v_json_request = JSON.stringify({ func: p_function_name, beginDate: v_start_date, endDate: v_end_date });        

        switch (p_function_name) {
            case "GetGenFuelPerf":
                v_table_name = "ORAP.GEN_FUEL_PERF2";
                columnNames = "(LOCATION, REPORTING_LEVEL, PERIOD, SERVICE_HOURS, GROSS_DEPENDABLE_CAPACITY, NET_DEPENDABLE_CAPACITY, GROSS_GENERATION, NET_GENERATION, NET_CAPACITY_FACTOR, NET_EQUIV_AVAIL_FACTOR, NET_OUTPUT_FACTOR, NET_EFOR, BEGIN_DT, END_DT, LOAD_TS)";
                break;
            case "GetGenPerfIncent":
                v_table_name = "ORAP.GEN_PERF_INCENTIVES2";
                columnNames = "(LOCATION, REPORTING_LEVEL, PERIOD, NET_EQUIV_AVAIL_FACTOR, PH, SH, RH, UNAV_HRS, POH, FOH, MOH, PARTIAL_POH, LOAD_RED_PART_PLANNED, PARTIAL_FOH, LOAD_RED_PART_FORCED, PARTIAL_MOH, LOAD_RED_PART_MAINT, NET_GENERATION, NET_CAPACITY_FACTOR, NET_OUTPUT_FACTOR, NET_PERIOD_CAPACITY, NET_DEPENDABLE_CAPACITY, GROSS_DEPENDABLE_CAPACITY, BEGIN_DT, END_DT, LOAD_TS)";
                break;
            case "GetOutages":
                v_table_name = "ORAP.OUTAGES2";
                columnNames = "(STATION, PLANT, UNIT, GADS_EVT_NUMBER, BEGIN_DATE, END_DATE, DURATION, GADS_EVT_TYPE, GADS_CAUSE_CODE, CAUSE_CODE_DESC, NARRATIVE, NET_AVAIL_CAPACITY, NET_DEPENDABLE_CAPACITY, BEGIN_DT, END_DT, LOAD_TS)";
                break;
            case "GetFleetRAMKPIs":
                v_table_name = "ORAP.FLEET_RAM_KPIS2";
                columnNames = "(LOCATION, PERIOD, WEAF, WEMOF, WEPOF, WEFOF, WEFOR, BEGIN_DT, END_DT, LOAD_TS)";
                break;
            case "GetEvtHrsSummary":
                v_table_name = "ORAP.EVENT_HOURS_SUMMARY2";
                columnNames = "(LOCATION, REPORTING_LEVEL, PERIOD, NEFDH, NEMDH, NEPDH, NEFDHRS, NEMDHRS, NEPDHRS, BEGIN_DT, END_DT, LOAD_TS)";
                break;
            case "GetNetFactorsRates":
                v_table_name = "ORAP.NET_FACTORS_RATES2";
                columnNames = "(LOCATION, REPORTING_LEVEL, PERIOD, NCF, NEAF, NEFOF, NEFOR, NEMOF, NEMOR, NEUF, NEUOF, NEUOR, NOF, BEGIN_DT, END_DT, LOAD_TS)";
                break;
        }
                                
        snowflake.execute({ 
            sqlText: 'INSERT INTO GADS.EXT_FUNC_CONTROL_TABLE (TABLE_NM, EXT_FUNC_START_DT, EXT_FUNC_STS_CD) VALUES (?, ?, ?)',                           
            binds: [v_table_name, v_load_timestamp, 'P']
        });
        log_messages.push("Inserted record in control table with status P");
        
        rs = snowflake.execute({
                sqlText: "SELECT ORAP.ORAP_EXT_FUNC(:1)",
                binds: [v_json_request]
            });
        log_messages.push(`Invoked ExtFn with params: ${p_function_name}, ${v_start_date}, ${v_end_date}`);
        
        if (rs.next()) {
            response = rs.getColumnValue(1);
            v_json_response = JSON.parse(response);

            for (var i = 0; i < v_json_response.length; i++) {
                let period = v_json_response[i].Period;

                let startDate = v_start_date;
                let endDate = v_end_date;
                
                if (period === 'YTD') {
                    const year = new Date(v_start_date).getUTCFullYear();
                    startDate = `${year}-01-01`;
                    endDate = `${year}-12-31`;
                }
                
                switch (p_function_name) {
                    case "GetGenFuelPerf":
                        let beginDateStr = v_json_response[i].Begin_Date;
                        let endDateStr = v_json_response[i].End_Date;
                        let period = getPeriod(beginDateStr, endDateStr);
                    
                        valuesList.push(`('${v_json_response[i].Location}', '${v_json_response[i].Reporting_Level}', '${period}', '${v_json_response[i].Service_Hours}', '${v_json_response[i].Gross_Dependable_Capacity}', '${v_json_response[i].Net_Dependable_Capacity}', '${v_json_response[i].Gross_Generation}', '${v_json_response[i].Net_Generation}', '${v_json_response[i].Net_Capacity_Factor}', '${v_json_response[i].Net_Equiv_Avail_Factor}', '${v_json_response[i].Net_Output_Factor}', '${v_json_response[i].Net_EFOR}', '${startDate}', '${endDate}', '${v_load_timestamp}')`);
                        break;
                    case "GetGenPerfIncent":
                        valuesList.push(`('${v_json_response[i].Location}', '${v_json_response[i].Reporting_Level}', '${v_json_response[i].Period}', '${v_json_response[i].Net_Equiv_Avail_Factor}', '${v_json_response[i].PH}', '${v_json_response[i].SH}', '${v_json_response[i].RH}', '${v_json_response[i].Unav_Hrs}', '${v_json_response[i].POH}', '${v_json_response[i].FOH}', '${v_json_response[i].MOH}', '${v_json_response[i].Partial_POH}', '${v_json_response[i].Load_Red_Part_Planned}', '${v_json_response[i].Partial_FOH}', '${v_json_response[i].Load_Red_Part_Forced}', '${v_json_response[i].Partial_MOH}', '${v_json_response[i].Load_Red_Part_Maint}', '${v_json_response[i].Net_Generation}', '${v_json_response[i].Net_Capacity_Factor}', '${v_json_response[i].Net_Output_Factor}', '${v_json_response[i].Net_Period_Capacity}', '${v_json_response[i].Net_Dependable_Capacity}', '${v_json_response[i].Gross_Dependable_Capacity}', '${startDate}', '${endDate}', '${v_load_timestamp}')`);
                        break;
                    case "GetOutages":
                        valuesList.push(`('${v_json_response[i].Station}', '${v_json_response[i].Plant}', '${v_json_response[i].Unit}', '${v_json_response[i].GADS_Evt_Number}', '${v_json_response[i].Begin_Date}', '${v_json_response[i].End_Date}', '${v_json_response[i].Duration}', '${v_json_response[i].GADS_Evt_Type}', '${v_json_response[i].GADS_Cause_Code}', '${v_json_response[i].Cause_Code_Desc}', '${escapeSql(v_json_response[i].Narrative)}', '${v_json_response[i].Net_Avail_Capacity}', '${v_json_response[i].Net_Dependable_Capacity}', '${endDate}', '${endDate}', '${v_load_timestamp}')`);
                        break;
                    case "GetFleetRAMKPIs":
                        valuesList.push(`('${v_json_response[i].Location}', '${v_json_response[i].Period}', '${v_json_response[i].WEAF}', '${v_json_response[i].WEFOF}', '${v_json_response[i].WEFOR}', '${v_json_response[i].WEMOF}', '${v_json_response[i].WEPOF}', '${startDate}', '${endDate}', '${v_load_timestamp}')`);
                        break;
                    case "GetEvtHrsSummary":
                        valuesList.push(`('${v_json_response[i].Location}', '${v_json_response[i].Reporting_Level}', '${v_json_response[i].Period}', '${v_json_response[i].NEFDH}', '${v_json_response[i].NEMDH}', '${v_json_response[i].NEPDH}', '${v_json_response[i].NEFDHRS}', '${v_json_response[i].NEMDHRS}', '${v_json_response[i].NEPDHRS}', '${startDate}', '${endDate}', '${v_load_timestamp}')`);
                        break;
                    case "GetNetFactorsRates":
                        valuesList.push(`('${v_json_response[i].Location}', '${v_json_response[i].Reporting_Level}', '${v_json_response[i].Period}', '${v_json_response[i].NCF}', '${v_json_response[i].NEAF}', '${v_json_response[i].NEFOF}', '${v_json_response[i].NEFOR}', '${v_json_response[i].NEMOF}', '${v_json_response[i].NEMOR}', '${v_json_response[i].NEUF}', '${v_json_response[i].NEUOF}', '${v_json_response[i].NEUOR}', '${v_json_response[i].NOF}', '${startDate}', '${endDate}', '${v_load_timestamp}')`);
                        break;
                }
            }          
        }

        if (valuesList.length > 0) {
            insertQuery = `INSERT INTO ${v_table_name} ${columnNames} VALUES ` + valuesList.join(",");
            // return insertQuery;
            snowflake.execute({ sqlText: insertQuery });            
        }
        var summary = "Data inserted into " + v_table_name + ", Rows inserted: " + v_json_response.length;
        log_messages.push(summary);
                
        snowflake.execute({ 
            sqlText: "UPDATE GADS.EXT_FUNC_CONTROL_TABLE SET EXT_FUNC_STS_CD = ?, EXT_FUNC_END_DT = CURRENT_TIMESTAMP, NOTES = ? WHERE TABLE_NM = ? AND EXT_FUNC_STS_CD = 'P';",
            binds: ['C', log_messages.join(';\n'), v_table_name]
        });
        
        return summary;
    } catch (err) {
        log_messages.push("ERROR: " + err.message);
        snowflake.execute({ 
            sqlText: "UPDATE GADS.EXT_FUNC_CONTROL_TABLE SET EXT_FUNC_STS_CD = ?, EXT_FUNC_END_DT = CURRENT_TIMESTAMP, NOTES = ? WHERE TABLE_NM = ? AND EXT_FUNC_STS_CD = 'P';",
            binds: ['E', log_messages.join(';\n'), v_table_name]
        });
        return JSON.stringify({ status: "Error", message: err.message });
    }
$$;

-- call ORAP_KPI_CDC2('GetGenPerfIncent','2025-01-01','2025-01-31')

----

create schema IF NOT EXISTS PGSDSS;
USE SCHEMA PGSDSS;

create TABLE IF NOT EXISTS GADS_HOUR2 (
    TAGID NUMBER(38,0),
    TIME TIMESTAMP_NTZ(9),
    VALUE NUMBER(38,21),
    DATAQUALITY VARCHAR,
    MINTIME VARCHAR,
    MINVALUE VARCHAR,
    MINDATAQUALITY VARCHAR,
    MAXTIME VARCHAR,
    MAXVALUE VARCHAR,
    MAXDATAQUALITY VARCHAR,
    AVERAGEVALUE NUMBER(38,21),
    OWNSYSID VARCHAR,
    DATASETID VARCHAR,
    BEGIN_DT VARCHAR,
    END_DT VARCHAR,
	LOAD_TS TIMESTAMP_NTZ(9)
);

CREATE EXTERNAL FUNCTION IF NOT EXISTS PGSDSS_EXT_FUNC(payload STRING)
RETURNS VARIANT
API_INTEGRATION = INT_API_AZURE_DAP_DEV_GADS
HEADERS = ('Content-Type' = 'application/json')
AS 'https://apim-teco-ahubgads-dev.azure-api.net/azfun-teco-ahubgads-eastus-dev/pgsdss';

CREATE PROCEDURE IF NOT EXISTS GADS_DATA_CDC2(p_function_name STRING, p_start_date STRING, p_end_date STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var log_messages = [];
    var v_table_name;
    try {
        var p_function_name = arguments[0];
        var v_now = new Date();
        
        // First day of the current month without time
        var v_start_date = new Date(v_now.getFullYear(), v_now.getUTCMonth(), 1);
        v_start_date = P_START_DATE || v_start_date.toISOString().split('T')[0];
        
        // Last day of the current month without time
        var v_end_date = new Date(v_now.getFullYear(), v_now.getUTCMonth() + 1, 0);
        v_end_date = P_END_DATE || v_end_date.toISOString().split('T')[0];
        
        var v_json_request;
        var v_json_response;
        var v_load_timestamp;        
        var insertQuery;
        var columnNames;
        var valuesList = [];        

        log_messages.push("Procedure started");
        
        var rs = snowflake.execute({sqlText: "SELECT TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF')"});
        if (rs.next()) {
            v_load_timestamp = rs.getColumnValue(1);
        }

        // columns for both Day and Hour are the same
        columnNames = "(TAGID, TIME, VALUE, DATAQUALITY, MINTIME, MINVALUE, MINDATAQUALITY, MAXTIME, MAXVALUE, MAXDATAQUALITY, AVERAGEVALUE, OWNSYSID, DATASETID,  BEGIN_DT, END_DT, LOAD_TS)";

        var v_end_date_adj = v_end_date;
        switch (p_function_name) {
            case "Hour":
                var v_end_date_obj = new Date(v_end_date);
                v_end_date_obj.setUTCDate(v_end_date_obj.getUTCDate() + 1);
                v_end_date_obj.setUTCHours(1, 0, 0, 0);
                v_end_date_adj = v_end_date_obj.toISOString();
                
				v_table_name = "PGSDSS.GADS_HOUR2";
                break;
        }

        v_json_request = JSON.stringify({ 
            func: p_function_name,
            tagIds: [1002246,1000479,1000475,1000478,1000474,1000583,1000570,1003313,1003394,1003404,2000053,2000034,2000006,1002247,1002246,1000801], 
            startTime: v_start_date, 
            endTime: v_end_date_adj
            });
            
        snowflake.execute({ 
            sqlText: 'INSERT INTO GADS.EXT_FUNC_CONTROL_TABLE (TABLE_NM, EXT_FUNC_START_DT, EXT_FUNC_STS_CD) VALUES (?, ?, ?)',                           
            binds: [v_table_name, v_load_timestamp, 'P']
        });
        log_messages.push("Inserted record in control table with status P");
        
        rs = snowflake.execute({
                sqlText: "SELECT PGSDSS.PGSDSS_EXT_FUNC(:1)",
                binds: [v_json_request]
            });
        log_messages.push(`Invoked ExtFn with params: ${p_function_name}, ${v_start_date}, ${v_end_date}`);
        
        if (rs.next()) {
            response = rs.getColumnValue(1);
            v_json_response = JSON.parse(response);

            for (var i = 0; i < v_json_response.length; i++) {
                switch (p_function_name) {
                    case "Hour":
                        valuesList.push(`('${v_json_response[i].TAGID}', '${v_json_response[i].TIME}', '${v_json_response[i].VALUE}', '${v_json_response[i].DATAQUALITY}', '${v_json_response[i].MINTIME}', '${v_json_response[i].MINVALUE}', '${v_json_response[i].MINDATAQUALITY}', '${v_json_response[i].MAXTIME}', '${v_json_response[i].MAXVALUE}', '${v_json_response[i].MAXDATAQUALITY}', '${v_json_response[i].AVERAGEVALUE}', '${v_json_response[i].OWNSYSID}', '${v_json_response[i].DATASETID}', '${v_start_date}', '${v_end_date}', '${v_load_timestamp}')`);
                        break; 
                }                
            }          
        }

        if (valuesList.length > 0) {
            insertQuery = `INSERT INTO ${v_table_name} ${columnNames} VALUES ` + valuesList.join(",");
            snowflake.execute({ sqlText: insertQuery });            
        }
        var summary = "Data inserted into " + v_table_name + ", Rows inserted: " + v_json_response.length;
        log_messages.push(summary);
                
        snowflake.execute({ 
            sqlText: "UPDATE GADS.EXT_FUNC_CONTROL_TABLE SET EXT_FUNC_STS_CD = ?, EXT_FUNC_END_DT = CURRENT_TIMESTAMP, NOTES = ? WHERE TABLE_NM = ? AND EXT_FUNC_STS_CD = 'P';",
            binds: ['C', log_messages.join(';\n'), v_table_name]
        });
        
        return summary;
    } catch (err) {
        log_messages.push("ERROR: " + err.message);
        snowflake.execute({ 
            sqlText: "UPDATE GADS.EXT_FUNC_CONTROL_TABLE SET EXT_FUNC_STS_CD = ?, EXT_FUNC_END_DT = CURRENT_TIMESTAMP, NOTES = ? WHERE TABLE_NM = ? AND EXT_FUNC_STS_CD = 'P';",
            binds: ['E', log_messages.join(';\n'), v_table_name]
        });
        return JSON.stringify({ status: "Error", message: err.message });
    }
$$;

-- call PGSDSS.GADS_DATA_CDC2('Day','2025-01-01','2025-01-31');

------

create schema IF NOT EXISTS PI;
USE SCHEMA PI;

create TABLE IF NOT EXISTS GADS_METADATA2 (
	PI_GADS_META_KEY_NB NUMBER(38,0),
	PI_LINKED_SERVER VARCHAR,
	ASSET_TYPE_CD VARCHAR,
	ASSET_CD VARCHAR,
	TAG VARCHAR,
	FORMULA VARCHAR,
	PARAMS_OBJ VARIANT,
	DESCRIPTION VARCHAR,
	METRIC_CD VARCHAR,
	IS_ACTIVE NUMBER(38,0)
);

create TABLE IF NOT EXISTS GADS_DATA2 (
	ASSET_CD VARCHAR,
	ASSET_TYPE_CD VARCHAR,
    METRIC_CD VARCHAR,
	TIME TIMESTAMP_NTZ(9),
	TAG VARCHAR,
	VALUE NUMBER(38,20),
	META_KEY_NB_FK NUMBER(38,0),
    BEGIN_DT VARCHAR,
    END_DT VARCHAR,
	LOAD_TS TIMESTAMP_NTZ(9)
);

CREATE EXTERNAL FUNCTION IF NOT EXISTS PI_EXT_FUNC(payload STRING)
RETURNS VARIANT
API_INTEGRATION = INT_API_AZURE_DAP_DEV_GADS
HEADERS = ('Content-Type' = 'application/json')
AS 'https://apim-teco-ahubgads-dev.azure-api.net/azfun-teco-ahubgads-eastus-dev/pi';

CREATE PROCEDURE IF NOT EXISTS GADS_DATA_CDC2(p_function_name STRING, p_start_date STRING, p_end_date STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS 
$$    
    var log_messages = [];
    var v_table_name;
    try {
        var p_function_name = arguments[0];
        var v_now = new Date();
        
        // First day of the current month without time
        var v_start_date = new Date(v_now.getFullYear(), v_now.getUTCMonth(), 1);
        v_start_date = (P_START_DATE && new Date(P_START_DATE).toISOString().replace('T', ' ').substring(0, 23)) || v_start_date.toISOString().replace('T', ' ').substring(0, 23);
        
        // Last day of the current month without time
        var v_end_date = new Date(v_now.getFullYear(), v_now.getUTCMonth() + 1, 0);
        v_end_date = (P_END_DATE && new Date(P_END_DATE).toISOString().replace('T', ' ').substring(0, 23)) || v_end_date.toISOString().replace('T', ' ').substring(0, 23);
        
        var v_json_request;
        var v_json_response;
        var v_load_timestamp;        
        var insertQuery;
        var columnNames;                       
        
        log_messages.push("Procedure started");

        var rs = snowflake.execute({sqlText: "SELECT TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF')"});
        if (rs.next()) {
            v_load_timestamp = rs.getColumnValue(1);
        }
        // log_messages.push("Retrieved current timestamp - " + v_load_timestamp);

        switch (p_function_name) {
            case "PiData":
                v_table_name = "PI.GADS_DATA2";
                columnNames = "(ASSET_CD, ASSET_TYPE_CD, TIME, TAG, METRIC_CD, VALUE, META_KEY_NB_FK, BEGIN_DT, END_DT, LOAD_TS)";
                break;
        }
                
        snowflake.execute({ 
            sqlText: 'INSERT INTO GADS.EXT_FUNC_CONTROL_TABLE (TABLE_NM, EXT_FUNC_START_DT, EXT_FUNC_STS_CD) VALUES (?, ?, ?)',
            binds: [v_table_name, v_load_timestamp, 'P']
        });
        log_messages.push("Inserted record in control table with status P");        
        

        rs = snowflake.execute({sqlText: "SELECT * FROM PI.GADS_METADATA2 WHERE IS_ACTIVE = 1"});
        
        while (rs.next()) {
            var valuesList = []; 
            var tag = rs.getColumnValue("TAG");
            
            log_messages.push("Start iterating over the metadata - " + tag); 

            var metric_cd = rs.getColumnValue("METRIC_CD");
            var asset_cd = rs.getColumnValue("ASSET_CD");
            var asset_type_cd = rs.getColumnValue("ASSET_TYPE_CD");
            
            var metadataId = rs.getColumnValue("PI_GADS_META_KEY_NB");
            v_json_request = rs.getColumnValue("PARAMS_OBJ");
            v_json_request.tags = [];
            v_json_request.tags.push(tag);
            
            v_json_request.startTime = v_start_date;

            var v_end_date_obj = new Date(v_end_date);
            v_end_date_obj.setUTCDate(v_end_date_obj.getUTCDate() + 1);
            v_end_date_obj.setUTCHours(0, 0, 0, 0);
            var v_end_date_adj = v_end_date_obj.toISOString().replace('T', ' ').substring(0, 23);
            
            v_json_request.endTime = v_end_date_adj;
            v_json_request.server = rs.getColumnValue("PI_LINKED_SERVER");
            v_json_request.func = rs.getColumnValue("FORMULA");
            var filt = v_json_request.filtExp;

            
            if (filt) {
                filt = filt.replaceAll("'", "''''");
                //filt = filt.replaceAll('"', '\\"');
                v_json_request.filtExp = filt;
            }            
            
            var temp = JSON.stringify(v_json_request);
            var v_req = temp.replace(/\$tag/g, tag);
            
            // log_messages.push("Request json - " + v_req);             
            
            var extFuncResultSet;
            extFuncResultSet = snowflake.execute({
                sqlText: "SELECT PI.PI_EXT_FUNC(:1)",
                binds: [v_req]
            });
            log_messages.push(`Invoked ExtFn with params: ${metadataId}, ${v_start_date}, ${v_end_date}`);
            
            if (extFuncResultSet.next()) {
                response = extFuncResultSet.getColumnValue(1);
                v_json_response = JSON.parse(response);
    
                for (var i = 0; i < v_json_response.length; i++) {
                    switch (p_function_name) {
                        case "PiData":
                            valuesList.push(`('${asset_cd}', '${asset_type_cd}', '${v_json_response[i].time}', '${v_json_response[i].tag}', '${metric_cd}', '${v_json_response[i].value}', ${metadataId}, '${v_start_date}', '${v_end_date}', '${v_load_timestamp}')`);
                            break;
                    }
                }          
            }
    
            if (valuesList.length > 0) {
                insertQuery = `INSERT INTO ${v_table_name} ${columnNames} VALUES ` + valuesList.join(",");
                snowflake.execute({ sqlText: insertQuery });            
            }
            log_messages.push("Data inserted into " + v_table_name + ", Rows inserted: " + v_json_response.length);
        }

        var summary = "Completed processing all the PI tags";
        log_messages.push(summary);
        
        snowflake.execute({ 
            sqlText: "UPDATE GADS.EXT_FUNC_CONTROL_TABLE SET EXT_FUNC_STS_CD = ?, EXT_FUNC_END_DT = CURRENT_TIMESTAMP, NOTES = ? WHERE TABLE_NM = ? AND EXT_FUNC_STS_CD = 'P';",
            binds: ['C', log_messages.join(';\n'), v_table_name]
        });
        
        return summary;
    } catch (err) {
        log_messages.push("ERROR: " + err.message);
        snowflake.execute({ 
            sqlText: "UPDATE GADS.EXT_FUNC_CONTROL_TABLE SET EXT_FUNC_STS_CD = ?, EXT_FUNC_END_DT = CURRENT_TIMESTAMP, NOTES = ? WHERE TABLE_NM = ? AND EXT_FUNC_STS_CD = 'P';",
            binds: ['E', log_messages.join(';\n'), v_table_name]
        });
        return JSON.stringify({ status: "Error", message: err.message});
    }
$$;

-- call PI.GADS_DATA_CDC2('PiData','2025-01-01','2025-01-31');
!spool off