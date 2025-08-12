CREATE OR REPLACE PROCEDURE SF_DB_AHUB_DEV2.AHUB_PSA.GADS_DATA_CDC(p_start_date STRING, p_end_date STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try {
        var p_function_name = arguments[0];
        var v_start_date = P_START_DATE
        var v_end_date = P_END_DATE;
        var log_messages = []; 

        var procsMap = {
            "ORAP_KPI_CDC": [
                'GetGenFuelPerf',
                'GetGenPerfIncent',
                'GetOutages',
                'GetFleetRAMKPIs',
                'GetEvtHrsSummary',
                'GetNetFactorsRates'
            ],
            "GRIDMAX_GEN_CDC": [
                "ALL"
            ],
            "PGSDSS_GADS_DATA_CDC": [
                "Day"
            ],
            "ALLEGRO_GADS_DATA_CDC": [
                "SlfConsumptionQuality"
            ],
            "PI_GADS_DATA_CDC": [
                "PiData"
            ]
        }

        for (var procName in procsMap) {
            var startTime = Date.now();
            
            var functionNames = procsMap[procName];
            for (var i = 0; i < functionNames.length; i++) {
                snowflake.execute({ 
                    sqlText: `CALL AHUB_PSA.${procName}(?, ?, ?)`,
                    binds: [functionNames[i], v_start_date, v_end_date]
                });
            }

            var endTime = Date.now();
            var durationMs = endTime - startTime;
            log_messages.push(`Procedure ${procName} took ${durationMs} ms`);
        }              
        
        return log_messages.join('\n');
    } catch (err) {
        return JSON.stringify({ status: "Error", message: err.message });
    }
$$;