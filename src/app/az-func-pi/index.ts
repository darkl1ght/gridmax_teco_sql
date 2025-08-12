import { AzureFunction, Context, HttpRequest } from "@azure/functions";
import * as dotenv from "dotenv";
import { DateTime } from "luxon";
import { querySqlServer } from "../../common";
import mock_pi_data from "./mock-pi-data.json";

dotenv.config();

// supported funcs
const funcs: any = {
  // funcName : [isActive, translaterFn]
  PIAdvCalcFilDat: [1, PIAdvCalcFilDat_ToSql],
  PIAdvCalcDat: [1, PIAdvCalcDat_ToSql],
};

const httpTrigger: AzureFunction = async function (
  context: Context,
  req: HttpRequest
): Promise<void> {
  try {
    const paramsObj = JSON.parse(req.body.data[0][1]);
    const { func, tags, startTime, endTime, server, isMock }: any =
      paramsObj;

    const errObj = validate(func, tags, startTime, endTime, server);
    if (errObj) {
      context.res = errObj;
      return;
    }
    const translaterFn = funcs[func][1];
    const sql = translaterFn(paramsObj);
    context.log(`${func} sql ${process.env["PI_SQL_SERVER"]}: ${sql}`);
    let result = [];
    if (!!isMock) {
      context.log("Sending Mock Response");
      result = mock_pi_data;
    } else {
      result = await querySqlServer("PI", sql);
    }
    // context.log({result});
    context.res = {
      body: { data: [[0, JSON.stringify(result)]] },
      headers: {
        "Content-Type": "application/json",
      },
    };
  } catch (error: any) {
    context.log(`Exception Querying PI: ${error} / ${error.message}`);
    context.res = {
      status: 500,
      body: JSON.stringify({ error: error.message }),
      headers: {
        "Content-Type": "application/json",
      },
    };
  }
};

function validate(
  func: string,
  tags: Array<string>,
  startTime: string,
  endTime: string,
  server: string
) {
  let errObj = null;
  if (!func || !startTime || !endTime || !server) {
    errObj = {
      status: 400,
      body: "Missing required parameters.",
    };
  } else if (!(funcs[func] && funcs[func][0] === 1)) {
    errObj = {
      status: 400,
      body: `Function ${func} does not exist. Please resubmit with supported function.`,
    };
  }
  return errObj;
}

function PIAdvCalcDat_ToSql({
  tags,
  startTime,
  endTime,
  interval,
  mode,
  calcBasis,
  minPctGood,
  cFactor,
  server,
}: any) {
  const tagsStr = "''" + tags.join("'',''") + "''";
  const calcBasisStr =
    calcBasis == "time-weighted" ? "TimeWeighted" : "EventWeighted";
  const startTimeAdj =
    interval == "1d" ? addOneDay(startTime) : startTime;
  const endTimeAdj = interval == "1d" ? addOneDay(endTime) : endTime;
  const modeAdj = mode == "average" ? "avg" : mode;
  const query = `
    SELECT tag, dateadd(day,-1,time) as time, value*${cFactor} as value
    FROM OPENQUERY(${server}, '
      SELECT  tag,time,value
      FROM piarchive..pi${modeAdj}
      WHERE tag in (${tagsStr})
      AND time >=''${startTimeAdj}'' AND time <''${endTimeAdj}''
      AND calcbasis=''${calcBasisStr}''
      AND timestep = ''${interval}''
      AND pctgood >= ${minPctGood}
    ')
    order by time
  `;
  return query;
}
function PIAdvCalcFilDat_ToSql({
  tags,
  startTime,
  endTime,
  interval,
  filtExp,
  mode,
  calcBasis,
  sampMode,
  sampFreq,
  minPctGood,
  cFactor,
  server,
}: any) {
  
  const calcBasisStr =
    calcBasis == "time-weighted" ? "TimeWeighted" : "EventWeighted";
  const startTimeAdj =
    interval == "1d" ? addOneDay(startTime) : startTime;  
  const modeAdj = mode == "average" ? "avg" : mode;

  let query = "";
  if (sampMode == "interpolated") {
    const tagsStr = "'" + tags.join("','") + "'";
    const threshold = filtExp.match(/>\s*(.+)/)[1].trim();
    
    query = `
      SELECT
        tag,
        CAST(time AS DATE) as time,
        count(CAST(value AS FLOAT)) AS value
      FROM
        ${server}.piarchive..piinterp2
      WHERE
        tag in (${tagsStr})
        AND time >= '${startTime}' AND time < '${endTime}'
        and timestep = '01:00:00'
        and CAST(value AS FLOAT) > ${threshold}
      group by tag, CAST(time AS DATE)
    `;
  } else {
    const tagsStr = "''" + tags.join("'',''") + "''";
    const sampModeAdj = sampMode == "pt.compressed" ? "PIPointRecordedValues" : sampMode;
    const endTimeAdj = interval == "1d" ? addOneDay(endTime) : endTime;
    
    query = `
      SELECT tag, dateadd(day,-1,time) as time, value*${cFactor} as value
      FROM OPENQUERY(${server}, '
        SELECT  tag,time,value
        FROM piarchive..pi${modeAdj}
        WHERE tag in (${tagsStr})
        AND time >=''${startTimeAdj}'' AND time <''${endTimeAdj}''
        AND calcbasis=''${calcBasisStr}''
        AND timestep = ''${interval}''
        AND pctgood >= ${minPctGood}
        AND filterexpr=''${filtExp}''
        AND filtersampletype=''${sampModeAdj}''
        AND filtersampleinterval=''${sampFreq}''
      ')
      order by time
    `;
  }
  
  return query;
}

function addOneDay(dateStr: string): string {
  const dt = DateTime.fromFormat(dateStr, "yyyy-MM-dd HH:mm:ss.SSS");
  const nextDay = dt.plus({ days: 1 });
  const retVal = nextDay.toFormat("yyyy-MM-dd HH:mm:ss.SSS");
  return retVal;
}

export default httpTrigger;
