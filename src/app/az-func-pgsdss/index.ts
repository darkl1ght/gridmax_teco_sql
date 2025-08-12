import { AzureFunction, Context, HttpRequest } from "@azure/functions";
import mock_pgsdss_data from "./mock-pgsdss-data.json";
import { querySqlServer } from "../../common";

const httpTrigger: AzureFunction = async function (
  context: Context,
  req: HttpRequest
): Promise<void> {
  try {
    const paramsObj = JSON.parse(req.body.data[0][1]);
    const { func, tagIds, startTime, endTime, isMock }: any = paramsObj;
    const errObj = validateReq(startTime, endTime, tagIds);
    if (errObj) {
      context.res = errObj;
      return;
    }
    const sql = PGSDSS_Query_ToSql({
      func,
      tagIds,
      startTime,
      endTime,
    });
    context.log("PGSDSS_Query_ToSql sql:", sql);
    context.log("Querying SQL Server");
    let result = [];
    if (!!isMock) {
      context.log("Sending Mock Response");
      result = mock_pgsdss_data;
    } else {
      context.log(
        `${func} sql ${process.env["PGSDSS_SQL_SERVER"]}: ${sql}`
      );
      result = await querySqlServer("PGSDSS", sql);
    }
    context.res = {
      status: 200,
      body: { data: [[0, JSON.stringify(result)]] },
      headers: {
        "Content-Type": "application/json",
      },
    };
  } catch (error: any) {
    context.log(
      `Exception Querying PGSDSS: ${error} / ${error.message}`
    );
    context.res = {
      status: 500,
      body: JSON.stringify({ error: error.message }),
      headers: {
        "Content-Type": "application/json",
      },
    };
  }
};

const PGSDSS_Query_ToSql = ({
  func,
  tagIds,
  startTime,
  endTime,
}: any) => {
  // func is 'Day' or 'Hour'
  let query = `SELECT 
                [TAGID]
                ,[TIME]
                ,[VALUE]
                ,[DATAQUALITY]
                ,[MINTIME]
                ,[MINVALUE]
                ,[MINDATAQUALITY]
                ,[MAXTIME]
                ,[MAXVALUE]
                ,[MAXDATAQUALITY]
                ,[AVERAGEVALUE]
                ,[OWNSYSID]
                ,[DATASETID]
              FROM [TimeSeries].[dbo].[${func}]
              WHERE TAGID in (${tagIds.join(",")})
                AND TIME >= CONVERT(DATETIME, '${startTime}') 
                AND TIME < CONVERT(DATETIME, '${endTime}')
                ORDER BY TAGID, TIME
  `;
  return query;
};

const validateReq = (
  startTime: string,
  endTime: string,
  tagIds: Array<number>
) => {
  let errObj = null;
  const userId = process.env.PGSDSS_SQL_USER;
  const password = process.env.PGSDSS_SQL_PASSWORD;
  const server = process.env.PGSDSS_SQL_SERVER;
  const database = process.env.PGSDSS_SQL_DATABASE;
  if (!startTime || !endTime || !tagIds) {
    errObj = {
      status: 400,
      body: "Missing required parameters.",
    };
  } else if (!Array.isArray(tagIds) || tagIds.length < 1) {
    errObj = {
      status: 400,
      body: "Invalid tagIds parameter.",
    };
  } else if (!userId || !password || !server || !database) {
    errObj = {
      status: 400,
      body: "Missing environment variables. Contact support.",
    };
  }
  return errObj;
};

export default httpTrigger;
