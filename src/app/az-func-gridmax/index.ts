import { AzureFunction, Context, HttpRequest } from "@azure/functions";
import { querySqlServer } from "../../common";
import mock_gridmax_data from "./mock-gridmax-data.json";

type GridMaxDataType = {
  [key: string]: {
    YEAR: number;
    MONTH: number;
    DATEPART: number;
    STATION: string;
    PLANT: string;
    UNIT: string;
    UNIT_GROSS: number;
    UNIT_NET: number;
  }[];
};

const funcs: any = {
  // plantName : []
  BAYSIDE: [],
  BIGBEND: [],
  POLK: [],
  ALL: [],
  SOLAR: [],
  MACDILL: [],
};

// Explicitly type the imported JSON
const typedGridmaxData: GridMaxDataType = funcs as GridMaxDataType;
const typedGridmaxMockData: GridMaxDataType =
  mock_gridmax_data as unknown as GridMaxDataType;

const httpTrigger: AzureFunction = async function (
  context: Context,
  req: HttpRequest
): Promise<void> {
  try {
    const paramsObj = JSON.parse(req.body.data[0][1]);
    const { func, year, month, isMock } = paramsObj;
    const errObj = validateReq(func, year, month);
    if (errObj) {
      context.res = errObj;
      return;
    }

    let result: any[] = [];

    if (!!isMock) {
      result = typedGridmaxMockData[func];
      context.log("Sending Mock Response");
    } else {
      const query = `select * from HIS_USER.GADS.PLANT_MONTHLY_GEN_FN('${func}',${year},${month})`;
      context.log(
        `Sending query to ${process.env["GRIDMAX_SQL_SERVER"]}: ${query}`
      );
      result = await querySqlServer("GRIDMAX", query);
    }
    context.log("Array length " + result.length);

    context.res = {
      status: 200,
      body: { data: [[0, JSON.stringify(result)]] },
      headers: {
        "Content-Type": "application/json",
      },
    };
  } catch (error: any) {
    context.log(
      `Exception Querying Gridmax: ${error} / ${error.message}`
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

const validateReq = (func: string, year: string, month: string) => {
  let errObj = null;
  const userId = process.env.GRIDMAX_SQL_USER;
  const password = process.env.GRIDMAX_SQL_PASSWORD;
  const server = process.env.GRIDMAX_SQL_SERVER;
  const database = process.env.GRIDMAX_SQL_DATABASE;
  if (!func || !year || !month) {
    errObj = {
      status: 400,
      body: "Missing required parameters.",
    };
  } else if (!(func in typedGridmaxData)) {
    errObj = {
      status: 400,
      body: `Function ${func} does not exist. Please resubmit with supported function.`,
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
