import { AzureFunction, Context, HttpRequest } from "@azure/functions";
import { querySqlServer } from "../../common";
import mock_allegro_data from "./mock-allegro-data.json";

type AllegroDataType = {
  [key: string]: {
    counterparty: string;
    ScheduleType: string;
    delivery_month: string;
    mwh: number;
  }[];
};

const funcs: any = {
  SlfConsumptionQuality: [],
};

// Explicitly type the imported JSON
const typedAllegroData: AllegroDataType = funcs as AllegroDataType;
const typedAllegroMockData: AllegroDataType =
  mock_allegro_data as unknown as AllegroDataType;

const httpTrigger: AzureFunction = async function (
  context: Context,
  req: HttpRequest
): Promise<void> {
  try {
    const paramsObj = JSON.parse(req.body.data[0][1]);
    // isMock is a boolean flag for mocking the response
    const { func, beginDate, endDate, isMock } = paramsObj;
    const errObj = validateReq(func, beginDate, endDate);
    if (errObj) {
      context.res = errObj;
      return;
    }

    let result: any[] = [];

    if (!!isMock) {
      context.log("Sending Mock Response");
      result = typedAllegroMockData[func];
    } else {
      const sql = `select 
          accountingdate,
          operationaldate,
          area,
          BTU as btu_factor,
          dischargelocation, 
          commodityclass,
          quantity
        from
          [dbo].[vw_TECO_SLF_Consumption_Quality] 
        where 
          accountingdate >= '${beginDate}' and accountingdate <= '${endDate}';`;
      context.log(
        `${func} sql ${process.env["ALLEGRO_SQL_SERVER"]}: ${sql}`
      );
      if (func === "SlfConsumptionQuality") {
        result = await querySqlServer("ALLEGRO", sql);
      }
    }
    context.log(
      "Number of records returned by the query " + result.length
    );

    context.res = {
      status: 200,
      body: { data: [[0, JSON.stringify(result)]] },
      headers: {
        "Content-Type": "application/json",
      },
    };
  } catch (error: any) {
    context.log(
      `Exception Querying ALLEGRO: ${error} / ${error.message}`
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

const validateReq = (
  func: string,
  beginDate: string,
  endDate: string
) => {
  let errObj = null;
  const userId = process.env.ALLEGRO_SQL_USER;
  const password = process.env.ALLEGRO_SQL_PASSWORD;
  const server = process.env.ALLEGRO_SQL_SERVER;
  const database = process.env.ALLEGRO_SQL_DATABASE;
  if (!func || !beginDate || !endDate) {
    errObj = {
      status: 400,
      body: "Missing required parameters.",
    };
  } else if (!(func in typedAllegroData)) {
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
