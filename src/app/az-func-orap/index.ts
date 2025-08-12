import { AzureFunction, Context, HttpRequest } from "@azure/functions";
import axios, { AxiosRequestConfig } from "axios";
import * as xml2js from "xml2js";
import * as dotenv from "dotenv";
import { mockXMLResponse } from "./mock-orap-xml";

dotenv.config();
// supported funcs
const funcs: any = {
  // funcName : [isActive, DatasetElement, RecordElement]
  GetGenFuelPerf: [1, "GenFuelPerfs", "GenFuelPerf"],
  GetGenPerfIncent: [1, "GenPerfIncents", "GenPerfIncent"],
  GetOutages: [1, "Outages", "Outage"],
  GetFleetRAMKPIs: [1, "FleetRAMKPIs", "FleetRAMKPI"],
  GetEvtHrsSummary: [1, "EvtHrsSummarys", "EvtHrsSummary"],
  GetNetFactorsRates: [1, "NetFactorsRates", "NetFactorsRate"],
};

const httpTrigger: AzureFunction = async function (
  context: Context,
  req: HttpRequest
): Promise<void> {
  try {
    context.log("req.body", req.body);
    const paramsObj = JSON.parse(req.body.data[0][1]);
    const { func, beginDate, endDate, isMock } = paramsObj;
    const errObj = validateReq(func, beginDate, endDate);
    if (errObj) {
      context.res = errObj;
      return;
    }
    const userId = process.env.ORAP_USERID;
    const password = process.env.ORAP_PASSWORD;
    const serviceUrl = process.env.ORAP_SERVICE_URL;
    const soapPayload = `
        <soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:tem="http://tempuri.org/" xmlns:wsa="http://www.w3.org/2005/08/addressing">
        <soap:Header>
          <wsa:To>${serviceUrl}</wsa:To>   
          <wsa:Action>http://tempuri.org/IService1/${func}</wsa:Action>
        </soap:Header>
        <soap:Body>
            <tem:${func}>
              <tem:aUserID>${userId}</tem:aUserID>
              <tem:aPassword>${password}</tem:aPassword>
              <tem:aBeginDate>${beginDate}</tem:aBeginDate>
              <tem:anEndDate>${endDate}</tem:anEndDate>
            </tem:${func}>
        </soap:Body>
      </soap:Envelope>
    `;
    const config: AxiosRequestConfig = {
      method: "POST",
      url: serviceUrl,
      headers: {
        "Content-Type": "application/soap+xml;charset=UTF-8;",
      },
      data: soapPayload,
    };
    let response: any = null;
    if (!!isMock) {
      response = {
        data: mockXMLResponse,
      };
    } else {
      response = await axios.request(config);
      if (response.data.toUpperCase().includes("NO DATA FOUND")) {
        context.res = {
          body: { data: [[0, "[]"]] },
          headers: {
            "Content-Type": "application/json",
          },
        };
        return;
      }
    }

    const parser = new xml2js.Parser({
      explicitArray: false,
      ignoreAttrs: true,
    });
    const parsedXml = await new Promise((resolve, reject) => {
      parser.parseString(response.data, (err, result) => {
        if (err) {
          reject(err);
          return;
        }
        try {
          const envelope: any = result["s:Envelope"] || result.Envelope;
          const body: any =
            envelope && (envelope["s:Body"] || envelope.Body);
          if (!body) {
            reject(new Error("SOAP Body not found"));
            return;
          }

          const functionResponse: any =
            body[`${func}Response`] || body[`tns:${func}`];

          if (!functionResponse) {
            reject(new Error(`${func}Response element not found`));
            return;
          }
          const funcResult = functionResponse[`${func}Result`];
          if (!funcResult) {
            reject(new Error(`${func}Result element not found`));
            return;
          } else if (
            funcResult &&
            typeof funcResult == "string" &&
            !funcResult.startsWith("<")
          ) {
            reject({ message: `${func}Result = ${funcResult}` });
            return;
          }
          xml2js.parseString(funcResult, (err2, result2) => {
            if (err2) {
              reject(err2);
              return;
            }
            resolve(result2);
          });
        } catch (e) {
          reject(e);
        }
      });
    });
    const json = JSON.parse(JSON.stringify(parsedXml));
    let records = [];
    if (json.Error) {
      records = [json.Error];
    } else {
      let datasetElement = json[funcs[func][1]];
      if (!datasetElement) {
        throw new Error(
          `Function ${func} does not have the ${funcs[func][1]} dataset element.`
        );
      }

      records = datasetElement[funcs[func][2]];
      if (!records) {
        throw new Error(
          `Function ${func} does not have the ${funcs[func][2]} record element.`
        );
      }
    }
    // fix the values array into scalar values
    const flatRecords: any = [];
    records.forEach((record: any) => {
      const nRec: any = {};
      Object.keys(record).forEach((key) => {
        nRec[key] = record[key][0];
      });
      flatRecords.push(nRec);
    });
    // context.log({ flatRecords });
    context.res = {
      body: { data: [[0, JSON.stringify(flatRecords)]] },
      headers: {
        "Content-Type": "application/json",
      },
    };
  } catch (error: any) {
    context.log(`Exception Querying ORAP: ${error} / ${error.message}`);
    const status = error?.status || 500;
    context.res = {
      status,
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
  const userId = process.env.ORAP_USERID;
  const password = process.env.ORAP_PASSWORD;
  const serviceUrl = process.env.ORAP_SERVICE_URL;
  if (!func || !beginDate || !endDate) {
    errObj = {
      status: 400,
      body: "Missing required parameters.",
    };
  } else if (!(funcs[func] && funcs[func][0] === 1)) {
    errObj = {
      status: 400,
      body: `Function ${func} does not exist. Please resubmit with supported function.`,
    };
  } else if (!userId || !password || !serviceUrl) {
    errObj = {
      status: 400,
      body: "Missing environment variables. Contact support.",
    };
  }
  return errObj;
};

export default httpTrigger;
