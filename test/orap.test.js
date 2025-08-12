import httpTrigger from "../src/app/az-func-orap";
import axios from "axios";
import * as xml2js from "xml2js";
import {
  mockXMLResponse,
  falseMockXMLResponse,
} from "../src/app/az-func-orap/mock-orap-xml";
const mockOrapData = require("../src/app/az-func-orap/mock-orap-data.json");

jest.mock("axios");

describe("ORAP - httpTrigger Azure Function", () => {
  let context;

  beforeEach(() => {
    context = {
      log: jest.fn(),
      res: undefined,
    };

    process.env.ORAP_USERID = "testUser";
    process.env.ORAP_PASSWORD = "testPassword";
    process.env.ORAP_SERVICE_URL = "https://testservice.com";
    process.env.ORAP_COOKIE = "testCookie";
  });

  afterEach(() => {
    jest.resetAllMocks();
  });

  test("should return error if required parameters are missing", async () => {
    const req = {
      body: { data: [[0, JSON.stringify({})]] }, // Missing func, beginDate, endDate
    };

    await httpTrigger(context, req);

    expect(context.res.status).toBe(400);
    expect(context.res.body).toBe("Missing required parameters.");
  });

  test("should return error if unsupported function is provided", async () => {
    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "INVALID_FUNC",
              beginDate: "2024-01-01",
              endDate: "2024-01-31",
            }),
          ],
        ],
      },
    };

    await httpTrigger(context, req);

    expect(context.res.status).toBe(400);
    expect(context.res.body).toMatch(
      /Function INVALID_FUNC does not exist/
    );
  });

  test("should return error if environment variables are missing", async () => {
    delete process.env.ORAP_USERID;

    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "GetGenFuelPerf",
              beginDate: "2024-01-01",
              endDate: "2024-01-31",
            }),
          ],
        ],
      },
    };

    await httpTrigger(context, req);

    expect(context.res.status).toBe(400);
    expect(context.res.body).toBe(
      "Missing environment variables. Contact support."
    );
  });

  test("should use mock response when isMock is true", async () => {
    // Define a mock XML response with the correct structure
    const xmlContent = mockXMLResponse;

    // Setup mock response for any external SOAP call
    global.mockXMLResponse = mockXMLResponse;

    // Spy on axios to ensure it's NOT called
    const axiosSpy = jest.spyOn(axios, "request");

    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "GetGenFuelPerf",
              beginDate: "2024-01-01",
              endDate: "2024-01-31",
              isMock: true, // Set to true to use the mock path
            }),
          ],
        ],
      },
    };

    const context = {
      log: jest.fn(),
      res: {},
    };

    await httpTrigger(context, req);

    // Verify axios was NOT called since we used the mock path
    expect(axiosSpy).not.toHaveBeenCalled();

    // Verify the response structure and content
    expect(context.res.body).toBeDefined();
    expect(context.res.body.data).toBeDefined();
    expect(context.res.body.data[0]).toBeDefined();

    const responseData = JSON.parse(context.res.body.data[0][1]);

    expect(responseData).toBeDefined();
    expect(responseData[0]).toBeDefined();
    expect(responseData[0].Reporting_Level).toBe("Plant");
    expect(responseData[0].Net_Equiv_Avail_Factor).toBe("0.98");
    expect(context.res.headers["Content-Type"]).toBe(
      "application/json"
    );
  });

  test("should return error on SOAP parsing failure", async () => {
    const invalidData = "invalid_base64_data";
    // Mock axios to return a response that will cause a parsing error
    axios.request.mockResolvedValue({
      data: `<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
      <s:Body>
        <GetGenFuelPerfResponse>
          <GetGenFuelPerfResult>${invalidData}</GetGenFuelPerfResult>
        </GetGenFuelPerfResponse>
      </s:Body>
    </s:Envelope>`,
    });

    // Use the real parseString function - no need to mock it
    // The invalid base64 data will cause a natural error

    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "GetGenFuelPerf",
              beginDate: "2024-01-01",
              endDate: "2024-01-31",
              isMock: false,
            }),
          ],
        ],
      },
    };

    const context = {
      log: jest.fn(),
      res: {},
    };

    await httpTrigger(context, req);

    expect(context.res.status).toBe(500);
    expect(typeof context.res.body).toBe("string");

    const errorObj = JSON.parse(context.res.body);
    expect(errorObj.error).toBe(
      `GetGenFuelPerfResult = ${invalidData}`
    );
  });

  test("should return error on SOAP parsing failure - no body", async () => {
    // Mock axios to return a response that will cause a parsing error
    axios.request.mockResolvedValue({
      data: `<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
      
    </s:Envelope>`,
    });

    // Use the real parseString function - no need to mock it
    // The invalid base64 data will cause a natural error

    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "GetGenFuelPerf",
              beginDate: "2024-01-01",
              endDate: "2024-01-31",
              isMock: false,
            }),
          ],
        ],
      },
    };

    const context = {
      log: jest.fn(),
      res: {},
    };

    await httpTrigger(context, req);

    expect(context.res.status).toBe(500);
    expect(typeof context.res.body).toBe("string");

    const errorObj = JSON.parse(context.res.body);
    expect(errorObj.error).toBe("SOAP Body not found");
  });

  test("should return error on SOAP parsing failure - parse error", async () => {
    // Mock axios to return a response that will cause a parsing error
    axios.request.mockResolvedValue({
      data: `<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
      <s:Body>
        
    </s:Envelope>`,
    });

    // Use the real parseString function - no need to mock it
    // The invalid base64 data will cause a natural error

    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "GetGenFuelPerf",
              beginDate: "2024-01-01",
              endDate: "2024-01-31",
              isMock: false,
            }),
          ],
        ],
      },
    };

    const context = {
      log: jest.fn(),
      res: {},
    };

    await httpTrigger(context, req);

    expect(context.res.status).toBe(500);
    expect(typeof context.res.body).toBe("string");

    const errorObj = JSON.parse(context.res.body);
    expect(errorObj.error).toBe(
      "Unexpected close tag\nLine: 3\nColumn: 17\nChar: >"
    );
  });

  test("should return error on SOAP parsing failure - no function response", async () => {
    // Mock axios to return a response that will cause a parsing error
    axios.request.mockResolvedValue({
      data: `<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
      <s:Body>
        
      </s:Body>
    </s:Envelope>`,
    });

    // Use the real parseString function - no need to mock it
    // The invalid base64 data will cause a natural error

    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "GetGenFuelPerf",
              beginDate: "2024-01-01",
              endDate: "2024-01-31",
              isMock: false,
            }),
          ],
        ],
      },
    };

    const context = {
      log: jest.fn(),
      res: {},
    };

    await httpTrigger(context, req);

    expect(context.res.status).toBe(500);
    expect(typeof context.res.body).toBe("string");

    const errorObj = JSON.parse(context.res.body);
    expect(errorObj.error).toBe(
      "GetGenFuelPerfResponse element not found"
    );
  });

  test("should return error on SOAP parsing failure - no function result", async () => {
    // Mock axios to return a response that will cause a parsing error
    axios.request.mockResolvedValue({
      data: `<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
      <s:Body>
        <GetGenFuelPerfResponse>
         
        </GetGenFuelPerfResponse>
      </s:Body>
    </s:Envelope>`,
    });

    // Use the real parseString function - no need to mock it
    // The invalid base64 data will cause a natural error

    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "GetGenFuelPerf",
              beginDate: "2024-01-01",
              endDate: "2024-01-31",
              isMock: false,
            }),
          ],
        ],
      },
    };

    const context = {
      log: jest.fn(),
      res: {},
    };

    await httpTrigger(context, req);

    expect(context.res.status).toBe(500);
    expect(typeof context.res.body).toBe("string");

    const errorObj = JSON.parse(context.res.body);
    expect(errorObj.error).toBe(
      "GetGenFuelPerfResult element not found"
    );
  });

  test("should fetch and parse SOAP response when isMock is false", async () => {
    // Create a mock SOAP response with the correct structure
    const xmlContent = mockXMLResponse;

    // Mock the axios response
    axios.request.mockResolvedValue({ data: xmlContent });

    // Let the real XML parser work - no need to mock parseString
    // Remove this mock if you're using it:
    // jest.spyOn(xml2js, "parseString").mockImplementation(...

    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "GetGenFuelPerf",
              beginDate: "2024-01-01",
              endDate: "2024-01-31",
              isMock: false,
            }),
          ],
        ],
      },
    };

    const context = {
      log: jest.fn(),
      res: {},
    };

    await httpTrigger(context, req);

    // Now check the response
    expect(context.res.body).toBeDefined();
    expect(context.res.body.data).toBeDefined();
    expect(context.res.body.data[0]).toBeDefined();

    const responseData = JSON.parse(context.res.body.data[0][1]);

    expect(responseData).toBeDefined();
    expect(responseData[0]).toBeDefined();
    expect(responseData[0].Reporting_Level).toBe("Plant");
    expect(responseData[0].Net_Equiv_Avail_Factor).toBe("0.98");
    expect(context.res.headers["Content-Type"]).toBe(
      "application/json"
    );
  });

  test("should fetch and parse SOAP response when isMock is false - wrong GenFuelPerf string", async () => {
    // Create a mock SOAP response with the correct structure
    const xmlContent = falseMockXMLResponse;

    // Mock the axios response
    axios.request.mockResolvedValue({ data: xmlContent });

    // Let the real XML parser work - no need to mock parseString
    // Remove this mock if you're using it:
    // jest.spyOn(xml2js, "parseString").mockImplementation(...

    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "GetGenFuelPerf",
              beginDate: "2024-01-01",
              endDate: "2024-01-31",
              isMock: false,
            }),
          ],
        ],
      },
    };

    const context = {
      log: jest.fn(),
      res: {},
    };

    await httpTrigger(context, req);

    expect(context.res.status).toBe(500);
    expect(typeof context.res.body).toBe("string");

    const errorObj = JSON.parse(context.res.body);
    expect(errorObj.error).toBe(
      "Unclosed root tag\nLine: 0\nColumn: 23363\nChar: "
    );
  });
});
