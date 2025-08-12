// httpTrigger.test.js

// Import the function and the mock data
import httpTrigger from "../src/app/az-func-allegro";
import * as mockData from "../src/app/az-func-allegro/mock-allegro-data.json";
import { querySqlServer } from "../src/common";

jest.mock("../src/common");

// Use a simple in-memory representation of your "funcs" object (as defined in your module)
const funcs = {
  mockFunc: [1, "mockDataset", "mockRecord"],
};

describe("ALLEGRO - httpTrigger Azure Function", () => {
  let context;

  // Set up a new context and required environment variables before each test
  beforeEach(() => {
    context = {
      log: jest.fn(),
      res: undefined,
    };
    process.env.ALLEGRO_SQL_USER = "testUser";
    process.env.ALLEGRO_SQL_PASSWORD = "testPassword";
    process.env.ALLEGRO_SQL_SERVER = "testServer";
    process.env.ALLEGRO_SQL_DATABASE = "testDatabase";
  });

  // Clean up mocks after each test
  afterEach(() => {
    jest.resetAllMocks();
  });

  test("should return error if required parameters are missing", async () => {
    const params = {}; // missing func, beginDate, endDate
    const req = { body: { data: [[0, JSON.stringify(params)]] } };
    await httpTrigger(context, req);
    expect(context.res.status).toBe(400);
    expect(context.res.body).toBe("Missing required parameters.");
  });

  test("should return error if unsupported function is provided", async () => {
    const params = {
      func: "nonExistingFunc",
      beginDate: "2022-01-01",
      endDate: "2022-01-02",
      isMock: false,
    };
    const req = { body: { data: [[0, JSON.stringify(params)]] } };
    await httpTrigger(context, req);
    expect(context.res.status).toBe(400);
    expect(context.res.body).toMatch(
      /Function nonExistingFunc does not exist/
    );
  });

  test("should return error if environment variables are missing", async () => {
    // Remove one required environment variable
    delete process.env.ALLEGRO_SQL_USER;
    const params = {
      func: "SlfConsumptionQuality",
      beginDate: "2022-01-01",
      endDate: "2022-01-02",
      isMock: false,
    };
    const req = { body: { data: [[0, JSON.stringify(params)]] } };
    await httpTrigger(context, req);
    expect(context.res.status).toBe(400);
    expect(context.res.body).toBe(
      "Missing environment variables. Contact support."
    );
  });

  test("should return valid response with mock data when isMock is true", async () => {
    const params = {
      func: "SlfConsumptionQuality",
      beginDate: "2022-01-01",
      endDate: "2022-01-02",
      isMock: true,
    };
    const req = { body: { data: [[0, JSON.stringify(params)]] } };
    await httpTrigger(context, req);
    expect(context.log).toHaveBeenCalledWith("Sending Mock Response");
    expect(context.res.status).toBe(200);
    expect(JSON.parse(context.res.body.data[0][1])).toEqual(
      mockData["SlfConsumptionQuality"]
    );
  });

  test("should return valid response with empty array when isMock is false", async () => {
    const spy = querySqlServer.mockResolvedValueOnce([]);
    const params = {
      func: "SlfConsumptionQuality",
      beginDate: "2022-01-01",
      endDate: "2022-01-02",
      isMock: false,
    };
    const req = { body: { data: [[0, JSON.stringify(params)]] } };
    await httpTrigger(context, req);
    expect(context.res.status).toBe(200);
    // When isMock is false, the function returns an empty array
    expect(JSON.parse(context.res.body.data[0][1])).toEqual([]);
  });

  test("should format date and call querySqlServer for SlfConsumptionQuality", async () => {
    const spy = querySqlServer.mockResolvedValueOnce([
      {
        counterparty: "A",
        schedule_type: "BASE",
        delivery_month: "01/01/2022",
        mwh: 100,
      },
    ]);

    const params = {
      func: "SlfConsumptionQuality",
      beginDate: "2022-01-01",
      endDate: "2022-01-02",
      isMock: false,
    };

    const req = { body: { data: [[0, JSON.stringify(params)]] } };
    await httpTrigger(context, req);

    expect(spy).toHaveBeenCalled();
    expect(context.res.status).toBe(200);
    expect(JSON.parse(context.res.body.data[0][1])).toEqual([
      {
        counterparty: "A",
        schedule_type: "BASE",
        delivery_month: "01/01/2022",
        mwh: 100,
      },
    ]);
  });
});
