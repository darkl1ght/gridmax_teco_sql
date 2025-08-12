// Import the function and dependencies
import httpTrigger from "../src/app/az-func-gridmax";
const mockData = require("../src/app/az-func-gridmax/mock-gridmax-data.json");
import { querySqlServer } from "../src/common";

// Mock querySqlServer to avoid real DB calls
jest.mock("../src/common", () => ({
  querySqlServer: jest.fn(),
}));

describe("GRIDMAX - httpTrigger Azure Function", () => {
  let context;

  beforeEach(() => {
    context = {
      log: jest.fn(),
      res: undefined,
    };

    // Set required environment variables
    process.env.GRIDMAX_SQL_USER = "testUser";
    process.env.GRIDMAX_SQL_PASSWORD = "testPassword";
    process.env.GRIDMAX_SQL_SERVER = "testServer";
    process.env.GRIDMAX_SQL_DATABASE = "testDatabase";
  });

  afterEach(() => {
    jest.resetAllMocks();
  });

  test("should return error if required parameters are missing", async () => {
    const req = {
      body: { data: [[0, JSON.stringify({})]] }, // missing func, year, month
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
              year: 2023,
              month: 1,
              isMock: false,
            }),
          ],
        ],
      },
    };

    await httpTrigger(context, req);

    expect(context.res.status).toBe(400);
    expect(context.res.body).toMatch(
      /Function INVALID_FUNC does not exist. Please resubmit with supported function./
    );
  });

  test("should return error if environment variables are missing", async () => {
    delete process.env.GRIDMAX_SQL_USER;

    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "BAYSIDE",
              year: 2023,
              month: 1,
              isMock: false,
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

  test("should return mock data when isMock is true", async () => {
    const funcName = "BAYSIDE";
    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: funcName,
              year: 2023,
              month: 1,
              isMock: true,
            }),
          ],
        ],
      },
    };

    await httpTrigger(context, req);

    expect(context.log).toHaveBeenCalledWith("Sending Mock Response");
    expect(context.res.status).toBe(200);
    expect(JSON.parse(context.res.body.data[0][1])).toEqual(
      mockData[funcName.toUpperCase()]
    );
  });

  test("should fetch data from database when isMock is false", async () => {
    querySqlServer.mockResolvedValue([
      { YEAR: 2023, MONTH: 1, UNIT_NET: 500 },
    ]);

    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "BAYSIDE",
              year: 2023,
              month: 1,
              isMock: false,
            }),
          ],
        ],
      },
    };

    await httpTrigger(context, req);

    expect(querySqlServer).toHaveBeenCalledWith(
      "GRIDMAX",
      "select * from HIS_USER.GADS.PLANT_MONTHLY_GEN_FN('BAYSIDE',2023,1)"
    );
    expect(context.res.status).toBe(200);
    expect(JSON.parse(context.res.body.data[0][1])).toEqual([
      { YEAR: 2023, MONTH: 1, UNIT_NET: 500 },
    ]);
  });
});
