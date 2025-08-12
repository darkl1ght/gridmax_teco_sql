// httpTrigger.test.js

// Import the function and the mock data
import httpTrigger from "../src/app/az-func-pgsdss";
const mockPGSDSSData = require("../src/app/az-func-pgsdss/mock-pgsdss-data.json");
import { querySqlServer } from "../src/common";

jest.mock("../src/common", () => ({
  querySqlServer: jest.fn(),
}));

describe("PGSDSS - httpTrigger Azure Function", () => {
  let context;

  // Set up a new context and required environment variables before each test
  beforeEach(() => {
    context = {
      log: jest.fn(),
      res: undefined,
    };
    process.env.PGSDSS_SQL_USER = "testUser";
    process.env.PGSDSS_SQL_PASSWORD = "testPassword";
    process.env.PGSDSS_SQL_SERVER = "testServer";
    process.env.PGSDSS_SQL_DATABASE = "testDatabase";
  });

  // Clean up mocks after each test
  afterEach(() => {
    jest.resetAllMocks();
  });

  test("should return error if required startTime parameters are missing", async () => {
    const params = { tagIds: [123], endTime: "2021-07-01" };
    const req = { body: { data: [[0, JSON.stringify(params)]] } };
    await httpTrigger(context, req);
    expect(context.res.status).toBe(400);
    expect(context.res.body).toBe("Missing required parameters.");
  });

  test("should return error if required endTime parameters are missing", async () => {
    const params = { tagIds: [123], startTime: "2021-07-01" };
    const req = { body: { data: [[0, JSON.stringify(params)]] } };
    await httpTrigger(context, req);
    expect(context.res.status).toBe(400);
    expect(context.res.body).toBe("Missing required parameters.");
  });

  test("should return error if required tagIds parameters are missing", async () => {
    const params = { endTime: "2021-07-31", startTime: "2021-07-01" };
    const req = { body: { data: [[0, JSON.stringify(params)]] } };
    await httpTrigger(context, req);
    expect(context.res.status).toBe(400);
    expect(context.res.body).toBe("Missing required parameters.");
  });

  test("should return error if required tagIds parameters is not array", async () => {
    const params = {
      tagIds: "123",
      endTime: "2021-07-31",
      startTime: "2021-07-01",
    };
    const req = { body: { data: [[0, JSON.stringify(params)]] } };
    await httpTrigger(context, req);
    expect(context.res.status).toBe(400);
    expect(context.res.body).toBe("Invalid tagIds parameter.");
  });

  test("should return error if required tagIds parameter is less than 1 length", async () => {
    const params = {
      tagIds: [],
      endTime: "2021-07-31",
      startTime: "2021-07-01",
    };
    const req = { body: { data: [[0, JSON.stringify(params)]] } };
    await httpTrigger(context, req);
    expect(context.res.status).toBe(400);
    expect(context.res.body).toBe("Invalid tagIds parameter.");
  });

  test("should return error if environment variables are missing", async () => {
    // Remove one required environment variable
    delete process.env.PGSDSS_SQL_USER;
    const params = {
      tagIds: [123, 123],
      startTime: "2022-01-01",
      endTime: "2022-01-02",
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
      tagIds: [123],
      startTime: "2022-01-01",
      endTime: "2022-01-02",
      isMock: true,
    };
    const req = { body: { data: [[0, JSON.stringify(params)]] } };
    await httpTrigger(context, req);
    expect(context.log).toHaveBeenCalledWith("Sending Mock Response");
    expect(context.res.status).toBe(200);
    expect(JSON.parse(context.res.body.data[0][1])).toEqual(
      mockPGSDSSData
    );
  });

  test("should return valid response with empty array when isMock is false", async () => {
    const params = {
      tagIds: [123],
      startTime: "2022-01-01",
      endTime: "2022-01-02",
      isMock: false,
    };
    const mockData = [
      {
        DATE: "2021-07-01T14:00:00.000Z",
        TAGID: 1000479,
        AVERAGEVALUE: 56740.78115942032,
      },
    ];
    querySqlServer.mockResolvedValue(mockData);
    const req = { body: { data: [[0, JSON.stringify(params)]] } };
    await httpTrigger(context, req);
    expect(context.log).toHaveBeenCalledWith("Querying SQL Server");
    expect(context.res.status).toBe(200);
    // When isMock is false, the function returns an empty array
    expect(JSON.parse(context.res.body.data[0][1])).toEqual(mockData);
  });

  test("should return 500 on error", async () => {
    querySqlServer.mockRejectedValue(new Error("Database error"));

    const params = {
      tagIds: [123],
      startTime: "2022-01-01",
      endTime: "2022-01-02",
      isMock: false,
    };
    const req = { body: { data: [[0, JSON.stringify(params)]] } };

    await httpTrigger(context, req);

    expect(context.res.status).toBe(500);
    expect(context.res.body).toBe(
      JSON.stringify({ error: "Database error" })
    );
  });
});
