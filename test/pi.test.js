import httpTrigger from "../src/app/az-func-pi";
import { querySqlServer } from "../src/common";
const mock_pi_data = require("../src/app/az-func-pi/mock-pi-data.json");

jest.mock("../src/common", () => ({
  querySqlServer: jest.fn(),
}));

describe("httpTrigger Function", () => {
  let context;

  beforeEach(() => {
    context = {
      log: jest.fn(),
      res: {},
    };
  });

  test("should return 400 when missing required parameters", async () => {
    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "",
              startTime: "",
              endTime: "",
              server: "",
            }),
          ],
        ],
      },
    };

    await httpTrigger(context, req);

    expect(context.res.status).toBe(400);
    expect(context.res.body).toBe("Missing required parameters.");
  });

  test("should return 400 when function is not supported", async () => {
    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "InvalidFunc",
              startTime: "2024-01-01",
              endTime: "2024-01-02",
              server: "Server1",
            }),
          ],
        ],
      },
    };

    await httpTrigger(context, req);

    expect(context.res.status).toBe(400);
    expect(context.res.body).toBe(
      "Function InvalidFunc does not exist. Please resubmit with supported function."
    );
  });

  test("should return mock data when isMock is true", async () => {
    const params = {
      func: "PIAdvCalcFilDat",
      isMock: true,
      tags: ["1ABMFI901"],
      startTime: "2024-10-01 00:00:00.000",
      endTime: "2024-11-01 00:00:00.000",
      interval: "1d",
      mode: "total",
      calcBasis: "time-weighted",
      minPctGood: 0,
      filtExp: "(''''1abmbl962''''=\"Yes\")",
      sampMode: "pt.compressed",
      sampFreq: "1h",
      cFactor: 0.000277777777777777,
      server: "PLKPISRV01P",
    };
    const req = { body: { data: [[0, JSON.stringify(params)]] } };

    await httpTrigger(context, req);
    expect(context.log).toHaveBeenCalledWith("Sending Mock Response");
    const parsedData = JSON.parse(context.res.body.data[0][1]);
    expect(mock_pi_data).toEqual(parsedData);
  });

  test("should query database when func is PIAdvCalcFilDat and isMock is false", async () => {
    querySqlServer.mockResolvedValue([
      { DATE: "2024-01-01", TAG: "1ABMFI901", CALC_VALUE: 100 },
    ]);

    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "PIAdvCalcFilDat",
              isMock: false,
              tags: ["1ABMFI901"],
              startTime: "2024-10-01 00:00:00.000",
              endTime: "2024-11-01 00:00:00.000",
              interval: "1d",
              mode: "total",
              calcBasis: "time-weighted",
              minPctGood: 0,
              filtExp: "(''''1abmbl962''''=\"Yes\")",
              sampMode: "pt3",
              sampFreq: "1h",
              cFactor: 0.000277777777777777,
              server: "PLKPISRV01P",
            }),
          ],
        ],
      },
    };

    await httpTrigger(context, req);

    expect(querySqlServer).toHaveBeenCalled();
    expect(context.res.body.data[0][1]).toBe(
      JSON.stringify([
        { DATE: "2024-01-01", TAG: "1ABMFI901", CALC_VALUE: 100 },
      ])
    );
  });

  test("should query database when func is PIAdvCalcDat and isMock is false", async () => {
    querySqlServer.mockResolvedValue([
      { DATE: "2024-01-01", TAG: "Tag1", CALC_VALUE: 100 },
    ]);

    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "PIAdvCalcDat",
              tags: ["1FGSFI001VO"],
              startTime: "2024-10-01 00:00:00.000",
              endTime: "2024-11-01 00:00:00.000",
              server: "PLKPISRV01P",
              isMock: false,
            }),
          ],
        ],
      },
    };

    await httpTrigger(context, req);

    expect(querySqlServer).toHaveBeenCalled();
    expect(context.res.body.data[0][1]).toBe(
      JSON.stringify([
        { DATE: "2024-01-01", TAG: "Tag1", CALC_VALUE: 100 },
      ])
    );
  });

  test("should query database when func is PIAdvCalcDat with different set of params and isMock is false", async () => {
    querySqlServer.mockResolvedValue([
      { DATE: "2024-10-01", TAG: "1FGSFI001VO", CALC_VALUE: 100 },
    ]);

    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "PIAdvCalcDat",
              tags: ["1FGSFI001VO"],
              startTime: "2024-10-01 00:00:00.000",
              endTime: "2024-11-01 00:00:00.000",
              server: "PLKPISRV01P",
              isMock: false,
              interval: "1d",
              mode: "total",
              calcBasis: "time-weighted",
              minPctGood: 0,
              cFactor: 86.4,
            }),
          ],
        ],
      },
    };

    await httpTrigger(context, req);

    expect(querySqlServer).toHaveBeenCalled();
    expect(context.res.body.data[0][1]).toBe(
      JSON.stringify([
        { DATE: "2024-10-01", TAG: "1FGSFI001VO", CALC_VALUE: 100 },
      ])
    );
  });

  test("should query database when func is PIAdvCalcFilDat with different set of params and isMock is false", async () => {
    querySqlServer.mockResolvedValue([
      { DATE: "2024-10-01", TAG: "1ABMFI901", CALC_VALUE: 100 },
    ]);

    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "PIAdvCalcFilDat",
              tags: ["1ABMFI901"],
              startTime: "2024-10-01 00:00:00.000",
              endTime: "2024-11-01 00:00:00.000",
              interval: "1d",
              mode: "total",
              calcBasis: "time-weighted",
              minPctGood: 0,
              filtExp: "(''''1abmbl962''''=\"Yes\")",
              sampMode: "pt.compressed",
              sampFreq: "1h",
              cFactor: 0.000277777777777777,
              server: "PLKPISRV01P",
            }),
          ],
        ],
      },
    };

    await httpTrigger(context, req);

    expect(querySqlServer).toHaveBeenCalled();
    expect(context.res.body.data[0][1]).toBe(
      JSON.stringify([
        { DATE: "2024-10-01", TAG: "1ABMFI901", CALC_VALUE: 100 },
      ])
    );
  });

  test("should return 500 on error", async () => {
    querySqlServer.mockRejectedValue(new Error("Database error"));

    const req = {
      body: {
        data: [
          [
            0,
            JSON.stringify({
              func: "PIAdvCalcFilDat",
              tags: ["1ABMFI901"],
              startTime: "2024-10-01 00:00:00.000",
              endTime: "2024-11-01 00:00:00.000",
              mode: "total",
              filtExp: "(''''1abmbl962''''=\"Yes\")",
              sampFreq: "1h",
              cFactor: 0.000277777777777777,
              server: "PLKPISRV01P",
            }),
          ],
        ],
      },
    };

    await httpTrigger(context, req);

    expect(context.res.status).toBe(500);
    expect(context.res.body).toBe(
      JSON.stringify({ error: "Database error" })
    );
  });
});
