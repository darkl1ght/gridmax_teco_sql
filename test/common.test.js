const sql = require("mssql");
const { querySqlServer } = require("../src/common");

jest.mock("mssql");

describe("querySqlServer", () => {
  const mockConnect = sql.connect;
  const mockQuery = sql.query;

  const source = "ALLEGRO";
  const query = "SELECT * FROM test_table";

  beforeEach(() => {
    process.env.ALLEGRO_SQL_USER = "user";
    process.env.ALLEGRO_SQL_PASSWORD = "pass";
    process.env.ALLEGRO_SQL_SERVER = "server";
    process.env.ALLEGRO_SQL_DATABASE = "database";
    process.env.ALLEGRO_SQL_TRUST_CERT = "true";
    process.env.SQL_CONN_TIMEOUT = "100000";
    process.env.SQL_REQ_TIMEOUT = "200000";

    jest.clearAllMocks();
  });

  test("should execute SQL query and return result", async () => {
    const mockRecordset = [{ id: 1, name: "test" }];
    const mockClose = jest.fn();

    mockConnect.mockResolvedValueOnce({ close: mockClose });
    mockQuery.mockResolvedValueOnce({ recordset: mockRecordset });

    const result = await querySqlServer(source, query);

    expect(mockConnect).toHaveBeenCalled();
    expect(mockQuery).toHaveBeenCalledWith(query);
    expect(result).toEqual(mockRecordset);
    expect(mockClose).toHaveBeenCalled();
  });

  test("should execute SQL query and return result", async () => {
    process.env.PGSDSS_SQL_USER = "user";
    process.env.PGSDSS_SQL_PASSWORD = "pass";
    process.env.PGSDSS_SQL_SERVER = "server";
    process.env.PGSDSS_SQL_DATABASE = "database";
    const mockRecordset = [{ id: 1, name: "test" }];
    const mockClose = jest.fn();
    mockConnect.mockResolvedValueOnce({ close: mockClose });
    mockQuery.mockResolvedValueOnce({ recordset: mockRecordset });

    const result = await querySqlServer("PGSDSS", query);

    expect(mockConnect).toHaveBeenCalled();
    expect(mockQuery).toHaveBeenCalledWith(query);
    expect(result).toEqual(mockRecordset);
    expect(mockClose).toHaveBeenCalled();
  });

  test("should throw error if query fails", async () => {
    const error = new Error("DB Error");

    mockConnect.mockResolvedValueOnce({ close: jest.fn() });
    mockQuery.mockRejectedValueOnce(error);

    await expect(querySqlServer(source, query)).rejects.toThrow(
      "DB Error"
    );

    expect(mockConnect).toHaveBeenCalled();
    expect(mockQuery).toHaveBeenCalled();
  });

  test("should use default timeouts when env variables are not set", async () => {
    delete process.env.SQL_CONN_TIMEOUT;
    delete process.env.SQL_REQ_TIMEOUT;

    process.env.ALLEGRO_SQL_USER = "user";
    process.env.ALLEGRO_SQL_PASSWORD = "pass";
    process.env.ALLEGRO_SQL_SERVER = "server";
    process.env.ALLEGRO_SQL_DATABASE = "database";
    process.env.ALLEGRO_SQL_TRUST_CERT = "true";

    const mockRecordset = [{ id: 2, name: "defaultTimeoutTest" }];
    const mockClose = jest.fn();

    sql.connect.mockResolvedValueOnce({ close: mockClose });
    sql.query.mockResolvedValueOnce({ recordset: mockRecordset });

    const result = await querySqlServer("ALLEGRO", "SELECT 1");

    expect(result).toEqual(mockRecordset);
    expect(mockClose).toHaveBeenCalled();
  });
});
