import * as sql from "mssql";
import * as dotenv from "dotenv";

dotenv.config();

interface QueryResult {
  [key: string]: any;
}

export async function querySqlServer(
  source: string,
  query: string
): Promise<QueryResult[]> {
  let conn = null;
  try {
    const config: any = {
      user: process.env[`${source}_SQL_USER`],
      password: process.env[`${source}_SQL_PASSWORD`],
      server: process.env[`${source}_SQL_SERVER`],
      database: process.env[`${source}_SQL_DATABASE`],
      options: {
        // encrypt: process.env.GRIDMAX_SQL_ENCRYPT === 'true',
        trustServerCertificate:
          process.env[`${source}_SQL_TRUST_CERT`] === "true",
        // trustedConnection: true,
      },
      connectionTimeout:
        Number(process.env["SQL_CONN_TIMEOUT"]) || 120000,
      requestTimeout: Number(process.env["SQL_REQ_TIMEOUT"]) || 240000,
    };
    if (source == "PGSDSS") {
      // Note: HACKY workaround to circumvent connectivity issue due to it is a instance on non-default port
      const connStr = `Server=${config.server};Database=${config.database};User Id=${config.user};Password=${config.password};trustServerCertificate=true`;
      conn = await sql.connect(connStr);
    } else {
      conn = await sql.connect(config);
    }
    const result = await sql.query(query);
    return result.recordset;
  } catch (err: any) {
    // console.error(`Error querying sql server: ${source} :`, err);
    throw err;
  } finally {
    conn?.close();
  }
}
