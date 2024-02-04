global.chai = require("chai");
global.assert = chai.assert;
global.expect = chai.expect;
chai.should();
chai.config.includeStack = true;

const TEST_INSTALLED = process.env.TEST_INSTALLED || false;
if (TEST_INSTALLED) {
  console.log("Testing installed package...");
  global.kuzu = require("kuzu");
} else {
  console.log("Testing locally built version...");
  global.kuzu = require("../build/");
}

const tmp = require("tmp");
const fs = require("fs/promises");
const initTests = async () => {
  const dbPath = await new Promise((resolve, reject) => {
    tmp.dir({ unsafeCleanup: true }, (err, path, _) => {
      if (err) {
        return reject(err);
      }
      return resolve(path);
    });
  });

  const db = new kuzu.Database(dbPath, 1 << 28 /* 256MB */);
  const conn = new kuzu.Connection(db, 4);

  const schema = (await fs.readFile("../../dataset/tinysnb/schema.cypher"))
    .toString()
    .split("\n");
  for (const line of schema) {
    if (line.trim().length === 0) {
      continue;
    }
    await conn.query(line);
  }

  const copy = (await fs.readFile("../../dataset/tinysnb/copy.cypher"))
    .toString()
    .split("\n");

  for (const line of copy) {
    if (line.trim().length === 0) {
      continue;
    }
    const statement = line.replace("dataset/tinysnb", "../../dataset/tinysnb");
    await conn.query(statement);
  }

  await conn.query(
     "create node table moviesSerial (ID SERIAL, name STRING, length INT32, note STRING, PRIMARY KEY (ID))"
  );
  await conn.query(
     'copy moviesSerial from "../../dataset/tinysnb-serial/vMovies.csv"'
  );

  await conn.query("CREATE RDFGraph T;");
  await conn.query(
      ` 
        CREATE (:T_l {val:cast(12, "INT64")}),
            (:T_l {val:cast(43, "INT32")}),
            (:T_l {val:cast(33, "INT16")}),
            (:T_l {val:cast(2, "INT8")}),
            (:T_l {val:cast(90, "UINT64")}),
            (:T_l {val:cast(77, "UINT32")}),
            (:T_l {val:cast(12, "UINT16")}),
            (:T_l {val:cast(1, "UINT8")}),
            (:T_l {val:cast(4.4, "DOUBLE")}),
            (:T_l {val:cast(1.2, "FLOAT")}),
            (:T_l {val:true}),
            (:T_l {val:"hhh"}),
            (:T_l {val:cast("2024-01-01", "DATE")}),
            (:T_l {val:cast("2024-01-01 11:25:30Z+00:00", "TIMESTAMP")}),
            (:T_l {val:cast("2 day", "INTERVAL")}),
            (:T_l {val:cast("\\\\xB2", "BLOB")})
        `
    );

  global.dbPath = dbPath;
  global.db = db;
  global.conn = conn;
};

global.initTests = initTests;
