global.chai = require("chai");
global.assert = chai.assert;
global.expect = chai.expect;
chai.should();
chai.config.includeStack = true;

process.env.NODE_ENV = "test";
global.kuzu = require("../build/kuzu");

const tmp = require("tmp");
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
  await conn.execute(`CREATE NODE TABLE person (ID INT64, fName STRING, 
                      gender INT64, isStudent BOOLEAN, isWorker BOOLEAN, 
                      age INT64, eyeSight DOUBLE, birthdate DATE, 
                      registerTime TIMESTAMP, lastJobDuration INTERVAL, 
                      workedHours INT64[], usedNames STRING[], 
                      courseScoresPerTerm INT64[][], grades INT64[], 
                      height DOUBLE, PRIMARY KEY (ID))`);
  await conn.execute(`CREATE REL TABLE knows (FROM person TO person, 
                      date DATE, meetTime TIMESTAMP, validInterval INTERVAL, 
                      comments STRING[], MANY_MANY);`);
  await conn.execute(`CREATE NODE TABLE organisation (ID INT64, name STRING, 
                      orgCode INT64, mark DOUBLE, score INT64, history STRING, 
                      licenseValidInterval INTERVAL, rating DOUBLE, 
                      PRIMARY KEY (ID))`);
  await conn.execute(`CREATE REL TABLE workAt (FROM person TO organisation, 
                      year INT64, listdec DOUBLE[], height DOUBLE, MANY_ONE)`);

  await conn.execute(
    `COPY person FROM "../../dataset/tinysnb/vPerson.csv" (HEADER=true)`
  );
  await conn.execute(`COPY knows FROM "../../dataset/tinysnb/eKnows.csv"`);
  await conn.execute(
    `COPY organisation FROM "../../dataset/tinysnb/vOrganisation.csv"`
  );
  await conn.execute(`COPY workAt FROM "../../dataset/tinysnb/eWorkAt.csv"`);
  global.dbPath = dbPath
  global.db = db;
  global.conn = conn;
};

global.initTests = initTests;
