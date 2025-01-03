"use strict";

const dispatcher = require("./dispatcher");

const main = async () => {
  const worker = await dispatcher.getWorker();
  let db = await worker.databaseConstruct("");
  console.log(db);
  let conn = await worker.connectionConstruct(db.id);
  console.log(conn);
  let queryRes = await worker.connectionQuery(conn.id, "RETURN 1 + 1, 'hello', 3.14");
  console.log(queryRes);
  let isSucc = await worker.queryResultIsSuccess(queryRes.id);
  console.log(isSucc);
  let allObjs = await worker.queryResultGetAllObjects(queryRes.id);
  console.log(allObjs);
  dispatcher.close();
}

module.exports = main;
