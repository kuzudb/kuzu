"use strict";

const dispatcher = require("./dispatcher");
const Database = require("./database");
const Connection = require("./connection");
const PreparedStatement = require("./prepared_statement");
const QueryResult = require("./query_result");


module.exports = {
  getVersion: async () => {
    const worker = await dispatcher.getWorker();
    const version = await worker.getVersion();
    return version;
  },
  getStorageVersion: async () => {
    const worker = await dispatcher.getWorker();
    const storageVersion = await worker.getStorageVersion();
    return storageVersion;
  },
  Database,
  Connection,
  PreparedStatement,
  QueryResult,
}
