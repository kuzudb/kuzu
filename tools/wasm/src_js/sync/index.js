"use strict";

const KuzuWasm = require("./kuzu.js");
const Database = require("./database.js");
const Connection = require("./connection.js");
const PreparedStatement = require("./prepared_statement.js");
const QueryResult = require("./query_result.js");

module.exports = {
  init: () => {
    return KuzuWasm.init();
  },
  getVersion: () => {
    return KuzuWasm.getVersion();
  },
  getStorageVersion: () => {
    return KuzuWasm.getStorageVersion();
  },
  Database,
  Connection,
  PreparedStatement,
  QueryResult,
};
