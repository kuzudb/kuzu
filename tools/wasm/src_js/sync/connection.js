"use strict";

const KuzuWasm = require("./kuzu.js");
const QueryResult = require("./query_result.js");
const PreparedStatement = require("./prepared_statement.js");

class Connection {
  constructor(database, numThreads = null) {
    KuzuWasm.checkInit();
    const kuzu = KuzuWasm._kuzu;
    if (!database || typeof database !== "object") {
      throw new Error("Database must be an object.");
    }
    if (database._isClosed) {
      throw new Error("Database is already closed.");
    }
    numThreads = parseInt(numThreads);

    this._connection = new kuzu.Connection(database._database);
    if (numThreads && numThreads > 0) {
      this._connection.setMaxNumThreadForExec(numThreads);
    }
    this._isClosed = false;
  }

  _checkConnection() {
    KuzuWasm.checkInit();
    if (this._isClosed) {
      throw new Error("Connection is already closed.");
    }
  }

  setMaxNumThreadForExec(numThreads) {
    this._checkConnection();
    this._connection.setMaxNumThreadForExec(numThreads);
    this._numThreads = numThreads;
  }

  setQueryTimeout(timeout) {
    this._checkConnection();
    this._connection.setQueryTimeout(timeout);
  }

  getMaxNumThreadForExec() {
    this._checkConnection();
    return this._connection.getMaxNumThreadForExec();
  }

  query(statement) {
    this._checkConnection();
    if (typeof statement !== "string") {
      throw new Error("Statement must be a string.");
    }
    const _queryResult = this._connection.query(statement);
    return new QueryResult(_queryResult);
  }

  prepare(statement) {
    this._checkConnection();
    if (typeof statement !== "string") {
      throw new Error("Statement must be a string.");
    }
    const _preparedStatement = this._connection.prepare(statement);
    return new PreparedStatement(_preparedStatement);
  }

  execute(preparedStatement, params = {}) {
    this._checkConnection();
    if (!preparedStatement ||
      typeof preparedStatement !== "object" ||
      preparedStatement.constructor.name !== "PreparedStatement" ||
      preparedStatement._isClosed) {
      throw new Error("preparedStatement must be a valid PreparedStatement object.");
    }
    if (!preparedStatement.isSuccess()) {
      throw new Error(preparedStatement.getErrorMessage());
    }
    if (params.constructor.name !== "Object") {
      throw new Error("params must be a plain object.");
    }
    const paramsArray = [];
    for (const key in params) {
      paramsArray.push({
        name: key,
        value: params[key]
      });
    }
    const _queryResult = this._connection.execute(preparedStatement._statement, paramsArray);
    return new QueryResult(_queryResult);
  }

  close() {
    if (!this._isClosed) {
      this._connection.close();
      this._isClosed = true;
    }
  }
}

module.exports = Connection;
