/**
 * @file connection.js is the file for the Connection class. Connection 
 * is used to interact with a Database instance. 
 */
"use strict";

const KuzuWasm = require("./kuzu.js");
const QueryResult = require("./query_result.js");
const PreparedStatement = require("./prepared_statement.js");

class Connection {
  /**
   * Initialize a new Connection object.
   *
   * @param {kuzu.sync.Database} database the database object to connect to.
   * @param {Number} numThreads the maximum number of threads to use for query 
   * execution.
   */
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

  /**
   * Internal function to check if the connection is closed.
   * @throws {Error} if the connection is closed.
   * @private
   */
  _checkConnection() {
    KuzuWasm.checkInit();
    if (this._isClosed) {
      throw new Error("Connection is already closed.");
    }
  }

  /**
   * Set the maximum number of threads to use for query execution.
   * @param {Number} numThreads the maximum number of threads to use for query 
   * execution.
   * @throws {Error} if the connection is closed.
   * @throws {Error} if numThreads is not a positive integer.
   */
  setMaxNumThreadForExec(numThreads) {
    this._checkConnection();
    this._connection.setMaxNumThreadForExec(numThreads);
    this._numThreads = numThreads;
  }

  /**
   * Set the query timeout in milliseconds.
   * @param {Number} timeout the query timeout in milliseconds.
   * @throws {Error} if the connection is closed.
   * @throws {Error} if timeout is not a positive integer.
   */
  setQueryTimeout(timeout) {
    this._checkConnection();
    this._connection.setQueryTimeout(timeout);
  }

  /**
   * Get the maximum number of threads to use for query execution.
   * @returns {Number} the maximum number of threads to use for query execution.
   * @throws {Error} if the connection is closed.
   */
  getMaxNumThreadForExec() {
    this._checkConnection();
    return this._connection.getMaxNumThreadForExec();
  }

  /**
   * Execute a query.
   * @param {String} statement the statement to execute.
   * @returns {kuzu.sync.QueryResult} the query result.
   * @throws {Error} if the connection is closed.
   * @throws {Error} if statement is not a string.
   */
  query(statement) {
    this._checkConnection();
    if (typeof statement !== "string") {
      throw new Error("Statement must be a string.");
    }
    const _queryResult = this._connection.query(statement);
    return new QueryResult(_queryResult);
  }

  /**
   * Prepare a statement for execution.
   * @param {String} statement the statement to prepare.
   * @returns {Promise<kuzu.sync.PreparedStatement>} the prepared statement.
   */
  prepare(statement) {
    this._checkConnection();
    if (typeof statement !== "string") {
      throw new Error("Statement must be a string.");
    }
    const _preparedStatement = this._connection.prepare(statement);
    return new PreparedStatement(_preparedStatement);
  }

  /**
   * Execute a prepared statement.
   * @param {kuzu.sync.PreparedStatement} preparedStatement the prepared 
   * statement to execute.
   * @param {Object} params a plain object mapping parameter names to values.
   * @returns {kuzu.sync.QueryResult} the query result.
   * @throws {Error} if the connection is closed.
   * @throws {Error} if preparedStatement is not a valid PreparedStatement 
   * object.
   * @throws {Error} if preparedStatement is not successful.
   * @throws {Error} if params is not a plain object.
   */
  execute(preparedStatement, params = {}) {
    this._checkConnection();
    if (!preparedStatement ||
      preparedStatement._isClosed) {
      throw new Error("preparedStatement is not valid or is closed.");
    }
    if (!preparedStatement.isSuccess()) {
      throw new Error(preparedStatement.getErrorMessage());
    }
    if (!params) {
      throw new Error("params is not valid.");
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

  /**
   * Close the connection.
   */
  close() {
    if (!this._isClosed) {
      this._connection.delete();
      this._isClosed = true;
    }
  }
}

module.exports = Connection;
