/**
 * @file connection.js is the file for the Connection class. Connection is used 
 * to interact with a Database instance. 
 */
"use strict";

const dispatcher = require("./dispatcher.js");
const QueryResult = require("./query_result.js");
const PreparedStatement = require("./prepared_statement.js");

class Connection {
  /**
   * Initialize a new Connection object. Note that the initialization is done
   * lazily, so the connection is not initialized until the first query is
   * executed. To initialize the connection immediately, call the `init()`
   * function on the returned object.
   * @param {kuzu.Database} database the database object to connect to.
   * @param {Number} numThreads the maximum number of threads to use for query 
   * execution.
   */
  constructor(database, numThreads = null) {
    this._isInitialized = false;
    this._initPromise = null;
    this._id = null;
    this._isClosed = false;
    this._database = database;
    this.numThreads = numThreads;
  }

  /**
   * Initialize the connection. Calling this function is optional, as the
   * connection is initialized automatically when the first query is executed.
   */
  async init() {
    if (!this._isInitialized) {
      if (!this._initPromise) {
        this._initPromise = (async () => {
          const databaseId = await this._database._getDatabaseObjectId();
          const worker = await dispatcher.getWorker();
          const res = await worker.connectionConstruct(databaseId, this.numThreads);
          if (res.isSuccess) {
            this._id = res.id;
            this._isInitialized = true;
          } else {
            throw new Error(res.error);
          }
        })();
      }
      await this._initPromise;
      this._initPromise = null;
    }
  }

  /**
   * Internal function to get the connection object ID.
   * @private
   * @throws {Error} if the connection is closed.
   */
  async _getConnectionObjectId() {
    if (this._isClosed) {
      throw new Error("Connection is closed.");
    }
    if (!this._isInitialized) {
      await this.init();
    }
    return this._id;
  }

  /**
   * Set the maximum number of threads to use for query execution.
   * @param {Number} numThreads the maximum number of threads to use for query 
   * execution.
   */
  async setMaxNumThreadForExec(numThreads) {
    const connectionId = await this._getConnectionObjectId();
    const worker = await dispatcher.getWorker();
    const res = await worker.connectionSetMaxNumThreadForExec(connectionId, numThreads);
    if (!res.isSuccess) {
      throw new Error(res.error);
    }
  }

  /**
   * Set the query timeout in milliseconds.
   * @param {Number} timeout the query timeout in milliseconds.
   */
  async setQueryTimeout(timeout) {
    const connectionId = await this._getConnectionObjectId();
    const worker = await dispatcher.getWorker();
    const res = await worker.connectionSetQueryTimeout(connectionId, timeout);
    if (!res.isSuccess) {
      throw new Error(res.error);
    }
  }

  /**
   * Get the maximum number of threads to use for query execution.
   * @returns {Number} the maximum number of threads to use for query execution.
   */
  async getMaxNumThreadForExec() {
    const connectionId = await this._getConnectionObjectId();
    const worker = await dispatcher.getWorker();
    const res = await worker.connectionGetMaxNumThreadForExec(connectionId);
    if (!res.isSuccess) {
      throw new Error(res.error);
    }
    return res.numThreads;
  }

  /**
   * Execute a query.
   * @param {String} statement the statement to execute.
   * @returns {kuzu.QueryResult} the query result.
   */
  async query(statement) {
    const connectionId = await this._getConnectionObjectId();
    const worker = await dispatcher.getWorker();
    const res = await worker.connectionQuery(connectionId, statement);
    if (!res.isSuccess) {
      throw new Error(res.error);
    }
    const queryResult = new QueryResult(res.id);
    await queryResult._syncValues();
    return queryResult;
  }

  /**
   * Prepare a statement for execution.
   * @param {String} statement the statement to prepare.
   * @returns {kuzu.PreparedStatement} the prepared statement.
   */
  async prepare(statement) {
    const connectionId = await this._getConnectionObjectId();
    const worker = await dispatcher.getWorker();
    const res = await worker.connectionPrepare(connectionId, statement);
    if (!res.isSuccess) {
      throw new Error(res.error);
    }
    const preparedStatement = new PreparedStatement(res.id);
    await preparedStatement._syncValues();
    return preparedStatement;
  }

  /**
   * Execute a prepared statement.
   * @param {kuzu.sync.PreparedStatement} preparedStatement the prepared 
   * statement to execute.
   * @param {Object} params a plain object mapping parameter names to values.
   * @returns {kuzu.QueryResult} the query result.
   */
  async execute(preparedStatement, params = {}) {
    const connectionId = await this._getConnectionObjectId();
    const worker = await dispatcher.getWorker();
    const res = await worker.connectionExecute(connectionId, preparedStatement._id, params);
    if (!res.isSuccess) {
      throw new Error(res.error);
    }
    const queryResult = new QueryResult(res.id);
    await queryResult._syncValues();
    return queryResult;
  }

  /**
   * Close the connection.
   */
  async close() {
    if (!this._isClosed) {
      const connectionId = await this._getConnectionObjectId();
      const worker = await dispatcher.getWorker();
      await worker.connectionClose(connectionId);
      this._isClosed = true;
    }
  }
}

module.exports = Connection;
