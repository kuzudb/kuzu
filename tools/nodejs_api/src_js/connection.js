"use strict";

const KuzuNative = require("./kuzujs.node");
const QueryResult = require("./query_result.js");
const PreparedStatement = require("./prepared_statement.js");

const PRIMARY_KEY_TEXT = "(PRIMARY KEY)";
const SRC_NODE_TEXT = "src node:";
const DST_NODE_TEXT = "dst node:";
const PROPERTIES_TEXT = "properties:";

class Connection {
  /**
   * Initialize a new Connection object. Note that the initialization is done
   * lazily, so the connection is not initialized until the first query is
   * executed. To initialize the connection immediately, call the `init()`
   * function on the returned object.
   *
   * @param {kuzu.Database} database the database object to connect to.
   * @param {Number} numThreads the maximum number of threads to use for query execution.
   */
  constructor(database, numThreads = null) {
    if (
      typeof database !== "object" ||
      database.constructor.name !== "Database"
    ) {
      throw new Error("database must be a valid Database object.");
    }
    this._database = database;
    this._connection = null;
    this._isInitialized = false;
    this._initPromise = null;
    numThreads = parseInt(numThreads);
    if (numThreads && numThreads > 0) {
      this._numThreads = numThreads;
    }
  }

  /**
   * Initialize the connection. Calling this function is optional, as the
   * connection is initialized automatically when the first query is executed.
   */
  async init() {
    if (!this._connection) {
      const database = await this._database._getDatabase();
      this._connection = new KuzuNative.NodeConnection(database);
    }
    if (!this._isInitialized) {
      if (!this._initPromise) {
        this._initPromise = new Promise((resolve, reject) => {
          this._connection.initAsync((err) => {
            if (err) {
              reject(err);
            } else {
              this._isInitialized = true;
              if (this._numThreads) {
                this._connection.setMaxNumThreadForExec(this._numThreads);
              }
              if (this._queryTimeout) {
                this._connection.setQueryTimeout(this._queryTimeout);
              }
              resolve();
            }
          });
        });
      }
      await this._initPromise;
      this._initPromise = null;
    }
  }

  /**
   * Internal function to get the underlying native connection object.
   * @returns {KuzuNative.NodeConnection} the underlying native connection.
   */
  async _getConnection() {
    await this.init();
    return this._connection;
  }

  /**
   * Execute a prepared statement with the given parameters.
   * @param {kuzu.PreparedStatement} preparedStatement the prepared statement to execute.
   * @param {Object} params a plain object mapping parameter names to values.
   * @returns {Promise<kuzu.QueryResult>} a promise that resolves to the query result. The promise is rejected if there is an error.
   */
  execute(preparedStatement, params = {}) {
    return new Promise((resolve, reject) => {
      if (
        !typeof preparedStatement === "object" ||
        preparedStatement.constructor.name !== "PreparedStatement"
      ) {
        return reject(
          new Error(
            "preparedStatement must be a valid PreparedStatement object."
          )
        );
      }
      if (!preparedStatement.isSuccess()) {
        return reject(new Error(preparedStatement.getErrorMessage()));
      }
      if (params.constructor !== Object) {
        return reject(new Error("params must be a plain object."));
      }
      const paramArray = [];
      for (const key in params) {
        const value = params[key];
        if (value === null || value === undefined) {
          return reject(
            new Error(
              "The value of each parameter must not be null or undefined."
            )
          );
        }
        if (
          typeof value === "boolean" ||
          typeof value === "number" ||
          typeof value === "string" ||
          (typeof value === "object" && value.constructor.name === "Date")
        ) {
          paramArray.push([key, value]);
        } else {
          return reject(
            new Error(
              "The value of each parameter must be a boolean, number, string, or Date object."
            )
          );
        }
      }
      this._getConnection().then((connection) => {
        const nodeQueryResult = new KuzuNative.NodeQueryResult();
        try {
          connection.executeAsync(
            preparedStatement._preparedStatement,
            nodeQueryResult,
            paramArray,
            (err) => {
              if (err) {
                return reject(err);
              }
              return resolve(new QueryResult(this, nodeQueryResult));
            }
          );
        } catch (e) {
          return reject(e);
        }
      });
    });
  }

  /**
   * Prepare a statement for execution.
   * @param {String} statement the statement to prepare.
   * @returns {Promise<kuzu.PreparedStatement>} a promise that resolves to the prepared statement. The promise is rejected if there is an error.
   */
  prepare(statement) {
    return new Promise((resolve, reject) => {
      if (typeof statement !== "string") {
        return reject(new Error("statement must be a string."));
      }
      this._getConnection().then((connection) => {
        const preparedStatement = new KuzuNative.NodePreparedStatement(
          connection,
          statement
        );
        preparedStatement.initAsync((err) => {
          if (err) {
            return reject(err);
          }
          return resolve(new PreparedStatement(this, preparedStatement));
        });
      });
    });
  }

  /**
   * Execute a query.
   * @param {String} statement the statement to execute.
   * @returns {Promise<kuzu.QueryResult>} a promise that resolves to the query result. The promise is rejected if there is an error.
   */
  async query(statement) {
    if (typeof statement !== "string") {
      throw new Error("statement must be a string.");
    }
    const preparedStatement = await this.prepare(statement);
    const queryResult = await this.execute(preparedStatement);
    return queryResult;
  }

  /**
   * Set the maximum number of threads to use for query execution.
   * @param {Number} numThreads the maximum number of threads to use for query execution.
   */
  setMaxNumThreadForExec(numThreads) {
    // If the connection is not initialized yet, store the logging level
    // and defer setting it until the connection is initialized.
    if (typeof numThreads !== "number" || !numThreads || numThreads < 0) {
      throw new Error("numThreads must be a positive number.");
    }
    if (this._isInitialized) {
      this._connection.setMaxNumThreadForExec(numThreads);
    } else {
      this._numThreads = numThreads;
    }
  }

  /**
   * Set the timeout for queries. Queries that take longer than the timeout
   * will be aborted.
   * @param {Number} timeoutInMs the timeout in milliseconds.
   */
  setQueryTimeout(timeoutInMs) {
    if (
      typeof timeoutInMs !== "number" ||
      isNaN(timeoutInMs) ||
      timeoutInMs <= 0
    ) {
      throw new Error("timeoutInMs must be a positive number.");
    }
    if (this._isInitialized) {
      this._connection.setQueryTimeout(timeoutInMs);
    } else {
      this._queryTimeout = timeoutInMs;
    }
  }
}

module.exports = Connection;
