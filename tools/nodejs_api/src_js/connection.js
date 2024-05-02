"use strict";

const KuzuNative = require("./kuzu_native.js");
const QueryResult = require("./query_result.js");
const PreparedStatement = require("./prepared_statement.js");

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
    this._isClosed = false;
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
    if (this._isClosed) {
      throw new Error("Connection is closed.");
    }
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
    if (this._isClosed) {
      throw new Error("Connection is closed.");
    }
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
          (typeof value === "object" && value.constructor.name === "Date") ||
          typeof value === "bigint"
        ) {
          paramArray.push([key, value]);
        } else {
          return reject(
            new Error(
              "The value of each parameter must be a boolean, number, string, Date or BigInt."
            )
          );
        }
      }
      this._getConnection()
        .then((connection) => {
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
                this._unwrapMultipleQueryResults(nodeQueryResult)
                  .then((queryResults) => {
                    return resolve(queryResults);
                  })
                  .catch((err) => {
                    return reject(err);
                  });
              }
            );
          } catch (e) {
            return reject(e);
          }
        })
        .catch((err) => {
          return reject(err);
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
      this._getConnection()
        .then((connection) => {
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
        })
        .catch((err) => {
          return reject(err);
        });
    });
  }

  /**
   * Execute a query.
   * @param {String} statement the statement to execute.
   * @returns {Promise<kuzu.QueryResult>} a promise that resolves to the query result. The promise is rejected if there is an error.
   */
  query(statement) {
    return new Promise((resolve, reject) => {
      if (typeof statement !== "string") {
        return reject(new Error("statement must be a string."));
      }
      this._getConnection()
        .then((connection) => {
          const nodeQueryResult = new KuzuNative.NodeQueryResult();
          try {
            connection.queryAsync(statement, nodeQueryResult, (err) => {
              if (err) {
                return reject(err);
              }
              this._unwrapMultipleQueryResults(nodeQueryResult)
                .then((queryResults) => {
                  return resolve(queryResults);
                })
                .catch((err) => {
                  return reject(err);
                });
            });
          } catch (e) {
            return reject(e);
          }
        })
        .catch((err) => {
          return reject(err);
        });
    });
  }

  /**
   * Internal function to get the next query result for multiple query results.
   * @param {KuzuNative.NodeQueryResult} nodeQueryResult the current node query result.
   * @returns {Promise<kuzu.QueryResult>} a promise that resolves to the next query result. The promise is rejected if there is an error.
   */
  _getNextQueryResult(nodeQueryResult) {
    return new Promise((resolve, reject) => {
      const nextNodeQueryResult = new KuzuNative.NodeQueryResult();
      nodeQueryResult.getNextQueryResultAsync(nextNodeQueryResult, (err) => {
        if (err) {
          return reject(err);
        }
        return resolve(new QueryResult(this, nextNodeQueryResult));
      });
    });
  }

  /**
   * Internal function to unwrap multiple query results into an array of query results.
   * @param {KuzuNative.NodeQueryResult} nodeQueryResult the node query result.
   * @returns {Promise<Array<kuzu.QueryResult>> | kuzu.QueryResult} a promise that resolves to an array of query results. The promise is rejected if there is an error.
   */
  async _unwrapMultipleQueryResults(nodeQueryResult) {
    const wrappedQueryResult = new QueryResult(this, nodeQueryResult);
    if (!nodeQueryResult.hasNextQueryResult()) {
      return wrappedQueryResult;
    }
    const queryResults = [wrappedQueryResult];
    let currentQueryResult = nodeQueryResult;
    while (currentQueryResult.hasNextQueryResult()) {
      queryResults.push(await this._getNextQueryResult(currentQueryResult));
      currentQueryResult = queryResults[queryResults.length - 1]._queryResult;
    }
    return queryResults;
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

  /**
   * Close the connection. 
   * 
   * Note: Call to this method is optional. The connection will be closed
   * automatically when the object goes out of scope.
   */
  async close() {
    if (this._isClosed) {
      return;
    }
    if (!this._isInitialized) {
      if (this._initPromise) {
        // Connection is initializing, wait for it to finish first.
        await this._initPromise;
      } else {
        // Connection is not initialized, simply mark it as closed and initialized.
        this._isInitialized = true;
        this._isClosed = true;
        delete this._connection;
        return;
      }
    }
    // Connection is initialized, close it.
    this._connection.close();
    delete this._connection;
    this._isClosed = true;
  }
}

module.exports = Connection;
