"use strict";

const assert = require("assert");

class QueryResult {
  /**
   * Internal constructor. Use `Connection.query` or `Connection.execute`
   * to get a `QueryResult` object.
   * @param {Connection} connection the connection object.
   * @param {KuzuNative.NodeQueryResult} queryResult the native query result object.
   */
  constructor(connection, queryResult) {
    assert(
      typeof connection === "object" &&
        connection.constructor.name === "Connection"
    );
    assert(
      typeof queryResult === "object" &&
        queryResult.constructor.name === "NodeQueryResult"
    );
    this._connection = connection;
    this._queryResult = queryResult;
  }

  /**
   * Reset the iterator of the query result to the beginning.
   * This function is useful if the query result is iterated multiple times.
   */
  resetIterator() {
    this._queryResult.resetIterator();
  }

  /**
   * Check if the query result has more rows.
   * @returns {Boolean} true if the query result has more rows.
   */
  hasNext() {
    return this._queryResult.hasNext();
  }

  /**
   * Get the number of rows of the query result.
   * @returns {Number} the number of rows of the query result.
   */
  getNumTuples() {
    return this._queryResult.getNumTuples();
  }

  /**
   * Get the next row of the query result.
   * @returns {Promise<Object>} a promise that resolves to the next row of the query result. The promise is rejected if there is an error.
   */
  getNext() {
    return new Promise((resolve, reject) => {
      this._queryResult.getNextAsync((err, result) => {
        if (err) {
          return reject(err);
        }
        return resolve(result);
      });
    });
  }

  /**
   * Iterate through the query result with callback functions.
   * @param {Function} resultCallback the callback function that is called for each row of the query result.
   * @param {Function} doneCallback the callback function that is called when the iteration is done.
   * @param {Function} errorCallback the callback function that is called when there is an error.
   */
  each(resultCallback, doneCallback, errorCallback) {
    if (!this.hasNext()) {
      return doneCallback();
    }
    this.getNext()
      .then((row) => {
        resultCallback(row);
        this.each(resultCallback, doneCallback, errorCallback);
      })
      .catch((err) => {
        errorCallback(err);
      });
  }

  /**
   * Get all rows of the query result.
   * @returns {Promise<Array<Object>>} a promise that resolves to all rows of the query result. The promise is rejected if there is an error.
   */
  async getAll() {
    this._queryResult.resetIterator();
    const result = [];
    while (this.hasNext()) {
      result.push(await this.getNext());
    }
    return result;
  }

  /**
   * Get all rows of the query result with callback functions.
   * @param {Function} resultCallback the callback function that is called with all rows of the query result.
   * @param {Function} errorCallback the callback function that is called when there is an error.
   */
  all(resultCallback, errorCallback) {
    this.getAll()
      .then((result) => {
        resultCallback(result);
      })
      .catch((err) => {
        errorCallback(err);
      });
  }

  /**
   * Get the data types of the columns of the query result.
   * @returns {Promise<Array<String>>} a promise that resolves to the data types of the columns of the query result. The promise is rejected if there is an error.
   */
  getColumnDataTypes() {
    return new Promise((resolve, reject) => {
      this._queryResult.getColumnDataTypesAsync((err, result) => {
        if (err) {
          return reject(err);
        }
        return resolve(result);
      });
    });
  }

  /**
   * Get the names of the columns of the query result.
   * @returns {Promise<Array<String>>} a promise that resolves to the names of the columns of the query result. The promise is rejected if there is an error.
   */
  getColumnNames() {
    return new Promise((resolve, reject) => {
      this._queryResult.getColumnNamesAsync((err, result) => {
        if (err) {
          return reject(err);
        }
        return resolve(result);
      });
    });
  }
}

module.exports = QueryResult;
