/**
 * @file QueryResult stores the result of a query execution.
 */
"use strict";

const KuzuWasm = require("./kuzu.js");

class QueryResult {
  /**
   * Internal constructor. Use `Connection.query` or `Connection.execute`
   * to get a `QueryResult` object.
   * @param {kuzu.sync.QueryResult} _queryResult the native query result object.
   */
  constructor(_queryResult, _isClosable = true) {
    this._result = _queryResult;
    this._isClosed = false;
    this._isClosable = _isClosable;
  }

  /**
   * Internal function to check if the query result is closed.
   * @throws {Error} if the query result is closed.
   * @private
   */
  _checkQueryResult() {
    KuzuWasm.checkInit();
    if (this._isClosed) {
      throw new Error("QueryResult is already closed.");
    }
  }

  /**
   * Check if the query result is successfully executed.
   * @returns {Boolean} true if the query result is successfully executed.
   * @throws {Error} if the query result is closed.
   */
  isSuccess() {
    this._checkQueryResult();
    return this._result.isSuccess();
  }

  /**
   * Get the error message if the query result is not successfully executed.
   * @returns {String} the error message.
   * @throws {Error} if the query result is closed.
   */
  getErrorMessage() {
    this._checkQueryResult();
    return this._result.getErrorMessage();
  }

  /**
   * Reset the iterator of the query result to the beginning.
   * This function is useful if the query result is iterated multiple times.
   * @throws {Error} if the query result is closed.
   */
  resetIterator() {
    this._checkQueryResult();
    this._result.resetIterator();
  }

  /**
   * Get the number of rows of the query result.
   * @returns {Number} the number of rows of the query result.
   * @throws {Error} if the query result is closed.
   */
  hasNext() {
    this._checkQueryResult();
    return this._result.hasNext();
  }

  /**
   * Check if the query result has a following query result when multiple 
   * statements are executed within a single query.
   * @returns {Boolean} true if the query result has a following query result.
   * @throws {Error} if the query result is closed.
   */
  hasNextQueryResult() {
    this._checkQueryResult();
    return this._result.hasNextQueryResult();
  }

  /**
   * Get the number of columns of the query result.
   * @returns {Number} the number of columns of the query result.
   * @throws {Error} if the query result is closed.
   */
  getNumColumns() {
    this._checkQueryResult();
    return this._result.getNumColumns();
  }

  /**
   * Get the number of rows of the query result.
   * @returns {Number} the number of rows of the query result.
   * @throws {Error} if the query result is closed.
   */
  getNumTuples() {
    this._checkQueryResult();
    return this._result.getNumTuples();
  }

  /**
   * Get the column names of the query result.
   * @returns {Array<String>} the column names of the query result.
   * @throws {Error} if the query result is closed.
   */
  getColumnNames() {
    this._checkQueryResult();
    return this._result.getColumnNames();
  }

  /**
   * Get the column types of the query result.
   * @returns {Array<String>} the column types of the query result.
   * @throws {Error} if the query result is closed.
   */
  getColumnTypes() {
    this._checkQueryResult();
    return this._result.getColumnTypes();
  }

  /**
   * Get the string representation of the query result.
   * @returns {String} the string representation of the query result.
   * @throws {Error} if the query result is closed.
   */
  toString() {
    this._checkQueryResult();
    return this._result.toString();
  }

  /**
   * Get the query summary (execution time and compiling time) of the query 
   * result.
   * @returns {Object} the query summary of the query result.
   * @throws {Error} if the query result is closed.
   */
  getQuerySummary() {
    this._checkQueryResult();
    return this._result.getQuerySummary();
  }

  /**
   * Get the following query result when multiple statements are executed within 
   * a single query.
   * @returns {QueryResult} the next query result.
   * @throws {Error} if the query result is closed.
   */
  getNextQueryResult() {
    this._checkQueryResult();
    return new QueryResult(this._result.getNextQueryResult(), false);
  }

  /**
   * Get the next row of the query result.
   * @returns {Array} the next row of the query result.
   * @throws {Error} if the query result is closed.
   */
  getNext() {
    this._checkQueryResult();
    return this._result.getNext();
  }

  /**
   * Get all rows of the query result.
   * @returns {Array<Array>} all rows of the query result.
   * @throws {Error} if the query result
   */
  getAllRows() {
    this._checkQueryResult();
    return this._result.getAsJsNestedArray();
  }

  /**
   * Get all objects of the query result.
   * @returns {Array<Object>} all objects of the query result.
   * @throws {Error} if the query result is closed.
   */
  getAllObjects() {
    this._checkQueryResult();
    return this._result.getAsJsArrayOfObjects();
  }

  /**
   * Close the query result.
   */
  close() {
    if (!this._isClosed) {
      if (this._isClosable) {
        this._result.delete();
      }
      this._isClosed = true;
    }
  }
}

module.exports = QueryResult;
