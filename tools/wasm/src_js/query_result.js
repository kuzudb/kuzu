/**
 * @file QueryResult stores the result of a query execution.
 */
"use strict";

const dispatcher = require("./dispatcher.js");

class QueryResult {
  /**
   * Internal constructor. Use `Connection.query` or `Connection.execute`
   * to get a `QueryResult` object.
   * @param {String} id the internal ID of the query result object.
   */
  constructor(id) {
    this._id = id;
    this._isClosed = false;
    this._hasNext = undefined;
    this._hasNextQueryResult = undefined;
    this._isSuccess = undefined;
  }

  /**
   * Internal function to update the local fields with the values from the
   * worker.
   * @private
   * @throws {Error} if the query result is closed.
   */
  async _syncValues() {
    if (this._isClosed) {
      return;
    }
    const worker = await dispatcher.getWorker();
    let res = await worker.queryResultHasNext(this._id);
    if (res.isSuccess) {
      this._hasNext = res.result;
    } else {
      throw new Error(res.error);
    }
    res = await worker.queryResultHasNextQueryResult(this._id);
    if (res.isSuccess) {
      this._hasNextQueryResult = res.result;
    } else {
      throw new Error(res.error);
    }
    res = await worker.queryResultIsSuccess(this._id);
    if (res.isSuccess) {
      this._isSuccess = res.result;
    } else {
      throw new Error(res.error);
    }
  }

  /**
   * Check if the query result is successfully executed.
   * @returns {Boolean} true if the query result is successfully executed.
   */
  isSuccess() {
    return this._isSuccess;
  }

  /**
   * Get the error message if the query result is not successfully executed.
   * @returns {String} the error message.
   */
  async getErrorMessage() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultGetErrorMessage(this._id);
    if (res.isSuccess) {
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }

  /**
   * Reset the iterator of the query result to the beginning.
   * This function is useful if the query result is iterated multiple times.
   */
  async resetIterator() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultResetIterator(this._id);
    if (!res.isSuccess) {
      throw new Error(res.error);
    }
    await this._syncValues();
  }

  /**
   * Get the number of rows of the query result.
   * @returns {Number} the number of rows of the query result.
   */
  hasNext() {
    return this._hasNext;
  }

  /**
   * Check if the query result has a following query result when multiple 
   * statements are executed within a single query.
   * @returns {Boolean} true if the query result has a following query result.
   */
  hasNextQueryResult() {
    return this._hasNextQueryResult;
  }

  /**
   * Get the number of columns of the query result.
   * @returns {Number} the number of columns of the query result.
   */
  async getNumColumns() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultGetNumColumns(this._id);
    if (res.isSuccess) {
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }

  /**
   * Get the number of rows of the query result.
   * @returns {Number} the number of rows of the query result.
   */
  async getNumTuples() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultGetNumTuples(this._id);
    if (res.isSuccess) {
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }


  /**
   * Get the column names of the query result.
   * @returns {Array<String>} the column names of the query result.
   */
  async getColumnNames() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultGetColumnNames(this._id);
    if (res.isSuccess) {
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }

  /**
   * Get the column types of the query result.
   * @returns {Array<String>} the column types of the query result.
   */
  async getColumnTypes() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultGetColumnTypes(this._id);
    if (res.isSuccess) {
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }

  /**
   * Get the string representation of the query result.
   * @returns {String} the string representation of the query result.
   */
  async toString() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultToString(this._id);
    if (res.isSuccess) {
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }

  /**
   * Get the query summary (execution time and compiling time) of the query 
   * result.
   * @returns {Object} the query summary of the query result.
   */
  async getQuerySummary() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultGetQuerySummary(this._id);
    if (res.isSuccess) {
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }

  /**
   * Get the following query result when multiple statements are executed within 
   * a single query.
   * @returns {QueryResult} the next query result.
   */
  async getNextQueryResult() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultGetNextQueryResult(this._id);
    if (res.isSuccess) {
      const nextQueryResultId = res.result;
      const nextQueryResult = new QueryResult(nextQueryResultId);
      await nextQueryResult._syncValues();
      return nextQueryResult;
    } else {
      throw new Error(res.error);
    }
  }

  /**
   * Get the next row of the query result.
   * @returns {Array} the next row of the query result.
   */
  async getNext() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultGetNext(this._id);
    if (res.isSuccess) {
      await this._syncValues();
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }

  /**
   * Get all rows of the query result.
   * @returns {Array<Array>} all rows of the query result.
   */
  async getAllRows() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultGetAllRows(this._id);
    if (res.isSuccess) {
      await this._syncValues();
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }

  /**
   * Get all objects of the query result.
   * @returns {Array<Object>} all objects of the query result.
   */
  async getAllObjects() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultGetAllObjects(this._id);
    if (res.isSuccess) {
      await this._syncValues();
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }

  /**
   * Close the query result.
   */
  async close() {
    if (!this._isClosed) {
      const worker = await dispatcher.getWorker();
      await worker.queryResultClose(this._id);
      this._isClosed = true;
    }
  }
}

module.exports = QueryResult;
