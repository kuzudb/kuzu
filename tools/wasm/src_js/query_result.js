"use strict";

const dispatcher = require("./dispatcher.js");

class QueryResult {
  constructor(id) {
    this._id = id;
    this._isClosed = false;
    this._hasNext = undefined;
    this._hasNextQueryResult = undefined;
    this._isSuccess = undefined;
  }

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

  isSuccess() {
    return this._isSuccess;
  }

  async getErrorMessage() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultGetErrorMessage(this._id);
    if (res.isSuccess) {
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }

  async resetIterator() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultResetIterator(this._id);
    if (!res.isSuccess) {
      throw new Error(res.error);
    }
    await this._syncValues();
  }

  hasNext() {
    return this._hasNext;
  }

  hasNextQueryResult() {
    return this._hasNextQueryResult;
  }

  async getNumColumns() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultGetNumColumns(this._id);
    if (res.isSuccess) {
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }

  async getNumTuples() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultGetNumTuples(this._id);
    if (res.isSuccess) {
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }

  async getColumnNames() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultGetColumnNames(this._id);
    if (res.isSuccess) {
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }

  async getColumnTypes() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultGetColumnTypes(this._id);
    if (res.isSuccess) {
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }


  async toString() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultToString(this._id);
    if (res.isSuccess) {
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }

  async getQuerySummary() {
    const worker = await dispatcher.getWorker();
    const res = await worker.queryResultGetQuerySummary(this._id);
    if (res.isSuccess) {
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }

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

  async close() {
    if (!this._isClosed) {
      const worker = await dispatcher.getWorker();
      await worker.queryResultClose(this._id);
      this._isClosed = true;
    }
  }
}

module.exports = QueryResult;
