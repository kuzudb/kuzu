"use strict";

const KuzuWasm = require("./kuzu.js");

class QueryResult {
  constructor(_queryResult, _isClosable = true) {
    this._result = _queryResult;
    this._isClosed = false;
    this._isClosable = _isClosable;
  }

  _checkQueryResult() {
    KuzuWasm.checkInit();
    if (this._isClosed) {
      throw new Error("QueryResult is already closed.");
    }
  }

  isSuccess() {
    this._checkQueryResult();
    return this._result.isSuccess();
  }

  getErrorMessage() {
    this._checkQueryResult();
    return this._result.getErrorMessage();
  }

  resetIterator() {
    this._checkQueryResult();
    this._result.resetIterator();
  }

  hasNext() {
    this._checkQueryResult();
    return this._result.hasNext();
  }

  hasNextQueryResult() {
    this._checkQueryResult();
    return this._result.hasNextQueryResult();
  }

  getNumColumns() {
    this._checkQueryResult();
    return this._result.getNumColumns();
  }

  getNumTuples() {
    this._checkQueryResult();
    return this._result.getNumTuples();
  }

  getColumnNames() {
    this._checkQueryResult();
    return this._result.getColumnNames();
  }

  getColumnTypes() {
    this._checkQueryResult();
    return this._result.getColumnTypes();
  }

  toString() {
    this._checkQueryResult();
    return this._result.toString();
  }

  getQuerySummary() {
    this._checkQueryResult();
    return this._result.getQuerySummary();
  }

  getNextQueryResult() {
    this._checkQueryResult();
    return new QueryResult(this._result.getNextQueryResult(), false);
  }

  getNext() {
    this._checkQueryResult();
    return this._result.getNext();
  }

  getAllRows() {
    this._checkQueryResult();
    return this._result.getAsJsNestedArray();
  }

  getAllObjects() {
    this._checkQueryResult();
    return this._result.getAsJsArrayOfObjects();
  }

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
