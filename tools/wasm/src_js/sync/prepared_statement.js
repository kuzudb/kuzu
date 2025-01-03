"use strict";

const KuzuWasm = require("./kuzu.js");

class PreparedStatement {
  constructor(_preparedStatement) {
    this._statement = _preparedStatement;
    this._isClosed = false;
  }

  _checkPreparedStatement() {
    KuzuWasm.checkInit();
    if (this._isClosed) {
      throw new Error("PreparedStatement is already closed.");
    }
  }

  isSuccess() {
    this._checkPreparedStatement();
    return this._statement.isSuccess();
  }

  getErrorMessage() {
    this._checkPreparedStatement();
    return this._statement.getErrorMessage();
  }

  close() {
    if (!this._isClosed) {
      this._statement.delete();
      this._isClosed = true;
    }
  }
}

module.exports = PreparedStatement;
