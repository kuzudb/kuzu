"use strict";

const dispatcher = require("./dispatcher.js");

class PreparedStatement {
  constructor(id) {
    this._id = id;
    this._isClosed = false;
    this._isSuccess = undefined;
  }

  async _syncValues() {
    if (this._isClosed) {
      return;
    }
    const worker = await dispatcher.getWorker();
    let res = await worker.preparedStatementIsSuccess(this._id);
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
    const res = await worker.preparedStatementGetErrorMessage(this._id);
    if (res.isSuccess) {
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }

  async close() {
    if (!this._isClosed) {
      const worker = await dispatcher.getWorker();
      await worker.preparedStatementClose(this._id);
      this._isClosed = true;
    }
  }
}

module.exports = PreparedStatement;
