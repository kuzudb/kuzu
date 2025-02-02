/**
 * @file prepared_statement.js is the file for the PreparedStatement class. 
 * A prepared statement is a parameterized query which can avoid planning the 
 * same query for repeated execution.
 */
"use strict";

const dispatcher = require("./dispatcher.js");

class PreparedStatement {
  /**
   * Internal constructor. Use `Connection.prepare` to get a
   * `PreparedStatement` object.
   * @param {String} id the internal ID of the prepared statement object.
   * statement object.
   */
  constructor(id) {
    this._id = id;
    this._isClosed = false;
    this._isSuccess = undefined;
  }

  /**
   * Internal function to update the local fields with the values from the 
   * worker.
   * @private
   * @throws {Error} if the prepared statement is closed.
   */
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

  /**
   * Check if the prepared statement is successfully prepared.
   * @returns {Boolean} true if the prepared statement is successfully 
   * prepared.
   */
  isSuccess() {
    return this._isSuccess;
  }

  /**
   * Get the error message if the prepared statement is not successfully 
   * prepared.
   * @returns {String} the error message.
   */
  async getErrorMessage() {
    const worker = await dispatcher.getWorker();
    const res = await worker.preparedStatementGetErrorMessage(this._id);
    if (res.isSuccess) {
      return res.result;
    } else {
      throw new Error(res.error);
    }
  }

  /**
   * Close the prepared statement.
   * @throws {Error} if the prepared statement is closed.
   */
  async close() {
    if (!this._isClosed) {
      const worker = await dispatcher.getWorker();
      await worker.preparedStatementClose(this._id);
      this._isClosed = true;
    }
  }
}

module.exports = PreparedStatement;
