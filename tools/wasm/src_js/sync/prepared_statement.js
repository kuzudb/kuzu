/**
 * @file prepared_statement.js is the file for the PreparedStatement class. 
 * A prepared statement is a parameterized query which can avoid planning the 
 * same query for repeated execution.
 */
"use strict";

const KuzuWasm = require("./kuzu.js");

class PreparedStatement {
  /**
   * Internal constructor. Use `Connection.prepare` to get a
   * `PreparedStatement` object.
   * @param {kuzu.sync.PreparedStatement} _preparedStatement the native prepared 
   * statement object.
   */
  constructor(_preparedStatement) {
    this._statement = _preparedStatement;
    this._isClosed = false;
  }

  /**
   * Internal function to check if the prepared statement is closed.
   * @throws {Error} if the prepared statement is closed.
   * @private
   */
  _checkPreparedStatement() {
    KuzuWasm.checkInit();
    if (this._isClosed) {
      throw new Error("PreparedStatement is already closed.");
    }
  }

  /**
   * Check if the prepared statement is successfully prepared.
   * @returns {Boolean} true if the prepared statement is successfully prepared.
   * @throws {Error} if the prepared statement is closed.
   */
  isSuccess() {
    this._checkPreparedStatement();
    return this._statement.isSuccess();
  }

  /**
   * Get the error message if the prepared statement is not successfully 
   * prepared.
   * @returns {String} the error message.
   * @throws {Error} if the prepared statement is closed.
   */
  getErrorMessage() {
    this._checkPreparedStatement();
    return this._statement.getErrorMessage();
  }

  /**
   * Close the prepared statement.
   * @throws {Error} if the prepared statement is closed.
   */
  close() {
    if (!this._isClosed) {
      this._statement.delete();
      this._isClosed = true;
    }
  }
}

module.exports = PreparedStatement;
