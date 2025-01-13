"use strict";

const dispatcher = require("./dispatcher.js");
const QueryResult = require("./query_result.js");
const PreparedStatement = require("./prepared_statement.js");

class Connection {
  constructor(database, numThreads = null) {
    this._isInitialized = false;
    this._initPromise = null;
    this._id = null;
    this._isClosed = false;
    this._database = database;
    this.numThreads = numThreads;
  }

  async init() {
    if (!this._isInitialized) {
      if (!this._initPromise) {
        this._initPromise = (async () => {
          const databaseId = await this._database._getDatabaseObjectId();
          const worker = await dispatcher.getWorker();
          const res = await worker.connectionConstruct(databaseId, this.numThreads);
          if (res.isSuccess) {
            this._id = res.id;
            this._isInitialized = true;
          } else {
            throw new Error(res.error);
          }
        })();
      }
      await this._initPromise;
      this._initPromise = null;
    }
  }

  async _getConnectionObjectId() {
    if (this._isClosed) {
      throw new Error("Connection is closed.");
    }
    if (!this._isInitialized) {
      await this.init();
    }
    return this._id;
  }

  async setMaxNumThreadForExec(numThreads) {
    const connectionId = await this._getConnectionObjectId();
    const worker = await dispatcher.getWorker();
    const res = await worker.connectionSetMaxNumThreadForExec(connectionId, numThreads);
    if (!res.isSuccess) {
      throw new Error(res.error);
    }
  }

  async setQueryTimeout(timeout) {
    const connectionId = await this._getConnectionObjectId();
    const worker = await dispatcher.getWorker();
    const res = await worker.connectionSetQueryTimeout(connectionId, timeout);
    if (!res.isSuccess) {
      throw new Error(res.error);
    }
  }

  async getMaxNumThreadForExec() {
    const connectionId = await this._getConnectionObjectId();
    const worker = await dispatcher.getWorker();
    const res = await worker.connectionGetMaxNumThreadForExec(connectionId);
    if (!res.isSuccess) {
      throw new Error(res.error);
    }
    return res.numThreads;
  }

  async query(statement) {
    const connectionId = await this._getConnectionObjectId();
    const worker = await dispatcher.getWorker();
    const res = await worker.connectionQuery(connectionId, statement);
    if (!res.isSuccess) {
      throw new Error(res.error);
    }
    const queryResult = new QueryResult(res.id);
    await queryResult._syncValues();
    return queryResult;
  }

  async prepare(statement) {
    const connectionId = await this._getConnectionObjectId();
    const worker = await dispatcher.getWorker();
    const res = await worker.connectionPrepare(connectionId, statement);
    if (!res.isSuccess) {
      throw new Error(res.error);
    }
    const preparedStatement = new PreparedStatement(res.id);
    await preparedStatement._syncValues();
    return preparedStatement;
  }

  async execute(preparedStatement, params = {}) {
    const connectionId = await this._getConnectionObjectId();
    const worker = await dispatcher.getWorker();
    const res = await worker.connectionExecute(connectionId, preparedStatement._id, params);
    if (!res.isSuccess) {
      throw new Error(res.error);
    }
    const queryResult = new QueryResult(res.id);
    await queryResult._syncValues();
    return queryResult;
  }

  async close() {
    if (!this._isClosed) {
      const connectionId = await this._getConnectionObjectId();
      const worker = await dispatcher.getWorker();
      await worker.connectionClose(connectionId);
      this._isClosed = true;
    }
  }
}

module.exports = Connection;
