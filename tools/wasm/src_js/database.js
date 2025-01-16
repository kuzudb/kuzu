"use strict";

const dispatcher = require("./dispatcher.js");

class Database {
  constructor(
    databasePath,
    bufferPoolSize = 0,
    maxNumThreads = 0,
    enableCompression = true,
    readOnly = false,
    autoCheckpoint = true,
    checkpointThreshold = 16777216
  ) {
    this._isInitialized = false;
    this._initPromise = null;
    this._id = null;
    this._isClosed = false;
    this.databasePath = databasePath;
    this.bufferPoolSize = bufferPoolSize;
    this.maxNumThreads = maxNumThreads;
    this.enableCompression = enableCompression;
    this.readOnly = readOnly;
    this.autoCheckpoint = autoCheckpoint;
    this.checkpointThreshold = checkpointThreshold;
  }

  async init() {
    if (!this._isInitialized) {
      if (!this._initPromise) {
        this._initPromise = (async () => {
          const worker = await dispatcher.getWorker();
          const res = await worker.databaseConstruct(
            this.databasePath,
            this.bufferPoolSize,
            this.maxNumThreads,
            this.enableCompression,
            this.readOnly,
            this.autoCheckpoint,
            this.checkpointThreshold
          );
          if (res.isSuccess) {
            this._id = res.id;
            this._isInitialized = true;
          } else {
            throw new Error(res.error);
          }
        })();
      }
    }
    await this._initPromise;
    this._initPromise = null;
  }

  async _getDatabaseObjectId() {
    if (this._isClosed) {
      throw new Error("Database is closed.");
    }
    if (!this._isInitialized) {
      await this.init();
    }
    return this._id;
  }

  async close() {
    if (!this._isClosed) {
      const worker = await dispatcher.getWorker();
      await worker.databaseClose(this._id);
      this._isClosed = true;
    }
  }
}

module.exports = Database;
