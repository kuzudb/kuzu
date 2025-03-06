/**
 * @file database.js is the file for the Database class. Database class is the 
 * main class of Kuzu. It manages all database components.
 */
"use strict";

const dispatcher = require("./dispatcher.js");

class Database {
  /**
   * Initialize a new Database object. Note that the initialization is done
   * lazily, so the database file is not opened until the first query is
   * executed. To initialize the database immediately, call the `init()`
   * function on the returned object.
   *
   * @param {String} databasePath path to the database file. If the path is not 
   * specified, or empty, or equal to `:memory:`, the database will be created 
   * in memory.
   * @param {Number} bufferManagerSize size of the buffer manager in bytes.
   * @param {Boolean} enableCompression whether to enable compression.
   * @param {Boolean} readOnly if true, database will be opened in read-only 
   * mode.
   * @param {Boolean} autoCheckpoint if true, automatic checkpointing will be 
   * enabled.
   * @param {Number} checkpointThreshold threshold for automatic checkpointing 
   * in bytes. Default is 16MB.
   */
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

  /**
   * Initialize the database. Calling this function is optional, as the
   * database is initialized automatically when the first query is executed.
   */
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

  /**
   * Internal function to get the database object ID.
   * @private
   * @throws {Error} if the database is closed.
   */
  async _getDatabaseObjectId() {
    if (this._isClosed) {
      throw new Error("Database is closed.");
    }
    if (!this._isInitialized) {
      await this.init();
    }
    return this._id;
  }

  /**
   * Close the database.
   */
  async close() {
    if (!this._isClosed) {
      const worker = await dispatcher.getWorker();
      await worker.databaseClose(this._id);
      this._isClosed = true;
    }
  }
}

module.exports = Database;
