/**
 * @file database.js is the file for the Database class. Database class is the 
 * main class of Kuzu. It manages all database components.
 */
"use strict";

const KuzuWasm = require("./kuzu.js");

class Database {
  /**
   * Initialize a new Database object.
   *
   * @param {String} databasePath path to the database file. If the path is not 
   * specified, or empty, or equal to`:memory:`, the database will be created in 
   * memory.
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
    KuzuWasm.checkInit();
    const kuzu = KuzuWasm._kuzu;
    if (!databasePath) {
      databasePath = ":memory:";
    }
    else if (typeof databasePath !== "string") {
      throw new Error("Database path must be a string.");
    }
    if (typeof bufferPoolSize !== "number" || bufferPoolSize < 0) {
      throw new Error("Buffer manager size must be a positive integer.");
    }
    if (typeof maxNumThreads !== "number" || maxNumThreads < 0) {
      throw new Error("Max number of threads must be a positive integer.");
    }
    if (typeof checkpointThreshold !== "number" || checkpointThreshold < 0) {
      throw new Error("Checkpoint threshold must be a positive integer.");
    }
    bufferPoolSize = Math.floor(bufferPoolSize);
    const defaultSystemConfig = new kuzu.SystemConfig();
    const systemConfig = new kuzu.SystemConfig(
      bufferPoolSize,
      maxNumThreads,
      !!enableCompression,
      !!readOnly,
      defaultSystemConfig.maxDBSize,
      !!autoCheckpoint,
      checkpointThreshold
    );
    defaultSystemConfig.delete();
    try {
      this._database = new kuzu.Database(databasePath, systemConfig);
    } finally {
      systemConfig.delete();
    }
    this._isClosed = false;
  }

  /**
   * Close the database.
   */
  close() {
    if (!this._isClosed) {
      this._database.delete();
      this._isClosed = true;
    }
  }
}

module.exports = Database;
