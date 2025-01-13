"use strict";

const KuzuWasm = require("./kuzu.js");

class Database {
  /**
 * Initialize a new Database object.
 *
 * @param {String} databasePath path to the database file. If the path is not specified, or empty, or equal to
 `:memory:`, the database will be created in memory.
 * @param {Number} bufferManagerSize size of the buffer manager in bytes.
 * @param {Boolean} enableCompression whether to enable compression.
 * @param {Boolean} readOnly if true, database will be opened in read-only mode.
 * @param {Number} maxDBSize maximum size of the database file in bytes. Note that
 * this is introduced temporarily for now to get around with the default 8TB mmap
 * address space limit some environment.
 */
  constructor(
    databasePath,
    bufferPoolSize = 0,
    maxNumThreads = 0,
    enableCompression = true,
    readOnly = false,
    maxDBSize = 0
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
    if (typeof maxDBSize !== "number" || maxDBSize < 0) {
      throw new Error("Max DB size must be a positive integer.");
    }
    const systemConfig = new kuzu.SystemConfig();
    bufferPoolSize = Math.floor(bufferPoolSize);
    maxDBSize = Math.floor(maxDBSize);
    if (bufferPoolSize > 0) {
      systemConfig.bufferPoolSize = bufferPoolSize;
    }
    if (maxNumThreads > 0) {
      systemConfig.maxNumThreads = maxNumThreads;
    }
    systemConfig.enableCompression = enableCompression;
    systemConfig.readOnly = readOnly;
    if (maxDBSize > 0) {
      systemConfig.maxDBSize = maxDBSize;
    }
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
