"use strict";

const KuzuNative = require("./kuzu_native.js");
const LoggingLevel = require("./logging_level.js");

class Database {
  /**
   * Initialize a new Database object. Note that the initialization is done
   * lazily, so the database file is not opened until the first query is
   * executed. To initialize the database immediately, call the `init()`
   * function on the returned object.
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
    bufferManagerSize = 0,
    enableCompression = true,
    readOnly = false,
    maxDBSize = 0
  ) {
    if (!databasePath) {
      databasePath = ":memory:";
    }
    else if (typeof databasePath !== "string") {
      throw new Error("Database path must be a string.");
    }
    if (typeof bufferManagerSize !== "number" || bufferManagerSize < 0) {
      throw new Error("Buffer manager size must be a positive integer.");
    }
    if (typeof maxDBSize !== "number" || maxDBSize < 0) {
      throw new Error("Max DB size must be a positive integer.");
    }
    bufferManagerSize = Math.floor(bufferManagerSize);
    maxDBSize = Math.floor(maxDBSize);
    this._database = new KuzuNative.NodeDatabase(
      databasePath,
      bufferManagerSize,
      enableCompression,
      readOnly,
      maxDBSize
    );
    this._isInitialized = false;
    this._initPromise = null;
    this._isClosed = false;
  }

  /**
   * Get the version of the library.
   * @returns {String} the version of the library.
   */
  static getVersion() {
    return KuzuNative.NodeDatabase.getVersion();
  }

  /**
   * Get the storage version of the library.
   * @returns {Number} the storage version of the library.
   */
  static getStorageVersion() {
    return KuzuNative.NodeDatabase.getStorageVersion();
  }

  /**
   * Initialize the database. Calling this function is optional, as the
   * database is initialized automatically when the first query is executed.
   */
  async init() {
    if (!this._isInitialized) {
      if (!this._initPromise) {
        this._initPromise = new Promise((resolve, reject) => {
          this._database.initAsync((err) => {
            if (err) {
              reject(err);
            } else {
              try {
                this._isInitialized = true;
                if (this._loggingLevel) {
                  this.setLoggingLevel(this._loggingLevel);
                  delete this._loggingLevel;
                }
              } catch (e) {
                return reject(e);
              }
              resolve();
            }
          });
        });
      }
      await this._initPromise;
      this._initPromise = null;
    }
  }

  /**
   * Internal function to get the underlying native database object.
   * @returns {KuzuNative.NodeDatabase} the underlying native database.
   */
  async _getDatabase() {
    if (this._isClosed) {
      throw new Error("Database is closed.");
    }
    await this.init();
    return this._database;
  }

  /**
   * Close the database.
   */
  async close() {
    if (this._isClosed) {
      return;
    }
    if (!this._isInitialized) {
      if (this._initPromise) {
        // Database is initializing, wait for it to finish first.
        await this._initPromise;
      } else {
        // Database is not initialized, simply mark it as closed and initialized.
        this._isInitialized = true;
        this._isClosed = true;
        delete this._database;
        return;
      }
    }
    // Database is initialized, close it.
    this._database.close();
    delete this._database;
    this._isClosed = true;
  }
}

module.exports = Database;
