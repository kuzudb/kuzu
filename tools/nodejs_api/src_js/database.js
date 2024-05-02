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
   * @param {String} databasePath path to the database file.
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
    if (typeof databasePath !== "string") {
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
   * Set the logging level for the database.
   */
  setLoggingLevel(loggingLevel) {
    const validLoggingLevels = Object.values(LoggingLevel);

    if (!validLoggingLevels.includes(loggingLevel)) {
      const validLoggingLevelsEnum = Object.keys(LoggingLevel)
        .map((k) => "kuzu.LoggingLevel." + k)
        .join(", ");
      throw new Error(
        `Invalid logging level: ${loggingLevel}. Valid logging levels are: ${validLoggingLevelsEnum}.`
      );
    }

    // If the database is not initialized yet, store the logging level
    // and defer setting it until the database is initialized.
    if (this._isInitialized) {
      this._database.setLoggingLevel(loggingLevel);
      return;
    }
    this._loggingLevel = loggingLevel;
  }

  /**
   * Close the database. Once the database is closed, the lock on the database 
   * files is released and the database can be opened in another process.
   * 
   * Note: Call to this method is not required. 
   * The Node.js garbage collector will automatically close the database when no 
   * references to the database object exist. It is recommended not to call this 
   * method explicitly. If you decide to manually close the database, make sure 
   * that all the QueryResult and Connection objects are closed before calling 
   * this method.
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
