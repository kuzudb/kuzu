"use strict";

const KuzuNative = require("./kuzujs.node");
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
   */
  constructor(databasePath, bufferManagerSize = 0) {
    if (typeof databasePath !== "string") {
      throw new Error("Database path must be a string.");
    }
    if (typeof bufferManagerSize !== "number" || bufferManagerSize < 0) {
      throw new Error("Buffer manager size must be a positive integer.");
    }
    bufferManagerSize = Math.floor(bufferManagerSize);
    this._database = new KuzuNative.NodeDatabase(databasePath, bufferManagerSize);
    this._isInitialized = false;
    this._initPromise = null;
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
   * @private Internal function to get the underlying native database object.
   * @returns {KuzuNative.NodeDatabase} the underlying native database.
   */
  async _getDatabase() {
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
}

module.exports = Database;
