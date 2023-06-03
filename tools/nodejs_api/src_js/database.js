const kuzu = require("./kuzujs.node");
const LoggingLevel = require("./logging_level.js");

class Database {
  constructor(databasePath, bufferManagerSize = 0) {
    if (typeof databasePath !== "string") {
      throw new Error("Database path must be a string.");
    }
    if (typeof bufferManagerSize !== "number" || bufferManagerSize < 0) {
      throw new Error("Buffer manager size must be a positive integer.");
    }
    bufferManagerSize = Math.floor(bufferManagerSize);
    this._database = new kuzu.NodeDatabase(databasePath, bufferManagerSize);
    this._isInitialized = false;
    this._initPromise = null;
  }

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

  async getDatabase() {
    await this.init();
    return this._database;
  }

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
