const kuzu = require("./kuzujs.node");
const QueryResult = require("./query_result.js");

const PRIMARY_KEY_TEXT = "(PRIMARY KEY)";
const SRC_NODE_TEXT = "src node:";
const DST_NODE_TEXT = "dst node:";
const PROPERTIES_TEXT = "properties:";

class Connection {
  constructor(database) {
    if (
      typeof database !== "object" ||
      database.constructor.name !== "Database"
    ) {
      throw new Error("database must be a valid Database object.");
    }
    this._database = database;
    this._connection = null;
    this._isInitialized = false;
    this._initPromise = null;
  }

  async init() {
    if (!this._connection) {
      const database = await this._database.getDatabase();
      this._connection = new kuzu.NodeConnection(database);
    }
    if (!this._isInitialized) {
      if (!this._initPromise) {
        this._initPromise = new Promise((resolve, reject) => {
          this._connection.initAsync((err) => {
            if (err) {
              reject(err);
            } else {
              this._isInitialized = true;
              if (this._numThreads) {
                this._connection.setMaxNumThreadForExec(this._numThreads);
                delete this._numThreads;
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

  async _getConnection() {
    await this.init();
    return this._connection;
  }

  execute(preparedStatement, params = {}) {
    return new Promise((resolve, reject) => {
      if (
        !typeof preparedStatement === "object" ||
        preparedStatement.constructor.name !== "NodePreparedStatement"
      ) {
        return reject(
          new Error(
            "preparedStatement must be a valid PreparedStatement object."
          )
        );
      }
      if (!preparedStatement.isSuccess()) {
        return reject(new Error(preparedStatement.getErrorMessage()));
      }
      if (params.constructor !== Object) {
        return reject(new Error("params must be a plain object."));
      }
      const paramArray = [];
      for (const key in params) {
        const value = params[key];
        if (value === null || value === undefined) {
          return reject(
            new Error(
              "The value of each parameter must not be null or undefined."
            )
          );
        }
        if (
          typeof value === "boolean" ||
          typeof value === "number" ||
          typeof value === "string" ||
          (typeof value === "object" && value.constructor.name === "Date")
        ) {
          paramArray.push([key, value]);
        } else {
          return reject(
            new Error(
              "The value of each parameter must be a boolean, number, string, or Date object."
            )
          );
        }
      }
      this._getConnection().then((connection) => {
        const nodeQueryResult = new kuzu.NodeQueryResult();
        try {
          connection.executeAsync(
            preparedStatement,
            nodeQueryResult,
            paramArray,
            (err) => {
              if (err) {
                return reject(err);
              }
              return resolve(new QueryResult(this, nodeQueryResult));
            }
          );
        } catch (e) {
          return reject(e);
        }
      });
    });
  }

  prepare(statement) {
    return new Promise((resolve, reject) => {
      if (typeof statement !== "string") {
        return reject(new Error("statement must be a string."));
      }
      this._getConnection().then((connection) => {
        const preparedStatement = new kuzu.PreparedStatement(
          connection,
          statement
        );
        preparedStatement.initAsync((err) => {
          if (err) {
            return reject(err);
          }
          return resolve(preparedStatement);
        });
      });
    });
  }

  async query(statement) {
    if (typeof statement !== "string") {
      throw new Error("statement must be a string.");
    }
    const preparedStatement = await this.prepare(statement);
    const queryResult = await this.execute(preparedStatement);
    return queryResult;
  }

  setMaxNumThreadForExec(numThreads) {
    // If the connection is not initialized yet, store the logging level
    // and defer setting it until the connection is initialized.
    if (typeof numThreads !== "number" || !numThreads || numThreads < 0) {
      throw new Error("numThreads must be a positive number.");
    }
    if (!this.isInitialized) {
      this._connection.setMaxNumThreadForExec(numThreads);
    }
    this._numThreads = numThreads;
  }

  _parseNodeTableNames(nodeTableNames) {
    const results = [];
    nodeTableNames.split("\n").forEach((line, i) => {
      if (i === 0) {
        return;
      }
      if (line === "") {
        return;
      }
      results.push(line.trim());
    });
    return results;
  }

  getNodeTableNames() {
    return this._getConnection().then((connection) => {
      return new Promise((resolve, reject) => {
        connection.getNodeTableNamesAsync((err, result) => {
          if (err) {
            return reject(err);
          }
          return resolve(this._parseNodeTableNames(result));
        });
      });
    });
  }

  _parseRelTableNames(relTableNames) {
    const results = [];
    relTableNames.split("\n").forEach((line, i) => {
      if (i === 0) {
        return;
      }
      if (line === "") {
        return;
      }
      results.push(line.trim());
    });
    return results;
  }

  getRelTableNames() {
    return this._getConnection().then((connection) => {
      return new Promise((resolve, reject) => {
        connection.getRelTableNamesAsync((err, result) => {
          if (err) {
            return reject(err);
          }
          return resolve(this._parseRelTableNames(result));
        });
      });
    });
  }

  _parseNodePropertyNames(nodeTableNames) {
    const results = [];
    nodeTableNames.split("\n").forEach((line, i) => {
      if (i === 0) {
        return;
      }
      if (line === "") {
        return;
      }
      line = line.trim();
      let isPrimaryKey = false;
      if (line.includes(PRIMARY_KEY_TEXT)) {
        isPrimaryKey = true;
        line = line.replace(PRIMARY_KEY_TEXT, "");
      }
      const lineParts = line.split(" ");
      const name = lineParts[0];
      const type = lineParts[1];
      results.push({ name, type, isPrimaryKey });
    });
    return results;
  }

  getNodePropertyNames(tableName) {
    return this._getConnection().then((connection) => {
      return new Promise((resolve, reject) => {
        connection.getNodePropertyNamesAsync(tableName, (err, result) => {
          if (err) {
            return reject(err);
          }
          return resolve(this._parseNodePropertyNames(result));
        });
      });
    });
  }

  _parseRelPropertyNames(relTableNames) {
    const result = { props: [] };
    relTableNames.split("\n").forEach((line) => {
      line = line.trim();
      if (line === "") {
        return;
      }
      if (line.includes(SRC_NODE_TEXT)) {
        const lineParts = line.split(SRC_NODE_TEXT);
        result.src = lineParts[1].trim();
        result.name = lineParts[0].trim();
        return;
      }
      if (line.includes(DST_NODE_TEXT)) {
        const lineParts = line.split(DST_NODE_TEXT);
        result.dst = lineParts[1].trim();
        return;
      }
      if (line.includes(PROPERTIES_TEXT)) {
        return;
      }
      const lineParts = line.split(" ");
      const name = lineParts[0];
      const type = lineParts[1];
      result.props.push({ name, type });
    });
    return result;
  }

  getRelPropertyNames(tableName) {
    return this._getConnection().then((connection) => {
      return new Promise((resolve, reject) => {
        connection.getRelPropertyNamesAsync(tableName, (err, result) => {
          if (err) {
            return reject(err);
          }
          return resolve(this._parseRelPropertyNames(result));
        });
      });
    });
  }
}

module.exports = Connection;
