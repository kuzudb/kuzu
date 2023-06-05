"use strict";

const KuzuNative = require("./kuzujs.node");
const QueryResult = require("./query_result.js");
const PreparedStatement = require("./prepared_statement.js");

const PRIMARY_KEY_TEXT = "(PRIMARY KEY)";
const SRC_NODE_TEXT = "src node:";
const DST_NODE_TEXT = "dst node:";
const PROPERTIES_TEXT = "properties:";

class Connection {
  /**
   * Initialize a new Connection object. Note that the initialization is done
   * lazily, so the connection is not initialized until the first query is
   * executed. To initialize the connection immediately, call the `init()`
   * function on the returned object.
   *
   * @param {kuzu.Database} database the database object to connect to.
   */
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

  /**
   * Initialize the connection. Calling this function is optional, as the
   * connection is initialized automatically when the first query is executed.
   */
  async init() {
    if (!this._connection) {
      const database = await this._database._getDatabase();
      this._connection = new KuzuNative.NodeConnection(database);
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

  /**
   * @private Internal function to get the underlying native connection object.
   * @returns {KuzuNative.NodeConnection} the underlying native connection.
   */
  async _getConnection() {
    await this.init();
    return this._connection;
  }

  /**
   * Execute a prepared statement with the given parameters.
   * @param {kuzu.PreparedStatement} preparedStatement the prepared statement to execute.
   * @param {Object} params a plain object mapping parameter names to values.
   * @returns {Promise<kuzu.QueryResult>} a promise that resolves to the query result. The promise is rejected if there is an error.
   */
  execute(preparedStatement, params = {}) {
    return new Promise((resolve, reject) => {
      if (
        !typeof preparedStatement === "object" ||
        preparedStatement.constructor.name !== "PreparedStatement"
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
        const nodeQueryResult = new KuzuNative.NodeQueryResult();
        try {
          connection.executeAsync(
            preparedStatement._preparedStatement,
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

  /**
   * Prepare a statement for execution.
   * @param {String} statement the statement to prepare.
   * @returns {Promise<kuzu.PreparedStatement>} a promise that resolves to the prepared statement. The promise is rejected if there is an error.
   */
  prepare(statement) {
    return new Promise((resolve, reject) => {
      if (typeof statement !== "string") {
        return reject(new Error("statement must be a string."));
      }
      this._getConnection().then((connection) => {
        const preparedStatement = new KuzuNative.NodePreparedStatement(
          connection,
          statement
        );
        preparedStatement.initAsync((err) => {
          if (err) {
            return reject(err);
          }
          return resolve(new PreparedStatement(this, preparedStatement));
        });
      });
    });
  }

  /**
   * Execute a query.
   * @param {String} statement the statement to execute.
   * @returns {Promise<kuzu.QueryResult>} a promise that resolves to the query result. The promise is rejected if there is an error.
   */
  async query(statement) {
    if (typeof statement !== "string") {
      throw new Error("statement must be a string.");
    }
    const preparedStatement = await this.prepare(statement);
    const queryResult = await this.execute(preparedStatement);
    return queryResult;
  }

  /**
   * Set the maximum number of threads to use for query execution.
   * @param {Number} numThreads the maximum number of threads to use for query execution.
   */
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

  /**
   * @private Internal function to parse the result for `getNodeTableNames()`.
   * @param {String} nodeTableNames the result from `getNodeTableNames()`.
   * @returns {Array<String>} an array of table names.
   */
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

  /**
   * Get the names of all node tables in the database.
   * @returns {Promise<Array<String>>} a promise that resolves to an array of table names. The promise is rejected if there is an error.
   */
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

  /**
   * @private Internal function to parse the result for `getRelTableNames()`.
   * @param {String} relTableNames the result from `getRelTableNames()`.
   * @returns {Array<String>} an array of table names.
   */
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

  /**
   * Get the names of all relationship tables in the database.
   * @returns {Promise<Array<String>>} a promise that resolves to an array of table names. The promise is rejected if there is an error.
   */
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

  /**
   * @private Internal function to parse the result for `getNodePropertyNames()`.
   * @param {String} nodeTableNames the result from `getNodePropertyNames()`.
   * @returns {Array<Object>} an array of property names.
   */
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

  /**
   * Get the names and types of all properties in the given node table. Each
   * property is represented as an object with the following properties:
   * - `name`: the name of the property.
   * - `type`: the type of the property.
   * - `isPrimaryKey`: whether the property is a primary key.
   * @param {String} tableName the name of the node table.
   * @returns {Promise<Array<Object>>} a promise that resolves to an array of property names. The promise is rejected if there is an error.
   */
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

  /**
   * @private Internal function to parse the result for `getRelPropertyNames()`.
   * @param {String} relTableNames the result from `getRelPropertyNames()`.
   * @returns {Object} an object representing the properties of the rel table.
   */
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

  /**
   * Get the names and types of all properties in the given rel table.
   * The result is an object with the following properties:
   * - `name`: the name of the rel table.
   * - `src`: the name of the source node table.
   * - `dst`: the name of the destination node table.
   * - `props`: an array of property names and types.
   * @param {String} tableName the name of the rel table.
   * @returns {Promise<Object>} a promise that resolves to an object representing the properties of the rel table. The promise is rejected if there is an error.
   */
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
