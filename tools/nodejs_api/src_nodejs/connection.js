const kuzu = require("../Release/kuzujs.node");
const QueryResult = require("./queryResult.js");

class Connection {
    #connection;
    #database;
    #initialized;
    constructor(database, numThreads = 0) {
        if (typeof database !== "object" || database.constructor.name !== "Database"){
            throw new Error("Connection constructor requires a valid database object");
        } else if (typeof numThreads !== "number" || !Number.isInteger(numThreads)) {
            throw new Error("numThreads is not a valid integer in the Connection Constructor");
        }
        this.#database = database;
        this.#connection = new kuzu.NodeConnection(database.database, numThreads);
        this.#initialized = false;
    }

    async getConnection() {
        if (this.#initialized) {
            return this.#connection;
        }
        const promiseResult = await this.#connection.getConnection();
        if (promiseResult) {
            this.#connection.transferConnection();
            this.#initialized = true;
        } else {
            throw new Error("GetConnection Failed");
        }
        return this.#connection;
    }

    async execute(query, opts = {}) {
        const connection = await this.getConnection();
        const optsKeys = Object.keys(opts);
        if (typeof opts !== "object") {
            throw new Error("optional opts in execute must be an object");
        } else if (optsKeys.length > 2) {
            throw new Error("opts can only have optional fields 'callback' and/or 'params'");
        }

        const validSet = new Set(optsKeys.concat(['callback', 'params']));
        if (validSet.size > 2) {
            throw new Error("opts has at least 1 invalid field: it can only have optional fields 'callback' and/or 'params'");
        }

        let params = [];
        if ('params' in opts) {
            params = opts['params'];
        }
        if (typeof query !== "string" || !Array.isArray(params)) {
            throw new Error("execute takes a string query and optional parameter array");
        }

        const nodeQueryResult = new kuzu.NodeQueryResult();
        const queryResult = new QueryResult(this, nodeQueryResult);
        if ('callback' in opts) {
            const callback = opts['callback'];
            if (typeof callback !== "function" || callback.length > 2) {
                throw new Error("if execute is given a callback, it must at most take 2 arguments: (err, result)");
            }
            connection.execute(query, err => {
                callback(err, queryResult);
            }, nodeQueryResult, params);
        } else {
            return new Promise((resolve, reject) => {
                connection.execute(query, err => {
                    if (err) {
                        return reject(err);
                    }
                    return resolve(queryResult);
                }, nodeQueryResult, params);
            })
        }
    }

    async setMaxNumThreadForExec(numThreads) {
        const connection = await this.getConnection();
        if (typeof numThreads !== "number" || !Number.isInteger(numThreads)) {
            throw new Error("setMaxNumThreadForExec needs an integer numThreads as an argument");
        }
        connection.setMaxNumThreadForExec(numThreads);
    }

    async getNodePropertyNames(tableName) {
        const connection = await this.getConnection();
        if (typeof tableName !== "string") {
            throw new Error("getNodePropertyNames needs a string tableName as an argument");
        }
        return connection.getNodePropertyNames(tableName);
    }

}

module.exports = Connection
