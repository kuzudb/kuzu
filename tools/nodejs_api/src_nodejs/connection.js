const kuzu = require("../Release/kuzujs.node");
const QueryResult = require("./queryResult.js");

class Connection {
    #connection;
    #database;
    constructor(database, numThreads = 0) {
        if (typeof database !== "object" || database.constructor.name !==  "Database" || typeof numThreads !== "number" || !Number.isInteger(numThreads)){
            throw new Error("Connection constructor requires a database object and optional numThreads integer as argument(s)");
        }
        this.#database = database;
        this.#connection = new kuzu.NodeConnection(database.database, numThreads);
    }

    execute(query, opts = {}) {
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
            if (typeof callback !== "function" || callback.length !== 2){
                throw new Error("if execute is given a callback, it must take 2 arguments: (err, result)");
            }
            this.#connection.execute(query, err => {
                callback(err, queryResult);
            }, nodeQueryResult, params);
        } else {
            return new Promise ((resolve, reject) => {
                this.#connection.execute(query, err => {
                    if (err) { return reject(err); }
                    return resolve(queryResult);
                }, nodeQueryResult, params);
            })
        }
    }

    setMaxNumThreadForExec(numThreads) {
        if (typeof numThreads !== "number" || !Number.isInteger(numThreads)){
            throw new Error("setMaxNumThreadForExec needs an integer numThreads as an argument");
        }
        this.#connection.setMaxNumThreadForExec(numThreads);
    }

    getNodePropertyNames(tableName) {
        if (typeof tableName !== "string"){
            throw new Error("getNodePropertyNames needs a string tableName as an argument");
        }
        return this.#connection.getNodePropertyNames(tableName);
    }

}

module.exports = Connection
