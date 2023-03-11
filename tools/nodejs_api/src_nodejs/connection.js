const kuzu = require("../Release/kuzujs.node");
const QueryResult = require("./queryResult.js");

class Connection {
    #connection;
    constructor(database, numThreads = 0) {
        if (typeof database !== "object" || database.constructor.name !==  "Database" || typeof numThreads !== "number" || !Number.isInteger(numThreads)){
            throw new Error("Connection constructor requires a database object and optional numThreads integer as argument(s)");
        }
        this.#connection = new kuzu.NodeConnection(database.database, numThreads);
    }

    execute(query, opts = {}) {
        if (typeof opts !== "object") {
            throw new Error("optional opts in execute must be an object");
        }

        let params = [];
        if ('params' in opts) {
            params = opts['params'];
        }
        if (typeof query !== "string" || !Array.isArray(params)) {
            throw new Error("execute takes a string query and optional parameter array");
        }

        const nodeQueryResult = new kuzu.NodeQueryResult();
        if ('callback' in opts) {
            const callback = opts['callback'];
            if (typeof callback !== "function" || callback.length != 2){
                throw new Error("if execute is given a callback that takes 2 arguments");
            }
            this.#connection.execute(query, err => {
                const queryResult = new QueryResult(nodeQueryResult);
                callback(err, queryResult);
            }, nodeQueryResult, params);
        } else {
            return new Promise ((resolve, reject) => {
                this.#connection.execute(query, err => {
                    if (err) { return reject(err); }
                    return resolve(new QueryResult(nodeQueryResult));
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
