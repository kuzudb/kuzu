const kuzu = require("../Release/kuzujs.node");
const QueryResult = require("./queryResult.js");
const callbackWrapper = require("./common.js");

class Connection {
    #connection;
    constructor(database, numThreads = 0) {
        this.#connection = new kuzu.NodeConnection(database.database, numThreads);
    }

    execute(query, callback=null){
        if (callback) {
            this.#connection.execute(query, (err, queryResult) => {
                callbackWrapper(err, () => {
                        const queryResultJs = new QueryResult(queryResult);
                        callback(queryResultJs);
                });
            });
        } else {
            return new Promise ((resolve, reject) => {
                this.#connection.execute(query, (err, queryResult) => {
                    if (err) { return reject(err); }
                    return resolve(new QueryResult(queryResult));
                })
            })
        }
    }

    setMaxNumThreadForExec(numThreads) {
        this.#connection.setMaxNumThreadForExec(numThreads);
    }

    getNodePropertyNames(tableName) {
        return this.#connection.getNodePropertyNames(tableName);
    }

}

module.exports = Connection
