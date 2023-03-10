const kuzu = require("../Release/kuzujs.node");
const QueryResult = require("./queryResult.js");

class Connection {
    #connection;
    constructor(database, numThreads = 0) {
        this.#connection = new kuzu.NodeConnection(database.database, numThreads);
    }

    execute(query, callback=null, params=[]){
        const nodeQueryResult = new kuzu.NodeQueryResult();
        if (callback) {
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
        this.#connection.setMaxNumThreadForExec(numThreads);
    }

    getNodePropertyNames(tableName) {
        return this.#connection.getNodePropertyNames(tableName);
    }

}

module.exports = Connection
