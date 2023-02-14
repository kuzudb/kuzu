const kuzu = require("../Release/kuzujs.node");
const QueryResult = require("./queryResult.js");

class Connection {
    #connection;
    constructor(database, numThreads = 0) {
        this.#connection = new kuzu.NodeConnection(database.database, numThreads);
    }

    execute(query){
        this.#connection.execute(query);
    }

    execute(query, callback){
        let queryResult = this.#connection.execute(query);
        queryResult = new QueryResult(queryResult);
        return callback(queryResult);
    }
    setMaxNumThreadForExec(numThreads) {
        this.#connection.setMaxNumThreadForExec(numThreads);
    }

    getNodePropertyNames(tableName) {
        return this.#connection.getNodePropertyNames(tableName);
    }

}

module.exports = Connection
