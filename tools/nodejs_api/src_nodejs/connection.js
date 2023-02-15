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
        this.#connection.execute(query, (err, queryResult) => {
            console.log(err, queryResult);
            if (err){
                console.log(err);
                throw err;
            } else {
                const queryResultJs = new QueryResult(queryResult);
                callback(queryResultJs);
            }
        });
    }

    setMaxNumThreadForExec(numThreads) {
        this.#connection.setMaxNumThreadForExec(numThreads);
    }

    getNodePropertyNames(tableName) {
        return this.#connection.getNodePropertyNames(tableName);
    }

}

module.exports = Connection
