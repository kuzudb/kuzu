const kuzu = require("../Release/kuzujs.node");
class Connection {
    #connection;
    constructor(database, numThreads = 0) {
        this.#connection = new kuzu.NodeConnection(database.database, numThreads);
    }

    execute(query){
        return this.#connection.execute(query);
    }

    setMaxNumThreadForExec(numThreads) {
        this.#connection.setMaxNumThreadForExec(numThreads);
    }

    getNodePropertyNames(tableName) {
        return this.#connection.getNodePropertyNames(tableName);
    }

}

module.exports = Connection
