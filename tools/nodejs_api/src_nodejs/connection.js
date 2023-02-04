const kuzu = require("../Release/kuzujs.node");
class Connection {
    #connection;
    constructor(database) {
        this.#connection = new kuzu.NodeConnection(database.database);
    }

    execute(query){
        return this.#connection.execute(query);
    }
}

module.exports = Connection
