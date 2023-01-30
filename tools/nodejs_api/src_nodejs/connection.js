const kuzu = require("../build/Release/kuzujs.node");
class Connection {
    #connection;
    constructor(database) {
        this.#connection = new kuzu.NodeConnection(database.database);
        console.log('Connection Created', kuzu);
    }

    execute(query){
        console.log("The query is ", query);
        return this.#connection.execute(query);
    }
}

module.exports = Connection