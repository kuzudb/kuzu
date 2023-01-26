class Connection {
    #connection;
    constructor(databaseConfigString) {
        const kuzu = require("../build/Release/kuzujs.node");
        this.#connection = new kuzu.NodeConnection(databaseConfigString);
        console.log('Connection Created', kuzu);
    }

    execute(query){
        console.log("The query is ", query);
        return this.#connection.execute(query);
    }
}


module.exports = Connection
