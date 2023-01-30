class Connection {
    #connection;
    constructor(database) {
        const kuzu = require("../build/Release/kuzujs.node");
        this.#connection = new kuzu.NodeConnection(database.database);
        console.log('Connection Created', kuzu);
    }

    execute(query){
        console.log("The query is ", query);
        return this.#connection.execute(query);
    }
}

<<<<<<< HEAD

module.exports = Connection
=======
module.exports = Connection
>>>>>>> added Database Class, all its methods, database java class, and added it to connection
