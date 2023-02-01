const kuzu = require("../build/Release/kuzujs.node");
class Connection {
    #connection;
    constructor(database) {
        this.#connection = new kuzu.NodeConnection(database.database);
    }

    execute(query){
        return this.#connection.execute(query);
    }
}

<<<<<<< HEAD

module.exports = Connection
=======
module.exports = Connection
>>>>>>> added Database Class, all its methods, database java class, and added it to connection
