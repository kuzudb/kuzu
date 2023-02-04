const kuzu = require("../Release/kuzujs.node");
class Database {
    database;
    constructor(databaseConfigString, bufferSize = 0) {
        this.database = new kuzu.NodeDatabase(databaseConfigString, bufferSize);
    }

    resizeBufferManager(bufferSize) {
        this.database.resizeBufferManager(bufferSize);
    }
}

module.exports = Database
