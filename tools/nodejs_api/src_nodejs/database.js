const kuzu = require("../Release/kuzujs.node");
class Database {
    database;
    constructor(databasePath, bufferSize = 0) {
        this.database = new kuzu.NodeDatabase(databasePath, bufferSize);
    }

    resizeBufferManager(bufferSize) {
        this.database.resizeBufferManager(bufferSize);
    }
}

module.exports = Database
