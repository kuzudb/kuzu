const kuzu = require("../Release/kuzujs.node");
class Database {
    database;
    constructor(databaseConfigString, bufferSize = 0) {
        this.database = new kuzu.NodeDatabase(databaseConfigString, bufferSize);
    }

    resizeBufferManager(bufferSize) {
        console.log("Trying to resize the buffer to ", bufferSize);
    }
}

module.exports = Database