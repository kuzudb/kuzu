class Database {
    database;
    constructor(databaseConfigString, bufferSize) {
        const kuzu = require("../build/Release/kuzujs.node");
        this.database = new kuzu.NodeDatabase(databaseConfigString, bufferSize);
        console.log("The database class looks like ", this.database);
    }

    resizeBufferManager(bufferSize) {
        console.log("Trying to resize the buffer to ", bufferSize);
        this.database.resizeBufferManager(bufferSize);
    }
}

module.exports = Database