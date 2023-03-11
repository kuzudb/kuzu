const kuzu = require("../Release/kuzujs.node");
class Database {
    database;
    constructor(databasePath, bufferManagerSize = 0) {
        if (typeof databasePath !== "string" || typeof bufferManagerSize !== "number" || !Number.isInteger(bufferManagerSize)){
            throw new Error("Database constructor requires a databasePath string and optional bufferManagerSize integer as argument(s)");
        }
        this.database = new kuzu.NodeDatabase(databasePath, bufferManagerSize);
    }

    resizeBufferManager(bufferManagerSize) {
        if (typeof bufferManagerSize !== "number" || !Number.isInteger(bufferManagerSize)){
            throw new Error("resizeBufferManager needs an integer bufferManagerSize as an argument");
        }
        this.database.resizeBufferManager(bufferManagerSize);
    }
}

module.exports = Database
