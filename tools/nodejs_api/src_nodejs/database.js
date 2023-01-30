<<<<<<< HEAD
const kuzu = require("../build/Release/kuzujs.node");
class Database {
    database;
    constructor(databaseConfigString, bufferSize) {
=======
class Database {
    database;
    constructor(databaseConfigString, bufferSize) {
        const kuzu = require("../build/Release/kuzujs.node");
>>>>>>> added Database Class, all its methods, database java class, and added it to connection
        this.database = new kuzu.NodeDatabase(databaseConfigString, bufferSize);
        console.log("The database class looks like ", this.database);
    }

    resizeBufferManager(bufferSize) {
        console.log("Trying to resize the buffer to ", bufferSize);
        this.database.resizeBufferManager(bufferSize);
    }
}

module.exports = Database