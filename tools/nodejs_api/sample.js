// Make sure the test directory is removed as it will be recreated
const fs = require("fs");
const {Database, Connection} = require("./build/kuzu");

try {
  fs.rmSync("./test", { recursive: true });
} catch (e) {
  // ignore
}

// Basic Case
const database = new Database("test", 1000000000);
console.log("The database looks like: ", database);
const connection = new Connection(database);
console.log ("The connection looks like: ", connection);
connection.execute("create node table person (ID INt64, fName StRING, gender INT64, isStudent BoOLEAN, isWorker BOOLEAN, age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration interval, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], PRIMARY KEY (ID));");
connection.execute("COPY person FROM \"../../dataset/tinysnb/vPerson.csv\" (HEADER=true);");
let queryResult = connection.execute("MATCH (a:person) RETURN a.fName, a.age, a.eyeSight, a.isStudent;");
console.log(queryResult);

// // Extensive Case
database.resizeBufferManager(2000000000);
connection.setMaxNumThreadForExec(2);
queryResult = connection.execute("MATCH (a:person) RETURN a.fName, a.age, a.eyeSight, a.isStudent;");
console.log(queryResult);
console.log(connection.getNodePropertyNames("person"));
