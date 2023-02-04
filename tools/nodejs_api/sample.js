// Make sure the test directory is removed as it will be recreated
const fs = require("fs");
const {Database, Connection} = require("./build/kuzu");

try {
  fs.rmSync("./test", { recursive: true });
} catch (e) {
  // ignore
}
const database = new Database("test", 1 << 30);
console.log("The database looks like: ", database);
const connection = new Connection(database);
console.log ("The connection looks like: ", connection);
connection.execute("create node table person (ID INt64, fName StRING, gender INT64, isStudent BoOLEAN, isWorker BOOLEAN, age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration interval, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], PRIMARY KEY (ID));");
connection.execute("COPY person FROM \"../../dataset/tinysnb/vPerson.csv\" (HEADER=true);");
const queryResult = connection.execute("MATCH (a:person) RETURN a.fName, a.age, a.eyeSight, a.isStudent;");
console.log(queryResult);
