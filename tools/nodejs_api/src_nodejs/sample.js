// Make sure the test directory is removed as it will be recreated
const fs = require("fs");
try {
  fs.rmSync("./test", { recursive: true });
} catch (e) {
  // ignore
}

const Connection = require("./connection.js");
const connection = new Connection("test");
console.log(connection.execute("create node table person (ID INt64, fName StRING, gender INT64, isStudent BoOLEAN, isWorker BOOLEAN, age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration interval, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], PRIMARY KEY (ID));"));
console.log(connection.execute("COPY person FROM \"../../dataset/tinysnb/vPerson.csv\" (HEADER=true);"));
console.log(connection.execute("MATCH (a:person) RETURN a.fName, a.age, a.eyeSight, a.isStudent;"));