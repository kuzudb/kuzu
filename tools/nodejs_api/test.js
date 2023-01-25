// Make sure the test directory is removed as it will be recreated
const fs = require("fs");
try {
  fs.rmSync("./test", { recursive: true });
} catch (e) {
  // ignore
}


// Import the module
const kuzu = require("./build/Release/kuzujs.node");


// const reuslt = new kuzu.test("Argument 1");
// console.log(reuslt);

console.log('addon', kuzu);

const prevInstance = new kuzu.ClassExample();
console.log('Kuzu Create: ', prevInstance.temp("create node table person (ID INt64, fName StRING, gender INT64, isStudent BoOLEAN, isWorker BOOLEAN, age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration interval, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], PRIMARY KEY (ID));"));
console.log('Kuzu Copy: ', prevInstance.temp("COPY person FROM \"../../dataset/tinysnb/vPerson.csv\" (HEADER=true);"));
console.log('Kuzu Match: ', prevInstance.temp("MATCH (a:person) RETURN a.fName, a.age, a.eyeSight, a.isStudent;"));


module.exports = kuzu;