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

console.log('addon',kuzu);

const prevInstance = new kuzu.ClassExample(4.3);
console.log('Initial value : ', prevInstance.getValue());
console.log('After adding 3.3 : ', prevInstance.add(3.3));
console.log('Calling Kuzu: ', prevInstance.temp("Alice"));

module.exports = kuzu;