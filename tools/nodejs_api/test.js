// Make sure the test directory is removed as it will be recreated
const fs = require("fs");
try {
  fs.rmSync("./test", { recursive: true });
} catch (e) {
  // ignore
}

// Import the module
const kuzu = require("./build/Release/kuzujs.node");
const reuslt = kuzu.test("Argument 1");
console.log(reuslt);
