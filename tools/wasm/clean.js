const fs = require("fs");
const path = require("path");

const IS_CLEAN_ALL = process.argv[2] === "all";
const IS_DIST_EXCLUDED = process.argv[2] === "exclude-package";

const BUILD_DIR = path.resolve(__dirname, "build");
fs.rmSync(BUILD_DIR, { recursive: true, force: true });
console.log("build dir removed");
const DIST_DIR = path.resolve(__dirname, "package");
if (!IS_DIST_EXCLUDED) {
  fs.rmSync(DIST_DIR, { recursive: true, force: true });
  console.log("package dir removed");
}
const CMAKE_BUILD_DIR = path.resolve(__dirname, "..", "..", "build", "wasm");
fs.rmSync(CMAKE_BUILD_DIR, { recursive: true, force: true });
console.log("cmake build dir removed");

if (IS_CLEAN_ALL) {
  const NODE_MODULES_DIR = path.resolve(__dirname, "node_modules");
  fs.rmSync(NODE_MODULES_DIR, { recursive: true, force: true });
  console.log("node_modules dir removed");

  const PACKAGE_LOCK_JSON = path.resolve(__dirname, "package-lock.json");
  fs.rmSync(PACKAGE_LOCK_JSON, { force: true });
  console.log("package-lock.json removed");
}
