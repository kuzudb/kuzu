const fs = require("fs");
const path = require("path");
const { execSync } = require("child_process");

const SRC_PATH = path.resolve(__dirname, "../..");
const THREADS = require("os").cpus().length;

console.log(`Using ${THREADS} threads to build Kuzu.`);

execSync("npm run clean", { stdio: "inherit" });
execSync(`make nodejs NUM_THREADS=${THREADS}`, {
  cwd: SRC_PATH,
  stdio: "inherit",
});
