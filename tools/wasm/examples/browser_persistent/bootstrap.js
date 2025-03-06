const fs = require("fs");
const path = require("path");
const process = require("process");

const KUZU_WASM_INDEX_PATH = path.join(__dirname, "node_modules", "kuzu-wasm", "index.js");
const KUZU_WASM_WORKER_PATH = path.join(__dirname, "node_modules", "kuzu-wasm", "kuzu_wasm_worker.js");
const DESTINATION_PATH = path.join(__dirname, "public");

if (!fs.existsSync(KUZU_WASM_INDEX_PATH) || !fs.existsSync(KUZU_WASM_WORKER_PATH)) {
    console.log("Kuzu WebAssembly module not found. Please run `npm i` to install the dependencies.");
    process.exit(1);
}

console.log("Copying Kuzu WebAssembly module to public directory...");
console.log(`Copying ${KUZU_WASM_INDEX_PATH} to ${DESTINATION_PATH}...`);
fs.copyFileSync(KUZU_WASM_INDEX_PATH, path.join(DESTINATION_PATH, "index.js"));
console.log(`Copying ${KUZU_WASM_WORKER_PATH} to ${DESTINATION_PATH}...`);
fs.copyFileSync(KUZU_WASM_WORKER_PATH, path.join(DESTINATION_PATH, "kuzu_wasm_worker.js"));
console.log("Done.");
