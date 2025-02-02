/**
 * @file kuzu.js is the internal wrapper for the WebAssembly module.
 */
const kuzu_wasm = require("../kuzu/kuzu_wasm.js");

class kuzu {
  constructor() {
    this._kuzu = null;
  }

  async init() {
    this._kuzu = await kuzu_wasm();
  }

  checkInit() {
    if (!this._kuzu) {
      throw new Error("The WebAssembly module is not initialized.");
    }
  }

  getVersion() {
    this.checkInit();
    return this._kuzu.getVersion();
  }

  getStorageVersion() {
    this.checkInit();
    return this._kuzu.getStorageVersion();
  }

  getFS() {
    this.checkInit();
    return this._kuzu.FS;
  }

  getWasmMemory() {
    this.checkInit();
    return this._kuzu.wasmMemory;
  }
}

const kuzuInstance = new kuzu();
module.exports = kuzuInstance;
