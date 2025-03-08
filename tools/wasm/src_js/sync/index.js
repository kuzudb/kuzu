/**
 * @file index.js is the root file for the synchronous version of Kuzu
 * WebAssembly module. It exports the module's public interface.
 */
"use strict";

const KuzuWasm = require("./kuzu.js");
const Database = require("./database.js");
const Connection = require("./connection.js");
const PreparedStatement = require("./prepared_statement.js");
const QueryResult = require("./query_result.js");

/**
 * The synchronous version of Kuzu WebAssembly module.
 * @module kuzu-wasm
 */
module.exports = {
  /**
   * Initialize the Kuzu WebAssembly module.
   * @memberof module:kuzu-wasm
   * @returns {Promise<void>} a promise that resolves when the module is 
   * initialized. The promise is rejected if the module fails to initialize.
   */
  init: () => {
    return KuzuWasm.init();
  },

  /**
   * Get the version of the Kuzu WebAssembly module.
   * @memberof module:kuzu-wasm
   * @returns {String} the version of the Kuzu WebAssembly module.
   */
  getVersion: () => {
    return KuzuWasm.getVersion();
  },

  /**
   * Get the storage version of the Kuzu WebAssembly module.
   * @memberof module:kuzu-wasm
   * @returns {BigInt} the storage version of the Kuzu WebAssembly module.
   */
  getStorageVersion: () => {
    return KuzuWasm.getStorageVersion();
  },
  
  /**
   * Get the standard emscripten filesystem module (FS). Please refer to the 
   * emscripten documentation for more information.
   * @memberof module:kuzu-wasm
   * @returns {Object} the standard emscripten filesystem module (FS).
   */
  getFS: () => {
    return KuzuWasm.getFS();
  },

  /**
   * Get the WebAssembly memory. Please refer to the emscripten documentation 
   * for more information.
   * @memberof module:kuzu-wasm
   * @returns {Object} the WebAssembly memory object.
   */
  getWasmMemory: () => {
    return KuzuWasm.getWasmMemory();
  },

  Database,
  Connection,
  PreparedStatement,
  QueryResult,
};
