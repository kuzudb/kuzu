"use strict";

const KuzuWasm = require("./kuzu.js");
const Database = require("./database.js");
const Connection = require("./connection.js");
const PreparedStatement = require("./prepared_statement.js");
const QueryResult = require("./query_result.js");

module.exports = {
  /**
   * Initialize the KuzuWasm module.
   * @returns {Promise<void>} a promise that resolves when the module is initialized. The promise is rejected if the module fails to initialize.
   */
  init: () => {
    return KuzuWasm.init();
  },

  /**
   * Get the version of the KuzuWasm module.
   * @returns {String} the version of the KuzuWasm module.
   */
  getVersion: () => {
    return KuzuWasm.getVersion();
  },

  /**
   * Get the storage version of the KuzuWasm module.
   * @returns {BigInt} the storage version of the KuzuWasm module.
   */
  getStorageVersion: () => {
    return KuzuWasm.getStorageVersion();
  },
  
  /**
   * Get the standard emscripten filesystem module (FS).
   * @returns {Object} the standard emscripten filesystem module (FS).
   */
  getFS: () => {
    return KuzuWasm.getFS();
  },

  /**
   * Get the WebAssembly memory.
   * @returns {Object} the WebAssembly memory.
   */
  getWasmMemory: () => {
    return KuzuWasm.getWasmMemory();
  },

  Database,
  Connection,
  PreparedStatement,
  QueryResult,
};
