/**
 * @file index.js is the root file for the Kuzu WebAssembly module.
 * It exports the module's public interface.
 */
"use strict";

const dispatcher = require("./dispatcher");
const Database = require("./database");
const Connection = require("./connection");
const PreparedStatement = require("./prepared_statement");
const QueryResult = require("./query_result");
const FS = require("./fs");

/**
 * The default asynchronous version of Kuzu WebAssembly module.
 * @module kuzu-wasm
 */
module.exports = {
  /**
   * Initialize the Kuzu WebAssembly module. Calling this function is optional,
   * as the module is initialized automatically when the first query is executed.
   * @memberof module:kuzu-wasm
   */
  init: async () => {
    await dispatcher.init();
  },

  /**
   * Get the version of the Kuzu WebAssembly module.
   * @memberof module:kuzu-wasm
   * @returns {String} the version of the Kuzu WebAssembly module.
   */
  getVersion: async () => {
    const worker = await dispatcher.getWorker();
    const version = await worker.getVersion();
    return version;
  },

  /**
   * Get the storage version of the Kuzu WebAssembly module.
   * @memberof module:kuzu-wasm
   * @returns {BigInt} the storage version of the Kuzu WebAssembly module.
   */
  getStorageVersion: async () => {
    const worker = await dispatcher.getWorker();
    const storageVersion = await worker.getStorageVersion();
    return storageVersion;
  },

  /**
   * Set the path to the WebAssembly worker script. By default, the worker 
   * script is resolved under the same directory / URL prefix as the main 
   * module. If you want to change the location of the worker script, you can 
   * pass the worker path parameter to this function. This function must be 
   * called before any other function calls to the WebAssembly module. After the 
   * initialization is started, the worker script path cannot be changed and not 
   * finding the worker script will cause an error.
   * @memberof module:kuzu-wasm
   * @param {String} workerPath the path to the WebAssembly worker script.
   */
  setWorkerPath: (workerPath) => {
    dispatcher.setWorkerPath(workerPath);
  },

  /**
   * Destroy the Kuzu WebAssembly module and kill the worker. This function
   * should be called when the module is no longer needed to free up resources.
   * @memberof module:kuzu-wasm
   */
  close: async () => {
    await dispatcher.close();
  },
  Database,
  Connection,
  PreparedStatement,
  QueryResult,
  FS,
}
