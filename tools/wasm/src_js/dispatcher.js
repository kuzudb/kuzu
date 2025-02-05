/**
 * @file dispatcher.js is the file for the internal Dispatcher class. 
 * Dispatcher is used to manage the worker thread for the wasm module.
 */
"use strict";

const { spawn, Thread, Worker } = require("threads");
const isNodeJs = typeof process === "object" && process + "" === "[object process]";

class Dispatcher {
  constructor() {
    this.worker = null;
    this.workerPath = null;
  }
  
  setWorkerPath(workerPath) {
    this.workerPath = workerPath;
  }

  getWorkerPath() {
    if (this.workerPath) {
      return this.workerPath;
    }
    if (isNodeJs) {
      return "./kuzu_wasm_worker.js";
    }
    // Hack: importMeta will be replaced by esbuild with "import.meta"
    // This is a workaround for "This file is considered to be an ECMAScript 
    // module because of the use of "import.meta" here:"
    const scriptPath = importMeta.url;
    const basePath = scriptPath.substring(0, scriptPath.lastIndexOf("/"));
    return `${basePath}/kuzu_wasm_worker.js`;
  }

  async init() {
    const workerUrl = this.getWorkerPath();
    this.worker = await spawn(new Worker(workerUrl), { timeout: 60000 });
    const res = await this.worker.init();
    if (res.isSuccess) {
      return res;
    } else {
      throw new Error(res.error);
    }
  }

  async close() {
    if (this.worker) {
      await Thread.terminate(this.worker);
    }
  }

  async getWorker() {
    if (!this.worker) {
      if (!this.workerInitPromise) {
        this.workerInitPromise = this.init();
      }
      await this.workerInitPromise;
      delete this.workerInitPromise;
    }
    return this.worker;
  }
}

const dispatcher = new Dispatcher();
module.exports = dispatcher;
