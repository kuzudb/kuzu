"use strict";

const { spawn, Thread, Worker } = require("threads");
const isNodeJs = typeof process === "object" && process + "" === "[object process]";

class Dispatcher {
  constructor(workerPath) {
    if (workerPath) {
      this.workerPath = workerPath;
    }
    this.worker = null;
    this.workerInitPromise = this.init();
  }

  getWorkerPath() {
    if (this.workerPath) {
      return this.workerPath;
    }
    if (isNodeJs) {
      return "./worker.js";
    }
    // Hack: importMeta will be replaced by esbuild with "import.meta"
    // This is a workaround for "This file is considered to be an ECMAScript module because of the use of "import.meta" here:"
    const scriptPath = importMeta.url;
    const basePath = scriptPath.substring(0, scriptPath.lastIndexOf("/"));
    return `${basePath}/worker.js`;
  }

  async init() {
    const workerUrl = this.getWorkerPath();
    this.worker = await spawn(new Worker(workerUrl));
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
    await this.workerInitPromise;
    return this.worker;
  }
}

const dispatcher = new Dispatcher();
module.exports = dispatcher;