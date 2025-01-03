"use strict";

const { spawn, Thread, Worker } = require("threads");
const isNodeJs = typeof process === "object" && process + "" === "[object process]";

class Dispatcher {
  constructor() {
    this.worker = null;
    this.workerInitPromise = this.init();
  }

  async init() {
    const workerUrl = isNodeJs ? "./worker.js" : new URL("./worker.js", import.meta.url).href;
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
