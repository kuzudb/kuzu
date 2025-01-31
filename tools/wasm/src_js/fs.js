"use strict";

const dispatcher = require("./dispatcher.js");

class FS {
  constructor() { }

  async readFile(path) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSReadFile(path);
    if (result.isFail) {
      throw new Error(result.error);
    }
    return result;
  }

  async writeFile(path, data) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSWriteFile(path, data);
    if (!result.isSuccess) {
      throw new Error(result.error);
    }
  }

  async mkdir(path) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSMkdir(path);
    if (!result.isSuccess) {
      throw new Error(result.error);
    }
  }

  async unlink(path) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSUnlink(path);
    if (!result.isSuccess) {
      throw new Error(result.error);
    }
  }
}

module.exports = new FS();
