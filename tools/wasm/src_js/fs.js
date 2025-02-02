/**
 * @file fs.js is the file for the FS class. FS is used to interact with the 
 * WebAssembly filesystem. It is an (incomplete) asynchronous wrapper around the Emscripten filesystem API.
 */
"use strict";

const dispatcher = require("./dispatcher.js");

class FS {
  constructor() { }

  /*
   * Read a file from the filesystem.
   * @param {String} path the path to the file.
   * @returns {Buffer} the file contents.
   */
  async readFile(path) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSReadFile(path);
    if (result.isFail) {
      throw new Error(result.error);
    }
    return result;
  }

  /*
   * Write a file to the filesystem.
   * @param {String} path the path to the file.
   * @param {Buffer | String} data the data to write.
   */
  async writeFile(path, data) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSWriteFile(path, data);
    if (!result.isSuccess) {
      throw new Error(result.error);
    }
  }

  /*
   * Make a directory in the filesystem.
   * @param {String} path the path to the directory.
   */
  async mkdir(path) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSMkdir(path);
    if (!result.isSuccess) {
      throw new Error(result.error);
    }
  }

  /*
   * Remove a file from the filesystem.
   * @param {String} path the path to the file.
   */
  async unlink(path) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSUnlink(path);
    if (!result.isSuccess) {
      throw new Error(result.error);
    }
  }
}

module.exports = new FS();
