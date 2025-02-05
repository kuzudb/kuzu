/**
 * @file fs.js is the file for the FS class. FS is used to interact with the 
 * WebAssembly filesystem. It is an (incomplete) asynchronous wrapper around the 
 * Emscripten filesystem API.
 */
"use strict";

const dispatcher = require("./dispatcher.js");

class FS {
  /**
   * Initialize a new FS object.
   */
  constructor() {

  }

  /**
   * Read a file from the filesystem.
   * @param {String} path the path to the file.
   * @returns {Buffer} the file contents.
   * @throws {Error} if the file cannot be read.
   */
  async readFile(path) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSReadFile(path);
    if (result.isFail) {
      throw new Error(result.error);
    }
    return result;
  }

  /**
   * Write a file to the filesystem.
   * @param {String} path the path to the file.
   * @param {Buffer | String} data the data to write.
   * @throws {Error} if the file cannot be written.
   */
  async writeFile(path, data) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSWriteFile(path, data);
    if (!result.isSuccess) {
      throw new Error(result.error);
    }
  }

  /**
   * Make a directory in the filesystem.
   * @param {String} path the path to the directory.
   * @throws {Error} if the directory cannot be created.
   */
  async mkdir(path) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSMkdir(path);
    if (!result.isSuccess) {
      throw new Error(result.error);
    }
  }

  /**
   * Remove a file from the filesystem.
   * @param {String} path the path to the file.
   * @throws {Error} if the file cannot be removed.
   */
  async unlink(path) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSUnlink(path);
    if (!result.isSuccess) {
      throw new Error(result.error);
    }
  }

  /**
   * Rename a file in the filesystem.
   * @param {String} oldPath the old path to the file.
   * @param {String} newPath the new path to the file.
   * @throws {Error} if the file cannot be renamed.
   */
  async rename(oldPath, newPath) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSRename(oldPath, newPath);
    if (!result.isSuccess) {
      throw new Error(result.error);
    }
  }

  /**
   * Remove a directory from the filesystem.
   * @param {String} path the path to the directory.
   * @throws {Error} if the directory cannot be removed.
   */
  async rmdir(path) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSRmdir(path);
    if (!result.isSuccess) {
      throw new Error(result.error);
    }
  }

  /**
   * Get the status of a file in the filesystem.
   * @param {String} path the path to the file.
   * @returns {Object} the status of the file.
   * @throws {Error} if the status cannot be retrieved.
   */
  async stat(path) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSStat(path);
    if (result.isSuccess) {
      return result.result;
    }
    throw new Error(result.error);
  }

  /**
   * Get the files in a directory.
   * @param {String} path the path to the directory.
   * @returns {Array} the files in the directory.
   * @throws {Error} if the files cannot be retrieved.
   */
  async readDir(path) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSReadDir(path);
    if (result.isSuccess) {
      return result.result;
    }
    throw new Error(result.error);
  }

  /**
   * Mount a directory as the IDBFS filesystem (persistent storage).
   * @param {String} path the path to the directory.
   * @throws {Error} if the directory cannot be mounted.
   */
  async mountIdbfs(path) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSMountIdbfs(path);
    if (!result.isSuccess) {
      throw new Error(result.error);
    }
  }

  /**
   * Unmount a mounted filesystem.
   * @param {String} path the path to the filesystem.
   * @throws {Error} if the filesystem cannot be unmounted.
   */
  async unmount(path) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSUnmount(path);
    if (!result.isSuccess) {
      throw new Error(result.error);
    }
  }

  /**
   * Synchronize the IDBFS filesystem with the underlying storage.
   * @param {Boolean} populate control the intended direction of the 
   * synchronization `true` to initialize the file system data with the 
   * data from the file system’s persistent source, and `false` to save the 
   * file system data to the file system’s persistent source.
   * @throws {Error} if the filesystem cannot be synchronized.
   */
  async syncfs(populate) {
    const worker = await dispatcher.getWorker();
    const result = await worker.FSSyncfs(populate);
    if (!result.isSuccess) {
      throw new Error(result.error);
    }
  }
}

module.exports = new FS();
