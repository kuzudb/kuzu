/**
 * @file kuzu_wasm_worker.js is the file for the worker thread that wraps around 
 * the synchronous WebAssembly module to create an asynchronous API.
 */
"use strict";

const { expose, isWorkerRuntime, Transfer } = require('threads/worker');
const { v4: uuidv4 } = require('uuid');
const objectsStore = {};
let FS = null;
const kuzuSync = require('./sync');

if (!isWorkerRuntime) {
  // Do nothing if not in worker runtime (i.e. running in main thread)
}
else {
  expose({
    async init() {
      try {
        await kuzuSync.init();
        FS = kuzuSync.getFS();
        return { isSuccess: true };
      }
      catch (e) {
        return { error: e.message, isSuccess: false };
      }
    },

    getVersion() {
      return kuzuSync.getVersion();
    },

    getStorageVersion() {
      return kuzuSync.getStorageVersion();
    },

    databaseConstruct(databasePath,
      bufferPoolSize,
      maxNumThreads,
      enableCompression,
      readOnly,
      autoCheckpoint,
      checkpointThreshold
    ) {
      const id = uuidv4();
      try {
        objectsStore[id] = new kuzuSync.Database(
          databasePath,
          bufferPoolSize,
          maxNumThreads,
          enableCompression,
          readOnly,
          autoCheckpoint,
          checkpointThreshold
        );
      } catch (e) {
        delete objectsStore[id];
        return { error: e.message, isSuccess: false, id: null };
      }
      return { isSuccess: true, id };
    },

    databaseClose(id) {
      if (id in objectsStore) {
        objectsStore[id].close();
        delete objectsStore[id];
      }
    },

    connectionConstruct(databaseId, numThreads = null) {
      if (!(databaseId in objectsStore)) {
        return { error: "Database not found", isSuccess: false };
      }
      const id = uuidv4();
      try {
        objectsStore[id] = new kuzuSync.Connection(
          objectsStore[databaseId],
          numThreads
        );
      } catch (e) {
        delete objectsStore[id];
        return { error: e.message, isSuccess: false };
      }
      return { isSuccess: true, id };
    },

    connectionClose(id) {
      if (id in objectsStore) {
        objectsStore[id].close();
        delete objectsStore[id];
      }
    },

    connectionSetMaxNumThreadForExec(id, numThreads) {
      if (!(id in objectsStore)) {
        return {
          error: "Connection not found",
          isSuccess: false,
        }
      }
      try {
        objectsStore[id].setMaxNumThreadForExec(numThreads);
        return { isSuccess: true };
      }
      catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    connectionSetQueryTimeout(id, timeout) {
      if (!(id in objectsStore)) {
        return {
          error: "Connection not found",
          isSuccess: false,
        }
      }
      try {
        objectsStore[id].setQueryTimeout(timeout);
        return { isSuccess: true };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    connectionGetMaxNumThreadForExec(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Connection not found",
          isSuccess: false,
          numThreads: null,
        }
      }
      try {
        return {
          isSuccess: true,
          numThreads: objectsStore[id].getMaxNumThreadForExec(),
        };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    connectionQuery(id, statement) {
      if (!(id in objectsStore)) {
        return {
          error: "Connection not found",
          isSuccess: false,
        }
      }
      try {
        const result = objectsStore[id].query(statement);
        const resultId = uuidv4();
        objectsStore[resultId] = result;
        return { isSuccess: true, id: resultId };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    connectionPrepare(id, statement) {
      if (!(id in objectsStore)) {
        return {
          error: "Connection not found",
          isSuccess: false,
        }
      }
      try {
        const result = objectsStore[id].prepare(statement);
        const resultId = uuidv4();
        objectsStore[resultId] = result;
        return { isSuccess: true, id: resultId };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    connectionExecute(connectionId, statementId, params) {
      if (!(connectionId in objectsStore)) {
        return {
          error: "Connection not found",
          isSuccess: false,
        }
      }
      if (!(statementId in objectsStore)) {
        return {
          error: "Prepared statement not found",
          isSuccess: false,
        }
      }
      try {
        const result = objectsStore[connectionId].execute(
          objectsStore[statementId],
          params
        );
        const id = uuidv4();
        objectsStore[id] = result;
        return { isSuccess: true, id };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    preparedStatementClose(id) {
      if (id in objectsStore) {
        objectsStore[id].close();
        delete objectsStore[id];
      }
    },

    preparedStatementIsSuccess(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Statement not found",
          isSuccess: false,
        }
      }
      try {
        return { isSuccess: true, result: objectsStore[id].isSuccess() };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    preparedStatementGetErrorMessage(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Statement not found",
          isSuccess: false,
        }
      }
      try {
        return { isSuccess: true, result: objectsStore[id].getErrorMessage() };
      }
      catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    queryResultClose(id) {
      if (id in objectsStore) {
        objectsStore[id].close();
        delete objectsStore[id];
      }
    },

    queryResultIsSuccess(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Query result not found",
          isSuccess: false,
        }
      }
      try {
        return { isSuccess: true, result: objectsStore[id].isSuccess() };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    queryResultGetErrorMessage(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Query result not found",
          isSuccess: false,
        }
      }
      try {
        return { isSuccess: true, result: objectsStore[id].getErrorMessage() };
      }
      catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    queryResultResetIterator(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Query result not found",
          isSuccess: false,
        }
      }
      try {
        objectsStore[id].resetIterator();
        return { isSuccess: true };
      }
      catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    queryResultHasNext(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Query result not found",
          isSuccess: false,
        }
      }
      try {
        return { isSuccess: true, result: objectsStore[id].hasNext() };
      }
      catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    queryResultHasNextQueryResult(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Query result not found",
          isSuccess: false,
        }
      }
      try {
        return { isSuccess: true, result: objectsStore[id].hasNextQueryResult() };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    queryResultGetNumColumns(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Query result not found",
          isSuccess: false,
        }
      }
      try {
        return { isSuccess: true, result: objectsStore[id].getNumColumns() };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    queryResultGetNumTuples(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Query result not found",
          isSuccess: false,
        }
      }
      try {
        return { isSuccess: true, result: objectsStore[id].getNumTuples() };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    queryResultGetColumnNames(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Query result not found",
          isSuccess: false,
        }
      }
      try {
        return { isSuccess: true, result: objectsStore[id].getColumnNames() };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    queryResultGetColumnTypes(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Query result not found",
          isSuccess: false,
        }
      }
      try {
        return { isSuccess: true, result: objectsStore[id].getColumnTypes() };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    queryResultToString(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Query result not found",
          isSuccess: false,
        }
      }
      try {
        return { isSuccess: true, result: objectsStore[id].toString() };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    queryResultGetQuerySummary(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Query result not found",
          isSuccess: false,
        }
      }
      try {
        return { isSuccess: true, result: objectsStore[id].getQuerySummary() };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    queryResultGetNextQueryResult(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Query result not found",
          isSuccess: false,
        }
      }
      try {
        const result = objectsStore[id].getNextQueryResult();
        if (!result) {
          return { isSuccess: true, result: null };
        }
        const newId = uuidv4();
        objectsStore[newId] = result;
        return { isSuccess: true, result: newId };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    queryResultGetNext(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Query result not found",
          isSuccess: false,
        }
      }
      try {
        return { isSuccess: true, result: objectsStore[id].getNext() };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    queryResultGetAllRows(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Query result not found",
          isSuccess: false,
        }
      }
      try {
        return { isSuccess: true, result: objectsStore[id].getAllRows() };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    queryResultGetAllObjects(id) {
      if (!(id in objectsStore)) {
        return {
          error: "Query result not found",
          isSuccess: false,
        }
      }
      try {
        return { isSuccess: true, result: objectsStore[id].getAllObjects() };
      } catch (e) {
        return { error: e.message, isSuccess: false, };
      }
    },

    FSReadFile(path) {
      try {
        const result = FS.readFile(path);
        return Transfer(result.buffer, [result.buffer]);
      }
      catch (e) {
        // Use `isFail` instead of `isSuccess` to work with Transfer
        // because the return value in the normal case is an ArrayBuffer
        // instead of an object.
        return { error: e.message, isFail: true }
      }
    },

    FSWriteFile(path, data) {
      try {
        FS.writeFile(path, data);
        return { isSuccess: true };
      }
      catch (e) {
        return { error: e.message, isSuccess: false }
      }
    },

    FSMkdir(path) {
      try {
        FS.mkdir(path);
        return { isSuccess: true };
      }
      catch (e) {
        return { error: e.message, isSuccess: false }
      }
    },

    FSUnlink(path) {
      try {
        FS.unlink(path);
        return { isSuccess: true };
      }
      catch (e) {
        return { error: e.message, isSuccess: false }
      }
    },

    FSRename(oldPath, newPath) {
      try {
        FS.rename(oldPath, newPath);
        return { isSuccess: true };
      }
      catch (e) {
        return { error: e.message, isSuccess: false }
      }
    },

    FSRmdir(path) {
      try {
        FS.rmdir(path);
        return { isSuccess: true };
      }
      catch (e) {
        return { error: e.message, isSuccess: false }
      }
    },

    FSStat(path) {
      try {
        const result = FS.stat(path);
        return { isSuccess: true, result };
      }
      catch (e) {
        return { error: e.message, isSuccess: false }
      }
    },

    FSReadDir(path) {
      try {
        const result = FS.readdir(path);
        return { isSuccess: true, result };
      }
      catch (e) {
        return { error: e.message, isSuccess: false }
      }
    },

    FSUnmount(path) {
      try {
        FS.unmount(path);
        return { isSuccess: true };
      }
      catch (e) {
        return { error: e.message, isSuccess: false }
      }
    },

    FSMountIdbfs(path) {
      try {
        const IDBFS = FS.filesystems.IDBFS;
        FS.mount(IDBFS, {}, path);
        return { isSuccess: true };
      }
      catch (e) {
        return { error: e.message, isSuccess: false }
      }
    },

    async FSSyncfs(populate) {
      try {
        await new Promise((resolve, reject) => {
          FS.syncfs(populate, (err) => {
            if (err) {
              reject(err);
            } else {
              resolve();
            }
          });
        });
        return { isSuccess: true };
      } catch (e) {
        return { error: e.message, isSuccess: false }
      }
    },
  });
}
