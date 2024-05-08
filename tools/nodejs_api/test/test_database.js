const { assert } = require("chai");
const tmp = require("tmp");
const process = require("process");

const spwan = require("child_process").spawn;

const openDatabaseOnSubprocess = (dbPath) => {
  return new Promise((resolve, _) => {
    const node = process.argv[0];
    const code = `
      (async() => {
        const kuzu = require("${kuzuPath}");
        const db = new kuzu.Database("${dbPath}", 1 << 28);
        await db.init();
        console.log("Database initialized.");
      })();
    `;
    const child = spwan(node, ["-e", code]);
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (data) => {
      stdout += data;
    });
    child.stderr.on("data", (data) => {
      stderr += data;
    });
    child.on("close", (code) => {
      resolve({ code, stdout, stderr });
    });
  });
};

describe("Database constructor", function () {
  it("should create a database with a valid path and buffer size", async function () {
    const tmpDbPath = await new Promise((resolve, reject) => {
      tmp.dir({ unsafeCleanup: true }, (err, path, _) => {
        if (err) {
          return reject(err);
        }
        return resolve(path);
      });
    });
    const testDb = new kuzu.Database(tmpDbPath, 1 << 28 /* 256MB */);
    assert.exists(testDb);
    assert.equal(testDb.constructor.name, "Database");
    await testDb.init();
    assert.exists(testDb._database);
    assert.isTrue(testDb._isInitialized);
    assert.notExists(testDb._initPromise);
  });

  it("should create a database with a valid path and no buffer size", async function () {
    const tmpDbPath = await new Promise((resolve, reject) => {
      tmp.dir({ unsafeCleanup: true }, (err, path, _) => {
        if (err) {
          return reject(err);
        }
        return resolve(path);
      });
    });
    const testDb = new kuzu.Database(tmpDbPath);
    assert.exists(testDb);
    assert.equal(testDb.constructor.name, "Database");
    await testDb.init();
    assert.exists(testDb._database);
    assert.isTrue(testDb._isInitialized);
    assert.notExists(testDb._initPromise);
  });

  it("should create a database in read-only mode", async function () {
    // TODO: Enable this test on Windows when the read-only mode is implemented.
    if (process.platform === "win32") {
      this._runnable.title += " (skipped: not implemented on Windows)";
      this.skip();
    }

    const tmpDbPath = await new Promise((resolve, reject) => {
      tmp.dir({ unsafeCleanup: true }, (err, path, _) => {
        if (err) {
          return reject(err);
        }
        return resolve(path);
      });
    });
    const testDb = new kuzu.Database(tmpDbPath, 1 << 28 /* 256MB */);
    assert.exists(testDb);
    assert.equal(testDb.constructor.name, "Database");
    await testDb.init();
    assert.exists(testDb._database);
    assert.isTrue(testDb._isInitialized);
    assert.notExists(testDb._initPromise);
    delete testDb;
    const testDbReadOnly = new kuzu.Database(
      tmpDbPath,
      1 << 28 /* 256MB */,
      true /* compression */,
      true /* readOnly */
    );
    assert.exists(testDbReadOnly);
    assert.equal(testDbReadOnly.constructor.name, "Database");
    await testDbReadOnly.init();
    assert.exists(testDbReadOnly._database);
    assert.isTrue(testDbReadOnly._isInitialized);
    assert.notExists(testDbReadOnly._initPromise);
    const connection = new kuzu.Connection(testDbReadOnly);
    assert.exists(connection);
    assert.equal(connection.constructor.name, "Connection");
    await connection.init();
    assert.exists(connection._connection);
    assert.isTrue(connection._isInitialized);
    assert.notExists(connection._initPromise);
    try {
      const _ = await connection.query(
        "CREATE NODE TABLE test (id INT64, PRIMARY KEY(id))"
      );
      assert.fail(
        "No error thrown when executing CREATE TABLE in read-only mode."
      );
    } catch (e) {
      assert.equal(
        e.message,
        "Cannot execute write operations in a read-only database!"
      );
    }
  });

  it("should create a database with a valid max DB size", async function () {
    const tmpDbPath = await new Promise((resolve, reject) => {
      tmp.dir({ unsafeCleanup: true }, (err, path, _) => {
        if (err) {
          return reject(err);
        }
        return resolve(path);
      });
    });
    const testDb = new kuzu.Database(
      tmpDbPath,
      1 << 28 /* 256MB */,
      true /* compression */,
      false /* readOnly */,
      1 << 30 /* 1GB */
    );
    assert.exists(testDb);
    assert.equal(testDb.constructor.name, "Database");
    await testDb.init();
    assert.exists(testDb._database);
    assert.isTrue(testDb._isInitialized);
    assert.notExists(testDb._initPromise);
  });

  it("should throw error if the path is invalid", async function () {
    try {
      const _ = new kuzu.Database({}, 1 << 28 /* 256MB */);
      assert.fail("No error thrown when the path is invalid.");
    } catch (e) {
      assert.equal(e.message, "Database path must be a string.");
    }
  });

  it("should throw error if the buffer size is invalid", async function () {
    try {
      const _ = new kuzu.Database("", {});
      assert.fail("No error thrown when the buffer size is invalid.");
    } catch (e) {
      assert.equal(
        e.message,
        "Buffer manager size must be a positive integer."
      );
    }
  });

  it("should throw error if the buffer size is negative", async function () {
    try {
      const _ = new kuzu.Database("", -1);
      assert.fail("No error thrown when the buffer size is negative.");
    } catch (e) {
      assert.equal(
        e.message,
        "Buffer manager size must be a positive integer."
      );
    }
  });

  it("should throw error if the max DB size is invalid", async function () {
    try {
      const _ = new kuzu.Database("", 1 << 28 /* 256MB */, true, false, {});
      assert.fail("No error thrown when the max DB size is invalid.");
    } catch (e) {
      assert.equal(e.message, "Max DB size must be a positive integer.");
    }
  });
});

describe("Database close", function () {
  it("should allow initializing a new database after closing", async function () {
    if (process.platform === "win32") {
      this._runnable.title += " (skipped: not implemented on Windows)";
      this.skip();
    }
    const tmpDbPath = await new Promise((resolve, reject) => {
      tmp.dir({ unsafeCleanup: true }, (err, path, _) => {
        if (err) {
          return reject(err);
        }
        return resolve(path);
      });
    });
    const testDb = new kuzu.Database(tmpDbPath, 1 << 28 /* 256MB */);
    await testDb.init();
    let subProcessResult = await openDatabaseOnSubprocess(tmpDbPath);
    assert.notEqual(subProcessResult.code, 0);
    assert.include(
      subProcessResult.stderr,
      "Error: IO exception: Could not set lock on file"
    );
    await testDb.close();
    subProcessResult = await openDatabaseOnSubprocess(tmpDbPath);
    assert.equal(subProcessResult.code, 0);
    assert.isEmpty(subProcessResult.stderr);
    assert.include(subProcessResult.stdout, "Database initialized.");
  });

  it("should throw error if the database is closed", async function () {
    const tmpDbPath = await new Promise((resolve, reject) => {
      tmp.dir({ unsafeCleanup: true }, (err, path, _) => {
        if (err) {
          return reject(err);
        }
        return resolve(path);
      });
    });
    const testDb = new kuzu.Database(tmpDbPath, 1 << 28 /* 256MB */);
    await testDb.init();
    await testDb.close();
    try {
      await testDb._getDatabase();
      assert.fail("No error thrown when the database is closed.");
    } catch (e) {
      assert.equal(e.message, "Database is closed.");
    }
  });

  it("should close the database if it is initialized", async function () {
    const tmpDbPath = await new Promise((resolve, reject) => {
      tmp.dir({ unsafeCleanup: true }, (err, path, _) => {
        if (err) {
          return reject(err);
        }
        return resolve(path);
      });
    });
    const testDb = new kuzu.Database(tmpDbPath, 1 << 28 /* 256MB */);
    await testDb.init();
    assert.isTrue(testDb._isInitialized);
    assert.exists(testDb._database);
    await testDb.close();
    assert.notExists(testDb._database);
    assert.isTrue(testDb._isClosed);
  });

  it("should close the database if it is not initialized", async function () {
    const tmpDbPath = await new Promise((resolve, reject) => {
      tmp.dir({ unsafeCleanup: true }, (err, path, _) => {
        if (err) {
          return reject(err);
        }
        return resolve(path);
      });
    });
    const testDb = new kuzu.Database(tmpDbPath, 1 << 28 /* 256MB */);
    assert.isFalse(testDb._isInitialized);
    await testDb.close();
    assert.notExists(testDb._database);
    assert.isTrue(testDb._isClosed);
    assert.isTrue(testDb._isInitialized);
  });

  it("should close a initializing database", async function () {
    const tmpDbPath = await new Promise((resolve, reject) => {
      tmp.dir({ unsafeCleanup: true }, (err, path, _) => {
        if (err) {
          return reject(err);
        }
        return resolve(path);
      });
    });
    const testDb = new kuzu.Database(tmpDbPath, 1 << 28 /* 256MB */);
    await Promise.all([testDb.init(), testDb.close()]);
    assert.notExists(testDb._database);
    assert.isTrue(testDb._isClosed);
    assert.isTrue(testDb._isInitialized);
  });

  it("should gracefully close a database multiple times", async function () {
    const tmpDbPath = await new Promise((resolve, reject) => {
      tmp.dir({ unsafeCleanup: true }, (err, path, _) => {
        if (err) {
          return reject(err);
        }
        return resolve(path);
      });
    });
    const testDb = new kuzu.Database(tmpDbPath, 1 << 28 /* 256MB */);
    await testDb.init();
    await Promise.all([testDb.close(), testDb.close(), testDb.close()]);
    assert.notExists(testDb._database);
    assert.isTrue(testDb._isClosed);
    assert.isTrue(testDb._isInitialized);
  });
});
