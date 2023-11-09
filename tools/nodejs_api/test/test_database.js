const { assert } = require("chai");
const tmp = require("tmp");
const process = require("process");

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
      true, /* compression */
      true, /* readOnly */
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
});

describe("Set logging level", function () {
  it("should set the logging level if the database is initialized", async function () {
    assert.isTrue(db._isInitialized);
    assert.exists(db._database);
    db.setLoggingLevel(kuzu.LoggingLevel.DEBUG);
    db.setLoggingLevel(kuzu.LoggingLevel.INFO);
    db.setLoggingLevel(kuzu.LoggingLevel.ERROR);
  });

  it("should store the logging level if the database is not initialized", async function () {
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
    assert.isFalse(testDb._isInitialized);
    testDb.setLoggingLevel(kuzu.LoggingLevel.DEBUG);
    assert.equal(testDb._loggingLevel, kuzu.LoggingLevel.DEBUG);
    testDb.setLoggingLevel(kuzu.LoggingLevel.INFO);
    assert.equal(testDb._loggingLevel, kuzu.LoggingLevel.INFO);
    testDb.setLoggingLevel(kuzu.LoggingLevel.ERROR);
    assert.equal(testDb._loggingLevel, kuzu.LoggingLevel.ERROR);
    await testDb.init();
    assert.isTrue(testDb._isInitialized);
    assert.exists(testDb._database);
    // The logging level should be reset after initialization
    assert.notExists(testDb._loggingLevel);
  });

  it("should throw error if the logging level is invalid", async function () {
    assert.isTrue(db._isInitialized);
    assert.exists(db._database);
    try {
      db.setLoggingLevel("TEST");
      assert.fail("No error thrown when the logging level is invalid.");
    } catch (e) {
      assert.equal(
        e.message,
        "Invalid logging level: TEST. Valid logging levels are: kuzu.LoggingLevel.DEBUG, kuzu.LoggingLevel.INFO, kuzu.LoggingLevel.ERROR."
      );
    }
  });
});
