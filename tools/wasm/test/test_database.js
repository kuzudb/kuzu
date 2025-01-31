const { assert } = require("chai");
const tmp = require("tmp");


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
    assert.isTrue(testDb._isInitialized);
    assert.notExists(testDb._initPromise);
    await testDb.close();
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
    assert.isTrue(testDb._isInitialized);
    assert.notExists(testDb._initPromise);
    await testDb.close();
  });

  it("should create an in-memory database when no path is provided", async function () {
    const testDb = new kuzu.Database();
    const conn = new kuzu.Connection(testDb);
    let res = await conn.query("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));");
    res.close();
    res = await conn.query("CREATE (:person {name: 'Alice', age: 30});");
    res.close();
    res = await conn.query("CREATE (:person {name: 'Bob', age: 40});");
    res.close();
    res = await conn.query("MATCH (p:person) RETURN p.*;");
    const result = await res.getAllObjects();
    assert.equal(result.length, 2);
    assert.equal(result[0]["p.name"], "Alice");
    assert.equal(result[0]["p.age"], 30);
    assert.equal(result[1]["p.name"], "Bob");
    assert.equal(result[1]["p.age"], 40);
    await res.close();
    await conn.close();
    await testDb.close();
  });

  it("should create an in-memory database when empty path is provided", async function () {
    const testDb = new kuzu.Database("");
    const conn = new kuzu.Connection(testDb);
    let res = await conn.query("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));");
    res.close();
    res = await conn.query("CREATE (:person {name: 'Alice', age: 30});");
    res.close();
    res = await conn.query("CREATE (:person {name: 'Bob', age: 40});");
    res.close();
    res = await conn.query("MATCH (p:person) RETURN p.*;");
    const result = await res.getAllObjects();
    assert.equal(result.length, 2);
    assert.equal(result[0]["p.name"], "Alice");
    assert.equal(result[0]["p.age"], 30);
    assert.equal(result[1]["p.name"], "Bob");
    assert.equal(result[1]["p.age"], 40);
    await res.close();
    await conn.close();
    await testDb.close();
  });
});
