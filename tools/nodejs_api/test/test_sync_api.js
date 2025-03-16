const { assert } = require("chai");

const PERSON_IDS = [0, 2, 3, 5, 7, 8, 9, 10];

describe("Query execution", function () {
  it("should execute a query synchronously", function () {
    const queryResult = conn.querySync(
      "MATCH (a:person) RETURN a.ID ORDER BY a.ID"
    );
    assert.isTrue(queryResult.hasNext());
    for (let id of PERSON_IDS) {
      const tuple = queryResult.getNextSync();
      assert.equal(tuple["a.ID"], id);
    }
  });

  it("should execute a query with multiple statements synchronously", function () {
    const queryResults = conn.querySync(
      "RETURN 1 AS a; RETURN 2 AS a; RETURN 3 AS a;"
    );
    for (let i = 1; i <= 3; ++i) {
      const queryResult = queryResults[i - 1];
      assert.isTrue(queryResult.hasNext());
      const tuple = queryResult.getNextSync();
      assert.equal(tuple["a"], i);
      assert.isFalse(queryResult.hasNext());
    }
  });

  it("should execute a prepared statement synchronously", function () {
    const preparedStatement = conn.prepareSync(
      "RETURN $a as id"
    );
    const queryResult = conn.executeSync(
      preparedStatement,
      { a: 3 }
    );
    assert.isTrue(queryResult.hasNext());
    const tuple = queryResult.getNextSync();
    assert.equal(tuple["id"], 3);
    assert.isFalse(queryResult.hasNext());
  });
});


describe("Query result", function () {
  it("should get all rows of a query result synchronously", function () {
    const queryResult = conn.querySync(
      "MATCH (a:person) RETURN a.ID ORDER BY a.ID"
    );
    const rows = queryResult.getAllSync();
    assert.equal(rows.length, PERSON_IDS.length);
    for (let i = 0; i < PERSON_IDS.length; ++i) {
      assert.equal(rows[i]["a.ID"], PERSON_IDS[i]);
    }
  });

  it("should get column names of a query result synchronously", function () {
    const queryResult = conn.querySync(
      "MATCH (a:person) RETURN a.ID, a.fName ORDER BY a.ID"
    );
    const columnNames = queryResult.getColumnNamesSync();
    assert.deepEqual(columnNames, ["a.ID", "a.fName"]);
  });

  it("should get column data types of a query result synchronously", function () {
    const queryResult = conn.querySync(
      "MATCH (a:person) RETURN a.ID, a.fName ORDER BY a.ID"
    );
    const columnDataTypes = queryResult.getColumnDataTypesSync();
    assert.deepEqual(columnDataTypes, ["INT64", "STRING"]);
  });
});

describe("Synchronous initialization", function () {
  it("should initialize a database synchronously", function () {
    const database = new kuzu.Database(":memory:", 1 << 28 /* 256 MB */);
    database.initSync();
    assert.isTrue(database._isInitialized);
    assert.isFalse(database._isClosed);
    database.closeSync();
    assert.isTrue(database._isClosed);
  });

  it("should initialize a connection synchronously", function () {
    const connection = new kuzu.Connection(db);
    connection.initSync();
    assert.isTrue(connection._isInitialized);
    assert.isFalse(connection._isClosed);
    connection.closeSync();
    assert.isTrue(connection._isClosed);
  });

  it("should perform initialization automatically for lazily-constructed database and connection", function () {
    const database = new kuzu.Database(":memory:", 1 << 28 /* 256 MB */);
    const connection = new kuzu.Connection(database);
    assert.isFalse(database._isInitialized);
    assert.isFalse(connection._isInitialized);
    const queryResult = connection.querySync(
      "RETURN 1 AS a, 'hello' AS b"
    );
    assert.isTrue(database._isInitialized);
    assert.isTrue(connection._isInitialized);

    const rows = queryResult.getAllSync();
    assert.equal(rows.length, 1);
    assert.equal(rows[0]["a"], 1);
    assert.equal(rows[0]["b"], "hello");
    queryResult.close();
    connection.closeSync();
    assert.isTrue(connection._isClosed);
    assert.isFalse(database._isClosed);
    database.closeSync();
    assert.isTrue(database._isClosed);
  });
});
