const { assert } = require("chai");

describe("Connection constructor", function () {
  it("should create a connection with a valid database object", async function () {
    const connection = new kuzu.Connection(db);
    assert.exists(connection);
    assert.equal(connection.constructor.name, "Connection");
    await connection.init();
    assert.exists(connection._connection);
    assert.isTrue(connection._isInitialized);
    assert.notExists(connection._initPromise);
  });

  it("should throw error if the database object is invalid", async function () {
    try {
      const _ = new kuzu.Connection({});
      assert.fail("No error thrown when the database object is invalid.");
    } catch (e) {
      assert.equal(e.message, "database must be a valid Database object.");
    }
  });
});

describe("Prepare", function () {
  it("should prepare a valid statement", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.ID = $1 RETURN COUNT(*)"
    );
    assert.exists(preparedStatement);
    assert.isTrue(preparedStatement.isSuccess());
    assert.equal(preparedStatement.getErrorMessage(), "");
  });

  it("should return error message if the statement is invalid", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:dog) WHERE a.ID = $1 RETURN COUNT(*)"
    );
    assert.exists(preparedStatement);
    assert.isFalse(preparedStatement.isSuccess());
    assert.equal(
      preparedStatement.getErrorMessage(),
      "Binder exception: Table dog does not exist."
    );
  });

  it("should return error message if the statement is not a string", async function () {
    try {
      const _ = await conn.prepare({});
      assert.fail("No error thrown when the query is not a string.");
    } catch (e) {
      assert.equal(e.message, "statement must be a string.");
    }
  });

  it("should throw error if the statement is not a string", async function () {
    try {
      const _ = await conn.prepare({});
      assert.fail("No error thrown when the query is not a string.");
    } catch (e) {
      assert.equal(e.message, "statement must be a string.");
    }
  });
});

describe("Execute", function () {
  it("should execute a valid prepared statement", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.ID = $1 RETURN COUNT(*)"
    );
    assert.exists(preparedStatement);
    assert.isTrue(preparedStatement.isSuccess());
    const queryResult = await conn.execute(preparedStatement, { 1: 0 });
    assert.exists(queryResult);
    assert.equal(queryResult.constructor.name, "QueryResult");
    assert.isTrue(queryResult.hasNext());
    const tuple = await queryResult.getNext();
    assert.exists(tuple);
    assert.exists(tuple["COUNT_STAR()"]);
    assert.equal(tuple["COUNT_STAR()"], 1);
  });

  it("should throw error if the prepared statement is invalid", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:dog) WHERE a.ID = $1 RETURN COUNT(*)"
    );
    assert.exists(preparedStatement);
    assert.isFalse(preparedStatement.isSuccess());
    try {
      await conn.execute(preparedStatement, { 1: 0 });
      assert.fail("No error thrown when the prepared statement is invalid.");
    } catch (e) {
      assert.equal(e.message, "Binder exception: Table dog does not exist.");
    }
  });

  it("should throw error if the prepared statement is not a PreparedStatement object", async function () {
    try {
      const _ = await conn.execute({}, { 1: 0 });
      assert.fail(
        "No error thrown when the prepared statement is not a PreparedStatement object."
      );
    } catch (e) {
      assert.equal(
        e.message,
        "preparedStatement must be a valid PreparedStatement object."
      );
    }
  });

  it("should throw error if the parameters is not a plain object", async function () {
    try {
      const preparedStatement = await conn.prepare(
        "MATCH (a:person) WHERE a.ID = $1 RETURN COUNT(*)"
      );
      assert.exists(preparedStatement);
      assert.isTrue(preparedStatement.isSuccess());
      await conn.execute(preparedStatement, []);
      assert.fail("No error thrown when params is not a plain object.");
    } catch (e) {
      assert.equal(e.message, "params must be a plain object.");
    }
  });

  it("should throw error if a parameter is not valid", async function () {
    try {
      const preparedStatement = await conn.prepare(
        "MATCH (a:person) WHERE a.ID = $1 RETURN COUNT(*)"
      );
      assert.exists(preparedStatement);
      assert.isTrue(preparedStatement.isSuccess());
      await conn.execute(preparedStatement, { 1: [] });
      assert.fail("No error thrown when a parameter is not valid.");
    } catch (e) {
      assert.equal(
        e.message,
        "The value of each parameter must be a boolean, number, string, or Date object."
      );
    }
  });

  it("should throw error if a parameter is undefined or null", async function () {
    const preparedStatement = await conn.prepare(
      "MATCH (a:person) WHERE a.ID = $1 RETURN COUNT(*)"
    );
    assert.exists(preparedStatement);
    assert.isTrue(preparedStatement.isSuccess());
    try {
      await conn.execute(preparedStatement, { 1: undefined });
      assert.fail("No error thrown when a parameter is undefined.");
    } catch (e) {
      assert.equal(
        e.message,
        "The value of each parameter must not be null or undefined."
      );
    }
    try {
      await conn.execute(preparedStatement, { 1: null });
      assert.fail("No error thrown when a parameter is null.");
    } catch (e) {
      assert.equal(
        e.message,
        "The value of each parameter must not be null or undefined."
      );
    }
  });
});

describe("Query", function () {
  it("should run a valid query", async function () {
    const queryResult = await conn.query("MATCH (a:person) RETURN COUNT(*)");
    assert.exists(queryResult);
    assert.equal(queryResult.constructor.name, "QueryResult");
    assert.isTrue(queryResult.hasNext());
    const tuple = await queryResult.getNext();
    assert.exists(tuple);
    assert.exists(tuple["COUNT_STAR()"]);
    assert.equal(tuple["COUNT_STAR()"], 8);
  });

  it("should throw error if the statement is invalid", async function () {
    try {
      await conn.query("MATCH (a:dog) RETURN COUNT(*)");
      assert.fail("No error thrown when the query is invalid.");
    } catch (e) {
      assert.equal(e.message, "Binder exception: Table dog does not exist.");
    }
  });

  it("should throw error if the statement is not a string", async function () {
    try {
      const _ = await conn.query(42);
      assert.fail("No error thrown when the query is not a string.");
    } catch (e) {
      assert.equal(e.message, "statement must be a string.");
    }
  });
});

describe("Timeout", function () {
  it("should abort a query if the timeout is reached", async function () {
    try {
      const newConn = new kuzu.Connection(db);
      await newConn.init();
      newConn.setQueryTimeout(1);
      await newConn.query(
        "MATCH (a:person)-[:knows*1..28]->(b:person) RETURN COUNT(*);"
      );
      assert.fail("No error thrown when the query times out.");
    } catch (err) {
      assert.equal(err.message, "Interrupted.");
    }
  });

  it("should allow setting a timeout before the connection is initialized", async function () {
    try {
      const newConn = new kuzu.Connection(db);
      newConn.setQueryTimeout(1);
      await newConn.init();
      await newConn.query(
        "MATCH (a:person)-[:knows*1..28]->(b:person) RETURN COUNT(*);"
      );
      assert.fail("No error thrown when the query times out.");
    } catch (err) {
      assert.equal(err.message, "Interrupted.");
    }
  });
});
